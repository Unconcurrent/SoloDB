namespace SoloDatabase

open System
open System.Linq.Expressions
open SqlDu.Engine.C1.Spec
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorBase
open SoloDatabase.QueryTranslatorVisitCore
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.QueryTranslatorVisitDbRefPeelers
open SoloDatabase.QueryTranslatorVisitDbRefPeelers2
open SoloDatabase.QueryTranslatorVisitDbRefPeelers3
open SoloDatabase.DBRefManyDescriptor
open DBRefTypeHelpers

/// Entry point for DBRefMany expression handling.
/// Dispatches to unified descriptor path or legacy set-op handler.
module internal QueryTranslatorVisitDbRef =
    let private handleDBRefManyExpression (qb: QueryBuilder) (exp: Expression) : bool =
        let actualExp =
            match exp with
            | :? LambdaExpression as le -> le.Body
            | _ -> exp
        // Try unified descriptor path first (handles most operator compositions).
        let unifiedResult =
            match DBRefManyExtractor.tryExtract actualExp with
            | ValueSome desc ->
                match tryGetDBRefManyOwnerRefWithOfType qb desc.Source with
                | ValueSome (ownerRef, ofTypeName) ->
                    let descWithOfType =
                        match ofTypeName with
                        | Some tn when desc.OfTypeName.IsNone -> { desc with OfTypeName = Some tn }
                        | _ -> desc
                    let builderOwnerRef: DBRefManyDescriptor.DBRefManyOwnerRef = {
                        OwnerCollection = ownerRef.OwnerCollection
                        OwnerAliasSql = ownerRef.OwnerAliasSql
                        OwnerIdExpr = ownerRef.OwnerIdExpr
                        PropertyExpr = ownerRef.PropertyExpr
                    }
                    DBRefManyBuilder.tryBuild qb descWithOfType builderOwnerRef
                | ValueNone -> ValueNone
            | ValueNone -> ValueNone

        match unifiedResult with
        | ValueSome sqlExpr ->
            qb.DuHandlerResult.Value <- ValueSome sqlExpr
            true
        | ValueNone ->
        // Legacy handler — set ops + reject arms only. All other cases handled by unified path above.
        // Set ops need tryBuildProjectedSetOperand + nullSafeEq which live in this file.
        match exp with
        // Set-op terminals (Any/Count over Intersect/Except/Union/Concat),
        // including outer Distinct wrappers that preserve the underlying set-op semantics.
        | :? MethodCallExpression as mce
            when (mce.Method.Name = "Any" || mce.Method.Name = "Count" || mce.Method.Name = "LongCount") ->
            let sourceArg, _predArg = extractSourceAndPredicate mce
            match sourceArg with
            | ValueSome sourceExpr ->
                let normalizedSourceExpr, distinctOuter =
                    match sourceExpr with
                    | :? MethodCallExpression as srcMc when srcMc.Method.Name = "Distinct" ->
                        let inner =
                            if not (isNull srcMc.Object) then srcMc.Object
                            elif srcMc.Arguments.Count > 0 then srcMc.Arguments.[0]
                            else sourceExpr
                        inner, true
                    | _ -> sourceExpr, false
                match tryMatchSetOperation normalizedSourceExpr with
                | ValueSome (setOpName, leftExpr, rightExpr) ->
                    match tryBuildProjectedSetOperand qb leftExpr, tryBuildProjectedSetOperand qb rightExpr with
                    | ValueSome (leftProjDu, leftCore, _), ValueSome (_rightProjDu, rightCore, _rightTgtAlias) ->
                        let addExistsCheck (isIntersect: bool) (core: SelectCore) =
                            let rightCoreWithEq =
                                { rightCore with
                                    Where =
                                        match rightCore.Where with
                                        | Some w -> Some(SqlExpr.Binary(w, BinaryOperator.And, nullSafeEq _rightProjDu leftProjDu))
                                        | None -> Some(nullSafeEq _rightProjDu leftProjDu) }
                            let rightExistsSel = { Ctes = []; Body = SingleSelect rightCoreWithEq }
                            let existsExpr = if isIntersect then SqlExpr.Exists rightExistsSel else SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists rightExistsSel)
                            { core with
                                Where =
                                    match core.Where with
                                    | Some w -> Some(SqlExpr.Binary(w, BinaryOperator.And, existsExpr))
                                    | None -> Some existsExpr }
                        if mce.Method.Name = "Any" then
                            // Set-op Any terminals
                            match setOpName with
                            | "Intersect" ->
                                let filtered = addExistsCheck true leftCore
                                qb.DuHandlerResult.Value <- ValueSome(SqlExpr.Exists { Ctes = []; Body = SingleSelect filtered })
                                true
                            | "Except" ->
                                let filtered = addExistsCheck false leftCore
                                qb.DuHandlerResult.Value <- ValueSome(SqlExpr.Exists { Ctes = []; Body = SingleSelect filtered })
                                true
                            | "Union" | "Concat" ->
                                let leftSel = { Ctes = []; Body = SingleSelect leftCore }
                                let rightSel = { Ctes = []; Body = SingleSelect rightCore }
                                qb.DuHandlerResult.Value <- ValueSome(SqlExpr.Binary(SqlExpr.Exists leftSel, BinaryOperator.Or, SqlExpr.Exists rightSel))
                                true
                            | _ -> false
                        else
                            // Set-op Count terminals
                            let mkSubCoreLocal projs src whr = { Distinct = false; Projections = ProjectionSetOps.ofList projs; Source = src; Joins = []; Where = whr; GroupBy = []; Having = None; OrderBy = []; Limit = None; Offset = None }
                            match setOpName with
                            | "Intersect" | "Except" ->
                                let filtered = addExistsCheck (setOpName = "Intersect") { leftCore with Distinct = true }
                                let innerSel = { Ctes = []; Body = SingleSelect filtered }
                                let dtAlias = sprintf "_sc%d" (System.Threading.Interlocked.Increment(&subqueryAliasCounter))
                                let countProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
                                let outerCore = mkSubCoreLocal countProj (Some(DerivedTable(innerSel, dtAlias))) None
                                qb.DuHandlerResult.Value <- ValueSome(SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore })
                                true
                            | "Concat" when not distinctOuter ->
                                let leftSel = { Ctes = []; Body = SingleSelect leftCore }
                                let rightSel = { Ctes = []; Body = SingleSelect rightCore }
                                let lAlias = sprintf "_cl%d" (System.Threading.Interlocked.Increment(&subqueryAliasCounter))
                                let rAlias = sprintf "_cr%d" (System.Threading.Interlocked.Increment(&subqueryAliasCounter))
                                let countProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
                                let lCount = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect (mkSubCoreLocal countProj (Some(DerivedTable(leftSel, lAlias))) None) }
                                let rCount = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect (mkSubCoreLocal countProj (Some(DerivedTable(rightSel, rAlias))) None) }
                                qb.DuHandlerResult.Value <- ValueSome(SqlExpr.Binary(lCount, BinaryOperator.Add, rCount))
                                true
                            | _ -> // Union.Count via inclusion-exclusion
                                match tryBuildProjectedSetOperand qb leftExpr, tryBuildProjectedSetOperand qb rightExpr with
                                | ValueSome (leftProjDu2, leftCore2, _), ValueSome (_rightProjDu2, rightCore2, _) ->
                                    let leftSel = { Ctes = []; Body = SingleSelect { leftCore2 with Distinct = true } }
                                    let rightSel = { Ctes = []; Body = SingleSelect { rightCore2 with Distinct = true } }
                                    let lAlias = sprintf "_ul%d" (System.Threading.Interlocked.Increment(&subqueryAliasCounter))
                                    let rAlias = sprintf "_ur%d" (System.Threading.Interlocked.Increment(&subqueryAliasCounter))
                                    let countProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
                                    let mkCount sel alias = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect (mkSubCoreLocal countProj (Some(DerivedTable(sel, alias))) None) }
                                    let lCount = mkCount leftSel lAlias
                                    let rCount = mkCount rightSel rAlias
                                    // Build intersection with fresh pair of left+right cores
                                    match tryBuildProjectedSetOperand qb leftExpr, tryBuildProjectedSetOperand qb rightExpr with
                                    | ValueSome (leftProjDu3, leftCore3, _), ValueSome (_rightProjDu3, rightCore3, _) ->
                                    let rightCoreWithEq3 =
                                        { rightCore3 with
                                            Where =
                                                match rightCore3.Where with
                                                | Some w -> Some(SqlExpr.Binary(w, BinaryOperator.And, nullSafeEq _rightProjDu3 leftProjDu3))
                                                | None -> Some(nullSafeEq _rightProjDu3 leftProjDu3) }
                                    let rightExistsSel3 = { Ctes = []; Body = SingleSelect rightCoreWithEq3 }
                                    let intersectCore =
                                        { leftCore3 with
                                            Distinct = true
                                            Where =
                                                match leftCore3.Where with
                                                | Some w -> Some(SqlExpr.Binary(w, BinaryOperator.And, SqlExpr.Exists rightExistsSel3))
                                                | None -> Some(SqlExpr.Exists rightExistsSel3) }
                                    let iSel = { Ctes = []; Body = SingleSelect intersectCore }
                                    let iAlias = sprintf "_ui%d" (System.Threading.Interlocked.Increment(&subqueryAliasCounter))
                                    let iCount = mkCount iSel iAlias
                                    qb.DuHandlerResult.Value <- ValueSome(SqlExpr.Binary(SqlExpr.Binary(lCount, BinaryOperator.Add, rCount), BinaryOperator.Sub, iCount))
                                    true
                                    | _ -> false
                                | _ -> false
                    | _ -> false
                | ValueNone -> false
            | ValueNone -> false

        // ToList and ToArray identity passthrough.
        | :? MethodCallExpression as mce when mce.Method.Name = "ToList" || mce.Method.Name = "ToArray" ->
            let sourceExpr =
                if not (isNull mce.Object) then mce.Object
                elif mce.Arguments.Count > 0 then mce.Arguments.[0]
                else null
            if isNull sourceExpr then false
            else
                let innerDu = visitDu sourceExpr qb
                qb.DuHandlerResult.Value <- ValueSome innerDu
                true

        // Reject: bare DBRefMany.Where(pred) materialization.
        | :? MethodCallExpression as mce when mce.Method.Name = "Where" ->
            match tryPeelWhereFromDBRefMany exp with
            | ValueSome _ ->
                raise (NotSupportedException(filteredWhereUnsupportedTerminalMessage))
            | ValueNone -> false

        // Reject: unsupported operators after DBRefMany chain.
        | :? MethodCallExpression as mce when mce.Method.Name = "SelectMany" ->
            let sourceExpr =
                if not (isNull mce.Object) then mce.Object
                elif mce.Arguments.Count > 0 then mce.Arguments.[0]
                else null
            if isNull sourceExpr then false
            else
                let sourceAfterOrder =
                    match tryPeelOrderByFromSource sourceExpr with
                    | ValueSome (inner, _) -> inner
                    | ValueNone -> sourceExpr
                match tryPeelWhereFromDBRefMany sourceAfterOrder with
                | ValueSome _ ->
                    raise (NotSupportedException(filteredWhereUnsupportedTerminalMessage))
                | ValueNone -> false

        | _ -> false

    do preExpressionHandler.Add(Func<QueryBuilder, Expression, bool>(QueryTranslatorVisitDbRefSingleRef.handleDBRefExpression))
    do preExpressionHandler.Add(Func<QueryBuilder, Expression, bool>(handleDBRefManyExpression))

    /// Module initialization sentinel — accessing this value forces execution of module do-bindings.
    let internal handlerCount = preExpressionHandler.Count
