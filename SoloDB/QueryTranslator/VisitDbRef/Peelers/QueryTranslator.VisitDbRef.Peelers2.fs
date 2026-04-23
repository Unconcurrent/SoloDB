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
open SoloDatabase.QueryTranslatorVisitPostJoin
open DBRefTypeHelpers
open Utils

/// Shared peeler functions and builder helpers (part 2).
module internal QueryTranslatorVisitDbRefPeelers2 =
    let private normalizeOrderKeyExpr (expr: SqlExpr) (clrType: Type) =
        DateTimeFunctions.canonicalizeForCompareOrOrder clrType expr

    /// Returns ValueNone if the expression is not a .Where() on a DBRefMany source.
    let rec internal tryPeelWhereFromDBRefMany (expr: Expression) : (Expression * Expression list) voption =
        match expr with
        | :? MethodCallExpression as mce when mce.Method.Name = "Where" ->
            let sourceExpr, predExpr =
                if not (isNull mce.Object) then
                    mce.Object, (if mce.Arguments.Count >= 1 then ValueSome mce.Arguments.[0] else ValueNone)
                elif mce.Arguments.Count >= 2 then
                    mce.Arguments.[0], ValueSome mce.Arguments.[1]
                else
                    null, ValueNone
            if isNull sourceExpr then ValueNone
            else
                let unwrappedSource = unwrapConvert sourceExpr
                // Accept DBRefMany, OfType(DBRefMany), or OrderBy(DBRefMany) as a valid source.
                // This allows Where at any position relative to OrderBy in the LINQ chain.
                let rec isValidDBRefManyChainSource (e: Expression) =
                    let e = unwrapConvert e
                    if isDBRefManyType e.Type then true
                    else
                        match e with
                        | :? MethodCallExpression as mc
                            when mc.Method.Name = "OfType"
                              || mc.Method.Name = "OrderBy"
                              || mc.Method.Name = "OrderByDescending"
                              || mc.Method.Name = "ThenBy"
                              || mc.Method.Name = "ThenByDescending" ->
                            let innerSrc = if not (isNull mc.Object) then mc.Object elif mc.Arguments.Count > 0 then mc.Arguments.[0] else null
                            not (isNull innerSrc) && isValidDBRefManyChainSource innerSrc
                        | _ -> false
                let isValidSource = isValidDBRefManyChainSource unwrappedSource
                if isValidSource then
                    // Direct DBRefMany.Where(pred) or OfType(DBRefMany).Where(pred) — single predicate.
                    match predExpr with
                    | ValueSome pred -> ValueSome (sourceExpr, [pred])
                    | ValueNone -> ValueNone
                else
                    // Chained: source.Where(pred) where source is itself a .Where() call.
                    match tryPeelWhereFromDBRefMany sourceExpr with
                    | ValueSome (innerSource, preds) ->
                        match predExpr with
                        | ValueSome pred -> ValueSome (innerSource, preds @ [pred])
                        | ValueNone -> ValueSome (innerSource, preds)
                    | ValueNone -> ValueNone
        | _ -> ValueNone

    /// Peel .OrderBy/.OrderByDescending/.ThenBy/.ThenByDescending calls from a source expression.
    /// Returns (innerExpr, sortKeys) where sortKeys is a list of (keySelector, SortDirection).
    /// ThenBy/ThenByDescending APPEND to existing keys; OrderBy/OrderByDescending REPLACE all previous.
    /// Returns ValueNone if the expression has no ordering calls.
    let rec internal tryPeelOrderByFromSource (expr: Expression) : (Expression * (Expression * SortDirection) list) voption =
        match expr with
        | :? MethodCallExpression as mce
            when mce.Method.Name = "OrderBy"
              || mce.Method.Name = "OrderByDescending"
              || mce.Method.Name = "ThenBy"
              || mce.Method.Name = "ThenByDescending" ->
            let sourceExpr =
                if not (isNull mce.Object) then mce.Object
                elif mce.Arguments.Count > 0 then mce.Arguments.[0]
                else null
            let keyExpr =
                if not (isNull mce.Object) then
                    if mce.Arguments.Count >= 1 then ValueSome mce.Arguments.[0] else ValueNone
                elif mce.Arguments.Count >= 2 then ValueSome mce.Arguments.[1]
                else ValueNone
            if isNull sourceExpr then ValueNone
            else
                let dir =
                    match mce.Method.Name with
                    | "OrderBy" | "ThenBy" -> SortDirection.Asc
                    | _ -> SortDirection.Desc
                let isThen = mce.Method.Name = "ThenBy" || mce.Method.Name = "ThenByDescending"
                match keyExpr with
                | ValueSome key ->
                    match tryPeelOrderByFromSource sourceExpr with
                    | ValueSome (innerSource, existingKeys) ->
                        if isThen then
                            // ThenBy appends to existing ordering.
                            ValueSome (innerSource, existingKeys @ [(key, dir)])
                        else
                            // OrderBy replaces all previous ordering.
                            ValueSome (innerSource, [(key, dir)])
                    | ValueNone ->
                        if isThen then
                            // ThenBy without preceding OrderBy — reject.
                            raise (NotSupportedException(
                                "Error: ThenBy/ThenByDescending requires a preceding OrderBy on DBRefMany.\nReason: Composite ordering must start with OrderBy.\nFix: Add .OrderBy(key) before .ThenBy(key)."))
                        else
                            ValueSome (sourceExpr, [(key, dir)])
                | ValueNone -> ValueNone
        | _ -> ValueNone

    /// Build sort key DU expressions from peeled OrderBy key selectors.
    let internal buildSortKeyDus (qb: QueryBuilder) (tgtAlias: string) (targetTable: string) (sortKeys: (Expression * SortDirection) list) : OrderBy list =
        sortKeys
        |> List.map (fun (keyExpr, dir) ->
            match tryExtractLambdaExpression keyExpr with
            | ValueSome keyLambda ->
                let subQb = qb.ForSubquery(tgtAlias, keyLambda, subqueryRootTable = targetTable)
                let keyDu = visitDu keyLambda.Body subQb
                { Expr = normalizeOrderKeyExpr keyDu keyLambda.Body.Type; Direction = dir }
            | ValueNone ->
                raise (NotSupportedException(
                    "Error: Cannot extract key selector lambda for OrderBy on DBRefMany.\nFix: Use a simple lambda (e.g., x => x.Name).")))

    /// Peel .Take(n) and .Skip(n) calls from a source expression.
    /// Returns (innerExpr, limit: Expression option, offset: Expression option).
    /// Admits only: Take(n), Skip(n), Skip(m).Take(n) (canonical pagination).
    /// Rejects chained: Take.Take, Skip.Skip, Take.Skip (complex composition deferred).
    let internal tryPeelTakeSkipFromSource (expr: Expression) : (Expression * Expression option * Expression option) voption =
        let extractSourceAndArg (mce: MethodCallExpression) =
            let sourceExpr =
                if not (isNull mce.Object) then mce.Object
                elif mce.Arguments.Count > 0 then mce.Arguments.[0]
                else null
            let argExpr =
                if not (isNull mce.Object) then
                    if mce.Arguments.Count >= 1 then Some mce.Arguments.[0] else None
                elif mce.Arguments.Count >= 2 then Some mce.Arguments.[1]
                else None
            sourceExpr, argExpr
        match expr with
        | :? MethodCallExpression as mce when mce.Method.Name = "Take" ->
            let sourceExpr, takeArg = extractSourceAndArg mce
            if isNull sourceExpr then ValueNone
            else
                // Check if source is Skip (canonical Skip.Take pagination).
                match sourceExpr with
                | :? MethodCallExpression as innerMce when innerMce.Method.Name = "Skip" ->
                    let innerSource, skipArg = extractSourceAndArg innerMce
                    if isNull innerSource then ValueNone
                    else ValueSome (innerSource, takeArg, skipArg)
                | _ ->
                    // Bare Take(n) — no Skip.
                    ValueSome (sourceExpr, takeArg, None)
        | :? MethodCallExpression as mce when mce.Method.Name = "Skip" ->
            let sourceExpr, skipArg = extractSourceAndArg mce
            if isNull sourceExpr then ValueNone
            else
                // Bare Skip(n) — no Take.
                ValueSome (sourceExpr, None, skipArg)
        | _ -> ValueNone

    /// Build LIMIT/OFFSET DU expressions from peeled Take/Skip arguments.
    let internal buildLimitOffset (qb: QueryBuilder) (limitExpr: Expression option) (offsetExpr: Expression option) : SqlExpr option * SqlExpr option =
        DBRefManyHelpers.buildLimitOffsetShared visitDu qb limitExpr offsetExpr

    /// Peel .OfType<T>() from a source expression.
    /// Returns (innerExpr, typeName) if the source is OfType on a DBRefMany chain.
    let internal tryPeelOfTypeFromSource (expr: Expression) : (Expression * string) voption =
        match expr with
        | :? MethodCallExpression as mce when mce.Method.Name = "OfType" ->
            let sourceExpr =
                if not (isNull mce.Object) then mce.Object
                elif mce.Arguments.Count > 0 then mce.Arguments.[0]
                else null
            if isNull sourceExpr then ValueNone
            else
                let genericArgs = mce.Method.GetGenericArguments()
                if genericArgs.Length = 1 then
                    let sourceElemType =
                        if not sourceExpr.Type.IsGenericType then null
                        else sourceExpr.Type.GetGenericArguments() |> Array.tryHead |> Option.defaultValue null
                    if not (isNull sourceElemType) then
                        DBRefManyHelpers.ensureOfTypeSupported sourceElemType
                    match Utils.typeToName genericArgs.[0] with
                    | Some typeName -> ValueSome (sourceExpr, typeName)
                    | None -> ValueNone
                else ValueNone
        | _ -> ValueNone

    /// Try to get DBRefMany owner ref, peeling through OfType if present.
    /// Returns (ownerRef, ofTypeName option).
    let internal tryGetDBRefManyOwnerRefWithOfType (qb: QueryBuilder) (expr: Expression) : (DBRefManyOwnerRef * string option) voption =
        match tryGetDBRefManyOwnerRef qb expr with
        | ValueSome ref -> ValueSome (ref, None)
        | ValueNone ->
            match tryPeelOfTypeFromSource expr with
            | ValueSome (innerExpr, typeName) ->
                match tryGetDBRefManyOwnerRef qb innerExpr with
                | ValueSome ref -> ValueSome (ref, Some typeName)
                | ValueNone -> ValueNone
            | ValueNone -> ValueNone

    /// Build a type discriminator WHERE predicate for OfType<T>.
    let internal buildOfTypePredicate (tgtAlias: string) (typeName: string) : SqlExpr =
        SqlExpr.Binary(
            SqlExpr.FunctionCall("jsonb_extract", [
                SqlExpr.Column(Some tgtAlias, "Value")
                SqlExpr.Literal(SqlLiteral.String "$.$type")]),
            BinaryOperator.Eq,
            SqlExpr.Literal(SqlLiteral.String typeName))

    /// Build correlated subquery parts from a Select-on-DBRefMany expression.
    /// Handles Select(DBRefMany, proj) and Select(Where(DBRefMany, pred), proj).
    /// Returns the subquery core for use in set operations.
    let internal tryBuildProjectedSetOperand (qb: QueryBuilder) (selectExpr: Expression) : (SqlExpr * SelectCore * string) voption =
        match selectExpr with
        | :? MethodCallExpression as selectMce when selectMce.Method.Name = "Select" ->
            let selectSource, projArg =
                if not (isNull selectMce.Object) then
                    selectMce.Object, (if selectMce.Arguments.Count >= 1 then Some selectMce.Arguments.[0] else None)
                elif selectMce.Arguments.Count >= 2 then
                    selectMce.Arguments.[0], Some selectMce.Arguments.[1]
                else null, None
            if isNull selectSource || projArg.IsNone then ValueNone
            else
                let unwrappedSrc = unwrapConvert selectSource
                // Peel Where from inside Select.
                let innerSrc, wherePreds =
                    if isDBRefManyType unwrappedSrc.Type then unwrappedSrc, []
                    else
                        match tryPeelWhereFromDBRefMany unwrappedSrc with
                        | ValueSome (s, p) -> s, p
                        | ValueNone -> unwrappedSrc, []
                match tryGetDBRefManyOwnerRefWithOfType qb innerSrc with
                | ValueSome (ownerRef, ofTypeName) ->
                    match projArg.Value |> tryExtractLambdaExpression with
                    | ValueSome projLambda ->
                        let propName = ownerRef.PropertyExpr.Member.Name
                        let linkTable = dbRefManyLinkTable qb.SourceContext ownerRef.OwnerCollection propName
                        let ownerUsesSource = dbRefManyOwnerUsesSource qb.SourceContext ownerRef.OwnerCollection propName
                        let ownerColumn = if ownerUsesSource then "SourceId" else "TargetId"
                        let targetColumn = if ownerUsesSource then "TargetId" else "SourceId"
                        let targetType = ownerRef.PropertyExpr.Type.GetGenericArguments().[0]
                        let targetTable = resolveTargetCollectionForRelation qb.SourceContext ownerRef.OwnerCollection propName targetType
                        let aliasId = System.Threading.Interlocked.Increment(&subqueryAliasCounter)
                        let tgtAlias = sprintf "_tgt%d" aliasId
                        let lnkAlias = sprintf "_lnk%d" aliasId

                        let wherePredDus =
                            let basePreds =
                                if wherePreds.IsEmpty then []
                                else buildFilteredPredicateDus qb tgtAlias targetTable wherePreds
                            match ofTypeName with
                            | Some tn -> basePreds @ [buildOfTypePredicate tgtAlias tn]
                            | None -> basePreds

                        let subQb = qb.ForSubquery(tgtAlias, projLambda, subqueryRootTable = targetTable)
                        let projectedDu = visitDu projLambda.Body subQb

                        let joinOn =
                            SqlExpr.Binary(
                                SqlExpr.Column(Some tgtAlias, "Id"),
                                BinaryOperator.Eq,
                                SqlExpr.Column(Some lnkAlias, targetColumn))
                        let ownerWhere =
                            SqlExpr.Binary(
                                SqlExpr.Column(Some lnkAlias, ownerColumn),
                                BinaryOperator.Eq,
                                ownerIdExpr ownerRef)
                        let fullWhere = DBRefManyHelpers.appendPredicatesWithAnd ownerWhere wherePredDus

                        let core =
                            { mkSubCore
                                [{ Alias = Some "v"; Expr = projectedDu }]
                                (Some(BaseTable(linkTable, Some lnkAlias)))
                                (Some fullWhere) with
                                Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)] }
                        ValueSome (projectedDu, core, tgtAlias)
                    | ValueNone -> ValueNone
                | ValueNone -> ValueNone
        | _ -> ValueNone

    /// Build a TakeWhile/SkipWhile windowed DerivedTable from a DBRefMany source.
    /// Returns the inner SELECT with _cf window column and the appropriate outer WHERE filter.
    /// The caller wraps this in the terminal (EXISTS, COUNT, json_group_array).
    let internal buildTakeWhileCore
        (qb: QueryBuilder) (ownerRef: DBRefManyOwnerRef) (wherePredicates: Expression list)
        (sortKeys: (Expression * SortDirection) list) (twPredLambda: LambdaExpression) (isTakeWhile: bool)
        (ofTypeName: string option) : SqlSelect =
        let propName = ownerRef.PropertyExpr.Member.Name
        let linkTable = dbRefManyLinkTable qb.SourceContext ownerRef.OwnerCollection propName
        let ownerUsesSource = dbRefManyOwnerUsesSource qb.SourceContext ownerRef.OwnerCollection propName
        let ownerColumn = if ownerUsesSource then "SourceId" else "TargetId"
        let targetColumn = if ownerUsesSource then "TargetId" else "SourceId"
        let targetType = ownerRef.PropertyExpr.Type.GetGenericArguments().[0]
        let targetTable = resolveTargetCollectionForRelation qb.SourceContext ownerRef.OwnerCollection propName targetType
        let aliasId = System.Threading.Interlocked.Increment(&subqueryAliasCounter)
        let tgtAlias = sprintf "_tgt%d" aliasId
        let lnkAlias = sprintf "_lnk%d" aliasId

        let wherePredDus =
            let basePreds = if wherePredicates.IsEmpty then [] else buildFilteredPredicateDus qb tgtAlias targetTable wherePredicates
            match ofTypeName with
            | Some tn -> basePreds @ [buildOfTypePredicate tgtAlias tn]
            | None -> basePreds
        let sortKeyDus = buildSortKeyDus qb tgtAlias targetTable sortKeys

        let twSubQb = qb.ForSubquery(tgtAlias, twPredLambda, subqueryRootTable = targetTable)
        let twPredDu = visitDu twPredLambda.Body twSubQb
        let caseExpr = SqlExpr.CaseExpr(
            (SqlExpr.Unary(UnaryOperator.Not, twPredDu), SqlExpr.Literal(SqlLiteral.Integer 1L)),
            [],
            Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))
        let windowSpec = {
            Kind = NamedWindowFunction "SUM"
            Arguments = [caseExpr]
            PartitionBy = []
            OrderBy = sortKeyDus |> List.map (fun ob -> (ob.Expr, ob.Direction))
        }
        let cfExpr = SqlExpr.WindowCall windowSpec

        let joinOn =
            SqlExpr.Binary(
                SqlExpr.Column(Some tgtAlias, "Id"), BinaryOperator.Eq,
                SqlExpr.Column(Some lnkAlias, targetColumn))
        let ownerWhere =
            SqlExpr.Binary(
                SqlExpr.Column(Some lnkAlias, ownerColumn), BinaryOperator.Eq,
                ownerIdExpr ownerRef)
        let fullWhere = DBRefManyHelpers.appendPredicatesWithAnd ownerWhere wherePredDus

        // Inner: SELECT 1 AS v, _cf FROM link JOIN target WHERE ... ORDER BY key
        let innerCore =
            { mkSubCore
                [{ Alias = Some "v"; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }
                 { Alias = Some "_cf"; Expr = cfExpr }]
                (Some(BaseTable(linkTable, Some lnkAlias)))
                (Some fullWhere) with
                Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)]
                OrderBy = sortKeyDus }
        let innerSelect = { Ctes = []; Body = SingleSelect innerCore }
        let ordAlias = sprintf "_tw%d" aliasId

        // Outer: SELECT v FROM (inner) WHERE _cf = 0 (TakeWhile) or _cf > 0 (SkipWhile)
        let cfFilter =
            if isTakeWhile then
                SqlExpr.Binary(SqlExpr.Column(Some ordAlias, "_cf"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L))
            else
                SqlExpr.Binary(SqlExpr.Column(Some ordAlias, "_cf"), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L))
        let outerCore =
            mkSubCore
                [{ Alias = Some "v"; Expr = SqlExpr.Column(Some ordAlias, "v") }]
                (Some(DerivedTable(innerSelect, ordAlias)))
                (Some cfFilter)
        { Ctes = []; Body = SingleSelect outerCore }

    /// Peel TakeWhile/SkipWhile from the expression tree.
    /// Returns (innerExpr, predicateLambda, isTakeWhile).
    let internal tryPeelTakeWhileSkipWhile (expr: Expression) : (Expression * LambdaExpression * bool) voption =
        match expr with
        | :? MethodCallExpression as mce when mce.Method.Name = "TakeWhile" || mce.Method.Name = "SkipWhile" ->
            let sourceExpr =
                if not (isNull mce.Object) then mce.Object
                elif mce.Arguments.Count > 0 then mce.Arguments.[0]
                else null
            let predExpr =
                if not (isNull mce.Object) then
                    if mce.Arguments.Count >= 1 then Some mce.Arguments.[0] else None
                elif mce.Arguments.Count >= 2 then Some mce.Arguments.[1]
                else None
            if isNull sourceExpr then ValueNone
            else
                match predExpr with
                | Some pe ->
                    match tryExtractLambdaExpression pe with
                    | ValueSome lambda -> ValueSome (sourceExpr, lambda, mce.Method.Name = "TakeWhile")
                    | ValueNone -> ValueNone
                | None -> ValueNone
        | _ -> ValueNone

    /// Null-safe equality using the SQLite IS operator (treats NULL IS NULL as true).
