namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open System.Text
open System.Runtime.CompilerServices
open Microsoft.Data.Sqlite
open SQLiteTools
open Utils
open JsonFunctions
open Connections
open SoloDatabase
open SoloDatabase.JsonSerializator
open SoloDatabase.RelationsTypes
open SoloDatabase.QueryTranslatorBaseTypes
open SqlDu.Engine.C1.Spec

module internal QueryableBuildQueryPartB =
    open QueryableHelperState
    open QueryableHelperJoin
    open QueryableHelperPreprocess
    open QueryableLayerBuild
    open QueryableHelperBase
    let internal apply<'T>
        (sourceCtx: QueryContext)
        (tableName: string)
        (statements: ResizeArray<SQLSubquery>)
        (translateQueryFn: QueryContext -> Dictionary<string, obj> -> Expression -> SqlSelect)
        (m: {| Value: SupportedLinqMethods; OriginalMethod: MethodInfo; Expressions: Expression array |}) =
        match m.Value with
                | SupportedLinqMethods.Count
                | SupportedLinqMethods.CountBy
                | SupportedLinqMethods.LongCount ->
                    match m.Expressions.Length with
                    | 0 -> ()
                    | 1 ->
                        let role =
                            match m.Value with
                            | SupportedLinqMethods.Count
                            | SupportedLinqMethods.CountBy -> CountPredicate
                            | SupportedLinqMethods.LongCount -> LongCountPredicate
                            | _ -> CountPredicate
                        addLoweredPredicate statements (lowerPredicateLambda sourceCtx tableName m.Expressions.[0] role)
                    | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))

                    addComplexFinal statements (fun ctx ->
                        // SELECT COUNT(Id) as Value FROM (inner) o
                        let projs = [{ Alias = Some "Value"; Expr = SqlExpr.FunctionCall("COUNT", [SqlExpr.Column(None, "Id")]) }]
                        let core = mkCore projs (Some (DerivedTable(ctx.Inner, "o")))
                        wrapCore core
                    )


                | SupportedLinqMethods.SelectMany ->
                    match tryMatchCanonicalLeftJoinComposite m.Expressions with
                    | Some composite ->
                        if composite.ResultSelector.Parameters.Count <> 2 then
                            raise (NotSupportedException(
                                "Error: GroupJoin SelectMany result selector is not supported.
Reason: The canonical left-join composite requires a two-parameter result selector.
Fix: Use SelectMany with the standard (carrier, inner) result selector or move the query after AsEnumerable()."))

                        if isCompositeJoinKeyBody composite.OuterKeySelector.Body || isCompositeJoinKeyBody composite.InnerKeySelector.Body then
                            raise (NotSupportedException(
                                "Error: Left-join composite key selectors are not supported.
Reason: Anonymous-type and composite key equality lowering is deferred in this cycle.
Fix: Join on a single scalar key or move the query after AsEnumerable()."))

                        let innerExpression = readSoloDBQueryableUntyped composite.InnerExpression
                        let innerRootTable =
                            match tryGetJoinRootSourceTable innerExpression with
                            | Some tableName -> tableName
                            | None ->
                                raise (NotSupportedException(
                                    "Error: Left-join inner source is not supported.
Reason: The inner query does not resolve to a SoloDB root collection.
Fix: Use another SoloDB IQueryable rooted in a collection or move the query after AsEnumerable()."))

                        let outerResultParam = composite.ResultSelector.Parameters.[0]
                        let innerResultParam = composite.ResultSelector.Parameters.[1]
                        let rewriter = LeftJoinCompositeResultRewriter(outerResultParam, composite.CarrierOuterMember, composite.CarrierGroupMember, composite.OuterKeySelector.Parameters.[0])
                        let rewrittenResultBody = rewriter.Visit(composite.ResultSelector.Body)

                        if rewriter.TouchesGroup || rewriter.TouchesCarrier then
                            raise (NotSupportedException(
                                "Error: Left-join result selector is not supported.
Reason: Only projections over the outer row and the matched inner row are supported; grouped-sequence and carrier-shaped projections are deferred in this cycle.
Fix: Project scalar members from the outer row and the SupportedLinqMethods.DefaultIfEmpty inner row, or move the query after AsEnumerable()."))

                        addComplexFinal statements (fun ctx ->
                            let outerAlias = "o"
                            let innerAlias = "j"
                            let innerCtx = QueryContext.SingleSource(innerRootTable)
                            let outerKeyExpr =
                                translateJoinSingleSourceExpression sourceCtx outerAlias ctx.Vars (Some composite.OuterKeySelector.Parameters.[0]) composite.OuterKeySelector.Body
                            let innerKeyExpr =
                                translateJoinSingleSourceExpression innerCtx innerAlias ctx.Vars (Some composite.InnerKeySelector.Parameters.[0]) composite.InnerKeySelector.Body
                            let resultExpr =
                                translateJoinResultSelectorExpression
                                    sourceCtx
                                    innerCtx
                                    ctx.Vars
                                    outerAlias
                                    innerAlias
                                    composite.OuterKeySelector.Parameters.[0]
                                    innerResultParam
                                    rewrittenResultBody

                            let core =
                                { mkCore
                                    [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some outerAlias, "Id") }
                                     { Alias = Some "Value"; Expr = resultExpr }]
                                    (Some (DerivedTable(ctx.Inner, outerAlias)))
                                  with
                                      Joins =
                                          [ConditionedJoin(
                                              JoinKind.Left,
                                              BaseTable(innerRootTable, Some innerAlias),
                                              SqlExpr.Binary(outerKeyExpr, BinaryOperator.Eq, innerKeyExpr))] }
                            wrapCore core
                        )
                    | None ->
                        // L6: Check for DBRefMany SelectMany — JOIN-based flattening.
                        let tryMatchDBRefManySelectMany () =
                            match m.Expressions.Length with
                            | 1 | 2 ->
                                match m.Expressions.[0] with
                                | :? UnaryExpression as ue when (ue.Operand :? LambdaExpression) ->
                                    let lambda = ue.Operand :?> LambdaExpression
                                    match lambda.Body with
                                    | :? MemberExpression as me when DBRefTypeHelpers.isDBRefManyType me.Type ->
                                        // L6 lock: direct owner-parameter member access only.
                                        // Reject member chains (o.Ref.Value.Tags) and non-parameter bases.
                                        match me.Expression with
                                        | :? ParameterExpression as pe when
                                            lambda.Parameters.Count = 1 &&
                                            System.Object.ReferenceEquals(pe, lambda.Parameters.[0]) ->
                                            Some (lambda, me, m.Expressions.Length)
                                        | _ -> None
                                    | _ -> None
                                | _ -> None
                            | _ -> None

                        match tryMatchDBRefManySelectMany () with
                        | Some (_selectorLambda, memberExpr, argCount) ->
                            // L6: DBRefMany SelectMany — JOIN owner → link → target.
                            let propName = memberExpr.Member.Name
                            let targetType = memberExpr.Type.GetGenericArguments().[0]
                            addComplexFinal statements (fun ctx ->
                                let innerSourceName = Utils.getVarName (hash ctx.Inner.Body % 10000 |> abs)
                                let lnkAlias = "_lnk"
                                let tgtAlias = "_tgt"

                                // Resolve link table and target table from relation metadata.
                                let linkTable =
                                    match sourceCtx.TryResolveRelationLink(tableName, propName) with
                                    | Some mapped when not (System.String.IsNullOrWhiteSpace mapped) -> formatName mapped
                                    | _ ->
                                        raise (InvalidOperationException(
                                            sprintf "Error: relation metadata missing for '%s.%s'.\nReason: link table cannot be resolved.\nFix: repair relation metadata." tableName propName))
                                let ownerUsesSource =
                                    match sourceCtx.TryResolveRelationOwnerUsesSource(tableName, propName) with
                                    | Some v -> v
                                    | None ->
                                        raise (InvalidOperationException(
                                            sprintf "Error: relation metadata missing for '%s.%s'.\nReason: owner-source direction cannot be resolved." tableName propName))
                                let ownerColumn = if ownerUsesSource then "SourceId" else "TargetId"
                                let targetColumn = if ownerUsesSource then "TargetId" else "SourceId"
                                let targetTable =
                                    let defaultTable = formatName targetType.Name
                                    match sourceCtx.TryResolveRelationTarget(tableName, propName) with
                                    | Some mapped when not (System.String.IsNullOrWhiteSpace mapped) -> formatName mapped
                                    | _ -> sourceCtx.ResolveCollectionForType(Utils.typeIdentityKey targetType, defaultTable)

                                // JOIN: owner → link (on owner.Id = link.ownerCol) → target (on target.Id = link.targetCol)
                                let linkJoinOn =
                                    SqlExpr.Binary(
                                        SqlExpr.Column(Some innerSourceName, "Id"),
                                        BinaryOperator.Eq,
                                        SqlExpr.Column(Some lnkAlias, ownerColumn))
                                let targetJoinOn =
                                    SqlExpr.Binary(
                                        SqlExpr.Column(Some tgtAlias, "Id"),
                                        BinaryOperator.Eq,
                                        SqlExpr.Column(Some lnkAlias, targetColumn))

                                // Wrap owner query to project only Id (strip Value to prevent ambiguity with target).
                                let ownerAlias = "_ow"
                                let ownerIdCore =
                                    mkCore
                                        [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some ownerAlias, "Id") }]
                                        (Some (DerivedTable(ctx.Inner, ownerAlias)))
                                let ownerIdSel = wrapCore ownerIdCore
                                let ownerIdName = Utils.getVarName (hash ownerIdSel.Body % 10000 |> abs)

                                // Re-bind link JOIN to use the Id-only owner source.
                                let linkJoinOnIdOnly =
                                    SqlExpr.Binary(
                                        SqlExpr.Column(Some ownerIdName, "Id"),
                                        BinaryOperator.Eq,
                                        SqlExpr.Column(Some lnkAlias, ownerColumn))

                                if argCount = 1 then
                                    // Case A: bare SelectMany — return target entities.
                                    let core =
                                        { mkCore
                                            [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some tgtAlias, "Id") }
                                             { Alias = Some "Value"; Expr = SqlExpr.Column(Some tgtAlias, "Value") }]
                                            (Some (DerivedTable(ownerIdSel, ownerIdName)))
                                          with
                                              Joins =
                                                  [ConditionedJoin(Inner, BaseTable(linkTable, Some lnkAlias), linkJoinOnIdOnly)
                                                   ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), targetJoinOn)] }
                                    wrapCore core
                                else
                                    // Case C: SelectMany with result selector (2 args).
                                    let resultSelectorExpr = m.Expressions.[1]
                                    match resultSelectorExpr with
                                    | :? UnaryExpression as ue when (ue.Operand :? LambdaExpression) ->
                                        let resultLambda = ue.Operand :?> LambdaExpression
                                        if resultLambda.Parameters.Count <> 2 then
                                            raise (NotSupportedException(
                                                "Error: SelectMany result selector must have exactly 2 parameters (owner, target).\nFix: Use (o, t) => new { ... } pattern."))
                                        let outerParam = resultLambda.Parameters.[0]
                                        let innerParam = resultLambda.Parameters.[1]
                                        let innerCtx = QueryContext.SingleSource(targetTable)
                                        let resultExpr =
                                            translateJoinResultSelectorExpression
                                                sourceCtx
                                                innerCtx
                                                ctx.Vars
                                                innerSourceName
                                                tgtAlias
                                                outerParam
                                                innerParam
                                                resultLambda.Body
                                        // Case C needs owner Value for result selector — use full owner DerivedTable.
                                        let core =
                                            { mkCore
                                                [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some innerSourceName, "Id") }
                                                 { Alias = Some "Value"; Expr = resultExpr }]
                                                (Some (DerivedTable(ctx.Inner, innerSourceName)))
                                              with
                                                  Joins =
                                                      [ConditionedJoin(Inner, BaseTable(linkTable, Some lnkAlias), linkJoinOn)
                                                       ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), targetJoinOn)] }
                                        wrapCore core
                                    | _ ->
                                        raise (NotSupportedException(
                                            "Error: Invalid SelectMany result selector.\nFix: Use a lambda expression."))
                            )
                        | None ->
                            // Fallback: json_each SelectMany.
                            addComplexFinal statements (fun ctx ->
                                match m.Expressions.Length with
                                | 1 ->
                                    let generics = GenericMethodArgCache.Get m.OriginalMethod
                                    if generics.[1] (*output*) = typeof<byte> then
                                        raise (InvalidOperationException "Cannot use SelectMany() on byte arrays, as they are stored as base64 strings in SQLite. To process the array anyway, first exit the SQLite context with .AsEnumerable().")
                                    // Use a stable inner source alias based on the inner select structure
                                    let innerSourceName = Utils.getVarName (hash ctx.Inner.Body % 10000 |> abs)
                                    // Build the json_each join source expression
                                    let jsonEachExpr =
                                        match m.Expressions.[0] with
                                        | :? UnaryExpression as ue when (ue.Operand :? LambdaExpression) ->
                                            let lambda = ue.Operand :?> LambdaExpression
                                            match lambda.Body with
                                            | :? MemberExpression as me ->
                                                // L6 guard: reject DBRefMany-typed members — they must go through
                                                // the link-table JOIN path, not json_each on embedded JSON.
                                                if DBRefTypeHelpers.isDBRefManyType me.Type || DBRefTypeHelpers.isDBRefType me.Type then
                                                    raise (NotSupportedException(
                                                        "Error: SelectMany on a relation-backed property requires direct owner-parameter access.\nReason: Non-direct member access (projected alias, computed selector) is not supported.\nFix: Use .SelectMany(o => o.RelationProperty) directly on the owner collection."))
                                                SqlExpr.FunctionCall("jsonb_extract", [
                                                    SqlExpr.Column(Some innerSourceName, "Value")
                                                    SqlExpr.Literal(SqlLiteral.String ("$." + me.Member.Name))
                                                ])
                                            | :? ParameterExpression ->
                                                SqlExpr.Column(Some innerSourceName, "Value")
                                            | _ ->
                                                raise (NotSupportedException(
                                                    "Error: Unsupported SelectMany selector structure.
Reason: The selector cannot be translated to SQL.
Fix: Simplify the selector or move SelectMany after AsEnumerable()."))
                                        | _ ->
                                            raise (NotSupportedException(
                                                "Error: Invalid SelectMany structure.
Reason: The SelectMany arguments are not a supported query pattern.
Fix: Rewrite the query or move SelectMany after AsEnumerable()."))
                                    // SELECT innerSource.Id AS Id, json_each.Value as Value FROM (inner) AS innerSource JOIN json_each(expr)
                                    let core =
                                        { mkCore
                                            [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some innerSourceName, "Id") }
                                             { Alias = Some "Value"; Expr = SqlExpr.Column(Some "json_each", "Value") }]
                                            (Some (DerivedTable(ctx.Inner, innerSourceName)))
                                          with Joins = [CrossJoin(FromJsonEach(jsonEachExpr, None))] }
                                    wrapCore core
                                | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))
                            )

                | SupportedLinqMethods.Join ->
                    match m.Expressions.Length with
                    | 5 ->
                        raise (NotSupportedException(
                            "Error: Join comparer overload is not supported.
Reason: SoloDB only supports SQL-translatable equality without custom comparers in this cycle.
Fix: Remove the comparer or move the join after AsEnumerable()."))
                    | 4 ->
                        let innerExpression = readSoloDBQueryableUntyped m.Expressions.[0]
                        let outerKeySelector = unwrapLambdaExpressionOrThrow "Join outer key selector" m.Expressions.[1]
                        let innerKeySelector = unwrapLambdaExpressionOrThrow "Join inner key selector" m.Expressions.[2]
                        let resultSelector = unwrapLambdaExpressionOrThrow "Join result selector" m.Expressions.[3]

                        if isCompositeJoinKeyBody outerKeySelector.Body || isCompositeJoinKeyBody innerKeySelector.Body then
                            raise (NotSupportedException(
                                "Error: Join composite key selectors are not supported.
Reason: Anonymous-type and composite key equality lowering is deferred in this cycle.
Fix: Join on a single scalar key or move the join after AsEnumerable()."))

                        let innerRootTable =
                            match tryGetJoinRootSourceTable innerExpression with
                            | Some tableName -> tableName
                            | None ->
                                raise (NotSupportedException(
                                    "Error: Join inner source is not supported.
Reason: The inner query does not resolve to a SoloDB root collection.
Fix: Use another SoloDB IQueryable rooted in a collection or move the join after AsEnumerable()."))

                        addComplexFinal statements (fun ctx ->
                            let outerAlias = "o"
                            let innerAlias = "j"
                            let innerCtx = QueryContext.SingleSource(innerRootTable)
                            let outerKeyExpr =
                                translateJoinSingleSourceExpression sourceCtx outerAlias ctx.Vars (Some outerKeySelector.Parameters.[0]) outerKeySelector.Body
                            let innerKeyExpr =
                                translateJoinSingleSourceExpression innerCtx innerAlias ctx.Vars (Some innerKeySelector.Parameters.[0]) innerKeySelector.Body
                            let resultExpr =
                                translateJoinResultSelectorExpression
                                    sourceCtx
                                    innerCtx
                                    ctx.Vars
                                    outerAlias
                                    innerAlias
                                    resultSelector.Parameters.[0]
                                    resultSelector.Parameters.[1]
                                    resultSelector.Body

                            let core =
                                { mkCore
                                    [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some outerAlias, "Id") }
                                     { Alias = Some "Value"; Expr = resultExpr }]
                                    (Some (DerivedTable(ctx.Inner, outerAlias)))
                                  with
                                      Joins =
                                          [ConditionedJoin(
                                              JoinKind.Inner,
                                              BaseTable(innerRootTable, Some innerAlias),
                                              SqlExpr.Binary(outerKeyExpr, BinaryOperator.Eq, innerKeyExpr))] }
                            wrapCore core
                        )
                    | other ->
                        raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))

                | SupportedLinqMethods.GroupJoin ->
                    match m.Expressions.Length with
                    | 4 ->
                        let innerExpression = m.Expressions.[0]
                        let outerKeySelector = unwrapLambdaExpressionOrThrow "GroupJoin outer key selector" m.Expressions.[1]
                        let innerKeySelector = unwrapLambdaExpressionOrThrow "GroupJoin inner key selector" m.Expressions.[2]
                        let resultSelector = unwrapLambdaExpressionOrThrow "GroupJoin result selector" m.Expressions.[3]

                        if isCompositeJoinKeyBody outerKeySelector.Body || isCompositeJoinKeyBody innerKeySelector.Body then
                            raise (NotSupportedException(
                                "Error: GroupJoin composite key selectors are not supported.\n" +
                                "Reason: Anonymous-type and composite key equality lowering is deferred.\n" +
                                "Fix: Join on a single scalar key or move the query after AsEnumerable()."))

                        // Find the root table for the inner source (needed for QueryContext).
                        let innerRootTable =
                            match tryGetJoinRootSourceTable innerExpression with
                            | Some tn -> tn
                            | None ->
                                raise (NotSupportedException(
                                    "Error: GroupJoin inner source is not supported.\n" +
                                    "Reason: The inner query does not resolve to a SoloDB root collection.\n" +
                                    "Fix: Use another SoloDB IQueryable rooted in a collection or move the query after AsEnumerable()."))

                        if resultSelector.Parameters.Count <> 2 then
                            raise (NotSupportedException(
                                "Error: GroupJoin result selector must have two parameters (outer, group).\n" +
                                "Reason: The result selector shape is not recognized.\n" +
                                "Fix: Use (outer, group) => new { ... } pattern."))

                        let outerParam = resultSelector.Parameters.[0]
                        let groupParam = resultSelector.Parameters.[1]
                        // Capture the inner expression for building a subquery inside addComplexFinal.
                        let capturedInnerExpr = innerExpression

                        addComplexFinal statements (fun ctx ->
                            let outerAlias = "o"
                            let innerAlias = "gj"
                            let innerCtx = QueryContext.SingleSource(innerRootTable)

                            // Build the inner source as a full subquery (supports composed inner sources with Where, etc.)
                            let innerSelect = translateQueryFn innerCtx ctx.Vars capturedInnerExpr
                            let innerSource = DerivedTable(innerSelect, innerAlias)

                            let outerKeyExpr =
                                translateJoinSingleSourceExpression sourceCtx outerAlias ctx.Vars (Some outerKeySelector.Parameters.[0]) outerKeySelector.Body
                            // Translate inner key against the inner alias (the DerivedTable exposes Id + Value columns).
                            let innerKeyExpr =
                                translateJoinSingleSourceExpression innerCtx innerAlias ctx.Vars (Some innerKeySelector.Parameters.[0]) innerKeySelector.Body

                            // Translate the result selector body. Outer references → outer table.
                            // Group references → SQL aggregates over the inner table.
                            let rec translateGroupJoinArg (expr: Expression) : SqlExpr =
                                match expr with
                                // outer.Prop — direct outer member access
                                | :? MemberExpression as me when not (isNull me.Expression) && me.Expression :? ParameterExpression && (me.Expression :?> ParameterExpression) = outerParam ->
                                    translateJoinSingleSourceExpression sourceCtx outerAlias ctx.Vars (Some outerParam) expr
                                // group.Count() — parameterless
                                | :? MethodCallExpression as mc when mc.Method.Name = "Count" && mc.Arguments.Count = 1 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                                    SqlExpr.AggregateCall(AggregateKind.Count, Some(SqlExpr.Column(Some innerAlias, "Id")), false, None)
                                // group.Count(predicate) — with predicate
                                | :? MethodCallExpression as mc when mc.Method.Name = "Count" && mc.Arguments.Count = 2 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                                    let pred = unwrapLambdaExpressionOrThrow "GroupJoin Count predicate" mc.Arguments.[1]
                                    let predDu = translateJoinSingleSourceExpression innerCtx innerAlias ctx.Vars (Some pred.Parameters.[0]) pred.Body
                                    SqlExpr.AggregateCall(AggregateKind.Sum, Some(SqlExpr.CaseExpr((predDu, SqlExpr.Literal(SqlLiteral.Integer 1L)), [], Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))), false, None)
                                // group.LongCount() — parameterless
                                | :? MethodCallExpression as mc when mc.Method.Name = "LongCount" && mc.Arguments.Count = 1 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                                    SqlExpr.AggregateCall(AggregateKind.Count, Some(SqlExpr.Column(Some innerAlias, "Id")), false, None)
                                // group.Sum(selector)
                                | :? MethodCallExpression as mc when mc.Method.Name = "Sum" && mc.Arguments.Count = 2 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                                    let sel = unwrapLambdaExpressionOrThrow "GroupJoin Sum selector" mc.Arguments.[1]
                                    let selDu = translateJoinSingleSourceExpression innerCtx innerAlias ctx.Vars (Some sel.Parameters.[0]) sel.Body
                                    SqlExpr.Coalesce(SqlExpr.AggregateCall(AggregateKind.Sum, Some selDu, false, None), [SqlExpr.Literal(SqlLiteral.Integer 0L)])
                                // group.Min(selector)
                                | :? MethodCallExpression as mc when mc.Method.Name = "Min" && mc.Arguments.Count = 2 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                                    let sel = unwrapLambdaExpressionOrThrow "GroupJoin Min selector" mc.Arguments.[1]
                                    SqlExpr.AggregateCall(AggregateKind.Min, Some(translateJoinSingleSourceExpression innerCtx innerAlias ctx.Vars (Some sel.Parameters.[0]) sel.Body), false, None)
                                // group.Max(selector)
                                | :? MethodCallExpression as mc when mc.Method.Name = "Max" && mc.Arguments.Count = 2 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                                    let sel = unwrapLambdaExpressionOrThrow "GroupJoin Max selector" mc.Arguments.[1]
                                    SqlExpr.AggregateCall(AggregateKind.Max, Some(translateJoinSingleSourceExpression innerCtx innerAlias ctx.Vars (Some sel.Parameters.[0]) sel.Body), false, None)
                                // group.Average(selector)
                                | :? MethodCallExpression as mc when mc.Method.Name = "Average" && mc.Arguments.Count = 2 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                                    let sel = unwrapLambdaExpressionOrThrow "GroupJoin Average selector" mc.Arguments.[1]
                                    SqlExpr.AggregateCall(AggregateKind.Avg, Some(translateJoinSingleSourceExpression innerCtx innerAlias ctx.Vars (Some sel.Parameters.[0]) sel.Body), false, None)
                                // group.Any() — parameterless
                                | :? MethodCallExpression as mc when mc.Method.Name = "Any" && mc.Arguments.Count = 1 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                                    SqlExpr.Binary(SqlExpr.AggregateCall(AggregateKind.Count, Some(SqlExpr.Column(Some innerAlias, "Id")), false, None), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L))
                                // Constant / no-group expression
                                | :? ConstantExpression ->
                                    translateJoinSingleSourceExpression sourceCtx outerAlias ctx.Vars None expr
                                // Conditional (ternary)
                                | :? ConditionalExpression as ce ->
                                    SqlExpr.CaseExpr((translateGroupJoinArg ce.Test, translateGroupJoinArg ce.IfTrue), [], Some(translateGroupJoinArg ce.IfFalse))
                                // Binary
                                | :? BinaryExpression as be ->
                                    let op =
                                        match be.NodeType with
                                        | ExpressionType.Add -> BinaryOperator.Add
                                        | ExpressionType.Subtract -> BinaryOperator.Sub
                                        | ExpressionType.Multiply -> BinaryOperator.Mul
                                        | ExpressionType.Divide -> BinaryOperator.Div
                                        | ExpressionType.Modulo -> BinaryOperator.Mod
                                        | ExpressionType.Equal -> BinaryOperator.Eq
                                        | ExpressionType.NotEqual -> BinaryOperator.Ne
                                        | ExpressionType.GreaterThan -> BinaryOperator.Gt
                                        | ExpressionType.GreaterThanOrEqual -> BinaryOperator.Ge
                                        | ExpressionType.LessThan -> BinaryOperator.Lt
                                        | ExpressionType.LessThanOrEqual -> BinaryOperator.Le
                                        | ExpressionType.AndAlso -> BinaryOperator.And
                                        | ExpressionType.OrElse -> BinaryOperator.Or
                                        | _ -> raise (NotSupportedException($"GroupJoin result selector binary operator {be.NodeType} not supported."))
                                    SqlExpr.Binary(translateGroupJoinArg be.Left, op, translateGroupJoinArg be.Right)
                                // Fallback — try outer translation
                                | _ ->
                                    translateJoinSingleSourceExpression sourceCtx outerAlias ctx.Vars (Some outerParam) expr

                            // Build the result projection from the selector body.
                            // Use json_object (not jsonb_object) so downstream jsonb_extract returns typed SQL values.
                            let buildJsonObject (pairs: (string * SqlExpr) list) =
                                let args = pairs |> List.collect (fun (name, expr) -> [SqlExpr.Literal(SqlLiteral.String name); expr])
                                SqlExpr.FunctionCall("json_object", args)

                            let resultExpr =
                                match resultSelector.Body with
                                | :? NewExpression as newExpr when not (isNull newExpr.Members) ->
                                    let memberNames =
                                        newExpr.Members |> Seq.map (fun m -> m.Name) |> Seq.toArray
                                    buildJsonObject
                                        [ for i in 0 .. newExpr.Arguments.Count - 1 ->
                                            memberNames.[i], translateGroupJoinArg newExpr.Arguments.[i] ]
                                | :? MemberInitExpression as mi ->
                                    buildJsonObject
                                        [ for binding in mi.Bindings do
                                            match binding with
                                            | :? MemberAssignment as ma ->
                                                yield ma.Member.Name, translateGroupJoinArg ma.Expression
                                            | _ ->
                                                raise (NotSupportedException("GroupJoin result selector: only member assignments supported.")) ]
                                | _ ->
                                    raise (NotSupportedException(
                                        "Error: GroupJoin result selector must produce an anonymous type or object initializer.\n" +
                                        "Reason: Scalar result selectors are not supported for GroupJoin.\n" +
                                        "Fix: Use new { ... } in the result selector."))

                            // LEFT JOIN inner subquery + GROUP BY outer.Id, outer.Value
                            let core =
                                { mkCore
                                    [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some outerAlias, "Id") }
                                     { Alias = Some "Value"; Expr = resultExpr }]
                                    (Some (DerivedTable(ctx.Inner, outerAlias)))
                                  with
                                      Joins =
                                          [ConditionedJoin(
                                              JoinKind.Left,
                                              innerSource,
                                              SqlExpr.Binary(outerKeyExpr, BinaryOperator.Eq, innerKeyExpr))]
                                      GroupBy = [SqlExpr.Column(Some outerAlias, "Id"); SqlExpr.Column(Some outerAlias, "Value")] }
                            wrapCore core
                        )
                    | other ->
                        raise (NotSupportedException(sprintf "Invalid number of arguments in GroupJoin: %A" other))

                | SupportedLinqMethods.Single | SupportedLinqMethods.SingleOrDefault
                | SupportedLinqMethods.First | SupportedLinqMethods.FirstOrDefault ->
                    match m.Value, m.Expressions with
                    | (SupportedLinqMethods.Single | SupportedLinqMethods.First), [||] -> ()
                    | (SupportedLinqMethods.Single | SupportedLinqMethods.First), [| predicate |] when isExpressionLikeArgument predicate ->
                        addFilter statements predicate
                    | (SupportedLinqMethods.SingleOrDefault | SupportedLinqMethods.FirstOrDefault), [||] -> ()
                    | (SupportedLinqMethods.SingleOrDefault | SupportedLinqMethods.FirstOrDefault), [| predicateOrDefault |] ->
                        if isExpressionLikeArgument predicateOrDefault then
                            addFilter statements predicateOrDefault
                    | (SupportedLinqMethods.SingleOrDefault | SupportedLinqMethods.FirstOrDefault), [| predicate; _defaultValue |]
                        when isExpressionLikeArgument predicate ->
                        addFilter statements predicate
                    | _ ->
                        raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name m.Expressions.Length))

                    let limit =
                        match m.Value with
                        | SupportedLinqMethods.Single | SupportedLinqMethods.SingleOrDefault -> 2
                        | _ -> 1

                    addTake statements (ExpressionHelper.constant limit)

                | SupportedLinqMethods.DefaultIfEmpty ->
                    // Edge case 11: SupportedLinqMethods.DefaultIfEmpty with UNION ALL — synthetic row when result set empty
                    addComplexFinal statements (fun ctx ->
                        let defaultValueExpr =
                            match m.Expressions.Length with
                            | 0 ->
                                let genericArg = (GenericMethodArgCache.Get m.OriginalMethod).[0]
                                if genericArg.IsValueType then
                                    let defaultValueType = Activator.CreateInstance(genericArg)
                                    let jsonObj = JsonSerializator.JsonValue.Serialize defaultValueType
                                    let jsonText = jsonObj.ToJsonString()
                                    SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.FunctionCall("jsonb", [SqlExpr.Literal(SqlLiteral.String jsonText)]); SqlExpr.Literal(SqlLiteral.String "$")])
                                else
                                    SqlExpr.Literal(SqlLiteral.Null)
                            | 1 ->
                                let o = QueryTranslator.evaluateExpr<obj> m.Expressions.[0]
                                let jsonObj = JsonSerializator.JsonValue.Serialize o
                                let jsonText = jsonObj.ToJsonString()
                                SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.FunctionCall("jsonb", [SqlExpr.Literal(SqlLiteral.String jsonText)]); SqlExpr.Literal(SqlLiteral.String "$")])
                            | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))

                        // SELECT Id, Value FROM (inner) o
                        let mainProjs = [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }; { Alias = None; Expr = SqlExpr.Column(None, "Value") }]
                        let mainCore = mkCore mainProjs (Some (DerivedTable(ctx.Inner, "o")))
                        // UNION ALL SELECT -1 as Id, defaultValue WHERE NOT EXISTS (SELECT 1 FROM (inner) o)
                        let defaultProjs = [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }; { Alias = None; Expr = defaultValueExpr }]
                        let defaultCore =
                            { mkCore defaultProjs None
                              with Where = Some (SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists(ctx.Inner))) }
                        { Ctes = []; Body = UnionAllSelect(mainCore, [defaultCore]) }
                    )
                
                | SupportedLinqMethods.Last | SupportedLinqMethods.LastOrDefault ->
                    // SQlite does not guarantee the order of elements without an ORDER BY.
                    match m.Expressions.Length with
                    | 0 -> () // If no arguments are provided, we assume the last element is the one with the highest Id.
                    | 1 ->
                        addFilter statements m.Expressions.[0]
                    | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %A: %A" m.Value other))

                    match statements.Last() with
                    | Simple x when x.Orders.Count <> 0 ->
                        for order in x.Orders do
                            order.Descending <- not order.Descending
                    | _ ->
                        addOrder statements (ExpressionHelper.get(fun (x: obj) -> x.Dyn<int64>("Id"))) true
                    addTake statements (ExpressionHelper.constant 1)

                | _ -> ()
