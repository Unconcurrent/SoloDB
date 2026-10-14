namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open System.Text
open System.Runtime.CompilerServices
open NativeSQLiteTools
open Utils
open JsonFunctions
open Connections
open SoloDatabase
open SoloDatabase.JsonSerializator
open SoloDatabase.RelationsTypes
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryableGroupByAliases
open SoloDatabase.SqlModel

module internal QueryableHelperJoin =
    open QueryableHelperState
    open QueryableHelperBase
    open QueryableHelperJoinBindings
    let rec internal readSoloDBQueryable<'T> (methodArg: Expression) =
        let unsupportedConcatMessage = "Cannot concat with an IEnumerable other than another SoloDB IQueryable on the same connection. To do this anyway, use AsEnumerable()."
        match methodArg with
        | :? UnaryExpression as convert when QueryTranslatorBaseHelpers.isReferenceUpcast convert -> readSoloDBQueryable<'T> convert.Operand
        | :? MemberExpression when typeof<IQueryable>.IsAssignableFrom(methodArg.Type) && QueryTranslatorBaseHelpers.isFullyConstant methodArg ->
            let query = ExpressionEvaluation.evaluateExpr<IQueryable> methodArg
            readSoloDBQueryable<'T> (Expression.Constant(query, typeof<IQueryable>))
        | :? ConstantExpression as ce -> 
            match ExpressionEvaluation.evaluateExpr<IEnumerable> ce with
            | :? IQueryable as appendingQuery when (match appendingQuery.Provider with :? SoloDBQueryProvider -> true | _other -> false) ->
                appendingQuery.Expression
            | :? IRootQueryable as rq ->
                Expression.Constant(rq, typeof<IRootQueryable>)
            | _other -> raise (NotSupportedException(unsupportedConcatMessage))
        | :? MethodCallExpression as mcl ->
            mcl
        | :? RelationQueryExpression -> methodArg
        | _other -> raise (NotSupportedException(unsupportedConcatMessage))

    let rec internal readSoloDBQueryableUntyped (methodArg: Expression) =
        let unsupportedJoinMessage = "Join is supported only when the inner source is another SoloDB IQueryable. To do this anyway, use AsEnumerable() first."

        match methodArg with
        | :? UnaryExpression as convert when QueryTranslatorBaseHelpers.isReferenceUpcast convert -> readSoloDBQueryableUntyped convert.Operand
        | :? MemberExpression when typeof<IQueryable>.IsAssignableFrom(methodArg.Type) && QueryTranslatorBaseHelpers.isFullyConstant methodArg ->
            let query = ExpressionEvaluation.evaluateExpr<IQueryable> methodArg
            readSoloDBQueryableUntyped (Expression.Constant(query, typeof<IQueryable>))
        | :? ConstantExpression as ce ->
            match ce.Value with
            | :? IRootQueryable -> methodArg
            | :? IQueryable as query when (query.Provider :? SoloDBQueryProvider) ->
                readSoloDBQueryableUntyped query.Expression
            | _ -> raise (NotSupportedException(unsupportedJoinMessage))
        | :? MethodCallExpression as call when call.Arguments.Count > 0 ->
            let source = readSoloDBQueryableUntyped call.Arguments.[0]
            if obj.ReferenceEquals(source,call.Arguments.[0]) then methodArg else
            let arguments = call.Arguments |> Seq.toArray
            arguments.[0] <- source
            call.Update(call.Object,arguments) :> Expression
        | :? RelationQueryExpression as directive ->
            let source = readSoloDBQueryableUntyped directive.Source
            if obj.ReferenceEquals(source,directive.Source) then methodArg
            else RelationQueryExpression(directive.Type,source,directive.Selector,directive.Operation) :> Expression
        | _ ->
            raise (NotSupportedException(unsupportedJoinMessage))

    let internal normalizeUnionArm
        (aliasFactory: unit -> string)
        (columnNames: string list)
        (core: SelectCore) =
        if core.OrderBy.Length > 0 || core.Distinct || core.Limit.IsSome || core.Offset.IsSome then
            let normAlias = aliasFactory ()
            let normSel = { Ctes = []; Body = SingleSelect core }
            { Distinct = false
              Projections =
                ProjectionSetOps.ofList (
                    columnNames
                    |> List.map (fun name -> { Alias = Some name; Expr = SqlExpr.Column(Some normAlias, name) }))
              Source = Some(DerivedTable(normSel, normAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = None
              Offset = None }
        else
            core

    let internal tryGetJoinRootSource (expression: Expression) =
        let rec loop (expr: Expression) =
            match expr with
            | :? ConstantExpression as ce ->
                match ce.Value with
                | :? IRootQueryable as rq -> Some(struct(rq.SourceTableName, QueryRootFacts.elementType rq))
                | _ -> None
            | :? MethodCallExpression as mce when mce.Arguments.Count > 0 ->
                loop mce.Arguments.[0]
            | :? RelationQueryExpression as directive -> loop directive.Source
            | _ -> None
        loop expression

    let internal tryGetJoinRootSourceTable expression =
        tryGetJoinRootSource expression |> Option.map (fun struct(table, _) -> table)

    type internal InnerJoinSource = {
        TableName: string
        SerializationType: Type
        WherePredicates: LambdaExpression list
    }

    let internal tryExtractInnerJoinSource (expression: Expression) =
        let rec loop (expr: Expression) (predicates: LambdaExpression list) =
            match expr with
            | :? ConstantExpression as ce ->
                match ce.Value with
                | :? IQueryable as q when (q.Provider :? SoloDBQueryProvider) ->
                    match tryGetJoinRootSource q.Expression with
                    | Some struct(tableName, originalType) -> Some { TableName = tableName; SerializationType = originalType; WherePredicates = List.rev predicates }
                    | None -> None
                | :? IRootQueryable as rq ->
                    Some { TableName = rq.SourceTableName; SerializationType = QueryRootFacts.elementType rq; WherePredicates = List.rev predicates }
                | _ -> None
            | :? MethodCallExpression as mce when mce.Method.Name = "Where" && mce.Arguments.Count = 2 ->
                match unwrapQuotedLambda mce.Arguments.[1] with
                | :? LambdaExpression as lambda ->
                    loop mce.Arguments.[0] (lambda :: predicates)
                | _ -> None
            | _ -> None
        loop expression []

    type internal GroupJoinCarrierMembers = {
        OuterMember: MemberInfo
        GroupMember: MemberInfo
    }

    let internal tryExtractGroupJoinCarrierMembers (lambda: LambdaExpression) =
        if lambda.Parameters.Count <> 2 then
            None
        else
            let outerParam = lambda.Parameters.[0]
            let groupParam = lambda.Parameters.[1]
            let mutable outerMember: MemberInfo option = None
            let mutable groupMember: MemberInfo option = None

            let recordBinding (memberInfo: MemberInfo) (expr: Expression) =
                if Object.ReferenceEquals(expr, outerParam) then
                    outerMember <- Some memberInfo
                    true
                elif Object.ReferenceEquals(expr, groupParam) then
                    groupMember <- Some memberInfo
                    true
                else
                    false

            let exactlyTwoBindings count ok =
                count = 2 && ok && outerMember.IsSome && groupMember.IsSome

            match lambda.Body with
            | :? NewExpression as n when not (isNull n.Members) ->
                let ok =
                    [ for i in 0 .. n.Arguments.Count - 1 -> recordBinding n.Members.[i] n.Arguments.[i] ]
                    |> List.forall id
                if exactlyTwoBindings n.Arguments.Count ok then
                    Some { OuterMember = outerMember.Value; GroupMember = groupMember.Value }
                else
                    None
            | :? MemberInitExpression as mi ->
                let ok =
                    mi.Bindings
                    |> Seq.map (function
                        | :? MemberAssignment as ma -> recordBinding ma.Member ma.Expression
                        | _ -> false)
                    |> Seq.forall id
                if exactlyTwoBindings mi.Bindings.Count ok then
                    Some { OuterMember = outerMember.Value; GroupMember = groupMember.Value }
                else
                    None
            | _ -> None

    let internal tryMatchGroupCarrierDefaultIfEmpty (groupMember: MemberInfo) (lambda: LambdaExpression) =
        if lambda.Parameters.Count <> 1 then
            false
        else
            match lambda.Body with
            | :? MethodCallExpression as mce when mce.Method.Name = "DefaultIfEmpty" && mce.Arguments.Count = 1 ->
                match mce.Arguments.[0] with
                | :? MemberExpression as me ->
                    Object.ReferenceEquals(me.Expression, lambda.Parameters.[0]) && me.Member = groupMember
                | _ -> false
            | _ -> false

    type internal LeftJoinCompositeResultRewriter
        (carrierParam: ParameterExpression, outerMember: MemberInfo, groupMember: MemberInfo, outerParam: ParameterExpression) =
        inherit ExpressionVisitor()

        let mutable touchesGroup = false
        let mutable touchesCarrier = false

        member _.TouchesGroup = touchesGroup
        member _.TouchesCarrier = touchesCarrier

        override _.VisitParameter(node: ParameterExpression) =
            if Object.ReferenceEquals(node, carrierParam) then
                touchesCarrier <- true
            base.VisitParameter(node)

        override _.VisitMember(node: MemberExpression) =
            if not (isNull node.Expression) && Object.ReferenceEquals(node.Expression, carrierParam) then
                if node.Member = outerMember then
                    outerParam :> Expression
                elif node.Member = groupMember then
                    touchesGroup <- true
                    node :> Expression
                else
                    touchesCarrier <- true
                    node :> Expression
            else
                base.VisitMember(node)

    type internal CanonicalLeftJoinComposite = {
        InnerExpression: Expression
        OuterKeySelector: LambdaExpression
        InnerKeySelector: LambdaExpression
        ResultSelector: LambdaExpression
        CarrierOuterMember: MemberInfo
        CarrierGroupMember: MemberInfo
    }

    let internal tryMatchCanonicalLeftJoinComposite (expressions: Expression array) =
        match expressions with
        | [| (:? MethodCallExpression as groupJoin); collectionSelectorExpr; resultSelectorExpr |] when groupJoin.Method.Name = "GroupJoin" ->
            if groupJoin.Arguments.Count <> 5 then
                None
            else
                let outerKeySelector = unwrapLambdaExpressionOrThrow "GroupJoin outer key selector" groupJoin.Arguments.[2]
                let innerKeySelector = unwrapLambdaExpressionOrThrow "GroupJoin inner key selector" groupJoin.Arguments.[3]
                let groupJoinResultSelector = unwrapLambdaExpressionOrThrow "GroupJoin result selector" groupJoin.Arguments.[4]
                let collectionSelector = unwrapLambdaExpressionOrThrow "GroupJoin SelectMany collection selector" collectionSelectorExpr
                let resultSelector = unwrapLambdaExpressionOrThrow "GroupJoin SelectMany result selector" resultSelectorExpr

                match tryExtractGroupJoinCarrierMembers groupJoinResultSelector with
                | Some carrierMembers when tryMatchGroupCarrierDefaultIfEmpty carrierMembers.GroupMember collectionSelector ->
                    Some {
                        InnerExpression = groupJoin.Arguments.[1]
                        OuterKeySelector = outerKeySelector
                        InnerKeySelector = innerKeySelector
                        ResultSelector = resultSelector
                        CarrierOuterMember = carrierMembers.OuterMember
                        CarrierGroupMember = carrierMembers.GroupMember
                    }
                | _ -> None
        | _ -> None

    let internal addUnionAll (queries: SQLSubquery ResizeArray) (fn: string -> Dictionary<string, obj> -> SelectCore) =
        match queries.Last() with
        | Simple last when last.Orders.Count = 0 ->
            last.UnionAll.Add fn
        | _ ->
            queries.Add (Simple { emptySQLStatement () with UnionAll = ResizeArray [ fn ] })

    /// <summary>
    /// Wraps an aggregate function translator to handle cases where the source sequence is empty, which would result in a NULL from SQL.
    /// This function generates SQL to return a specific error message that can be caught and thrown as an exception, mimicking .NET behavior.
    /// </summary>
    /// <param name="fnName">The name of the SQL aggregate function (e.g., "AVG", "MIN").</param>
    /// <param name="builder">The query builder instance.</param>
    /// <param name="expression">The LINQ method call expression for the aggregate.</param>
    /// <param name="errorMsg">The error message to return if the aggregation result is NULL.</param>
    let internal checkEmptyAggregate (queries: SQLSubquery ResizeArray) (method: MethodInfo) (errorMsg: string) =
        if QueryTranslatorNumeric.rejectsEmpty method.ReturnType then
            let value=SqlExpr.Column(None,"Value")
            let jsonExtractValue =
                if JsonFunctions.consumesSqlScalar method.ReturnType then value
                else SqlExpr.FunctionCall("jsonb_extract", [value; SqlExpr.Literal(SqlLiteral.String "$")])
            let isNullCheck = SqlExpr.Unary(UnaryOperator.IsNull, jsonExtractValue)
            addSelector queries (DuSelector (fun _currentCtx input _tableName _vars ->
                // Top-level aggregate-over-empty raises CardinalityError via the unforgeable
                // Id=NULL channel: when the aggregate result is NULL (empty source), the row
                // emits Id=NULL and Value=typed-payload JSON object {"kind":"CardinalityError",
                // "msg":errorMsg}. JsonFunctions.fromSQLite dispatches by kind to
                // InvalidOperationException matching .NET LINQ Min/Max/Avg-on-empty contract.
                let typedPayload = QueryableHelperBase.runtimeErrorPayload RuntimeErrorKind.CardinalityError errorMsg
                struct([{ Alias = Some "Id"; Expr = SqlExpr.CaseExpr((isNullCheck, SqlExpr.Literal(SqlLiteral.Null)), [], Some(SqlExpr.Literal(SqlLiteral.Integer -1L))) }
                        { Alias = Some "Value"; Expr = SqlExpr.CaseExpr((isNullCheck, typedPayload), [], Some(SqlExpr.Column(None, "Value"))) }], input.SerializationInput)
            ))

    let internal raiseIfNullAggregateTranslator sourceCtx fnName queries method args errorMsg =
        aggregateTranslator sourceCtx fnName queries method args
        checkEmptyAggregate queries method errorMsg

    /// <summary>
    /// Preprocesses an expression tree into a <see cref="PreprocessedQuery"/> structure,
    /// flattening nested method calls so the translation stage can avoid generating
    /// redundant subqueries.
    /// </summary>
