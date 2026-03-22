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

module internal QueryableHelperJoin =
    open QueryableHelperState
    open QueryableHelperBase
    let internal readSoloDBQueryable<'T> (methodArg: Expression) =
        let unsupportedConcatMessage = "Cannot concat with an IEnumerable other than another SoloDB IQueryable on the same connection. To do this anyway, use AsEnumerable()."
        match methodArg with
        | :? ConstantExpression as ce -> 
            match QueryTranslator.evaluateExpr<IEnumerable> ce with
            | :? IQueryable<'T> as appendingQuery when (match appendingQuery.Provider with :? SoloDBQueryProvider -> true | _other -> false) ->
                appendingQuery.Expression
            | :? IRootQueryable as rq ->
                Expression.Constant(rq, typeof<IRootQueryable>)
            | _other -> raise (NotSupportedException(unsupportedConcatMessage))
        | :? MethodCallExpression as mcl ->
            mcl
        | _other -> raise (NotSupportedException(unsupportedConcatMessage))

    let internal readSoloDBQueryableUntyped (methodArg: Expression) =
        let unsupportedJoinMessage = "Join is supported only when the inner source is another SoloDB IQueryable. To do this anyway, use AsEnumerable() first."

        let rec isSoloDBQueryableExpression (expr: Expression) =
            match expr with
            | :? ConstantExpression as ce ->
                match ce.Value with
                | :? IQueryable as q -> q.Provider :? SoloDBQueryProvider
                | :? IRootQueryable -> true
                | _ -> false
            | :? MethodCallExpression as mce when mce.Arguments.Count > 0 ->
                isSoloDBQueryableExpression mce.Arguments.[0]
            | _ -> false

        match methodArg with
        | :? ConstantExpression as ce ->
            match ce.Value with
            | :? IQueryable as q when (match q.Provider with | :? SoloDBQueryProvider -> true | _ -> false) ->
                q.Expression
            | :? IRootQueryable as rq ->
                Expression.Constant(rq, typeof<IRootQueryable>)
            | _ ->
                raise (NotSupportedException(unsupportedJoinMessage))
        | :? MethodCallExpression as mcl when isSoloDBQueryableExpression mcl ->
            mcl
        | _ ->
            raise (NotSupportedException(unsupportedJoinMessage))

    type internal JoinSourceBinding =
    | NoJoinSource
    | OuterJoinSource
    | InnerJoinSource
    | MixedJoinSource

    let internal combineJoinSourceBinding left right =
        match left, right with
        | MixedJoinSource, _
        | _, MixedJoinSource -> MixedJoinSource
        | NoJoinSource, other
        | other, NoJoinSource -> other
        | OuterJoinSource, OuterJoinSource -> OuterJoinSource
        | InnerJoinSource, InnerJoinSource -> InnerJoinSource
        | _ -> MixedJoinSource

    let internal unwrapLambdaExpressionOrThrow (operationName: string) (expr: Expression) =
        match unwrapQuotedLambda expr with
        | :? LambdaExpression as lambda -> lambda
        | _ ->
            raise (NotSupportedException(
                $"Error: {operationName} is not supported.
Reason: Expected a quoted lambda expression.
Fix: Rewrite the query to use direct lambda arguments or move it after AsEnumerable()."))

    let rec internal referencesParam (param: ParameterExpression) (expr: Expression) =
        if isNull expr then
            false
        elif obj.ReferenceEquals(expr, param) then
            true
        else
            match expr with
            | :? MethodCallExpression as mc ->
                (not (isNull mc.Object) && referencesParam param mc.Object)
                || (mc.Arguments |> Seq.exists (referencesParam param))
            | :? MemberExpression as me ->
                not (isNull me.Expression) && referencesParam param me.Expression
            | :? UnaryExpression as ue ->
                referencesParam param ue.Operand
            | :? BinaryExpression as be ->
                referencesParam param be.Left || referencesParam param be.Right
            | :? ConditionalExpression as ce ->
                referencesParam param ce.Test || referencesParam param ce.IfTrue || referencesParam param ce.IfFalse
            | :? NewExpression as ne ->
                ne.Arguments |> Seq.exists (referencesParam param)
            | :? MemberInitExpression as mi ->
                referencesParam param mi.NewExpression
                || (mi.Bindings
                    |> Seq.exists (fun binding ->
                        match binding with
                        | :? MemberAssignment as ma -> referencesParam param ma.Expression
                        | _ -> false))
            | :? NewArrayExpression as na ->
                na.Expressions |> Seq.exists (referencesParam param)
            | :? InvocationExpression as ie ->
                referencesParam param ie.Expression
                || (ie.Arguments |> Seq.exists (referencesParam param))
            | :? ListInitExpression as li ->
                referencesParam param li.NewExpression
                || (li.Initializers |> Seq.collect (fun init -> init.Arguments) |> Seq.exists (referencesParam param))
            | _ ->
                false

    let internal isCompositeJoinKeyBody (expr: Expression) =
        match expr with
        | :? NewExpression
        | :? MemberInitExpression
        | :? NewArrayExpression -> true
        | _ -> false

    let rec internal classifyJoinResultExpression (outerParam: ParameterExpression) (innerParam: ParameterExpression) (expr: Expression) =
        if isNull expr then
            NoJoinSource
        else
            match expr with
            | :? ParameterExpression as p ->
                if Object.ReferenceEquals(p, outerParam) then OuterJoinSource
                elif Object.ReferenceEquals(p, innerParam) then InnerJoinSource
                else NoJoinSource
            | :? MemberExpression as m ->
                classifyJoinResultExpression outerParam innerParam m.Expression
            | :? UnaryExpression as u ->
                classifyJoinResultExpression outerParam innerParam u.Operand
            | :? BinaryExpression as b ->
                combineJoinSourceBinding
                    (classifyJoinResultExpression outerParam innerParam b.Left)
                    (classifyJoinResultExpression outerParam innerParam b.Right)
            | :? ConditionalExpression as c ->
                [ classifyJoinResultExpression outerParam innerParam c.Test
                  classifyJoinResultExpression outerParam innerParam c.IfTrue
                  classifyJoinResultExpression outerParam innerParam c.IfFalse ]
                |> List.reduce combineJoinSourceBinding
            | :? MethodCallExpression as m ->
                let objectBinding =
                    if isNull m.Object then NoJoinSource
                    else classifyJoinResultExpression outerParam innerParam m.Object
                m.Arguments
                |> Seq.map (classifyJoinResultExpression outerParam innerParam)
                |> Seq.fold combineJoinSourceBinding objectBinding
            | :? NewExpression as n ->
                n.Arguments
                |> Seq.map (classifyJoinResultExpression outerParam innerParam)
                |> Seq.fold combineJoinSourceBinding NoJoinSource
            | :? MemberInitExpression as mi ->
                let start = classifyJoinResultExpression outerParam innerParam mi.NewExpression
                mi.Bindings
                |> Seq.fold (fun acc binding ->
                    match binding with
                    | :? MemberAssignment as ma ->
                        combineJoinSourceBinding acc (classifyJoinResultExpression outerParam innerParam ma.Expression)
                    | _ -> MixedJoinSource) start
            | :? NewArrayExpression as na ->
                na.Expressions
                |> Seq.map (classifyJoinResultExpression outerParam innerParam)
                |> Seq.fold combineJoinSourceBinding NoJoinSource
            | :? InvocationExpression as i ->
                let exprBinding = classifyJoinResultExpression outerParam innerParam i.Expression
                i.Arguments
                |> Seq.map (classifyJoinResultExpression outerParam innerParam)
                |> Seq.fold combineJoinSourceBinding exprBinding
            | :? ListInitExpression as li ->
                let start = classifyJoinResultExpression outerParam innerParam li.NewExpression
                li.Initializers
                |> Seq.collect (fun init -> init.Arguments)
                |> Seq.map (classifyJoinResultExpression outerParam innerParam)
                |> Seq.fold combineJoinSourceBinding start
            | :? ConstantExpression -> NoJoinSource
            | _ -> MixedJoinSource

    let internal translateJoinSingleSourceExpression (ctx: QueryContext) (tableAlias: string) (vars: Dictionary<string, obj>) (parameter: ParameterExpression option) (expr: Expression) =
        let lambdaExpr =
            match parameter with
            | Some p -> Expression.Lambda(expr, p) :> Expression
            | None -> Expression.Lambda(expr) :> Expression
        translateExprDu ctx tableAlias lambdaExpr vars

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

    let rec internal translateJoinResultSelectorExpression
        (outerCtx: QueryContext)
        (innerCtx: QueryContext)
        (vars: Dictionary<string, obj>)
        (outerAlias: string)
        (innerAlias: string)
        (outerParam: ParameterExpression)
        (innerParam: ParameterExpression)
        (expr: Expression) =

        match classifyJoinResultExpression outerParam innerParam expr with
        | NoJoinSource ->
            translateJoinSingleSourceExpression outerCtx outerAlias vars None expr
        | OuterJoinSource ->
            translateJoinSingleSourceExpression outerCtx outerAlias vars (Some outerParam) expr
        | InnerJoinSource ->
            translateJoinSingleSourceExpression innerCtx innerAlias vars (Some innerParam) expr
        | MixedJoinSource ->
            match expr with
            | :? NewExpression as n ->
                let memberNames =
                    if isNull n.Members then
                        [| for i in 0 .. n.Arguments.Count - 1 -> $"Item{i + 1}" |]
                    else
                        n.Members |> Seq.map (fun m -> m.Name) |> Seq.toArray
                // Use json_object (not jsonb_object via JsonObjectExpr) so downstream jsonb_extract returns typed SQL values.
                let pairs =
                    [ for i in 0 .. n.Arguments.Count - 1 ->
                        memberNames.[i],
                        translateJoinResultSelectorExpression outerCtx innerCtx vars outerAlias innerAlias outerParam innerParam n.Arguments.[i] ]
                let args = pairs |> List.collect (fun (name, expr) -> [SqlExpr.Literal(SqlLiteral.String name); expr])
                SqlExpr.FunctionCall("json_object", args)
            | :? MemberInitExpression as mi ->
                let pairs =
                    [ for binding in mi.Bindings do
                        match binding with
                        | :? MemberAssignment as ma ->
                            yield ma.Member.Name,
                                  translateJoinResultSelectorExpression outerCtx innerCtx vars outerAlias innerAlias outerParam innerParam ma.Expression
                        | _ ->
                            raise (NotSupportedException(
                                "Error: Join result selector is not supported.
Reason: Only direct member assignments are supported for mixed-source object initialization.
Fix: Use an anonymous object, tuple, or move the projection after AsEnumerable().")) ]
                let args = pairs |> List.collect (fun (name, expr) -> [SqlExpr.Literal(SqlLiteral.String name); expr])
                SqlExpr.FunctionCall("json_object", args)
            | _ ->
                raise (NotSupportedException(
                    "Error: Join result selector is not supported.
Reason: Mixed-source selector expressions are limited to object and tuple construction here.
Fix: Project outer and inner members into an anonymous object or move the projection after AsEnumerable()."))

    let internal tryGetJoinRootSourceTable (expression: Expression) =
        let rec loop (expr: Expression) =
            match expr with
            | :? ConstantExpression as ce ->
                match ce.Value with
                | :? IRootQueryable as rq -> Some rq.SourceTableName
                | _ -> None
            | :? MethodCallExpression as mce when mce.Arguments.Count > 0 ->
                loop mce.Arguments.[0]
            | _ -> None
        loop expression

    type internal InnerJoinSource = {
        TableName: string
        WherePredicates: LambdaExpression list
    }

    let internal tryExtractInnerJoinSource (expression: Expression) =
        let rec loop (expr: Expression) (predicates: LambdaExpression list) =
            match expr with
            | :? ConstantExpression as ce ->
                match ce.Value with
                | :? IQueryable as q when (q.Provider :? SoloDBQueryProvider) ->
                    match tryGetJoinRootSourceTable q.Expression with
                    | Some tableName -> Some { TableName = tableName; WherePredicates = List.rev predicates }
                    | None -> None
                | :? IRootQueryable as rq ->
                    Some { TableName = rq.SourceTableName; WherePredicates = List.rev predicates }
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
    let internal raiseIfNullAggregateTranslator (sourceCtx: QueryContext) (fnName: string) (queries: SQLSubquery ResizeArray) (method: MethodInfo) (args: Expression array) (errorMsg: string) =
        aggregateTranslator sourceCtx fnName queries method args
        let jsonExtractValue = SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$")])
        let isNullCheck = SqlExpr.Unary(UnaryOperator.IsNull, jsonExtractValue)
        addSelector queries (DuSelector (fun _tableName _vars ->
            // In this case NULL is an invalid operation, therefore to emulate the .NET behavior
            // of throwing an exception we return the Id = NULL, and Value = {exception message}
            // And downstream the pipeline it will be checked and throwed.
            [{ Alias = Some "Id"; Expr = SqlExpr.CaseExpr((isNullCheck, SqlExpr.Literal(SqlLiteral.Null)), [], Some(SqlExpr.Literal(SqlLiteral.Integer -1L))) }
             { Alias = Some "Value"; Expr = SqlExpr.CaseExpr((isNullCheck, SqlExpr.Literal(SqlLiteral.String errorMsg)), [], Some(SqlExpr.Column(None, "Value"))) }]
        ))

    /// <summary>
    /// Preprocesses an expression tree into a <see cref="PreprocessedQuery"/> structure,
    /// flattening nested method calls so the translation stage can avoid generating
    /// redundant subqueries.
    /// </summary>
