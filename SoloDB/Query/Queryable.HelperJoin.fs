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

    /// Local input is transported once; database rows stay in the native query.
    let private localRowset (context: QueryContext) (expression: Expression) =
        let target = ModelMetadata.SequenceElement expression.Type
        let readId =
            if QueryTranslatorBaseHelpers.hasPhysicalRowIdentity target then
                (EntityIdentity.compiledInt64IdReader target).Invoke
            else fun _ -> -1L
        let encode (value: obj) =
            if isNull value then raise(ArgumentNullException("inner"))
            if value :? IQueryable && not(value :? EnumerableQuery) then
                raise(NotSupportedException("A query source must retain its database routing; it cannot be enumerated as a local input."))
            let output = StringBuilder("[")
            let iterator = (value :?> IEnumerable).GetEnumerator()
            try
                let mutable comma = false
                while iterator.MoveNext() do
                    let value = iterator.Current
                    if comma then output.Append(',') |> ignore
                    comma <- true
                    let id = if isNull value then -1L else readId value
                    output.Append('[').Append(id.ToString(Globalization.CultureInfo.InvariantCulture)).Append(',')
                          .Append(serializeForCollection target value).Append(']') |> ignore
            finally
                match iterator with :? IDisposable as owned -> owned.Dispose() | _ -> ()
            output.Append(']').ToString()
        let parameter =
            match context.BindQueryValue with
            | ValueSome binder ->
                binder.Scalar(BoundValueCallExpression([|expression|],typeof<string>,Func<obj array,obj>(fun values -> box(encode values[0]))))
            | _ -> SqlExpr.Literal(SqlLiteral.String(encode(ExpressionEvaluation.evaluateExpr<obj> expression)))
        fun (_variables: Dictionary<string,obj>) ->
            let alias = context.NextAlias()
            let packet = SqlExpr.Column(Some alias,"value")
            let token = SqlExpr.FunctionCall("jsonb",[SqlExpr.Binary(packet,BinaryOperator.JsonText,SqlExpr.Literal(SqlLiteral.String "$[1]"))])
            let input = { QuerySerializationInput.ForType target with Source=ValueSome token;Carrier=CanonicalToken }
            let value = QueryProduced.scalarValue target token input
            let result = DirectValue(value,{input with Carrier=if isSqlScalarResult target then SqlScalar else CanonicalToken})
            let core = mkCore
                           [{Alias=Some "Id";Expr=SqlExpr.FunctionCall("jsonb_extract",[packet;SqlExpr.Literal(SqlLiteral.String "$[0]")])}
                            {Alias=Some "Value";Expr=SqlExpr.Literal SqlLiteral.Null}]
                           (Some(FromJsonEach(SqlExpr.FunctionCall("jsonb",[parameter]),Some alias)))
            let struct(query,input) = QueryProduced.publishCore context.NextAlias core result false
            { Query=query;SerializationInput=input;KeyInput=ValueNone }

    let rec private fieldValue (expression: Expression) =
        match expression with
        | :? ConstantExpression -> true
        | :? MemberExpression as memberAccess when (memberAccess.Member :? FieldInfo) && not(isNull memberAccess.Expression) ->
            fieldValue memberAccess.Expression
        | :? UnaryExpression as conversion when QueryTranslatorBaseHelpers.isReferenceUpcast conversion -> fieldValue conversion.Operand
        | _ -> false

    /// Captured query properties expose their layout during preparation; local values bind afresh.
    let rec private nativeSequence (expression: Expression) =
        let queryValue (value: obj) =
            match value with
            | :? IRootQueryable -> Some(Expression.Constant(value) :> Expression)
            | :? IQueryable as query when (query.Provider :? SoloDBQueryProvider) -> nativeSequence query.Expression
            | _ -> None
        match expression with
        | :? UnaryExpression as conversion when QueryTranslatorBaseHelpers.isReferenceUpcast conversion -> nativeSequence conversion.Operand
        | :? ConstantExpression as constant -> queryValue constant.Value
        | :? MemberExpression when fieldValue expression ||
                                  typeof<IQueryable>.IsAssignableFrom(expression.Type) && QueryTranslatorBaseHelpers.isFullyConstant expression ->
            queryValue(ExpressionEvaluation.evaluateExpr<obj> expression)
        | :? MethodCallExpression as call when call.Arguments.Count > 0 &&
                                              (typeof<Queryable>.Equals(call.Method.DeclaringType) || typeof<Enumerable>.Equals(call.Method.DeclaringType)) ->
            nativeSequence call.Arguments[0] |> Option.map(fun source ->
                let arguments = call.Arguments |> Seq.toArray
                arguments[0] <- source
                call.Update(call.Object,arguments) :> Expression)
        | :? RelationQueryExpression as directive ->
            nativeSequence directive.Source |> Option.map(fun source ->
                RelationQueryExpression(directive.Type,source,directive.Selector,directive.Operation) :> Expression)
        | _ -> None

    let private completeSequenceSource (context: QueryContext) translate expression native =
        match native with
        | Some source -> fun variables -> translate context variables source
        | None -> localRowset context expression

    let internal prepareSequenceSource (context: QueryContext) translate (expression: Expression) =
        completeSequenceSource context translate expression (nativeSequence expression)

    let internal prepareInnerJoinSource
        (operation: string)
        (sourceCtx: QueryContext)
        (translate: QueryContext -> Dictionary<string, obj> -> Expression -> TranslatedQuery)
        expression =
        let native = nativeSequence expression
        let table = native |> Option.bind tryGetJoinRootSource |> Option.map(fun struct(table,_) -> table)
                           |> Option.defaultValue sourceCtx.RootTable
        let context = { QueryContext.ChildOf(sourceCtx,table) with AliasCounter=sourceCtx.AliasCounter }
        let prepare = completeSequenceSource context translate expression native
        fun variables ->
            let input = prepare variables
            struct({context with LayerPosition=OuterLayer;SerializationInput=input.SerializationInput},input)

    /// Key captures retain their original phase while source dependencies lower first.
    let internal reserveJoinKey (context: QueryContext) =
        match context.BindQueryValue with
        | ValueSome binder ->
            let group = binder.Order.ReserveProjection()
            fun lower -> binder.Order.Within(group, lower)
        | ValueNone -> fun lower -> lower()

    /// An unbounded physical identity SELECT can publish its key at the stored input.
    /// All other completed shapes retain their prepared input and relational boundary.
    let private tryPhysicalJoinInput (context: QueryContext) (input: TranslatedQuery) =
        match input.Query.Ctes, input.Query.Body, input.SerializationInput.PhysicalOwner with
        | [], SingleSelect core, ValueSome owner when owner.Sources.Length = 1 && input.KeyInput.IsNone
            && not core.Distinct && core.Joins.IsEmpty && core.GroupBy.IsEmpty && core.Having.IsNone
            && core.OrderBy.IsEmpty && core.Limit.IsNone && core.Offset.IsNone ->
            match core.Source, ProjectionSetOps.toList core.Projections with
            | Some(BaseTable(table, alias)), [identity; value] ->
                let source = defaultArg alias table
                let isColumn name = function
                    | SqlExpr.Column(Some qualifier, column) -> column = name && AliasRewrite.normalizeSourceAlias qualifier = source
                    | _ -> false
                let physical = owner.Sources.[0]
                if physical.TableName = table && physical.OwnerType.Equals(input.SerializationInput.OriginalType)
                   && AliasRewrite.projectionAlias identity = Some "Id" && isColumn "Id" identity.Expr
                   && AliasRewrite.projectionAlias value = Some "Value"
                   && (match value.Expr with SqlExpr.FunctionCall("jsonb", [column]) -> isColumn "Value" column | _ -> false) then
                    let stored = QueryTranslatorPhysicalSource.ownerInput context physical.OwnerType table (SqlExpr.Column(Some source, "Id"))
                    Some(core, source, { context with LayerPosition = BaseLayer; SerializationInput = stored; Joins = ResizeArray() })
                else None
            | _ -> None
        | _ -> None

    /// Publish keys beside the complete right source, including their reference edges.
    let internal publishInnerJoinKey shape variables (context: QueryContext) (input: TranslatedQuery) alias lower =
        let core,keyAlias,keyContext =
            match tryPhysicalJoinInput context input with
            | Some(core,source,context) -> core,source,context
            | None ->
                let fields = [{Alias=Some "Id";Expr=SqlExpr.Column(Some alias,"Id")}
                              {Alias=Some "Value";Expr=SqlExpr.Column(Some alias,"Value")}]
                             @ QueryTranslatorSerialization.companionProjections (Some alias) input.SerializationInput
                mkCore fields (Some(DerivedTable(input.Query,alias))),alias,{context with Joins=ResizeArray()}
        let key = lower keyContext keyAlias
        let values,edges = prepareJoinKey shape keyContext variables key
        let names = values |> List.map(fun _ -> "_join_key"+context.NextAlias())
        let projections = ProjectionSetOps.toList core.Projections @ List.map2(fun name value -> {Alias=Some name;Expr=value}) names values
        let core = {core with Projections=ProjectionSetOps.ofList projections
                              Joins=core.Joins @ QueryTranslatorReferenceJoin.preparedJoinClauses keyContext.Joins @ edges}
        struct(wrapCore core,names)

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

            match QueryTranslatorBaseHelpers.normalizeStoredConstruction lambda.Body with
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
            match QueryTranslatorBaseHelpers.normalizeStoredConstruction lambda.Body with
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

        override _.VisitExtension(node: Expression) =
            match node with
            | :? BoundValueAccessExpression as access when Object.ReferenceEquals(access.Parent,carrierParam) ->
                if access.MemberName = outerMember.Name && access.Type.Equals outerParam.Type then outerParam :> Expression
                elif access.MemberName = groupMember.Name then touchesGroup <- true; node
                else touchesCarrier <- true; node
            | _ -> base.VisitExtension(node)

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
                | Some carrierMembers when tryMatchGroupCarrierDefaultIfEmpty carrierMembers.GroupMember collectionSelector && resultSelector.Parameters.Count=2 ->
                    let rewriter = LeftJoinCompositeResultRewriter(resultSelector.Parameters[0],carrierMembers.OuterMember,carrierMembers.GroupMember,outerKeySelector.Parameters[0])
                    rewriter.Visit(resultSelector.Body) |> ignore
                    if rewriter.TouchesGroup || rewriter.TouchesCarrier then None else
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
                let typedPayload = QueryTranslatorBaseTypes.runtimeErrorPayload RuntimeErrorKind.CardinalityError errorMsg
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
