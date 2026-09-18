namespace SoloDatabase

open System
open System.Linq
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.ChainExpr

open SoloDatabase.OrderedChainPlan
open SoloDatabase.OrderedChainRows

/// Consumes completed ordered rowsets through shared terminal semantics.
module internal OrderedChain =
    let private supportedRight (adapter: Adapter) stages =
        let rec valid p =
            p.Stages |> List.forall (function Set(_, r, _) -> (parse adapter.IsRoot r |> Option.exists valid) || adapter.IsValue r | _ -> true)
        valid { Root=Expression.Constant(null);Stages=stages }

    /// Returns None before any translation when the complete chain is not owned here.
    /// Direct, unchained aggregates retain their specialized aggregate path.
    let rec private tryBuildCore (adapter: Adapter) (expression: Expression) =
        match expression with
        | :? MemberExpression as memberAccess when not (isNull memberAccess.Expression) ->
            tryBuildCore adapter memberAccess.Expression
            |> Option.map (fun value -> DateTimeFunctions.translateGroupKeyMemberAccess value memberAccess.Expression.Type memberAccess.Member.Name)
        | :? MethodCallExpression as call when call.Method.DeclaringType = typeof<Enumerable> || call.Method.DeclaringType = typeof<Queryable> ->
            validateOverload call
            let name = call.Method.Name
            let terminal = ["Count";"LongCount";"MinBy";"MaxBy";"ToArray";"ToList";"Any";"All";"Contains";"Sum";"Min";"Max";"Average";"First";"FirstOrDefault";"Last";"LastOrDefault";"Single";"SingleOrDefault";"ElementAt";"ElementAtOrDefault"] |> List.contains name
            if not terminal then
                // A sequence-valued projection is an existing terminal surface;
                // do not turn every intermediate operator into implicit materialization.
                let sequenceResult =
                    match name with
                    | "Select" | "Distinct" | "DistinctBy" | "Order" | "OrderDescending"
                    | "DefaultIfEmpty" | "Cast" | "OfType" | "CountBy"
                    | "UnionBy" | "IntersectBy" | "ExceptBy" -> true
                    | _ -> false
                match if sequenceResult then parse adapter.IsRoot expression else None with
                | Some p when not p.Stages.IsEmpty && supportedRight adapter p.Stages ->
                    let elementType=sequenceElementType expression.Type
                    tryBuildCore adapter (Expression.Call(typeof<Enumerable>,"ToArray",[|elementType|],expression))
                | _ -> None
            else
            match parse adapter.IsRoot (getSource call) with
            | Some plan when supportedRight adapter plan.Stages ->
                let arg = getArg call
                // Inspect the open signature: an explicit default can itself have a
                // delegate type, but it is a value, not a predicate or selector.
                let argumentLambda =
                    let method = if call.Method.IsGenericMethod then call.Method.GetGenericMethodDefinition() else call.Method
                    let parameters = method.GetParameters()
                    if parameters.Length > 1 then
                        let declared = parameters.[1].ParameterType
                        let requiresLambda =
                            typeof<Delegate>.IsAssignableFrom declared
                            || (declared.IsGenericType && declared.GetGenericTypeDefinition() = typedefof<Expression<_>>)
                        if requiresLambda then
                            match arg |> Option.bind lambda with
                            | Some value -> Some value
                            | None ->
                                raise (NotSupportedException(
                                    $"Error: Cannot translate {adapter.LambdaContext}.{name} predicate.\nReason: The predicate is not a translatable lambda expression (e.g., Func<> delegate instead of Expression<Func<>>).\nFix: Pass the predicate as an inline lambda, not a delegate variable."))
                        else None
                    else None
                let plan =
                    match name, argumentLambda with
                    | "All", Some pred ->
                        let inverted = Expression.Lambda(Expression.Not(pred.Body), pred.Parameters)
                        { plan with Stages=plan.Stages @ [Filter inverted] }
                    | ("Count"|"LongCount"|"Any"|"First"|"FirstOrDefault"|"Last"|"LastOrDefault"|"Single"|"SingleOrDefault"), Some pred -> { plan with Stages=plan.Stages @ [Filter pred] }
                    | _ -> plan
                // A filter-only cardinality terminal does not observe input order.
                // Keep bounds, projections and set stages on their ordered rowset path.
                let simpleCardinality =
                    (name = "Count" || name = "LongCount" || name = "Any" || name = "All")
                    && (plan.Stages |> List.forall (function Filter _ | Order _ -> true | _ -> false))
                let orderUnobserved =
                    let rec eligible bounded = function
                        | [] -> true
                        | Filter _ :: tail when not bounded -> eligible bounded tail
                        | Order _ :: tail -> eligible bounded tail
                        | (Skip _ | Take _) :: tail -> eligible true tail
                        | _ -> false
                    (name = "Count" || name = "LongCount" || name = "Any" || name = "All")
                    && eligible false plan.Stages
                let plan =
                    if orderUnobserved then { plan with Stages = plan.Stages |> List.filter (function Order _ -> false | _ -> true) }
                    else plan
                let rowAdapter =
                    if orderUnobserved then
                        let sourceWithoutOrder columns root =
                            let source = adapter.Source columns root
                            match source.Body with
                            | SingleSelect row -> { source with Body = SingleSelect { row with OrderBy = [] } }
                            | _ -> source
                        { adapter with Source = sourceWithoutOrder }
                    else adapter
                let identityOnly =
                    adapter.EntityMembershipById
                    && (name = "Contains" || name = "Count" || name = "LongCount" || name = "Any")
                    && (plan.Stages |> List.forall (function
                        | Skip _ | Take _ -> true
                        | Distinct elementType ->
                            hasId elementType && (sequenceElementType plan.Root.Type).IsAssignableFrom elementType
                        | Order(selector, _, _) ->
                            match selector.Body with
                            | :? MemberExpression as memberRead ->
                                memberRead.Member.Name = "Id" && obj.ReferenceEquals(memberRead.Expression, selector.Parameters.[0])
                            | _ -> false
                        | _ -> false))
                let rows = build rowAdapter (if identityOnly then IdentityOnly else FullValue) plan
                let a = adapter.Alias()
                let source = Some(DerivedTable(rows,a))
                let elementType = sequenceElementType (getSource call).Type
                let payload = if hasId elementType then entityValue a else col a "Value"
                let terminalCore expression =
                    match rows.Body with
                    | SingleSelect row when simpleCardinality && rows.Ctes.IsEmpty ->
                        { row with Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = expression }]; OrderBy = [] }
                    | _ -> core source [proj "v" expression]
                let scalar e = SqlExpr.ScalarSubquery(select (terminalCore e))
                match name with
                | "MinBy" | "MaxBy" ->
                    let selector = argumentLambda |> Option.defaultWith (fun () -> raise (NotSupportedException("A keyed element terminal requires a selector lambda.")))
                    let key, joins = adapter.Translate plan.Root a selector
                    let direction = if name = "MinBy" then SortDirection.Asc else SortDirection.Desc
                    let order =
                        (if not selector.ReturnType.IsValueType || not (isNull (Nullable.GetUnderlyingType selector.ReturnType)) then
                            [{ Expr = SqlExpr.Unary(UnaryOperator.IsNull, key); Direction = SortDirection.Asc }]
                         else [])
                        @ [{ Expr = key; Direction = direction }; { Expr = col a "__ord"; Direction = SortDirection.Asc }]
                    let value = SqlExpr.FunctionCall("jsonb_extract", [payload; SqlExpr.Literal(SqlLiteral.String "$")])
                    Some(SqlExpr.ScalarSubquery(select { core source [proj "v" value] with Joins = joins; OrderBy = order; Limit = Some(integer 1L) }))
                | "Count" | "LongCount" -> Some(scalar (SqlExpr.AggregateCall(AggregateKind.Count,None,false,None)))
                | "ToArray" | "ToList" ->
                    Some(scalar (SqlExpr.FunctionCall("jsonb_group_array",[SqlExpr.FunctionCall("json",[payload])])))
                | ("First"|"FirstOrDefault"|"Last"|"LastOrDefault"|"Single"|"SingleOrDefault"|"ElementAt"|"ElementAtOrDefault") ->
                    let direction = if name.StartsWith("Last",StringComparison.Ordinal) then SortDirection.Desc else SortDirection.Asc
                    let offset = if name.StartsWith("ElementAt",StringComparison.Ordinal) then arg |> Option.map adapter.Value else None
                    let value = SqlExpr.FunctionCall("jsonb_extract",[payload;SqlExpr.Literal(SqlLiteral.String "$")])
                    let one = SqlExpr.ScalarSubquery(select {core source [proj "v" value] with
                                                               OrderBy=[{Expr=col a "__ord";Direction=direction}];Limit=Some(integer 1L);Offset=offset})
                    let one =
                        if name.EndsWith("OrDefault",StringComparison.Ordinal)
                           && call.Type.IsValueType && isNull (Nullable.GetUnderlyingType call.Type) then
                            let zero = adapter.Value(Expression.Constant(Activator.CreateInstance(call.Type),call.Type))
                            SqlExpr.Coalesce(one,[zero])
                        else one
                    let fallback =
                        match name with
                        | "FirstOrDefault" | "LastOrDefault" | "SingleOrDefault" ->
                            if call.Arguments.Count = 3 then Some(adapter.Value call.Arguments.[2])
                            elif argumentLambda.IsNone then arg |> Option.map adapter.Value
                            else None
                        | _ -> None
                    if name.StartsWith("Single",StringComparison.Ordinal) then
                        let n = scalar (SqlExpr.AggregateCall(AggregateKind.Count,None,false,None))
                        let empty = fallback |> Option.map (fun value -> SqlExpr.Binary(n,BinaryOperator.Eq,integer 0L),value) |> Option.toList
                        Some(SqlExpr.CaseExpr((SqlExpr.Binary(n,BinaryOperator.Eq,integer 1L),one),empty,Some(SqlExpr.Literal SqlLiteral.Null)))
                    else
                        match fallback with
                        | None -> Some one
                        | Some value ->
                            let present = SqlExpr.Exists(select {core source [proj "v" (integer 1L)] with Limit=Some(integer 1L)})
                            Some(SqlExpr.CaseExpr((present,one),[],Some value))
                | "Any" ->
                    match rows.Body with
                    | SingleSelect row when (orderUnobserved || not row.GroupBy.IsEmpty) && rows.Ctes.IsEmpty ->
                        let limit = if not row.GroupBy.IsEmpty && row.Limit.IsNone then Some(integer 1L) else row.Limit
                        let checkedRow =
                            { row with
                                Projections = ProjectionSetOps.ofList [proj "v" (integer 1L)]
                                OrderBy = []
                                Limit = limit }
                        Some(SqlExpr.Exists(select checkedRow))
                    | _ -> Some(SqlExpr.Exists(select {terminalCore (integer 1L) with Limit=Some(integer 1L)}))
                | "Contains" ->
                    let key, value =
                        if adapter.EntityMembershipById && hasId elementType
                           && not (plan.Stages |> List.exists (function Project _ -> true | _ -> false)) then
                            col a "Id", adapter.Value(Expression.PropertyOrField(arg.Value, "Id"))
                        else rootValue a, adapter.Value arg.Value
                    Some(SqlExpr.Exists(select {core source [proj "v" (integer 1L)] with Where=Some(SqlExpr.Binary(key,BinaryOperator.Is,value));Limit=Some(integer 1L)}))
                | "All" ->
                    let checkedRows =
                        match rows.Body with
                        | SingleSelect row when not row.GroupBy.IsEmpty && rows.Ctes.IsEmpty ->
                            { row with Projections = ProjectionSetOps.ofList [proj "v" (integer 1L)]
                                       OrderBy = []; Limit = Some(integer 1L) }
                        | _ -> { terminalCore (integer 1L) with Limit = Some(integer 1L) }
                    Some(SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists(select checkedRows)))
                | _ ->
                    let kind=match name with "Sum"->AggregateKind.Sum|"Min"->AggregateKind.Min|"Max"->AggregateKind.Max|_->AggregateKind.Avg
                    let value, joins =
                        match argumentLambda with
                        | Some selector -> adapter.Translate plan.Root a selector
                        | None -> rootValue a, []
                    let agg=SqlExpr.AggregateCall(kind,Some(FlattenTransform.normalizeExprQuoting value),false,None)
                    let result = if name="Sum" then SqlExpr.Coalesce(agg,[integer 0L]) else agg
                    Some(SqlExpr.ScalarSubquery(select { terminalCore result with Joins = joins }))
            | _ -> None
        | _ -> None

    /// Source adapters select direct aggregates before calling this terminal owner.
    let tryBuild adapter expression = tryBuildCore adapter expression
