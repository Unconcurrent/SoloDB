namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryStructuralShape

/// All structural order consumers use one complete key and its occurrence policy.
module internal QueryStructuralOrdering =
    type Key = Selected of QueryProducedValue | Prepared of Graph * SqlExpr
    let private integer value=SqlExpr.Literal(SqlLiteral.Integer value)
    let private missing nextName value=SqlExpr.Unary(UnaryOperator.IsNull,QueryProduced.presence nextName value)
    let private choose condition yes no=SqlExpr.CaseExpr((condition,yes),[],Some no)
    let private absent=SqlExpr.Literal SqlLiteral.Null
    let supported target=match QueryStructuralShape.tryCreate target with Available graph -> graph.Root.Kind <> ScalarNode | _ -> false

    /// Original bound keys retain their own readers before stored serialization.
    let rec translate visitProduced (expression: Expression) (qb: QueryBuilder) =
        match expression with
        | :? UnaryExpression as quote when quote.NodeType=ExpressionType.Quote -> translate visitProduced quote.Operand qb
        | :? LambdaExpression as lambda -> translate visitProduced lambda.Body {qb with Parameters=lambda.Parameters}
        | _ ->
            QueryStructuralShape.requireComparison expression.Type
            match qb.SourceContext.BindQueryValue,QueryStructuralShape.tryCreate expression.Type with
            | ValueSome binder,Available graph when graph.Root.Kind <> ScalarNode && (binder.IsValue expression || isFullyConstant expression) ->
                let original=binder.Local expression
                let prepared=BoundValueAccessExpression(original,typeof<string>,Func<obj,obj>(fun value -> box(QueryStructuralShape.prepare graph value)))
                Prepared(graph,binder.Scalar prepared)
            | _ -> Selected(visitProduced expression qb)

    let prepare nextName variables target direction predicate key =
        let ordinary=match direction with SortDirection.AscNullsLast -> SortDirection.Asc | SortDirection.DescNullsFirst -> SortDirection.Desc | value -> value
        match key with
        | Prepared(graph,value) ->
            let encoded=QueryOrderingTraversal.encode nextName variables graph.Root QueryOrderingPolicy.Default (QueryOrderingTraversal.Prepared value)
            let presence=
                if direction=ordinary then [] else
                let absent=SqlExpr.FunctionCall("jsonb_extract",[value;SqlExpr.Literal(SqlLiteral.String "$[0][2]")])
                [{Expr=choose absent (SqlExpr.Literal SqlLiteral.Null) (integer 1L);Direction=direction}]
            presence@[{Expr=encoded;Direction=ordinary}],[]
        | Selected produced ->
            match QueryStructuralShape.tryCreate target with
            | Unsupported custom -> QueryStructuralShape.unsupported custom
            | Available { Root = { Kind = ScalarNode } }
            | Unavailable ->
                [{Expr=QueryProduced.value nextName produced |> DateTimeFunctions.canonicalizeForCompareOrOrder target;Direction=direction}],[]
            | Available graph ->
                let joins=ResizeArray<JoinShape>()
                let active=HashSet<struct(int*QueryOrderingPolicy.Role)>()
                let guarded guard value=match guard with Some guard -> choose guard absent value | None -> value
                let combine guard value=match guard with Some guard -> Some(SqlExpr.Binary(guard,BinaryOperator.Or,value)) | None -> Some value
                let rec walk (node: Node) role guard top produced =
                    let bindingPosition=joins.Count
                    let consume value input =
                        let produced=DirectValue(value,input)
                        let ordering value=[{Expr=guarded guard value;Direction=ordinary}]
                        let marker missing=[{Expr=guarded guard (choose missing absent (integer 1L));Direction=if top then direction else ordinary}]
                        if active.Contains(struct(node.Index,role)) then
                            QueryOrderingTraversal.encode nextName variables node role (QueryOrderingTraversal.Selected produced) |> ordering
                        else
                            active.Add(struct(node.Index,role)) |>ignore
                            let childRole=QueryOrderingPolicy.children role node
                            let result=
                                match node.Kind with
                                | ScalarNode ->
                                    (if typeof<string>.Equals node.Target then QueryStructuralAccess.stringValue nextName produced
                                     else QueryStructuralAccess.scalar variables nextName node.Target produced)
                                    |> QueryOrderingPolicy.finishDefault predicate
                                    |> QueryOrderingPolicy.scalar role node.Target |> ordering
                                | OptionalNode(child,_) ->
                                    let absent=missing nextName produced
                                    marker absent @ walk child childRole (combine guard absent) false (QueryStructuralAccess.payload nextName produced)
                                | ComponentNode fields ->
                                    let absent=if node.Target.IsValueType then None else Some(missing nextName produced)
                                    let childGuard=match absent with Some absent -> combine guard absent | None -> guard
                                    let presence=absent |> Option.map marker |> Option.defaultValue []
                                    presence @ (fields |> Array.toList |> List.collect(fun field ->
                                        QueryStructuralAccess.child variables nextName field.Name field.Node.Target produced
                                        |> walk field.Node childRole childGuard false))
                            active.Remove(struct(node.Index,role)) |>ignore
                            result
                    let terms,bindings=QueryProduced.bindTerms nextName consume produced
                    joins.InsertRange(bindingPosition,bindings)
                    terms
                let terms=walk graph.Root QueryOrderingPolicy.Default None true produced
                // An empty value type still supplies one stable native key.
                (if terms.IsEmpty then [{Expr=integer 0L;Direction=ordinary}] else terms),List.ofSeq joins
