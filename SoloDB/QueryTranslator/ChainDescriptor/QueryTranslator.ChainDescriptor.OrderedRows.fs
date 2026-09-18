namespace SoloDatabase

open System
open System.Linq
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.ChainExpr

open SoloDatabase.OrderedChainPlan

/// Lowers the ordered sequence stages without reconstructing operator position.
module internal OrderedChainRows =
    let col a n = SqlExpr.Column(Some a, n)
    let integer n = SqlExpr.Literal(SqlLiteral.Integer n)
    let proj n e = { Alias = Some n; Expr = e }
    let select core = { Ctes = []; Body = SingleSelect core }
    let core source projections =
        { Distinct = false; Projections = ProjectionSetOps.ofList projections; Source = source; Joins = []
          Where = None; GroupBy = []; Having = None; OrderBy = []; Limit = None; Offset = None }
    let private ordered a = [{ Expr = col a "__ord"; Direction = SortDirection.Asc }]
    let rootValue a = SqlExpr.JsonRootExtract(Some a, "Value")
    let private encode e = SqlExpr.FunctionCall("jsonb", [SqlExpr.FunctionCall("json_quote", [e])])
    let private jsonEachValue alias =
        let structured = SqlExpr.InList(col alias "type", SqlExpr.Literal(SqlLiteral.String "object"), [SqlExpr.Literal(SqlLiteral.String "array")])
        SqlExpr.CaseExpr((structured, SqlExpr.FunctionCall("jsonb", [col alias "value"])), [], Some(encode (col alias "value")))
    let hasId (t: Type) =
        // Only the Int64 Id convention denotes the separately carried rowid.
        // A Guid or string property with the same name remains document data.
        let property = t.GetProperty("Id")
        if not (isNull property) then property.PropertyType = typeof<int64>
        else
            let field = t.GetField("Id")
            not (isNull field) && field.FieldType = typeof<int64>
    let entityValue a = SqlExpr.FunctionCall("jsonb_set",[col a "Value";SqlExpr.Literal(SqlLiteral.String "$.Id");col a "Id"])
    let private rowNumber order =
        SqlExpr.WindowCall { Kind = NamedWindowFunction "ROW_NUMBER"; Arguments = []; PartitionBy = []; OrderBy = order }
    let private fields a = [proj "Id" (col a "Id"); proj "Value" (col a "Value"); proj "__ord" (col a "__ord")]
    let private nonnegative v =
        match v with
        | SqlExpr.Literal(SqlLiteral.Integer n) -> integer (max 0L n)
        | _ -> SqlExpr.FunctionCall("max", [integer 0L; v])
    let private inlineRow (source: SqlSelect) alias (joins: JoinShape list) expressions =
        match source.Body with
        | SingleSelect row when joins.IsEmpty && source.Ctes.IsEmpty
                                && row.Limit.IsNone && row.Offset.IsNone && not row.Distinct
                                && row.GroupBy.IsEmpty && row.Having.IsNone
                                && (ProjectionSetOps.toList row.Projections |> List.forall (fun p -> match p.Expr with Column _ -> true | _ -> false)) ->
            let local expression = SqlExpr.fold (fun valid node ->
                valid && match node with ScalarSubquery _ | Exists _ | InSubquery _ -> false | _ -> true) true expression
            if expressions |> List.forall local then
                let rewrite = AliasRewrite.rewriteDerivedAliasExpr
                                  { MatchEmptyDerivedAlias = false; OnSubquerySelect = id }
                                  (AliasRewrite.buildProjectionAliasMap row) alias
                Some(row, rewrite)
            else None
        | _ -> None
    let rec build (adapter: Adapter) columns (plan: Plan) =
        adapter.Validate plan
        let start = adapter.Source columns plan.Root
        let sourceElementType = sequenceElementType plan.Root.Type
        let isSourceEntity elementType = hasId elementType && sourceElementType.IsAssignableFrom elementType
        let bound source fallback limit offset =
            // Raw payload columns do not parse discarded rows. Internal row numbers
            // and order keys are computed before LIMIT in either placement; other
            // computed projections and existing bounds retain their boundary.
            match source.Body with
            | SingleSelect row when source.Ctes.IsEmpty && row.Limit.IsNone && row.Offset.IsNone
                                    && not row.Distinct && row.GroupBy.IsEmpty && row.Having.IsNone
                                    && (ProjectionSetOps.toList row.Projections |> List.forall (fun p ->
                                        match p.Alias, p.Expr with
                                        | _, Column _ -> true
                                        | Some "__ord", WindowCall { Kind = NamedWindowFunction "ROW_NUMBER" } -> true
                                        | Some key, expression when key.StartsWith("__key", StringComparison.Ordinal) ->
                                            row.OrderBy |> List.exists (fun ordering -> ordering.Expr = expression)
                                        | _ -> false)) ->
                select { row with Limit = limit; Offset = offset }
            | _ -> select { fallback with Limit = limit; Offset = offset }
        let rec lower (source: SqlSelect) keys stages =
            match stages with
            | [] -> source
            | Skip skip :: Take take :: rest ->
                let a=adapter.Alias()
                let ps=fields a @ (keys |> List.mapi (fun i _ -> proj ("__key"+string i) (col a ("__key"+string i))))
                let fallback = {core (Some(DerivedTable(source,a))) ps with OrderBy=ordered a}
                let bounded = bound source fallback (Some(nonnegative(adapter.Value take))) (Some(nonnegative(adapter.Value skip)))
                lower bounded keys rest
            | stage :: rest ->
                let a = adapter.Alias()
                let from = Some(DerivedTable(source, a))
                let pass = fields a @ (keys |> List.mapi (fun i _ -> proj ("__key" + string i) (col a ("__key" + string i))))
                let baseCore = { core from pass with OrderBy = ordered a }
                let next c k = lower (select c) k rest
                let distinctOn key joins =
                    let rank = SqlExpr.WindowCall {
                        Kind = NamedWindowFunction "ROW_NUMBER"; Arguments = []
                        PartitionBy = [key]; OrderBy = [col a "__ord", SortDirection.Asc] }
                    let marked = select { baseCore with
                                            Joins = joins
                                            Projections = ProjectionSetOps.ofList (pass @ [proj "__rank" rank]) }
                    let da = adapter.Alias()
                    lower (select { core (Some(DerivedTable(marked, da))) (fields da) with
                                        Where = Some(SqlExpr.Binary(col da "__rank", BinaryOperator.Eq, integer 1L))
                                        OrderBy = ordered da }) [] rest
                match stage with
                | Filter l ->
                    let pred, joins = adapter.Translate plan.Root a l
                    let pred = FlattenTransform.normalizeExprQuoting pred
                    let filtered = select { baseCore with Where = Some pred; Joins = joins }
                    let filtered =
                        match inlineRow source a joins [pred] with
                        | Some(row, rewrite) ->
                            let predicate = rewrite pred
                            let condition = match row.Where with None -> predicate | Some previous -> SqlExpr.Binary(previous, BinaryOperator.And, predicate)
                            select { row with Where = Some condition }
                        | _ -> filtered
                    lower filtered keys rest
                | Project l ->
                    let value, joins = adapter.Translate plan.Root a l
                    let value = FlattenTransform.normalizeExprQuoting value
                    let value = if obj.ReferenceEquals(l.Body,l.Parameters.[0]) && hasId l.ReturnType then entityValue a else encode value
                    let ps = pass |> List.map (fun p ->
                        if p.Alias = Some "Value" then proj "Value" value
                        elif p.Alias = Some "Id" && hasId l.ReturnType then
                            proj "Id" (SqlExpr.FunctionCall("jsonb_extract",[value;SqlExpr.Literal(SqlLiteral.String "$.Id")]))
                        else p)
                    let projected =
                        match inlineRow source a joins (ps |> List.map _.Expr) with
                        | Some(row, rewrite) ->
                            { row with Projections = ps |> List.map (fun p -> { p with Expr = rewrite p.Expr }) |> ProjectionSetOps.ofList
                                       OrderBy = baseCore.OrderBy |> List.map (fun order -> { order with Expr = rewrite order.Expr }) }
                        | None -> { baseCore with Projections = ProjectionSetOps.ofList ps; Joins = joins }
                    next projected keys
                | OfType(_, targetType) ->
                    let typeName = Utils.typeToName targetType
                    let condition =
                        match typeName with
                        | Some name -> SqlExpr.Binary(SqlExpr.JsonExtractExpr(Some a, "Value", JsonPath("$type", [])), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.String name))
                        | None -> integer 0L
                    next { baseCore with Where = Some condition } keys
                | Cast(_, targetType) ->
                    let projections =
                        match Utils.typeToName targetType with
                        | None -> pass
                        | Some name ->
                            let matches = SqlExpr.Binary(SqlExpr.JsonExtractExpr(Some a, "Value", JsonPath("$type", [])), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.String name))
                            pass |> List.map (fun projection ->
                                if projection.Alias = Some "Value" || projection.Alias = Some "Id" then
                                    { projection with Expr = SqlExpr.CaseExpr((matches, projection.Expr), [], Some(SqlExpr.Literal SqlLiteral.Null)) }
                                else projection)
                    next { baseCore with Projections = ProjectionSetOps.ofList projections } keys
                | Expand selector ->
                    let selector =
                        match selector.Body with
                        | :? MethodCallExpression as call when call.Method.DeclaringType = typeof<Enumerable> || call.Method.DeclaringType = typeof<Queryable> ->
                            let items = Expression.Call(typeof<Enumerable>, "ToArray", [|sequenceElementType selector.ReturnType|], selector.Body)
                            Expression.Lambda(items, selector.Parameters)
                        | _ -> selector
                    let collection, joins = adapter.Translate plan.Root a selector
                    let itemAlias = adapter.Alias()
                    let value = jsonEachValue itemAlias
                    let id =
                        if hasId (sequenceElementType selector.ReturnType) then
                            SqlExpr.FunctionCall("jsonb_extract", [value; SqlExpr.Literal(SqlLiteral.String "$.Id")])
                        else col itemAlias "key"
                    let order = [col a "__ord", SortDirection.Asc; col itemAlias "key", SortDirection.Asc]
                    next { core from [proj "Id" id; proj "Value" value; proj "__ord" (rowNumber order)] with
                               Joins = joins @ [CrossJoin(FromJsonEach(collection, Some itemAlias))]
                               OrderBy = order |> List.map (fun (expression, direction) -> { Expr = expression; Direction = direction }) } []
                | Order(l, direction, append) ->
                    let rec collect terms preceding remaining =
                        match remaining with
                        | Order(selector, direction, true) :: tail -> collect ((selector, direction) :: terms) preceding tail
                        | Order(selector, direction, false) :: tail ->
                            collect [selector, direction] (List.rev terms @ preceding) tail
                        | _ -> List.rev terms @ preceding, remaining
                    let terms, remaining = collect [l, direction] [] rest
                    let translated = terms |> List.map (fun (selector, direction) ->
                        let expression, joins = adapter.Translate plan.Root a selector
                        (expression, direction), joins)
                    let prior = if append then keys else []
                    let exprs = prior |> List.mapi (fun i d -> col a ("__key" + string i), d)
                    let order = exprs @ (translated |> List.map fst)
                    let joins = translated |> List.collect snd
                    let ps = [proj "Id" (col a "Id"); proj "Value" (col a "Value")
                              proj "__ord" (rowNumber (order @ [col a "__ord", SortDirection.Asc]))]
                             @ (order |> List.mapi (fun i (e, _) -> proj ("__key" + string i) e))
                    lower (select { baseCore with Projections = ProjectionSetOps.ofList ps; Joins = joins
                                                  OrderBy = order |> List.map (fun (e,d) -> { Expr = e; Direction = d }) })
                          (prior @ (terms |> List.map snd)) remaining
                | Skip n -> lower (bound source baseCore (Some(integer -1L)) (Some(nonnegative (adapter.Value n)))) keys rest
                | Take n -> lower (bound source baseCore (Some(nonnegative (adapter.Value n))) None) keys rest
                | Distinct elementType when isSourceEntity elementType ->
                    distinctOn (col a "Id") []
                | Distinct elementType ->
                    // Stored document payload excludes Id; equality observes the
                    // complete value, including that separately carried identity.
                    let value = if hasId elementType then entityValue a else rootValue a
                    let ps = [proj "Id" (SqlExpr.AggregateCall(AggregateKind.Min, Some(col a "Id"), false, None))
                              proj "Value" (encode value)
                              proj "__ord" (SqlExpr.AggregateCall(AggregateKind.Min, Some(col a "__ord"), false, None))]
                    let d = { core from ps with GroupBy = [value] }
                    let da = adapter.Alias()
                    lower (select { core (Some(DerivedTable(select d, da))) (fields da) with OrderBy = ordered da }) [] rest
                | DistinctBy selector ->
                    let key, joins = adapter.Translate plan.Root a selector
                    distinctOn key joins
                | Group(selector, countOnly) ->
                    let key, joins = adapter.Translate plan.Root a selector
                    let items =
                        if countOnly then SqlExpr.AggregateCall(AggregateKind.Count, None, false, None)
                        else
                            let item = if hasId selector.Parameters.[0].Type then entityValue a else col a "Value"
                            SqlExpr.FunctionCall("jsonb_group_array", [SqlExpr.FunctionCall("json", [item])])
                    let value = SqlExpr.JsonObjectExpr ["Key", key; (if countOnly then "Value" else "Items"), items]
                    let first = SqlExpr.AggregateCall(AggregateKind.Min, Some(col a "__ord"), false, None)
                    let predicateJoins = ResizeArray<JoinShape>()
                    let translateSelector lambda =
                        let expression, discovered = adapter.Translate plan.Root a lambda
                        predicateJoins.AddRange discovered
                        expression
                    let rec predicates having remaining =
                        match remaining with
                        | Filter predicate :: tail when not countOnly ->
                            let keyRead = lazy (
                                let memberRead = Expression.Property(predicate.Parameters.[0], "Key")
                                adapter.Translate plan.Root a (Expression.Lambda(memberRead, predicate.Parameters)) |> fst)
                            let translateValue value =
                                let translated, discovered = adapter.Translate plan.Root a (Expression.Lambda(value, predicate.Parameters))
                                predicateJoins.AddRange discovered
                                SqlExpr.map (fun node -> if node = keyRead.Value then key else node) translated
                            let expression = DBRefManyHelpers.translateGroupingExpression translateValue translateSelector key predicate.Parameters.[0] predicate.Body
                            let combined = match having with None -> expression | Some previous -> SqlExpr.Binary(previous, BinaryOperator.And, expression)
                            predicates (Some combined) tail
                        | _ -> having, remaining
                    let having, remaining = predicates None rest
                    lower (select { core from [proj "Id" (integer -1L); proj "Value" value; proj "__ord" first] with
                                      Joins = joins @ List.ofSeq predicateJoins; GroupBy = [key]; Having = having
                                      OrderBy = [{ Expr = first; Direction = SortDirection.Asc }] }) [] remaining
                | While(l,take) ->
                    let pred,joins=adapter.Translate plan.Root a l
                    let failed=SqlExpr.CaseExpr((pred,integer 0L),[],Some(integer 1L))
                    let count=SqlExpr.WindowCall {Kind=NamedWindowFunction "SUM";Arguments=[failed];PartitionBy=[];OrderBy=[col a "__ord",SortDirection.Asc]}
                    let marked=select {baseCore with Joins=joins;Projections=ProjectionSetOps.ofList(pass @ [proj "__failed" count])}
                    let wa=adapter.Alias()
                    let ps=fields wa @ (keys |> List.mapi (fun i _ -> proj ("__key"+string i) (col wa ("__key"+string i))))
                    let condition=SqlExpr.Binary(col wa "__failed",(if take then BinaryOperator.Eq else BinaryOperator.Gt),integer 0L)
                    lower (select {core (Some(DerivedTable(marked,wa))) ps with Where=Some condition;OrderBy=ordered wa}) keys rest
                | Set(name, right, keySelector) ->
                    let r =
                        match parse adapter.IsRoot right with
                        | Some p -> build adapter FullValue p
                        | None ->
                            let ra = adapter.Alias()
                            let payload = jsonEachValue ra
                            let id =
                                if hasId (sequenceElementType right.Type) then
                                    SqlExpr.FunctionCall("jsonb_extract", [payload; SqlExpr.Literal(SqlLiteral.String "$.Id")])
                                else col ra "key"
                            select {core (Some(FromJsonEach(adapter.Value right,Some ra)))
                                        [proj "Id" id;proj "Value" payload;proj "__ord" (col ra "key")]
                                    with OrderBy=[{Expr=col ra "key";Direction=SortDirection.Asc}]}
                    if name = "Concat" || name = "Union" || name = "UnionBy" then
                        let ra = adapter.Alias()
                        let arm src alias branch =
                            core (Some(DerivedTable(src, alias))) (fields alias @ [proj "__branch" (integer branch)])
                        let union = { Ctes = []; Body = UnionAllSelect(arm source a 0L, [arm r ra 1L]) }
                        let ua = adapter.Alias()
                        let merged = select { core (Some(DerivedTable(union, ua)))
                                                [proj "Id" (col ua "Id"); proj "Value" (col ua "Value")
                                                 proj "__ord" (rowNumber [col ua "__branch", SortDirection.Asc; col ua "__ord", SortDirection.Asc])]
                                              with OrderBy = [{Expr=col ua "__branch";Direction=SortDirection.Asc};{Expr=col ua "__ord";Direction=SortDirection.Asc}] }
                        let remaining =
                            match name, keySelector with
                            | "UnionBy", Some key -> DistinctBy key :: rest
                            | "Union", _ -> Distinct(sequenceElementType right.Type) :: rest
                            | _ -> rest
                        lower merged [] remaining
                    else
                        let ra = adapter.Alias()
                        let key, joins =
                            match keySelector with
                            | Some key -> adapter.Translate plan.Root a key
                            | None when isSourceEntity (sequenceElementType right.Type) -> col a "Id", []
                            | None -> (if hasId (sequenceElementType right.Type) then entityValue a else rootValue a), []
                        let rightKey =
                            if keySelector.IsNone && isSourceEntity (sequenceElementType right.Type) then col ra "Id"
                            elif keySelector.IsNone && hasId (sequenceElementType right.Type) then entityValue ra
                            else rootValue ra
                        let check = select { core (Some(DerivedTable(r, ra))) [proj "v" (integer 1L)] with
                                               Where = Some(SqlExpr.Binary(key, BinaryOperator.Is, rightKey)) }
                        let exists = SqlExpr.Exists check
                        let pred = if name = "Except" || name = "ExceptBy" then SqlExpr.Unary(UnaryOperator.Not, exists) else exists
                        let distinct = keySelector |> Option.map DistinctBy |> Option.defaultValue (Distinct(sequenceElementType right.Type))
                        lower (select { baseCore with Where = Some pred; Joins = joins }) keys (distinct :: rest)
                | Default value ->
                    let fallback = value |> Option.map adapter.Value |> Option.defaultValue (SqlExpr.Literal SqlLiteral.Null)
                    let exists = SqlExpr.Exists(select { core from [proj "v" (integer 1L)] with Limit=Some(integer 1L) })
                    let id =
                        if value |> Option.exists (fun expression -> hasId expression.Type) then
                            SqlExpr.FunctionCall("jsonb_extract", [fallback; SqlExpr.Literal(SqlLiteral.String "$.Id")])
                        else integer -1L
                    let empty = { core None [proj "Id" id;proj "Value" (encode fallback);proj "__ord" (integer 0L)] with Where=Some(SqlExpr.Unary(UnaryOperator.Not,exists)) }
                    let main = core from (fields a)
                    lower { Ctes=[];Body=UnionAllSelect(main,[empty]) } [] rest
        lower start [] plan.Stages
