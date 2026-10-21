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
    let hasId (t: Type) =
        // Only the Int64 Id convention denotes the separately carried rowid.
        // A Guid or string property with the same name remains document data.
        match ModelMetadata.MemberType(t,"Id",false) with
        | Some valueType -> typeof<int64>.Equals(valueType)
        | None ->
            match ModelMetadata.MemberType(t,"Id",true) with
            | Some valueType -> typeof<int64>.Equals(valueType)
            | None -> false
    let private rowNumber order =
        QueryableBuildQueryWindowHelpers.rowNumberOver order
    let private payloadFields a = [proj "Id" (col a "Id"); proj "Value" (col a "Value"); proj "__ord" (col a "__ord")]
    let private fields input a = payloadFields a @ QueryTranslatorSerialization.companionProjections (Some a) input
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
                                  { MatchEmptyDerivedAlias = false; MatchUnqualifiedColumns = true; OnSubquerySelect = id }
                                  (AliasRewrite.buildProjectionAliasMap row) alias
                Some(row, rewrite)
            else None
        | _ -> None
    let internal prepareGrouping (adapter: Adapter) root input alias (source: SelectCore) selector element =
        let keyProduced, joins = adapter.Translate root input alias selector ValueNone
        let keyName = "_key" + adapter.Alias()
        let projections = ProjectionSetOps.toList source.Projections @ [proj keyName (SqlExpr.Literal SqlLiteral.Null)]
        let keyCore = { source with Joins = source.Joins @ joins; Projections = ProjectionSetOps.ofList projections }
        let struct(keyRows, keyInput) = QueryProduced.publishSlot adapter.Alias keyName keyCore keyProduced false
        let struct(keyRows, keyInput, equalityNames) = QueryDefaultEquality.prepareGroups adapter.Alias keyName selector.ReturnType keyRows input keyInput "__ord"
        match element with
        | None -> struct(keyRows, input, keyInput, keyName, equalityNames)
        | Some element ->
            let alias = adapter.Alias()
            let produced, joins = adapter.Translate root input alias element ValueNone
            let projections = payloadFields alias
                              @ QueryTranslatorSerialization.companionProjections (Some alias) keyInput
                              @ [proj keyName (col alias keyName)]
                              @ (equalityNames |> List.filter ((<>) keyName) |> List.map (fun name -> proj name (col alias name)))
            let row = { core (Some(DerivedTable(keyRows, alias))) projections with Joins = joins; OrderBy = ordered alias }
            let struct(rows, output) = QueryProduced.publishGroupElement adapter.Alias row produced false
            struct(QueryProduced.materializeGroupElements adapter.Alias "__ord" rows, output, keyInput, keyName, equalityNames)

    let private pureLookupExpression expression =
        SqlExpr.fold (fun valid node ->
            valid && (match node with
                      | ScalarSubquery _ | Exists _ | InSubquery _ | AggregateCall _ | WindowCall _ -> false
                      | FunctionCall(name, _) ->
                          name = "jsonb" || name = "jsonb_extract" || name = "json_quote" || name = "json" || name = "NULLIF"
                      | _ -> true)) true expression

    let private hasPureRightLookup (source: SqlSelect) =
        let mutable valid = true
        let rec inspect source =
            valid <- valid && source.Ctes.IsEmpty
            let inspectCore (row: SelectCore) =
                valid <- valid && row.Limit.IsNone && row.Offset.IsNone && row.GroupBy.IsEmpty && row.Having.IsNone && not row.Distinct
                mapCoreExpressions (fun expression -> valid <- valid && pureLookupExpression expression; expression) inspect row
            mapSelectParts (mapCores inspectCore) inspect source
        inspect source |> ignore
        valid

    let rec private buildRows (adapter: Adapter) columns (plan: Plan) retainFinalRepresentative =
        for stage in plan.Stages do
            match stage with
            | Order(selector, _, _) -> QueryStructuralShape.requireComparison selector.ReturnType
            | _ -> ()
        adapter.Validate plan
        let struct(start, sourceInput) = adapter.Source columns plan.Root
        let bound (source: SqlSelect) fallback limit offset =
            // Raw payload columns do not parse discarded rows. Internal row numbers
            // and order keys are computed before LIMIT in either placement; other
            // computed projections and existing bounds retain their boundary.
            match source.Body with
            | SingleSelect row when source.Ctes.IsEmpty && row.Limit.IsNone && row.Offset.IsNone
                                    && not row.Distinct && row.GroupBy.IsEmpty && row.Having.IsNone
                                    && (ProjectionSetOps.toList row.Projections |> List.forall (fun p ->
                                        match p.Alias, p.Expr with
                                        | _, Column _ -> true
                                        | Some "__ord", WindowCall { Kind = WindowFunctionKind.RowNumber } -> true
                                        | Some key, expression when key.StartsWith("__key", StringComparison.Ordinal) ->
                                            row.OrderBy |> List.exists (fun ordering -> ordering.Expr = expression)
                                        | _ -> false)) ->
                select { row with Limit = limit; Offset = offset }
            | _ -> select { fallback with Limit = limit; Offset = offset }
        let value expression = adapter.Value expression |> QueryProduced.value adapter.Alias
        let translate root input alias selector =
            let produced, joins = adapter.Translate root input alias selector ValueNone
            QueryProduced.value adapter.Alias produced, joins
        let rec lower (source: SqlSelect) input keys stages =
            match stages with
            | [] -> struct(source, input)
            | Skip skip :: Take take :: rest ->
                let a=adapter.Alias()
                let ps=fields input a @ (keys |> List.mapi (fun i _ -> proj ("__key"+string i) (col a ("__key"+string i))))
                let fallback = {core (Some(DerivedTable(source,a))) ps with OrderBy=ordered a}
                let bounded = bound source fallback (Some(QueryTranslatorBaseHelpers.nonnegativeCount(value take))) (Some(QueryTranslatorBaseHelpers.nonnegativeCount(value skip)))
                lower bounded input keys rest
            | stage :: rest ->
                let a = adapter.Alias()
                let from = Some(DerivedTable(source, a))
                let pass = fields input a @ (keys |> List.mapi (fun i _ -> proj ("__key" + string i) (col a ("__key" + string i))))
                let baseCore = { core from pass with OrderBy = ordered a }
                let nextWith output c k = lower (select c) output k rest
                let next c k = nextWith input c k
                let distinctOn key joins =
                    let ranked = QueryDefaultEquality.firstRows adapter.Alias { baseCore with Joins = joins } key [col a "__ord", SortDirection.Asc]
                    lower ranked input [] rest
                match stage with
                | Filter l ->
                    let pred, joins = translate plan.Root input a l
                    let pred = FlattenTransform.normalizeExprQuoting pred
                    let filtered = select { baseCore with Where = Some pred; Joins = joins }
                    let filtered =
                        match inlineRow source a joins [pred] with
                        | Some(row, rewrite) ->
                            let predicate = rewrite pred
                            let condition = match row.Where with None -> predicate | Some previous -> SqlExpr.Binary(previous, BinaryOperator.And, predicate)
                            select { row with Where = Some condition }
                        | _ -> filtered
                    lower filtered input keys rest
                | Project l ->
                    let produced, joins = adapter.Translate plan.Root input a l ValueNone
                    let produced =
                        if obj.ReferenceEquals(l.Body, l.Parameters[0]) then
                            let input = QueryTranslatorSerialization.scopeSerializationInput (Some a) BaseLayer produced.Input
                            let value, input = QueryProduced.embedded (col a "Value", input)
                            DirectValue(value, { input with Carrier = CanonicalToken })
                        else QueryProduced.canonical adapter.Alias produced
                    let ps = payloadFields a @ (keys |> List.mapi (fun index _ -> proj ("__key" + string index) (col a ("__key" + string index))))
                    let struct(projected, output) = QueryProduced.publishCore adapter.Alias { baseCore with Projections = ProjectionSetOps.ofList ps; Joins = joins } produced false
                    let projected =
                        if hasId l.ReturnType && output.PhysicalOwner.IsNone then
                            match projected.Body with
                            | SingleSelect row ->
                                let value = ProjectionSetOps.toList row.Projections |> List.find (fun projection -> projection.Alias = Some "Value") |> _.Expr
                                let projections =
                                    row.Projections |> ProjectionSetOps.toList |> List.map (fun projection ->
                                        if projection.Alias = Some "Id" then { projection with Expr = SqlExpr.FunctionCall("jsonb_extract", [value; SqlExpr.Literal(SqlLiteral.String "$.Id")]) } else projection)
                                    |> ProjectionSetOps.ofList
                                { projected with Body = SingleSelect { row with Projections = projections } }
                            | _ -> projected
                        else projected
                    lower projected output keys rest
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
                        | :? MethodCallExpression as call when typeof<Enumerable>.Equals(call.Method.DeclaringType) || typeof<Queryable>.Equals(call.Method.DeclaringType) ->
                            let items = Expression.Call(typeof<Enumerable>, "ToArray", [|sequenceElementType selector.ReturnType|], selector.Body)
                            { selector with Body = items }
                        | _ -> selector
                    let collectionProduced, joins = adapter.Translate plan.Root input a selector ValueNone
                    let from, collection, collectionInput, ownerAlias, joins =
                        match collectionProduced with
                        | DirectValue(collection, collectionInput) -> from, collection, collectionInput, a, joins
                        | PackedValue _ ->
                            let struct(selected, selectedInput) = QueryProduced.publishCore adapter.Alias { baseCore with Joins = joins } collectionProduced false
                            let selectedAlias = adapter.Alias()
                            Some(DerivedTable(selected, selectedAlias)), col selectedAlias "Value",
                            QueryTranslatorSerialization.scopeSerializationInput (Some selectedAlias) OuterLayer selectedInput, selectedAlias, []
                    let itemAlias = adapter.Alias()
                    let token = SqlExpr.FunctionCall("jsonb", [SqlExpr.Binary(collection, BinaryOperator.JsonText, col itemAlias "fullkey")])
                    let elementInput = QueryTranslatorSerialization.elementInput collectionInput (col itemAlias "key") token
                    let value = QueryProduced.scalarValue (sequenceElementType selector.ReturnType) token { elementInput with Carrier = CanonicalToken }
                    let id =
                        if hasId (sequenceElementType selector.ReturnType) then
                            SqlExpr.FunctionCall("jsonb_extract", [value; SqlExpr.Literal(SqlLiteral.String "$.Id")])
                        else col itemAlias "key"
                    let order = [col ownerAlias "__ord", SortDirection.Asc; col itemAlias "key", SortDirection.Asc]
                    let produced = QueryProduced.canonical adapter.Alias (DirectValue(value, elementInput))
                    let expanded =
                        { core from [proj "Id" id; proj "Value" (SqlExpr.Literal SqlLiteral.Null); proj "__ord" (rowNumber order)] with
                            Joins = joins @ [CrossJoin(FromJsonEach(collection, Some itemAlias))]
                            OrderBy = order |> List.map (fun (expression, direction) -> { Expr = expression; Direction = direction }) }
                    let struct(expanded, output) = QueryProduced.publishCore adapter.Alias expanded produced false
                    lower expanded output [] rest
                | Order(l, direction, append) ->
                    let rec collect terms preceding remaining =
                        match remaining with
                        | Order(selector, direction, true) :: tail -> collect ((selector, direction) :: terms) preceding tail
                        | Order(selector, direction, false) :: tail ->
                            collect [selector, direction] (List.rev terms @ preceding) tail
                        | _ -> List.rev terms @ preceding, remaining
                    let terms, remaining = collect [l, direction] [] rest
                    let translated = terms |> List.map (fun (selector, direction) ->
                        let expression, joins = translate plan.Root input a selector
                        (expression, direction), joins)
                    let prior = if append then keys else []
                    let exprs = prior |> List.mapi (fun i d -> col a ("__key" + string i), d)
                    let order = exprs @ (translated |> List.map fst)
                    let joins = translated |> List.collect snd
                    let ps = [proj "Id" (col a "Id"); proj "Value" (col a "Value")
                              proj "__ord" (rowNumber (order @ [col a "__ord", SortDirection.Asc]))]
                             @ (order |> List.mapi (fun i (e, _) -> proj ("__key" + string i) e))
                             @ QueryTranslatorSerialization.companionProjections (Some a) input
                    lower (select { baseCore with Projections = ProjectionSetOps.ofList ps; Joins = joins
                                                  OrderBy = order |> List.map (fun (e,d) -> { Expr = e; Direction = d }) })
                          input (prior @ (terms |> List.map snd)) remaining
                | Skip n -> lower (bound source baseCore (Some(integer -1L)) (Some(QueryTranslatorBaseHelpers.nonnegativeCount(value n)))) input keys rest
                | Take n -> lower (bound source baseCore (Some(QueryTranslatorBaseHelpers.nonnegativeCount(value n))) None) input keys rest
                | Distinct elementType ->
                    let scoped = QueryTranslatorSerialization.scopeSerializationInput (Some a) BaseLayer input
                    distinctOn (QueryDefaultEquality.key adapter.Alias elementType (col a "Value") scoped) []
                | DistinctBy selector ->
                    let produced, joins = adapter.Translate plan.Root input a selector ValueNone
                    let name = "_distinct_key" + adapter.Alias()
                    let keyCore = { baseCore with Joins = joins; Projections = ProjectionSetOps.ofList(pass @ [proj name (SqlExpr.Literal SqlLiteral.Null)]) }
                    let struct(prepared, keyInput) = QueryProduced.publishSlot adapter.Alias name keyCore produced false
                    let alias = adapter.Alias()
                    let keyInput = QueryTranslatorSerialization.scopeSerializationInput (Some alias) OuterLayer keyInput
                    let key = QueryDefaultEquality.key adapter.Alias selector.ReturnType (col alias name) keyInput
                    let ranked = QueryDefaultEquality.firstRows adapter.Alias
                                     { core (Some(DerivedTable(prepared, alias))) (fields input alias) with OrderBy = ordered alias }
                                     key [col alias "__ord", SortDirection.Asc]
                    lower ranked input [] rest
                | Group(selector, element, countOnly, producedType) ->
                    let struct(keyRows, input, keyInput, keyName, equalityName) = prepareGrouping adapter plan.Root input a baseCore selector element
                    let bindings, keyRows = if element.IsSome then keyRows.Ctes, { keyRows with Ctes = [] } else [], keyRows
                    let a = adapter.Alias()
                    let from = Some(DerivedTable(keyRows, a))
                    let input = QueryTranslatorSerialization.scopeSerializationInput (Some a) OuterLayer input
                    let key = col a keyName
                    let keyInput = QueryTranslatorSerialization.scopeSerializationInput (Some a) OuterLayer
                                       { keyInput with Source = ValueSome(QueryTranslatorBaseTypes.jsonToken key) }
                    let items =
                        if countOnly then SqlExpr.AggregateCall(AggregateKind.Count, None, false, None)
                        else
                            let item, _ = QueryProduced.embedded (col a "Value", input)
                            SqlExpr.FunctionCall("jsonb_group_array", [QueryTranslatorBaseTypes.jsonToken item])
                    let keyValue, keyInput = QueryProduced.embedded (key, keyInput)
                    let value = SqlExpr.JsonObjectExpr ["Key", keyValue; (if countOnly then "Value" else "Items"), items]
                    let first = SqlExpr.AggregateCall(AggregateKind.Min, Some(col a "__ord"), false, None)
                    let rowInput = input
                    let itemsType = rowInput.OriginalType.MakeArrayType()
                    let itemsInput = { QuerySerializationInput.Sequence(itemsType, rowInput) with
                                        Operand = ValueSome { Descriptor = QuerySerializationSql.sequenceDescriptor itemsType rowInput; Accesses = Array.empty } }
                    let producedInput = QuerySerializationInput.Members(producedType,
                                            [|"Key"; if countOnly then "Value" else "Items"|],
                                            [|selector.ReturnType; if countOnly then typeof<int> else itemsType|],
                                            [|keyInput; if countOnly then (QueryProduced.typed typeof<int> items).Input else itemsInput|])
                    let groupCore = { core from [proj "Id" (integer -1L); proj "Value" value; proj "__ord" first] with
                                        GroupBy = equalityName |> List.map (col a); OrderBy = [{ Expr = first; Direction = SortDirection.Asc }] }
                    let struct(grouped, output) = QueryProduced.publishCore adapter.Alias groupCore (DirectValue(value, producedInput)) false
                    lower { grouped with Ctes = bindings @ grouped.Ctes } output [] rest
                | While(l,take) ->
                    let pred,joins=translate plan.Root input a l
                    let failed=SqlExpr.CaseExpr((pred,integer 0L),[],Some(integer 1L))
                    let count=SqlExpr.WindowCall {Kind=NamedWindowFunction "SUM";Arguments=[failed];PartitionBy=[];OrderBy=[col a "__ord",SortDirection.Asc]}
                    let marked=select {baseCore with Joins=joins;Projections=ProjectionSetOps.ofList(pass @ [proj "__failed" count])}
                    let wa=adapter.Alias()
                    let ps=fields input wa @ (keys |> List.mapi (fun i _ -> proj ("__key"+string i) (col wa ("__key"+string i))))
                    let condition=SqlExpr.Binary(col wa "__failed",(if take then BinaryOperator.Eq else BinaryOperator.Gt),integer 0L)
                    lower (select {core (Some(DerivedTable(marked,wa))) ps with Where=Some condition;OrderBy=ordered wa}) input keys rest
                | Set(name, right, keySelector) ->
                    let struct(r, rightInput) =
                        match parse adapter.IsRoot right with
                        | Some p -> buildRows adapter FullValue p true
                        | None ->
                            let ra = adapter.Alias()
                            let produced = adapter.Value right
                            let collectionCore = core None [proj "Value" (SqlExpr.Literal SqlLiteral.Null)]
                            let struct(bound, collectionInput) = QueryProduced.publishCore adapter.Alias collectionCore produced false
                            let ownerAlias = adapter.Alias()
                            let collection = col ownerAlias "Value"
                            let collectionInput = QueryTranslatorSerialization.scopeSerializationInput (Some ownerAlias) OuterLayer collectionInput
                            let token = SqlExpr.FunctionCall("jsonb", [SqlExpr.Binary(collection, BinaryOperator.JsonText, col ra "fullkey")])
                            let element = QueryTranslatorSerialization.elementInput collectionInput (col ra "key") token
                            let payload = QueryProduced.scalarValue (sequenceElementType right.Type) token { element with Carrier = CanonicalToken }
                            let id = if hasId (sequenceElementType right.Type) then
                                         SqlExpr.FunctionCall("jsonb_extract", [payload; SqlExpr.Literal(SqlLiteral.String "$.Id")])
                                     else col ra "key"
                            let produced = QueryProduced.canonical adapter.Alias (DirectValue(payload, element))
                            let rightCore =
                                { core (Some(DerivedTable(bound, ownerAlias)))
                                       [proj "Id" id; proj "Value" (SqlExpr.Literal SqlLiteral.Null); proj "__ord" (col ra "key")] with
                                    Joins = [CrossJoin(FromJsonEach(collection, Some ra))]
                                    OrderBy = [{Expr = col ra "key"; Direction = SortDirection.Asc}] }
                            QueryProduced.publishCore adapter.Alias rightCore produced false
                    if name = "Concat" || name = "Union" || name = "UnionBy" then
                        let ra = adapter.Alias()
                        let inputName = "_input" + adapter.Alias()
                        let produced = QueryTranslatorSerialization.fromDescriptor (SqlExpr.Column(None, inputName))
                                           (QueryTranslatorSerialization.mergeInputs (sequenceElementType right.Type) [|input; rightInput|])
                        let arm src fact alias branch =
                            let fact = QueryTranslatorSerialization.scopeSerializationInput (Some alias) BaseLayer fact
                            core (Some(DerivedTable(src, alias)))
                                (payloadFields alias @ [proj "__branch" (integer branch); proj inputName (QuerySerializationSql.inputDescriptor fact)])
                        let union = { Ctes = []; Body = UnionAllSelect(arm source input a 0L, [arm r rightInput ra 1L]) }
                        let ua = adapter.Alias()
                        let merged = select { core (Some(DerivedTable(union, ua)))
                                                [proj "Id" (col ua "Id"); proj "Value" (col ua "Value")
                                                 proj "__ord" (rowNumber [col ua "__branch", SortDirection.Asc; col ua "__ord", SortDirection.Asc]); proj inputName (col ua inputName)]
                                              with OrderBy = [{Expr=col ua "__branch";Direction=SortDirection.Asc};{Expr=col ua "__ord";Direction=SortDirection.Asc}] }
                        let remaining =
                            match name, keySelector with
                            | "UnionBy", Some key -> DistinctBy key :: rest
                            | "Union", _ -> Distinct(sequenceElementType right.Type) :: rest
                            | _ -> rest
                        lower merged produced [] remaining
                    else
                        let keyType = keySelector |> Option.map _.ReturnType |> Option.defaultValue (sequenceElementType right.Type)
                        let produced, joins =
                            match keySelector with
                            | Some selector -> adapter.Translate plan.Root input a selector ValueNone
                            | None -> DirectValue(col a "Value", QueryTranslatorSerialization.scopeSerializationInput (Some a) BaseLayer input), []
                        let keyName = "_set_key" + adapter.Alias()
                        let struct(prepared, keyInput) = QueryProduced.publishSlot adapter.Alias keyName
                                                           { baseCore with Joins = joins; Projections = ProjectionSetOps.ofList(pass @ [proj keyName (SqlExpr.Literal SqlLiteral.Null)]) } produced false
                        let leftAlias = adapter.Alias()
                        let rightAlias = adapter.Alias()
                        let keyInput = QueryTranslatorSerialization.scopeSerializationInput (Some leftAlias) OuterLayer keyInput
                        let equality = QueryDefaultEquality.key adapter.Alias keyType (col leftAlias keyName) keyInput
                        let rightInput = QueryTranslatorSerialization.scopeSerializationInput (Some rightAlias) BaseLayer rightInput
                        let context = if keySelector.IsNone then adapter.MembershipContext else DefaultMembership
                        let struct(left, rightKey) = QueryDefaultEquality.membershipKeys adapter.Alias keyType context (adapter.IsValue right)
                                                        (col leftAlias keyName, keyInput) (col rightAlias "Value", rightInput)
                                                        (fun () -> col rightAlias "Id")
                        let rightSource = core (Some(DerivedTable(r, rightAlias))) [proj "Value" rightKey.Value]
                        let matches =
                            if rest.IsEmpty && not retainFinalRepresentative && hasPureRightLookup r
                               && (QueryDefaultEquality.terms rightKey |> List.forall pureLookupExpression) then
                                QueryDefaultEquality.lookup left rightKey rightSource
                            else QueryDefaultEquality.membership left rightKey rightSource
                        let predicate = if name = "Except" || name = "ExceptBy" then SqlExpr.Unary(UnaryOperator.Not, matches) else matches
                        let filtered = { core (Some(DerivedTable(prepared, leftAlias))) (fields input leftAlias) with Where = Some predicate }
                        let rows =
                            if rest.IsEmpty && not retainFinalRepresentative then select filtered
                            else QueryDefaultEquality.firstRows adapter.Alias filtered equality [col leftAlias "__ord", SortDirection.Asc]
                        lower rows input [] rest
                | Default value ->
                    let fallback = value |> Option.map adapter.Value |> Option.defaultWith (fun () -> QueryProduced.typed input.OriginalType (SqlExpr.Literal SqlLiteral.Null))
                    let fallback = QueryProduced.canonical adapter.Alias fallback
                    let exists = SqlExpr.Exists(select { core from [proj "v" (integer 1L)] with Limit = Some(integer 1L) })
                    let fallbackCore =
                        { core None [proj "Id" (integer -1L); proj "Value" (SqlExpr.Literal SqlLiteral.Null); proj "__ord" (integer 0L)] with
                            Where = Some(SqlExpr.Unary(UnaryOperator.Not, exists)) }
                    let struct(empty, emptyInput) = QueryProduced.publishCore adapter.Alias fallbackCore fallback false
                    let empty =
                        if hasId input.OriginalType then
                            let alias = adapter.Alias()
                            let id = SqlExpr.FunctionCall("jsonb_extract", [col alias "Value"; SqlExpr.Literal(SqlLiteral.String "$.Id")])
                            select (core (Some(DerivedTable(empty, alias)))
                                (proj "Id" id :: (fields emptyInput alias |> List.filter (fun projection -> projection.Alias <> Some "Id"))))
                        else empty
                    let name = "_input" + adapter.Alias()
                    let arm source fact alias =
                        let fact = QueryTranslatorSerialization.scopeSerializationInput (Some alias) BaseLayer fact
                        core (Some(DerivedTable(source, alias))) (payloadFields alias @ [proj name (QuerySerializationSql.inputDescriptor fact)])
                    let produced = QueryTranslatorSerialization.fromDescriptor (SqlExpr.Column(None, name)) (QueryTranslatorSerialization.mergeInputs input.OriginalType [|input; emptyInput|])
                    let emptyAlias = adapter.Alias()
                    lower { Ctes = []; Body = UnionAllSelect(arm source input a, [arm empty emptyInput emptyAlias]) } produced [] rest
        lower start sourceInput [] plan.Stages

    let internal build adapter columns plan = buildRows adapter columns plan true

    /// Existence does not select a representative from the final set operation.
    let internal existenceRows adapter columns plan = buildRows adapter columns plan false
