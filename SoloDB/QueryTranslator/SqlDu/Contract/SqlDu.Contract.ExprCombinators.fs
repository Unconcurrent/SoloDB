namespace SoloDatabase.SqlModel

[<AutoOpen>]
module internal SqlExprCombinators =
    let private same left right = obj.ReferenceEquals(left, right)

    // Walk unchanged lists without allocating; map each element exactly once.
    let mapList (mapper: 'T -> 'T) (values: 'T list) =
        let mutable remaining = values
        let mutable prefixLength = 0
        let mutable changed: ResizeArray<_> = null
        while not remaining.IsEmpty do
            let original = remaining.Head
            let mapped = mapper original
            if isNull changed && not (same original mapped) then
                changed <- ResizeArray()
                let mutable prefix = values
                for _ in 1 .. prefixLength do
                    changed.Add prefix.Head
                    prefix <- prefix.Tail
            if not (isNull changed) then changed.Add mapped
            prefixLength <- prefixLength + 1
            remaining <- remaining.Tail
        if isNull changed then values else List.ofSeq changed

    let mapOption mapper value =
        match value with
        | None -> value
        | Some original ->
            let mapped = mapper original
            if same original mapped then value else Some mapped

    /// Map derived source queries only; expression scopes belong to the caller.
    let mapDerivedSources (mapQuery: SqlSelect -> SqlSelect) (core: SelectCore) =
        let source (original: TableSource) =
            match original with
            | DerivedTable(query, alias) ->
                let mapped = mapQuery query
                if same query mapped then original else DerivedTable(mapped, alias)
            | _ -> original
        let sources = mapOption source core.Source
        let joins = core.Joins |> mapList (fun original ->
            match original with
            | CrossJoin table ->
                let mapped = source table
                if same table mapped then original else CrossJoin mapped
            | ConditionedJoin(kind, table, predicate) ->
                let mapped = source table
                if same table mapped then original else ConditionedJoin(kind, mapped, predicate))
        if same core.Source sources && same core.Joins joins then core
        else { core with Source = sources; Joins = joins }

    /// Keep UNION arms outside a single-core rewrite unless its caller opts in.
    let mapSingleCore mapper body =
        match body with
        | SingleSelect core ->
            let mapped = mapper core
            if same core mapped then body else SingleSelect mapped
        | UnionAllSelect _ -> body

    /// Map each core without merging or redistributing UNION arms.
    let mapCores mapper body =
        match body with
        | SingleSelect _ -> mapSingleCore mapper body
        | UnionAllSelect(head, tail) ->
            let mappedHead = mapper head
            let mappedTail = mapList mapper tail
            if same head mappedHead && same tail mappedTail then body
            else UnionAllSelect(mappedHead, mappedTail)

    /// Preserve body-before-CTE evaluation and unchanged query identity.
    let mapSelectParts mapBody mapQuery (query: SqlSelect) =
        let body = mapBody query.Body
        let ctes = query.Ctes |> mapList (fun cte ->
            let mapped = mapQuery cte.Query
            if same cte.Query mapped then cte else { cte with Query = mapped })
        if same query.Body body && same query.Ctes ctes then query
        else { Ctes = ctes; Body = body }

    /// Maps immediate children; each caller owns recursion and subquery scope.
    let mapChildren (map: SqlExpr -> SqlExpr) (mapSelect: SqlSelect -> SqlSelect) (node: SqlExpr) =
        let valuePair ((key, value) as pair) =
            let mapped = map value
            if same value mapped then pair else key, mapped
        match node with
        | Column _ | Literal _ | Parameter _ | JsonExtractExpr _ | JsonRootExtract _ -> node
        | JsonSetExpr(target, assignments) ->
            let mappedTarget = map target
            let mappedAssignments = mapList valuePair assignments
            if same target mappedTarget && same assignments mappedAssignments then node
            else JsonSetExpr(mappedTarget, mappedAssignments)
        | JsonArrayExpr elements ->
            let mapped = mapList map elements
            if same elements mapped then node else JsonArrayExpr mapped
        | JsonObjectExpr properties ->
            let mapped = mapList valuePair properties
            if same properties mapped then node else JsonObjectExpr mapped
        | FunctionCall(name, arguments) ->
            let mapped = mapList map arguments
            if same arguments mapped then node else FunctionCall(name, mapped)
        | AggregateCall(kind, argument, distinct, separator) ->
            let mappedArgument = mapOption map argument
            let mappedSeparator = mapOption map separator
            if same argument mappedArgument && same separator mappedSeparator then node
            else AggregateCall(kind, mappedArgument, distinct, mappedSeparator)
        | WindowCall spec ->
            let arguments = mapList map spec.Arguments
            let partitions = mapList map spec.PartitionBy
            let ordering = mapList (fun ((expression, direction) as original) ->
                let mapped = map expression
                if same expression mapped then original else mapped, direction) spec.OrderBy
            if same spec.Arguments arguments && same spec.PartitionBy partitions && same spec.OrderBy ordering then node
            else WindowCall { spec with Arguments = arguments; PartitionBy = partitions; OrderBy = ordering }
        | Unary(op, inner) ->
            let mapped = map inner
            if same inner mapped then node else Unary(op, mapped)
        | Binary(left, op, right) ->
            let mappedLeft = map left
            let mappedRight = map right
            if same left mappedLeft && same right mappedRight then node else Binary(mappedLeft, op, mappedRight)
        | Between(value, lower, upper) ->
            let mappedValue = map value
            let mappedLower = map lower
            let mappedUpper = map upper
            if same value mappedValue && same lower mappedLower && same upper mappedUpper then node
            else Between(mappedValue, mappedLower, mappedUpper)
        | InList(value, head, tail) ->
            let mappedValue = map value
            let mappedHead = map head
            let mappedTail = mapList map tail
            if same value mappedValue && same head mappedHead && same tail mappedTail then node
            else InList(mappedValue, mappedHead, mappedTail)
        | InSubquery(value, query) ->
            let mappedValue = map value
            let mappedQuery = mapSelect query
            if same value mappedValue && same query mappedQuery then node else InSubquery(mappedValue, mappedQuery)
        | Cast(inner, sqlType) ->
            let mapped = map inner
            if same inner mapped then node else Cast(mapped, sqlType)
        | Coalesce(head, tail) ->
            let mappedHead = map head
            let mappedTail = mapList map tail
            if same head mappedHead && same tail mappedTail then node else Coalesce(mappedHead, mappedTail)
        | Exists query ->
            let mapped = mapSelect query
            if same query mapped then node else Exists mapped
        | ScalarSubquery query ->
            let mapped = mapSelect query
            if same query mapped then node else ScalarSubquery mapped
        | CaseExpr(first, rest, otherwise) ->
            let branch ((condition, value) as pair) =
                let mappedCondition = map condition
                let mappedValue = map value
                if same condition mappedCondition && same value mappedValue then pair
                else mappedCondition, mappedValue
            let mappedFirst = branch first
            let mappedRest = mapList branch rest
            let mappedOtherwise = mapOption map otherwise
            if same first mappedFirst && same rest mappedRest && same otherwise mappedOtherwise then node
            else CaseExpr(mappedFirst, mappedRest, mappedOtherwise)

    type SqlExpr with
        static member fold (folder: 'State -> SqlExpr -> 'State) (state: 'State) (expr: SqlExpr) : 'State =
            let rec loop (acc: 'State) (node: SqlExpr) : 'State =
                let acc = folder acc node
                match node with
                | Column _ -> acc
                | Literal _ -> acc
                | Parameter _ -> acc
                | JsonExtractExpr _ -> acc
                | JsonRootExtract _ -> acc
                | JsonSetExpr(target, assignments) ->
                    let acc = loop acc target
                    assignments |> List.fold (fun s (_, value) -> loop s value) acc
                | JsonArrayExpr(elements) ->
                    elements |> List.fold loop acc
                | JsonObjectExpr(properties) ->
                    properties |> List.fold (fun s (_, value) -> loop s value) acc
                | FunctionCall(_, arguments) ->
                    arguments |> List.fold loop acc
                | AggregateCall(_, argument, _, separator) ->
                    let acc =
                        match argument with
                        | Some arg -> loop acc arg
                        | None -> acc
                    match separator with
                    | Some sep -> loop acc sep
                    | None -> acc
                | WindowCall(spec) ->
                    let acc = spec.Arguments |> List.fold loop acc
                    let acc = spec.PartitionBy |> List.fold loop acc
                    spec.OrderBy |> List.fold (fun s (orderExpr, _) -> loop s orderExpr) acc
                | Unary(_, inner) ->
                    loop acc inner
                | Binary(left, _, right) ->
                    let acc = loop acc left
                    loop acc right
                | Between(valueExpr, lower, upper) ->
                    let acc = loop acc valueExpr
                    let acc = loop acc lower
                    loop acc upper
                | InList(valueExpr, head, tail) ->
                    let acc = loop acc valueExpr
                    let acc = loop acc head
                    tail |> List.fold loop acc
                | InSubquery(valueExpr, _) ->
                    loop acc valueExpr
                | Cast(inner, _) ->
                    loop acc inner
                | Coalesce(head, tail) ->
                    let acc = loop acc head
                    tail |> List.fold loop acc
                | Exists _ ->
                    acc
                | ScalarSubquery _ ->
                    acc
                | CaseExpr(firstBranch, restBranches, elseExpr) ->
                    let acc =
                        (firstBranch :: restBranches)
                        |> List.fold (fun s (condExpr, resultExpr) ->
                            let s = loop s condExpr
                            loop s resultExpr) acc
                    match elseExpr with
                    | Some elseNode -> loop acc elseNode
                    | None -> acc
            loop state expr
        static member map (mapper: SqlExpr -> SqlExpr) (expr: SqlExpr) : SqlExpr =
            let rec loop node = mapper (mapChildren loop id node)
            loop expr
        static member exists (predicate: SqlExpr -> bool) (expr: SqlExpr) : bool =
            let rec loop (node: SqlExpr) : bool =
                if predicate node then true
                else
                    match node with
                    | Column _ -> false
                    | Literal _ -> false
                    | Parameter _ -> false
                    | JsonExtractExpr _ -> false
                    | JsonRootExtract _ -> false
                    | JsonSetExpr(target, assignments) ->
                        loop target || (assignments |> List.exists (fun (_, value) -> loop value))
                    | JsonArrayExpr(elements) ->
                        elements |> List.exists loop
                    | JsonObjectExpr(properties) ->
                        properties |> List.exists (fun (_, value) -> loop value)
                    | FunctionCall(_, arguments) ->
                        arguments |> List.exists loop
                    | AggregateCall(_, argument, _, separator) ->
                        (argument |> Option.map loop |> Option.defaultValue false)
                        || (separator |> Option.map loop |> Option.defaultValue false)
                    | WindowCall(spec) ->
                        (spec.Arguments |> List.exists loop)
                        || (spec.PartitionBy |> List.exists loop)
                        || (spec.OrderBy |> List.exists (fun (orderExpr, _) -> loop orderExpr))
                    | Unary(_, inner) ->
                        loop inner
                    | Binary(left, _, right) ->
                        loop left || loop right
                    | Between(valueExpr, lower, upper) ->
                        loop valueExpr || loop lower || loop upper
                    | InList(valueExpr, head, tail) ->
                        loop valueExpr || loop head || (tail |> List.exists loop)
                    | InSubquery(valueExpr, _) ->
                        loop valueExpr
                    | Cast(inner, _) ->
                        loop inner
                    | Coalesce(head, tail) ->
                        loop head || (tail |> List.exists loop)
                    | Exists _ ->
                        false
                    | ScalarSubquery _ ->
                        false
                    | CaseExpr(firstBranch, restBranches, elseExpr) ->
                        ((firstBranch :: restBranches) |> List.exists (fun (condExpr, resultExpr) -> loop condExpr || loop resultExpr))
                        || (elseExpr |> Option.map loop |> Option.defaultValue false)
            loop expr
        static member tryMap (mapper: SqlExpr -> SqlExpr option) (expr: SqlExpr) : SqlExpr option =
            let mutable changed = false
            let rec loop node =
                let mapped = mapChildren loop id node
                match mapper mapped with
                | Some replacement ->
                    // Some is an explicit rewrite signal, even for the same object.
                    changed <- true
                    replacement
                | None -> mapped
            let result = loop expr
            if changed then Some result else None
