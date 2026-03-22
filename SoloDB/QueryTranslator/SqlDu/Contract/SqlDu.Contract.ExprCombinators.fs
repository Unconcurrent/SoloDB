namespace SqlDu.Engine.C1.Spec

[<AutoOpen>]
module internal SqlExprCombinators =
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
            | UpdateFragment(pathExpr, valueExpr) ->
                let acc = loop acc pathExpr
                loop acc valueExpr
        loop state expr
    static member map (mapper: SqlExpr -> SqlExpr) (expr: SqlExpr) : SqlExpr =
        let rec loop (node: SqlExpr) : SqlExpr =
            let mappedNode =
                match node with
                | Column _ -> node
                | Literal _ -> node
                | Parameter _ -> node
                | JsonExtractExpr _ -> node
                | JsonRootExtract _ -> node
                | JsonSetExpr(target, assignments) ->
                    JsonSetExpr(
                        loop target,
                        assignments |> List.map (fun (path, value) -> path, loop value))
                | JsonArrayExpr(elements) ->
                    JsonArrayExpr(elements |> List.map loop)
                | JsonObjectExpr(properties) ->
                    JsonObjectExpr(properties |> List.map (fun (key, value) -> key, loop value))
                | FunctionCall(name, arguments) ->
                    FunctionCall(name, arguments |> List.map loop)
                | AggregateCall(kind, argument, distinct, separator) ->
                    AggregateCall(
                        kind,
                        argument |> Option.map loop,
                        distinct,
                        separator |> Option.map loop)
                | WindowCall(spec) ->
                    WindowCall({
                        spec with
                            Arguments = spec.Arguments |> List.map loop
                            PartitionBy = spec.PartitionBy |> List.map loop
                            OrderBy = spec.OrderBy |> List.map (fun (orderExpr, dir) -> loop orderExpr, dir)
                    })
                | Unary(op, inner) ->
                    Unary(op, loop inner)
                | Binary(left, op, right) ->
                    Binary(loop left, op, loop right)
                | Between(valueExpr, lower, upper) ->
                    Between(loop valueExpr, loop lower, loop upper)
                | InList(valueExpr, head, tail) ->
                    InList(loop valueExpr, loop head, tail |> List.map loop)
                | InSubquery(valueExpr, query) ->
                    InSubquery(loop valueExpr, query)
                | Cast(inner, sqlType) ->
                    Cast(loop inner, sqlType)
                | Coalesce(head, tail) ->
                    Coalesce(loop head, tail |> List.map loop)
                | Exists query ->
                    Exists query
                | ScalarSubquery query ->
                    ScalarSubquery query
                | CaseExpr(firstBranch, restBranches, elseExpr) ->
                    CaseExpr(
                        let mapBranch (condExpr, resultExpr) = loop condExpr, loop resultExpr
                        mapBranch firstBranch,
                        restBranches |> List.map mapBranch,
                        elseExpr |> Option.map loop)
                | UpdateFragment(pathExpr, valueExpr) ->
                    UpdateFragment(loop pathExpr, loop valueExpr)
            mapper mappedNode
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
                | UpdateFragment(pathExpr, valueExpr) ->
                    loop pathExpr || loop valueExpr
        loop expr
    static member tryMap (mapper: SqlExpr -> SqlExpr option) (expr: SqlExpr) : SqlExpr option =
        let rec loop (node: SqlExpr) : SqlExpr option =
            let rebuiltNode, childChanged =
                match node with
                | Column _ -> node, false
                | Literal _ -> node, false
                | Parameter _ -> node, false
                | JsonExtractExpr _ -> node, false
                | JsonRootExtract _ -> node, false
                | JsonSetExpr(target, assignments) ->
                    let newTargetOpt = loop target
                    let newTarget = newTargetOpt |> Option.defaultValue target
                    let mutable changed = newTargetOpt.IsSome
                    let newAssignments =
                        assignments
                        |> List.map (fun (path, value) ->
                            let rewritten = loop value
                            if rewritten.IsSome then changed <- true
                            path, (rewritten |> Option.defaultValue value))
                    JsonSetExpr(newTarget, newAssignments), changed
                | JsonArrayExpr(elements) ->
                    let mutable changed = false
                    let newElements =
                        elements
                        |> List.map (fun element ->
                            let rewritten = loop element
                            if rewritten.IsSome then changed <- true
                            rewritten |> Option.defaultValue element)
                    JsonArrayExpr(newElements), changed
                | JsonObjectExpr(properties) ->
                    let mutable changed = false
                    let newProperties =
                        properties
                        |> List.map (fun (key, value) ->
                            let rewritten = loop value
                            if rewritten.IsSome then changed <- true
                            key, (rewritten |> Option.defaultValue value))
                    JsonObjectExpr(newProperties), changed
                | FunctionCall(name, arguments) ->
                    let mutable changed = false
                    let newArgs =
                        arguments
                        |> List.map (fun argument ->
                            let rewritten = loop argument
                            if rewritten.IsSome then changed <- true
                            rewritten |> Option.defaultValue argument)
                    FunctionCall(name, newArgs), changed
                | AggregateCall(kind, argument, distinct, separator) ->
                    let newArgument =
                        argument |> Option.bind loop
                    let newSeparator =
                        separator |> Option.bind loop
                    let changed = newArgument.IsSome || newSeparator.IsSome
                    let argumentValue =
                        match newArgument, argument with
                        | Some arg, _ -> Some arg
                        | None, original -> original
                    let separatorValue =
                        match newSeparator, separator with
                        | Some sep, _ -> Some sep
                        | None, original -> original
                    AggregateCall(
                        kind,
                        argumentValue,
                        distinct,
                        separatorValue), changed
                | WindowCall(spec) ->
                    let mutable changed = false
                    let newArguments =
                        spec.Arguments |> List.map (fun arg ->
                            let rewritten = loop arg
                            if rewritten.IsSome then changed <- true
                            rewritten |> Option.defaultValue arg)
                    let newPartitionBy =
                        spec.PartitionBy |> List.map (fun part ->
                            let rewritten = loop part
                            if rewritten.IsSome then changed <- true
                            rewritten |> Option.defaultValue part)
                    let newOrderBy =
                        spec.OrderBy |> List.map (fun (orderExpr, dir) ->
                            let rewritten = loop orderExpr
                            if rewritten.IsSome then changed <- true
                            (rewritten |> Option.defaultValue orderExpr), dir)
                    WindowCall({
                        spec with
                            Arguments = newArguments
                            PartitionBy = newPartitionBy
                            OrderBy = newOrderBy
                    }), changed
                | Unary(op, inner) ->
                    let innerOpt = loop inner
                    Unary(op, innerOpt |> Option.defaultValue inner), innerOpt.IsSome
                | Binary(left, op, right) ->
                    let leftOpt = loop left
                    let rightOpt = loop right
                    Binary(leftOpt |> Option.defaultValue left, op, rightOpt |> Option.defaultValue right), (leftOpt.IsSome || rightOpt.IsSome)
                | Between(valueExpr, lower, upper) ->
                    let valueOpt = loop valueExpr
                    let lowerOpt = loop lower
                    let upperOpt = loop upper
                    Between(
                        valueOpt |> Option.defaultValue valueExpr,
                        lowerOpt |> Option.defaultValue lower,
                        upperOpt |> Option.defaultValue upper), (valueOpt.IsSome || lowerOpt.IsSome || upperOpt.IsSome)
                | InList(valueExpr, head, tail) ->
                    let valueOpt = loop valueExpr
                    let headOpt = loop head
                    let mutable changed = valueOpt.IsSome || headOpt.IsSome
                    let newTail =
                        tail |> List.map (fun value ->
                            let rewritten = loop value
                            if rewritten.IsSome then changed <- true
                            rewritten |> Option.defaultValue value)
                    InList(
                        valueOpt |> Option.defaultValue valueExpr,
                        headOpt |> Option.defaultValue head,
                        newTail), changed
                | InSubquery(valueExpr, query) ->
                    let valueOpt = loop valueExpr
                    InSubquery(valueOpt |> Option.defaultValue valueExpr, query), valueOpt.IsSome
                | Cast(inner, sqlType) ->
                    let innerOpt = loop inner
                    Cast(innerOpt |> Option.defaultValue inner, sqlType), innerOpt.IsSome
                | Coalesce(head, tail) ->
                    let headOpt = loop head
                    let mutable changed = headOpt.IsSome
                    let newTail =
                        tail |> List.map (fun value ->
                            let rewritten = loop value
                            if rewritten.IsSome then changed <- true
                            rewritten |> Option.defaultValue value)
                    Coalesce(headOpt |> Option.defaultValue head, newTail), changed
                | Exists query ->
                    Exists query, false
                | ScalarSubquery query ->
                    ScalarSubquery query, false
                | CaseExpr(firstBranch, restBranches, elseExpr) ->
                    let rewriteBranch (condExpr, resultExpr) =
                            let condOpt = loop condExpr
                            let resultOpt = loop resultExpr
                            let changed = condOpt.IsSome || resultOpt.IsSome
                            (condOpt |> Option.defaultValue condExpr), (resultOpt |> Option.defaultValue resultExpr), changed
                    let firstCond, firstResult, firstChanged = rewriteBranch firstBranch
                    let mutable changed = firstChanged
                    let newRest =
                        restBranches |> List.map (fun branch ->
                            let condExpr, resultExpr, branchChanged = rewriteBranch branch
                            if branchChanged then changed <- true
                            (condExpr, resultExpr))
                    let newElseOpt =
                        elseExpr |> Option.bind loop
                    if newElseOpt.IsSome then changed <- true
                    CaseExpr(
                        (firstCond, firstResult),
                        newRest,
                        match newElseOpt, elseExpr with | Some e, _ -> Some e | None, original -> original), changed
                | UpdateFragment(pathExpr, valueExpr) ->
                    let pathOpt = loop pathExpr
                    let valueOpt = loop valueExpr
                    UpdateFragment(pathOpt |> Option.defaultValue pathExpr, valueOpt |> Option.defaultValue valueExpr), (pathOpt.IsSome || valueOpt.IsSome)
            match mapper rebuiltNode with
            | Some rewritten -> Some rewritten
            | None when childChanged -> Some rebuiltNode
            | None -> None
        loop expr
