module SoloDatabase.EmitWindow

open SqlDu.Engine.C1.Spec

/// Emit a window function call: func(args) OVER (PARTITION BY ... ORDER BY ...)
let emitWindowCall (ctx: EmitContext) (emitExprFn: EmitContext -> SqlExpr -> Emitted) (spec: WindowSpec) : Emitted =
    let funcName =
        match spec.Kind with
        | RowNumber -> "ROW_NUMBER"
        | DenseRank -> "DENSE_RANK"
        | Rank -> "RANK"
        | NamedWindowFunction name -> name

    let argsEmitted = spec.Arguments |> List.map (emitExprFn ctx)
    let argsSql = argsEmitted |> List.map (fun e -> e.Sql) |> String.concat ", "
    let argsParams = Emitted.collectParameters argsEmitted

    let partitionParts = spec.PartitionBy |> List.map (emitExprFn ctx)
    let partitionSql =
        match partitionParts with
        | [] -> ""
        | parts ->
            let sql = parts |> List.map (fun e -> e.Sql) |> String.concat ", "
            sprintf "PARTITION BY %s" sql
    let partitionParams = Emitted.collectParameters partitionParts

    let orderParts =
        spec.OrderBy
        |> List.map (fun (expr, dir) ->
            let e = emitExprFn ctx expr
            let dirStr = match dir with Asc -> "ASC" | Desc -> "DESC"
            { Sql = sprintf "%s %s" e.Sql dirStr; Parameters = Emitted.copyParameters e.Parameters })
    let orderSql =
        match orderParts with
        | [] -> ""
        | parts ->
            let sql = parts |> List.map (fun e -> e.Sql) |> String.concat ", "
            sprintf "ORDER BY %s" sql
    let orderParams = Emitted.collectParameters orderParts

    let overParts =
        [ partitionSql; orderSql ]
        |> List.filter (fun s -> s <> "")
        |> String.concat " "

    let allParams = Emitted.concatParameterSets [ argsParams; partitionParams; orderParams ]
    { Sql = sprintf "%s(%s) OVER (%s)" funcName argsSql overParts
      Parameters = allParams }
