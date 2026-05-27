module internal SoloDatabase.EmitExpr

open SqlDu.Engine.C1.Spec

/// Emit a binary operator as SQL text.
let private emitBinaryOp (op: BinaryOperator) : string =
    match op with
    | Eq -> "="
    | Ne -> "<>"
    | Lt -> "<"
    | Le -> "<="
    | Gt -> ">"
    | Ge -> ">="
    | And -> "AND"
    | Or -> "OR"
    | Add -> "+"
    | Sub -> "-"
    | Mul -> "*"
    | Div -> "/"
    | Mod -> "%"
    | Like -> "LIKE"
    | Glob -> "GLOB"
    | Regexp -> "REGEXP"
    | Is -> "IS"
    | IsNot -> "IS NOT"
    | In -> "IN"
    | NotInOp -> "NOT IN"
    | BitwiseAnd -> "&"

/// Emit a SQL literal value.
/// When ctx.InlineLiterals is true, Integer/Float/String are emitted inline (product behavior).
/// When false, they are parameterized (spec behavior).
/// NULL and Boolean are always emitted inline regardless of the flag.
let private emitLiteral (ctx: EmitContext) (lit: SqlLiteral) : Emitted =
    match lit with
    | SqlLiteral.Null -> { Sql = "NULL"; Parameters = Emitted.emptyParameters () }
    | SqlLiteral.Boolean true -> { Sql = "1"; Parameters = Emitted.emptyParameters () }
    | SqlLiteral.Boolean false -> { Sql = "0"; Parameters = Emitted.emptyParameters () }
    | SqlLiteral.Integer v ->
        if ctx.InlineLiterals then
            { Sql = string v; Parameters = Emitted.emptyParameters () }
        else
            ctx.AllocParam(box v)
    | SqlLiteral.Float v ->
        if ctx.InlineLiterals then
            { Sql = sprintf "%.17g" v; Parameters = Emitted.emptyParameters () }
        else
            ctx.AllocParam(box v)
    | SqlLiteral.String v ->
        if ctx.InlineLiterals then
            { Sql = sprintf "'%s'" (EmitJson.escapeSQLiteStringLiteral v); Parameters = Emitted.emptyParameters () }
        else
            ctx.AllocParam(box v)
    | SqlLiteral.Blob v -> ctx.AllocParam(box v)

/// Emit an aggregate function kind as SQL function name.
let private emitAggregateKind (kind: AggregateKind) : string =
    match kind with
    | Count -> "COUNT"
    | Sum -> "SUM"
    | Avg -> "AVG"
    | Min -> "MIN"
    | Max -> "MAX"
    | GroupConcat -> "GROUP_CONCAT"
    | JsonGroupArray -> "jsonb_group_array"

/// Emit a SqlExpr to SQL text with parameters.
/// Exhaustive pattern match over all 21 SqlExpr cases (including typed UpdateFragment).
/// The emitSubSelect parameter resolves the circular dependency between
/// expression and select emission — EmitSelect passes itself here.
let rec emitExprWith (emitSubSelect: EmitContext -> SqlSelect -> Emitted) (ctx: EmitContext) (expr: SqlExpr) : Emitted =
    let emitE ctx expr = emitExprWith emitSubSelect ctx expr

    match expr with
    // Case 1: Column reference
    | Column(sourceAlias, column) ->
        if column = "*" then
            match sourceAlias with
            | Some alias -> { Sql = sprintf "%s.*" (EmitJson.quoteIdentifier ctx alias); Parameters = Emitted.emptyParameters () }
            | None -> { Sql = "*"; Parameters = Emitted.emptyParameters () }
        else
            match sourceAlias with
            | Some alias ->
                { Sql = sprintf "%s.%s" (EmitJson.quoteIdentifier ctx alias) (EmitJson.quoteIdentifier ctx column)
                  Parameters = Emitted.emptyParameters () }
            | None ->
                { Sql = EmitJson.quoteIdentifier ctx column
                  Parameters = Emitted.emptyParameters () }

    // Case 2: Literal value
    | Literal lit ->
        emitLiteral ctx lit

    // Case 3: Named parameter reference
    | Parameter name ->
        { Sql = sprintf "@%s" name; Parameters = Emitted.emptyParameters () }

    // Case 4: JSON extract expression — jsonb_extract(source, '$.path')
    | JsonExtractExpr(sourceAlias, column, jsonPath) ->
        EmitJson.emitJsonExtract ctx "jsonb_extract" sourceAlias column jsonPath

    // Case 4b: Root JSON extraction — jsonb_extract(source, '$')
    | JsonRootExtract(sourceAlias, column) ->
        let src =
            match sourceAlias with
            | Some alias -> sprintf "%s.%s" (EmitJson.quoteIdentifier ctx alias) (EmitJson.quoteIdentifier ctx column)
            | None -> EmitJson.quoteIdentifier ctx column
        { Sql = sprintf "jsonb_extract(%s, '$')" src
          Parameters = Emitted.emptyParameters () }

    // Case 5: JSON set expression — jsonb_set(target, path, value)
    | JsonSetExpr(target, assignments) ->
        EmitJson.emitJsonSet ctx emitE target assignments

    // Case 6: JSON array expression — json_array(e1, e2, ...)
    | JsonArrayExpr elements ->
        EmitJson.emitJsonArray ctx emitE elements

    // Case 7: JSON object expression — jsonb_object('k1', v1, 'k2', v2, ...)
    | JsonObjectExpr properties ->
        EmitJson.emitJsonObject ctx emitE properties

    // Case 8: Function call — name(arg1, arg2, ...)
    | FunctionCall(name, arguments) ->
        let argsEmitted = arguments |> List.map (emitE ctx)
        let argsSql = argsEmitted |> List.map (fun e -> e.Sql) |> String.concat ", "
        let parms = Emitted.collectParameters argsEmitted
        { Sql = sprintf "%s(%s)" name argsSql; Parameters = parms }

    // Case 9: Aggregate call
    | AggregateCall(kind, argument, distinct, separator) ->
        let funcName = emitAggregateKind kind
        match kind, argument, distinct, separator with
        | Count, None, false, None ->
            { Sql = sprintf "%s(*)" funcName; Parameters = Emitted.emptyParameters () }
        | Count, None, _, _ ->
            raise (System.NotSupportedException("COUNT with no argument must emit COUNT(*) only."))
        | _, Some argExpr, _, _ ->
            let argEmitted = emitE ctx argExpr
            let distinctStr = if distinct then "DISTINCT " else ""
            match separator with
            | Some sepExpr ->
                let sepEmitted = emitE ctx sepExpr
                { Sql = sprintf "%s(%s%s, %s)" funcName distinctStr argEmitted.Sql sepEmitted.Sql
                  Parameters = Emitted.concatParameterSets [ argEmitted.Parameters; sepEmitted.Parameters ] }
            | None ->
                { Sql = sprintf "%s(%s%s)" funcName distinctStr argEmitted.Sql
                  Parameters = argEmitted.Parameters }
        | _, None, _, _ ->
            raise (System.NotSupportedException($"Aggregate '{funcName}' requires an argument."))

    // Case 10: Window call
    | WindowCall spec ->
        EmitWindow.emitWindowCall ctx emitE spec

    // Case 11: Unary operator
    | Unary(op, operand) ->
        let operandEmitted = emitE ctx operand
        match op with
        | Not ->
            { Sql = sprintf "NOT (%s)" operandEmitted.Sql
              Parameters = operandEmitted.Parameters }
        | Neg ->
            { Sql = sprintf "-%s" operandEmitted.Sql
              Parameters = operandEmitted.Parameters }
        | IsNull ->
            { Sql = sprintf "%s IS NULL" operandEmitted.Sql
              Parameters = operandEmitted.Parameters }
        | IsNotNull ->
            { Sql = sprintf "%s IS NOT NULL" operandEmitted.Sql
              Parameters = operandEmitted.Parameters }

    // Case 12: Binary operator — handles precedence via parenthesization
    | Binary(left, op, right) ->
        let leftEmitted = emitE ctx left
        let rightEmitted = emitE ctx right
        let opStr = emitBinaryOp op
        let leftSql =
            match left with
            | Binary(_, Or, _) when op = And -> sprintf "(%s)" leftEmitted.Sql
            | _ -> leftEmitted.Sql
        let rightSql =
            match right with
            | Binary(_, Or, _) when op = And -> sprintf "(%s)" rightEmitted.Sql
            | _ -> rightEmitted.Sql
        { Sql = sprintf "(%s %s %s)" leftSql opStr rightSql
          Parameters = Emitted.concatParameterSets [ leftEmitted.Parameters; rightEmitted.Parameters ] }

    // Case 13: BETWEEN
    | Between(expr, lower, upper) ->
        let exprEmitted = emitE ctx expr
        let lowerEmitted = emitE ctx lower
        let upperEmitted = emitE ctx upper
        { Sql = sprintf "%s BETWEEN %s AND %s" exprEmitted.Sql lowerEmitted.Sql upperEmitted.Sql
          Parameters = Emitted.concatParameterSets [ exprEmitted.Parameters; lowerEmitted.Parameters; upperEmitted.Parameters ] }

    // Case 14: IN list
    | InList(expr, head, tail) ->
        let exprEmitted = emitE ctx expr
        let valuesEmitted = (head :: tail) |> List.map (emitE ctx)
        let valuesSql = valuesEmitted |> List.map (fun e -> e.Sql) |> String.concat ", "
        let parms = Emitted.collectParameters valuesEmitted
        { Sql = sprintf "%s IN (%s)" exprEmitted.Sql valuesSql
          Parameters = Emitted.concatParameterSets [ exprEmitted.Parameters; parms ] }

    // Case 15: IN subquery
    | InSubquery(expr, subSelect) ->
        let exprEmitted = emitE ctx expr
        let subEmitted = emitSubSelect ctx subSelect
        { Sql = sprintf "%s IN (%s)" exprEmitted.Sql subEmitted.Sql
          Parameters = Emitted.concatParameterSets [ exprEmitted.Parameters; subEmitted.Parameters ] }

    // Case 16: CAST
    | Cast(expr, sqlType) ->
        let exprEmitted = emitE ctx expr
        { Sql = sprintf "CAST(%s AS %s)" exprEmitted.Sql sqlType
          Parameters = exprEmitted.Parameters }

    // Case 17: COALESCE
    | Coalesce(head, tail) ->
        let partsEmitted = (head :: tail) |> List.map (emitE ctx)
        let sql = partsEmitted |> List.map (fun e -> e.Sql) |> String.concat ", "
        let parms = Emitted.collectParameters partsEmitted
        { Sql = sprintf "COALESCE(%s)" sql; Parameters = parms }

    // Case 18: EXISTS
    | Exists subSelect ->
        let subEmitted = emitSubSelect ctx subSelect
        { Sql = sprintf "EXISTS (%s)" subEmitted.Sql
          Parameters = subEmitted.Parameters }

    // Case 19: Scalar subquery
    | ScalarSubquery subSelect ->
        let subEmitted = emitSubSelect ctx subSelect
        { Sql = sprintf "(%s)" subEmitted.Sql
          Parameters = subEmitted.Parameters }

    // Case 20: CASE expression
    | CaseExpr(firstBranch, restBranches, elseExpr) ->
        let branchParts =
            (firstBranch :: restBranches)
            |> List.map (fun (cond, result) ->
                let condEmitted = emitE ctx cond
                let resultEmitted = emitE ctx result
                { Sql = sprintf "WHEN %s THEN %s" condEmitted.Sql resultEmitted.Sql
                  Parameters = Emitted.concatParameterSets [ condEmitted.Parameters; resultEmitted.Parameters ] })
        let branchSql = branchParts |> List.map (fun e -> e.Sql) |> String.concat " "
        let branchParams = Emitted.collectParameters branchParts
        match elseExpr with
        | Some elseE ->
            let elseEmitted = emitE ctx elseE
            { Sql = sprintf "CASE %s ELSE %s END" branchSql elseEmitted.Sql
              Parameters = Emitted.concatParameterSets [ branchParams; elseEmitted.Parameters ] }
        | None ->
            { Sql = sprintf "CASE %s END" branchSql
              Parameters = branchParams }

    // Case 21: Update fragment — typed path/value pair for jsonb_set arguments
    // Emits "path,value," format consumed by SoloDB.fs jsonb_set wrapper
    | UpdateFragment(path, value) ->
        let pathE = emitE ctx path
        let valueE = emitE ctx value
        { Sql = sprintf "%s,%s," pathE.Sql valueE.Sql
          Parameters = Emitted.concatParameterSets [ pathE.Parameters; valueE.Parameters ] }
