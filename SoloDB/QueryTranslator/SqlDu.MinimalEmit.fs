namespace SoloDatabase

open SqlDu.Engine.C1.Spec
open SoloDatabase.QueryTranslatorBaseTypes

/// Minimal DU-to-SQL emitter for B1b adapter seam.
/// Converts SqlExpr DU values to SQL text via QueryBuilder's StringBuilder.
/// Temporary: replaced by full C2 emitter in Batch 4.
module internal SqlDuMinimalEmit =
    let rec emitExpr (qb: QueryBuilder) (expr: SqlExpr) : unit =
        match expr with
        | SqlExpr.Column(alias, col) ->
            match alias with
            | Some a ->
                qb.AppendRaw a
                qb.AppendRaw "."
            | None -> ()
            qb.AppendRaw col

        | SqlExpr.Literal lit ->
            match lit with
            | SqlLiteral.Null -> qb.AppendRaw "NULL"
            | SqlLiteral.Integer n -> qb.AppendRaw (string n)
            | SqlLiteral.Float f -> qb.AppendRaw (sprintf "%g" f)
            | SqlLiteral.String s ->
                qb.AppendRaw "'"
                qb.AppendRaw (escapeSQLiteString s)
                qb.AppendRaw "'"
            | SqlLiteral.Boolean b -> qb.AppendRaw (if b then "1" else "0")
            | SqlLiteral.Blob bytes -> qb.AppendVariableBoxed(box bytes)

        | SqlExpr.Parameter name ->
            qb.AppendRaw "@"
            qb.AppendRaw name

        | SqlExpr.JsonExtractExpr(alias, col, JsonPath pathParts) ->
            let prefix =
                match alias with
                | Some a -> a + "."
                | None -> ""
            match pathParts with
            | [] ->
                // Root extraction: use json_extract(json(...)) at top-level for readable output,
                // jsonb_extract(...) otherwise. Preserves legacy SB.Length=0 check.
                if qb.StringBuilder.Length = 0 then
                    qb.AppendRaw (sprintf "json_extract(json(%s%s), '$')" prefix col)
                else
                    qb.AppendRaw (sprintf "jsonb_extract(%s%s, '$')" prefix col)
            | parts ->
                let path = (System.String.Join(".", parts |> List.toArray)).Replace(".[", "[")
                let escapedPath = escapeSQLiteString path
                qb.AppendRaw (sprintf "jsonb_extract(%s%s, '$.%s')" prefix col escapedPath)

        | SqlExpr.JsonSetExpr(_target, _assignments) ->
            // UpdateMode path — not yet converted to DU emission.
            // This case should not be reached in B1b (UpdateMode uses legacy SB path).
            raise (System.NotSupportedException "JsonSetExpr emission not implemented in B1b minimal emitter")

        | SqlExpr.JsonArrayExpr elements ->
            qb.AppendRaw "json_array("
            let mutable first = true
            for elem in elements do
                if not first then qb.AppendRaw ","
                first <- false
                emitExpr qb elem
            qb.AppendRaw ")"

        | SqlExpr.JsonObjectExpr properties ->
            qb.AppendRaw "jsonb_object("
            let mutable first = true
            for (name, value) in properties do
                if not first then qb.AppendRaw ","
                first <- false
                qb.AppendRaw "'"
                qb.AppendRaw (escapeSQLiteString name)
                qb.AppendRaw "',"
                emitExpr qb value
            qb.AppendRaw ")"

        | SqlExpr.FunctionCall(name, args) ->
            // Special case: __raw__ escape hatch for handler-captured SQL
            if name = "__raw__" then
                match args with
                | [SqlExpr.Literal(SqlLiteral.String rawSql)] -> qb.AppendRaw rawSql
                | _ -> ()
            // Special case: __update_fragment__ for UpdateMode path/value pairs (Batch 3)
            elif name = "__update_fragment__" then
                match args with
                | [path; value] ->
                    emitExpr qb path
                    qb.AppendRaw ","
                    emitExpr qb value
                    qb.AppendRaw ","
                | _ -> ()
            else
                qb.AppendRaw name
                qb.AppendRaw "("
                let mutable first = true
                for arg in args do
                    if not first then qb.AppendRaw ", "
                    first <- false
                    emitExpr qb arg
                qb.AppendRaw ")"

        | SqlExpr.AggregateCall(kind, argument, distinct, separator) ->
            let name =
                match kind with
                | AggregateKind.Count -> "COUNT"
                | AggregateKind.Sum -> "SUM"
                | AggregateKind.Avg -> "AVG"
                | AggregateKind.Min -> "MIN"
                | AggregateKind.Max -> "MAX"
                | AggregateKind.GroupConcat -> "GROUP_CONCAT"
                | AggregateKind.JsonGroupArray -> "JSON_GROUP_ARRAY"
            qb.AppendRaw name
            qb.AppendRaw "("
            if distinct then qb.AppendRaw "DISTINCT "
            match argument with
            | Some arg -> emitExpr qb arg
            | None -> qb.AppendRaw "*"
            match separator with
            | Some sep -> qb.AppendRaw ", "; emitExpr qb sep
            | None -> ()
            qb.AppendRaw ")"

        | SqlExpr.WindowCall spec ->
            // Emit: FUNC(args) OVER (PARTITION BY ... ORDER BY ...)
            match spec.Kind with
            | WindowFunctionKind.RowNumber -> qb.AppendRaw "ROW_NUMBER"
            | WindowFunctionKind.DenseRank -> qb.AppendRaw "DENSE_RANK"
            | WindowFunctionKind.Rank -> qb.AppendRaw "RANK"
            | WindowFunctionKind.NamedWindowFunction name -> qb.AppendRaw name
            qb.AppendRaw "("
            spec.Arguments |> List.iteri (fun i arg ->
                if i > 0 then qb.AppendRaw ", "
                emitExpr qb arg)
            qb.AppendRaw ") OVER ("
            if not spec.PartitionBy.IsEmpty then
                qb.AppendRaw "PARTITION BY "
                spec.PartitionBy |> List.iteri (fun i e ->
                    if i > 0 then qb.AppendRaw ", "
                    emitExpr qb e)
                if not spec.OrderBy.IsEmpty then qb.AppendRaw " "
            if not spec.OrderBy.IsEmpty then
                qb.AppendRaw "ORDER BY "
                spec.OrderBy |> List.iteri (fun i (e, dir) ->
                    if i > 0 then qb.AppendRaw ", "
                    emitExpr qb e
                    match dir with
                    | SortDirection.Asc -> qb.AppendRaw " ASC"
                    | SortDirection.Desc -> qb.AppendRaw " DESC")
            qb.AppendRaw ")"

        | SqlExpr.Unary(op, operand) ->
            match op with
            | UnaryOperator.Not ->
                qb.AppendRaw "NOT "
                emitExpr qb operand
            | UnaryOperator.Neg ->
                qb.AppendRaw "-"
                emitExpr qb operand
            | UnaryOperator.IsNull ->
                emitExpr qb operand
                qb.AppendRaw " IS NULL"
            | UnaryOperator.IsNotNull ->
                emitExpr qb operand
                qb.AppendRaw " IS NOT NULL"

        | SqlExpr.Binary(left, op, right) ->
            qb.AppendRaw "("
            emitExpr qb left
            match op with
            | BinaryOperator.Eq -> qb.AppendRaw " = "
            | BinaryOperator.Ne -> qb.AppendRaw " <> "
            | BinaryOperator.Lt -> qb.AppendRaw " < "
            | BinaryOperator.Le -> qb.AppendRaw " <= "
            | BinaryOperator.Gt -> qb.AppendRaw " > "
            | BinaryOperator.Ge -> qb.AppendRaw " >= "
            | BinaryOperator.And -> qb.AppendRaw " AND "
            | BinaryOperator.Or -> qb.AppendRaw " OR "
            | BinaryOperator.Add -> qb.AppendRaw " + "
            | BinaryOperator.Sub -> qb.AppendRaw " - "
            | BinaryOperator.Mul -> qb.AppendRaw " * "
            | BinaryOperator.Div -> qb.AppendRaw " / "
            | BinaryOperator.Mod -> qb.AppendRaw " % "
            | BinaryOperator.Like -> qb.AppendRaw " LIKE "
            | BinaryOperator.Glob -> qb.AppendRaw " GLOB "
            | BinaryOperator.Regexp -> qb.AppendRaw " REGEXP "
            | BinaryOperator.Is -> qb.AppendRaw " IS "
            | BinaryOperator.IsNot -> qb.AppendRaw " IS NOT "
            | BinaryOperator.In -> qb.AppendRaw " IN "
            | BinaryOperator.NotInOp -> qb.AppendRaw " NOT IN "
            emitExpr qb right
            qb.AppendRaw ")"

        | SqlExpr.Between(expr, lower, upper) ->
            emitExpr qb expr
            qb.AppendRaw " BETWEEN "
            emitExpr qb lower
            qb.AppendRaw " AND "
            emitExpr qb upper

        | SqlExpr.InList(expr, list) ->
            emitExpr qb expr
            qb.AppendRaw " IN ("
            let mutable first = true
            for item in list do
                if not first then qb.AppendRaw ", "
                first <- false
                emitExpr qb item
            qb.AppendRaw ")"

        | SqlExpr.InSubquery(expr, query) ->
            emitExpr qb expr
            qb.AppendRaw " IN ("
            emitSelect qb query
            qb.AppendRaw ")"

        | SqlExpr.Cast(expr, sqlType) ->
            qb.AppendRaw "CAST("
            emitExpr qb expr
            qb.AppendRaw " AS "
            qb.AppendRaw sqlType
            qb.AppendRaw ")"

        | SqlExpr.Coalesce exprs ->
            qb.AppendRaw "COALESCE("
            let mutable first = true
            for e in exprs do
                if not first then qb.AppendRaw ", "
                first <- false
                emitExpr qb e
            qb.AppendRaw ")"

        | SqlExpr.Exists query ->
            qb.AppendRaw "EXISTS ("
            emitSelect qb query
            qb.AppendRaw ")"

        | SqlExpr.ScalarSubquery query ->
            qb.AppendRaw "("
            emitSelect qb query
            qb.AppendRaw ")"

        | SqlExpr.CaseExpr(branches, elseExpr) ->
            qb.AppendRaw "CASE"
            for (whenExpr, thenExpr) in branches do
                qb.AppendRaw " WHEN "
                emitExpr qb whenExpr
                qb.AppendRaw " THEN "
                emitExpr qb thenExpr
            match elseExpr with
            | Some e -> qb.AppendRaw " ELSE "; emitExpr qb e
            | None -> ()
            qb.AppendRaw " END"

    and emitSelect (qb: QueryBuilder) (sel: SqlSelect) : unit =
        let hasCtes = not (List.isEmpty sel.Ctes)
        if hasCtes then
            qb.AppendRaw "WITH "
            let mutable first = true
            for cte in sel.Ctes do
                if not first then qb.AppendRaw ", "
                first <- false
                qb.AppendRaw cte.Name
                qb.AppendRaw " AS ("
                emitSelect qb cte.Query
                qb.AppendRaw ")"
            qb.AppendRaw " "
        emitSelectBody qb sel.Body

    and emitSelectBody (qb: QueryBuilder) (body: SelectBody) : unit =
        match body with
        | SingleSelect core -> emitSelectCore qb core
        | UnionAllSelect(head, tail) ->
            emitSelectCore qb head
            for t in tail do
                qb.AppendRaw " UNION ALL "
                emitSelectCore qb t

    and emitSelectCore (qb: QueryBuilder) (core: SelectCore) : unit =
        qb.AppendRaw "SELECT "
        if core.Distinct then qb.AppendRaw "DISTINCT "
        match core.Projections with
        | [] -> qb.AppendRaw "*"
        | projs ->
            let mutable first = true
            for proj in projs do
                if not first then qb.AppendRaw ", "
                first <- false
                emitExpr qb proj.Expr
                match proj.Alias with
                | Some a -> qb.AppendRaw " AS "; qb.AppendRaw a
                | None -> ()
        match core.Source with
        | Some src ->
            qb.AppendRaw " FROM "
            emitTableSource qb src
        | None -> ()
        for join in core.Joins do
            match join.Kind with
            | JoinKind.Inner -> qb.AppendRaw " INNER JOIN "
            | JoinKind.Left -> qb.AppendRaw " LEFT JOIN "
            | JoinKind.Cross -> qb.AppendRaw " CROSS JOIN "
            emitTableSource qb join.Source
            match join.On with
            | Some on -> qb.AppendRaw " ON "; emitExpr qb on
            | None -> ()
        match core.Where with
        | Some w -> qb.AppendRaw " WHERE "; emitExpr qb w
        | None -> ()
        match core.GroupBy with
        | [] -> ()
        | groups ->
            qb.AppendRaw " GROUP BY "
            let mutable first = true
            for g in groups do
                if not first then qb.AppendRaw ", "
                first <- false
                emitExpr qb g
        match core.Having with
        | Some h -> qb.AppendRaw " HAVING "; emitExpr qb h
        | None -> ()
        match core.OrderBy with
        | [] -> ()
        | orders ->
            qb.AppendRaw " ORDER BY "
            let mutable first = true
            for o in orders do
                if not first then qb.AppendRaw ", "
                first <- false
                emitExpr qb o.Expr
                match o.Direction with
                | SortDirection.Asc -> ()
                | SortDirection.Desc -> qb.AppendRaw " DESC"
            ()
        match core.Limit with
        | Some l -> qb.AppendRaw " LIMIT "; emitExpr qb l
        | None -> ()
        match core.Offset with
        | Some o -> qb.AppendRaw " OFFSET "; emitExpr qb o
        | None -> ()

    and emitTableSource (qb: QueryBuilder) (src: TableSource) : unit =
        match src with
        | BaseTable(table, alias) ->
            qb.AppendRaw "\""
            qb.AppendRaw table
            qb.AppendRaw "\""
            match alias with
            | Some a -> qb.AppendRaw " AS "; qb.AppendRaw a
            | None -> ()
        | DerivedTable(query, alias) ->
            qb.AppendRaw "("
            emitSelect qb query
            qb.AppendRaw ") AS "
            qb.AppendRaw alias
        | FromJsonEach(valueExpr, alias) ->
            qb.AppendRaw "json_each("
            emitExpr qb valueExpr
            qb.AppendRaw ")"
            match alias with
            | Some a -> qb.AppendRaw " AS "; qb.AppendRaw a
            | None -> ()
