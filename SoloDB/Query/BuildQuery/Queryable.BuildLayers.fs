namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open System.Text
open System.Runtime.CompilerServices
open Microsoft.Data.Sqlite
open SQLiteTools
open Utils
open JsonFunctions
open Connections
open SoloDatabase
open SoloDatabase.JsonSerializator
open SoloDatabase.RelationsTypes
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryableGroupByAliases
open SoloDatabase.SqlModel

module internal QueryableLayerBuild =
    open QueryableHelperPreprocess
    open QueryableHelperBase
    let internal buildLayersDu<'T>
        (sourceCtx: QueryContext)
        (vars: Dictionary<string, obj>)
        (layers: SQLSubquery ResizeArray)
        : SqlSelect =

        let layerCount = layers.Count

        let tableName =
            if layerCount > 0 then
                match layers.[0] with
                | Simple layer -> layer.TableName
                | ComplexDu _ -> ""
            else
                ""

        /// Binds unqualified references to the derived source of a layer that also carries a
        /// relation join. Without this, a bare Value or Id is ambiguous between the derived
        /// source and the joined table. Every case is listed so a new SqlExpr case cannot be
        /// silently skipped here.
        let rec bindToDerivedSource (alias: string) (expr: SqlExpr) : SqlExpr =
            let recur = bindToDerivedSource alias
            match expr with
            | SqlExpr.Column(None, col) -> SqlExpr.Column(Some alias, col)
            | SqlExpr.JsonExtractExpr(None, col, path) -> SqlExpr.JsonExtractExpr(Some ("\"" + alias + "\""), col, path)
            | SqlExpr.JsonRootExtract(None, col) -> SqlExpr.JsonRootExtract(Some ("\"" + alias + "\""), col)
            | SqlExpr.Column(Some _, _)
            | SqlExpr.JsonExtractExpr(Some _, _, _)
            | SqlExpr.JsonRootExtract(Some _, _)
            | SqlExpr.Literal _
            | SqlExpr.Parameter _ -> expr
            | SqlExpr.JsonSetExpr(target, assignments) ->
                SqlExpr.JsonSetExpr(recur target, assignments |> List.map (fun (p, v) -> p, recur v))
            | SqlExpr.JsonArrayExpr elements -> SqlExpr.JsonArrayExpr(elements |> List.map recur)
            | SqlExpr.JsonObjectExpr properties -> SqlExpr.JsonObjectExpr(properties |> List.map (fun (n, v) -> n, recur v))
            | SqlExpr.FunctionCall(name, args) -> SqlExpr.FunctionCall(name, args |> List.map recur)
            | SqlExpr.AggregateCall(kind, arg, distinct, sep) ->
                SqlExpr.AggregateCall(kind, arg |> Option.map recur, distinct, sep |> Option.map recur)
            | SqlExpr.WindowCall spec ->
                SqlExpr.WindowCall
                    { spec with
                        Arguments = spec.Arguments |> List.map recur
                        PartitionBy = spec.PartitionBy |> List.map recur
                        OrderBy = spec.OrderBy |> List.map (fun (e, d) -> recur e, d) }
            | SqlExpr.Unary(op, e) -> SqlExpr.Unary(op, recur e)
            | SqlExpr.Binary(l, op, r) -> SqlExpr.Binary(recur l, op, recur r)
            | SqlExpr.Between(e, lo, hi) -> SqlExpr.Between(recur e, recur lo, recur hi)
            | SqlExpr.InList(e, head, tail) -> SqlExpr.InList(recur e, recur head, tail |> List.map recur)
            // Subqueries carry their own source scope; their internal references are not
            // ambiguous against this layer and must not be rebound.
            | SqlExpr.InSubquery(e, sel) -> SqlExpr.InSubquery(recur e, sel)
            | SqlExpr.Exists _
            | SqlExpr.ScalarSubquery _ -> expr
            | SqlExpr.Cast(e, t) -> SqlExpr.Cast(recur e, t)
            | SqlExpr.Coalesce(head, tail) -> SqlExpr.Coalesce(recur head, tail |> List.map recur)
            | SqlExpr.CaseExpr((c, r), rest, elseExpr) ->
                SqlExpr.CaseExpr((recur c, recur r), rest |> List.map (fun (a, b) -> recur a, recur b), elseExpr |> Option.map recur)

        // A DBRef join edge is discovered while translating whichever layer references it, which
        // is not always the base layer: a projection over a derived source (for example the
        // Select that follows a Join) discovers its own edges. Every layer therefore emits the
        // edges that are still unemitted, and records them here so a shared edge is emitted once.
        let materializedJoinAliases = System.Collections.Generic.HashSet<string>(System.StringComparer.Ordinal)

        let rec buildLayer (i: int) : SqlSelect =
            let layer = layers.[i]
            match layer with
            // Edge case 1: Empty query (root table only)
            | Simple layer when layer.IsEmptyWithTableName ->
                let isTypePrimitive = QueryTranslatorBaseTypes.isPrimitiveSQLiteType typeof<'T>
                if isTypePrimitive then
                    // Edge case 2: Primitive type extraction — jsonb_extract(Value, '$')
                    wrapCore (mkCore
                        [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }
                         { Alias = Some "Value"; Expr = SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$")]) }]
                        (Some (BaseTable(layer.TableName, None))))
                else
                    // Non-primitive: bare table reference (Id and Value are columns)
                    wrapCore (mkCore
                        [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }
                         { Alias = None; Expr = SqlExpr.Column(None, "Value") }]
                        (Some (BaseTable(layer.TableName, None))))

            | Simple layer ->
                let isLocalKeyProjection =
                    match layer.Selector with
                    | Some (KeyProjection _) -> true
                    | _ -> false
                let currentCtx =
                    if isLocalKeyProjection then cloneQueryContext sourceCtx
                    else sourceCtx
                let effectiveTableName = layer.TableName
                let hasTableName = not (String.IsNullOrEmpty effectiveTableName)
                let quotedTableName = "\"" + effectiveTableName + "\""
                let idColumnExpr = if hasTableName then SqlExpr.Column(Some quotedTableName, "Id") else SqlExpr.Column(None, "Id")
                let valueColumnExpr = if hasTableName then SqlExpr.Column(Some quotedTableName, "Value") else SqlExpr.Column(None, "Value")

                // Track whether the Value projection can be rewritten for JOIN materialization.
                let mutable needsValueMaterialization = false

                // Edges discovered while translating THIS layer belong to THIS layer. Layers are
                // built outer-projection first, then inner layers by recursion, then outer
                // clauses, so a shared counter would attach an outer edge to an inner layer.
                let joinsBeforeProjections = currentCtx.Joins.Count

                // Build projections based on selector.
                let projections =
                    match layer.Selector with
                    | Some (Expression selector) ->
                        let selectorExpr = translateExprDu sourceCtx layer.TableName selector vars
                        [{ Alias = None; Expr = idColumnExpr }
                         { Alias = Some "Value"; Expr = selectorExpr }]
                    | Some (KeyProjection selector) ->
                        let isTypePrimitive = QueryTranslatorBaseTypes.isPrimitiveSQLiteType typeof<'T>
                        let keyExpr = translateExprDu currentCtx effectiveTableName selector vars
                        if isTypePrimitive then
                            [{ Alias = None; Expr = idColumnExpr }
                             { Alias = Some "Value"; Expr = SqlExpr.FunctionCall("jsonb_extract", [valueColumnExpr; SqlExpr.Literal(SqlLiteral.String "$")]) }
                             { Alias = Some syntheticGroupKeyAlias; Expr = keyExpr }]
                        else
                            needsValueMaterialization <- true
                            [{ Alias = None; Expr = idColumnExpr }
                             { Alias = Some "Value"; Expr = valueColumnExpr }
                             { Alias = Some syntheticGroupKeyAlias; Expr = keyExpr }]
                    | Some (DuSelector buildProjections) ->
                        buildProjections layer.TableName vars
                    | None ->
                        let isTypePrimitive = QueryTranslatorBaseTypes.isPrimitiveSQLiteType typeof<'T>
                        if isTypePrimitive then
                            [{ Alias = None; Expr = idColumnExpr }
                             { Alias = Some "Value"; Expr = SqlExpr.FunctionCall("jsonb_extract", [valueColumnExpr; SqlExpr.Literal(SqlLiteral.String "$")]) }]
                        else
                            needsValueMaterialization <- true
                            [{ Alias = None; Expr = idColumnExpr }
                             { Alias = None; Expr = valueColumnExpr }]

                let projectionDiscoveredJoins =
                    if currentCtx.Joins.Count > joinsBeforeProjections then
                        currentCtx.Joins |> Seq.skip joinsBeforeProjections |> Seq.toList
                    else []

                // Build source.
                let isBaseTable = (i = 0)
                let source =
                    if isBaseTable then
                        Some (BaseTable(layer.TableName, None))
                    else
                        let innerSel = buildLayer (i - 1)
                        Some (DerivedTable(innerSel, "o"))

                // Build clauses (WHERE, ORDER BY, LIMIT, OFFSET, UNION ALL).
                // Side effect: expression translation discovers JOINs via QueryContext.
                let clauseCtx = if isLocalKeyProjection then currentCtx else sourceCtx
                let clauseTable = if isLocalKeyProjection then effectiveTableName else layer.TableName
                let joinsBeforeClauses = currentCtx.Joins.Count
                let struct (where, orderBy, limit, offset, unionAlls) =
                    buildClausesDu clauseCtx vars layer clauseTable
                let clauseDiscoveredJoins =
                    if currentCtx.Joins.Count > joinsBeforeClauses then
                        currentCtx.Joins |> Seq.skip joinsBeforeClauses |> Seq.toList
                    else []

                // Edge case 8: JOIN materialization (DBRef) — discovered during clause translation.
                let mutable finalProjections = projections
                let mutable joins = []
                let mutable boundWhere = where
                let mutable boundOrderBy = orderBy

                let pendingJoins =
                    projectionDiscoveredJoins @ clauseDiscoveredJoins
                    |> List.filter (fun j -> not (materializedJoinAliases.Contains j.TargetAlias))

                if not pendingJoins.IsEmpty then
                    for j in pendingJoins do materializedJoinAliases.Add j.TargetAlias |> ignore
                    // Build JoinShape list from discovered JoinEdges.
                    joins <-
                        pendingJoins
                        |> Seq.map (fun j ->
                            // At a non-base layer the owner row comes from the derived source, so an
                            // edge discovered without a source alias is bound to it; leaving it
                            // unqualified would be ambiguous against the joined table's own Value.
                            let onSourceAlias =
                                match j.OnSourceAlias with
                                | None when not isBaseTable -> Some "\"o\""
                                | existing -> existing
                            ConditionedJoin(
                                parseJoinKind j.JoinKind,
                                BaseTable(j.TargetTable, Some j.TargetAlias),
                                SqlExpr.Binary(
                                    SqlExpr.Column(Some j.TargetAlias, "Id"),
                                    BinaryOperator.Eq,
                                    SqlExpr.JsonExtractExpr(onSourceAlias, "Value", JsonPath(j.OnPropertyName, [])))))
                        |> Seq.toList

                    // A non-base layer selects from a derived source aliased "o" and now also has a
                    // joined relation table. Unqualified Id/Value would be ambiguous between them,
                    // so bind them to the derived source they were always meant to come from.
                    if not isBaseTable then
                        finalProjections <- finalProjections |> List.map (fun p -> { p with Expr = bindToDerivedSource "o" p.Expr })
                        boundWhere <- where |> Option.map (bindToDerivedSource "o")
                        boundOrderBy <- orderBy |> List.map (fun o -> { o with Expr = bindToDerivedSource "o" o.Expr })

                    // Rewrite Value projection for materialization (jsonb_set). Only meaningful at
                    // the base layer, where the projection is still a bare Value column.
                    if isBaseTable && needsValueMaterialization then
                        let materializedValueExpr = buildMaterializedValueExpr currentCtx quotedTableName valueColumnExpr
                        finalProjections <-
                            finalProjections |> List.map (fun p ->
                                // Replace bare Value column with materialized expression.
                                match p.Alias, p.Expr with
                                | None, SqlExpr.Column(_, "Value") ->
                                    { Alias = Some "Value"; Expr = materializedValueExpr }
                                | Some "Value", _ ->
                                    { Alias = Some "Value"; Expr = materializedValueExpr }
                                | _ -> p)

                // Assemble the SelectCore.
                let body =
                    match unionAlls with
                    | [] ->
                        let core =
                            { mkCore finalProjections source with
                                Joins = joins
                                Where = boundWhere
                                OrderBy = boundOrderBy
                                Limit = limit
                                Offset = offset }
                        SingleSelect core
                    | _ ->
                        // Edge case 4: UNION ALL chains
                        let headCore =
                            { mkCore finalProjections source with
                                Joins = joins
                                Where = boundWhere }
                        UnionAllSelect(headCore, unionAlls)

                wrapCoreBody body orderBy limit offset

            | ComplexDu buildFunc ->
                let tn =
                    if i = 0 then tableName
                    else ""
                let innerSel =
                    if i > 0 then buildLayer (i - 1)
                    else
                        // No inner layers — provide an empty select (shouldn't happen in practice)
                        wrapCore (mkCore [] None)
                buildFunc {| Vars = vars; Inner = innerSel; TableName = tn |}

        buildLayer (layerCount - 1)
