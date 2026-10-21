namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open System.Text
open System.Runtime.CompilerServices
open NativeSQLiteTools
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
    /// Bind this layer's unqualified references while the canonical mapper preserves
    /// nested query scopes and owns traversal of every expression kind.
    let private bindToDerivedSource (alias: string) (expr: SqlExpr) : SqlExpr =
        SqlExpr.map (function
            | SqlExpr.Column(None, col) -> SqlExpr.Column(Some alias, col)
            | SqlExpr.JsonExtractExpr(None, col, path) -> SqlExpr.JsonExtractExpr(Some ("\"" + alias + "\""), col, path)
            | SqlExpr.JsonRootExtract(None, col) -> SqlExpr.JsonRootExtract(Some ("\"" + alias + "\""), col)
            | node -> node) expr

    /// Construct relation joins only after this layer has discovered pending edges.
    let private materializeLayerJoins
        (currentCtx: QueryContext)
        (isBaseTable: bool)
        (needsValueMaterialization: bool)
        (quotedTableName: string)
        (valueColumnExpr: SqlExpr)
        (pendingJoins: JoinEdge list)
        (projections: Projection list)
        (where: SqlExpr option)
        (orderBy: OrderBy list) =
        let mutable finalProjections = projections
        let mutable boundWhere = where
        let mutable boundOrderBy = orderBy
        // Build JoinShape list from discovered JoinEdges.
        let joins =
            pendingJoins
            |> Seq.map (fun j ->
                // At a non-base layer the owner row comes from the derived source, so an
                // edge discovered without a source alias is bound to it; leaving it
                // unqualified would be ambiguous against the joined table's own Value.
                let onSourceAlias =
                    match j.OnSourceAlias with
                    | None when not isBaseTable -> Some "\"o\""
                    | existing -> existing
                let representation =
                    let derivedOwner =
                        not isBaseTable &&
                        (j.OnSourceAlias |> Option.forall (fun alias -> AliasRewrite.normalizeSourceAlias alias = "o"))
                    if derivedOwner then QueryTranslatorReferenceJoin.StoredOrHydratedId
                    else QueryTranslatorReferenceJoin.StoredId
                QueryTranslatorReferenceJoin.joinEdgeToClause (QueryTranslatorReferenceJoin.parseJoinKind j.JoinKind) onSourceAlias representation j)
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

        struct(finalProjections, joins, boundWhere, boundOrderBy)

    let internal buildLayersDu (rootElementType: Type)
        (sourceCtx: QueryContext)
        (vars: Dictionary<string, obj>)
        (layers: SQLSubquery ResizeArray)
        : TranslatedQuery =

        let layerCount = layers.Count

        let tableName =
            if layerCount > 0 then
                match layers.[0] with
                | Simple layer -> layer.TableName
                | ComplexDu _ -> ""
            else
                ""

        // A DBRef join edge is discovered while translating whichever layer references it, which
        // is not always the base layer: a projection over a derived source (for example the
        // Select that follows a Join) discovers its own edges. Every layer therefore emits the
        // edges that are still unemitted, and records them here so a shared edge is emitted once.
        let materializedJoinAliases = System.Collections.Generic.HashSet<string>(System.StringComparer.Ordinal)

        let rec buildLayer (i: int) : TranslatedQuery =
            let layer = layers.[i]
            match layer with
            | Simple layer ->
                let isBaseTable = i = 0
                let projectionOrder = sourceCtx.BindQueryValue |> ValueOption.map (fun binder -> binder.Order.ReserveProjection())
                let input =
                    if isBaseTable then
                        { Query = wrapCore(mkCore [] None)
                          SerializationInput = QueryTranslatorPhysicalSource.ownerInput sourceCtx rootElementType layer.TableName (SqlExpr.Column(None, "Id"))
                          KeyInput = ValueNone }
                    else buildLayer(i - 1)
                let isLocalKeyProjection = layer.Selector |> Option.exists (function KeyProjection _ -> true | _ -> false)
                let currentCtx =
                    if isLocalKeyProjection then { cloneQueryContext sourceCtx with AliasCounter = sourceCtx.AliasCounter }
                    else { sourceCtx with Joins = ResizeArray() }
                let currentCtx = { currentCtx with LayerPosition = (if isBaseTable then BaseLayer else OuterLayer)
                                                   SerializationInput = input.SerializationInput }
                let effectiveTableName = if isBaseTable then layer.TableName else "o"
                let hasTableName = not (String.IsNullOrEmpty effectiveTableName)
                let quotedTableName = "\"" + effectiveTableName + "\""
                let idColumnExpr = if hasTableName then SqlExpr.Column(Some quotedTableName, "Id") else SqlExpr.Column(None, "Id")
                let valueColumnExpr = if hasTableName then SqlExpr.Column(Some quotedTableName, "Value") else SqlExpr.Column(None, "Value")

                // Track whether the Value projection can be rewritten for JOIN materialization.
                let mutable needsValueMaterialization = false

                let joinsBeforeProjections = currentCtx.Joins.Count
                let mutable outputInput = input.SerializationInput
                let mutable keyInput = input.KeyInput
                let mutable producedValue = ValueNone
                let mutable producedKey = ValueNone
                let mutable companions = []
                let value =
                    if isBaseTable then rootValue rootElementType BaseLayer (if hasTableName then Some quotedTableName else None) "Value"
                    else valueColumnExpr
                let valueInput =
                    if isBaseTable then { input.SerializationInput with Carrier = if isSqlScalarResult rootElementType then SqlScalar else CanonicalToken }
                    else input.SerializationInput
                let buildProjections () =
                    match layer.Selector with
                    | Some(Expression selector) ->
                        let produced = QueryTranslator.translateProduced currentCtx effectiveTableName selector vars
                        producedValue <- ValueSome produced
                        [{ Alias = None; Expr = idColumnExpr }; { Alias = Some "Value"; Expr = SqlExpr.Literal SqlLiteral.Null }]
                    | Some(KeyProjection selector) ->
                        let produced = QueryTranslator.translateProduced currentCtx effectiveTableName selector vars
                        producedKey <- ValueSome produced
                        let key = SqlExpr.Literal SqlLiteral.Null
                        outputInput <- valueInput
                        needsValueMaterialization <- not(isSqlScalarResult rootElementType)
                        companions <- QueryTranslatorSerialization.companionProjections (if hasTableName then Some quotedTableName else None) input.SerializationInput
                        [{ Alias = None; Expr = idColumnExpr }; { Alias = Some "Value"; Expr = value }
                         { Alias = Some syntheticGroupKeyAlias; Expr = key }]
                    | Some(DuSelector buildProjections) ->
                        let struct(projections, producedInput) = buildProjections currentCtx input effectiveTableName vars
                        let value = projections |> List.find (fun projection -> projection.Alias = Some "Value") |> _.Expr
                        producedValue <- ValueSome(DirectValue(value, producedInput))
                        projections
                    | None ->
                        outputInput <- valueInput
                        needsValueMaterialization <- not(isSqlScalarResult rootElementType)
                        companions <- QueryTranslatorSerialization.companionProjections (if hasTableName then Some quotedTableName else None) input.SerializationInput
                        [{ Alias = None; Expr = idColumnExpr }; { Alias = Some "Value"; Expr = value }]

                let projections =
                    match sourceCtx.BindQueryValue, projectionOrder with
                    | ValueSome binder, ValueSome group -> binder.Order.Within(group, buildProjections)
                    | _ -> buildProjections()

                let projectionDiscoveredJoins =
                    if currentCtx.Joins.Count > joinsBeforeProjections then
                        currentCtx.Joins |> Seq.skip joinsBeforeProjections |> Seq.toList
                    else []

                let source =
                    if isBaseTable then Some(BaseTable(layer.TableName, None))
                    else Some(DerivedTable(input.Query, "o"))

                // Build clauses (WHERE, ORDER BY, LIMIT, OFFSET, UNION ALL).
                // Side effect: expression translation discovers JOINs via QueryContext.
                let clauseCtx = currentCtx
                let clauseTable = effectiveTableName
                let joinsBeforeClauses = currentCtx.Joins.Count
                let struct (where, orderBy, limit, offset, unionAlls, orderBindings) =
                    buildClausesDu clauseCtx vars layer clauseTable input.KeyInput
                let clauseDiscoveredJoins =
                    if currentCtx.Joins.Count > joinsBeforeClauses then
                        currentCtx.Joins |> Seq.skip joinsBeforeClauses |> Seq.toList
                    else []

                // Edge case 8: JOIN materialization (DBRef) — discovered during clause translation.
                let mutable finalProjections = projections @ companions
                let mutable joins = []
                let mutable boundWhere = where
                let mutable boundOrderBy = orderBy

                let pendingJoins =
                    projectionDiscoveredJoins @ clauseDiscoveredJoins
                    |> List.filter (fun j -> not (materializedJoinAliases.Contains j.TargetAlias))

                if not pendingJoins.IsEmpty then
                    for j in pendingJoins do materializedJoinAliases.Add j.TargetAlias |> ignore
                    let struct(projections, relationJoins, relationWhere, relationOrder) =
                        materializeLayerJoins currentCtx isBaseTable needsValueMaterialization
                            quotedTableName valueColumnExpr pendingJoins finalProjections where orderBy
                    finalProjections <- projections
                    joins <- relationJoins
                    boundWhere <- relationWhere
                    boundOrderBy <- relationOrder

                joins <- joins @ orderBindings

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

                let query = wrapCoreBody body orderBy limit offset
                match producedValue, query.Body with
                | ValueSome produced, SingleSelect core ->
                    let preserveErrors = not isBaseTable && canExposeNullId input.Query
                    let produced =
                        if preserveErrors then
                            QueryProduced.preserveError idColumnExpr valueColumnExpr produced
                        else produced
                    let struct(query, output) = QueryProduced.publishCore currentCtx.NextAlias core produced preserveErrors
                    { Query = query; SerializationInput = output; KeyInput = ValueNone }
                | ValueSome _, _ -> invalidOp "A scalar projection requires a completed select core."
                | ValueNone, SingleSelect core when producedKey.IsSome ->
                    let preserveErrors = not isBaseTable && canExposeNullId input.Query
                    let key =
                        if preserveErrors then
                            let failure = SqlExpr.FunctionCall(JsonFunctions.rowErrorFunction, [SqlExpr.FunctionCall("json", [valueColumnExpr])])
                            let failure = QueryProduced.typed producedKey.Value.Input.OriginalType failure
                            QueryProduced.choose producedKey.Value.Input.OriginalType (SqlExpr.Unary(UnaryOperator.IsNull, idColumnExpr)) failure producedKey.Value
                        else producedKey.Value
                    let struct(query, key) = QueryProduced.publishSlot currentCtx.NextAlias syntheticGroupKeyAlias core key preserveErrors
                    { Query = query; SerializationInput = outputInput; KeyInput = ValueSome key }
                | ValueNone, _ -> { Query = query; SerializationInput = outputInput; KeyInput = keyInput }

            | ComplexDu buildFunc ->
                let tn =
                    if i = 0 then tableName
                    else ""
                let inner =
                    if i > 0 then buildLayer(i - 1)
                    else { Query = wrapCore(mkCore [] None); SerializationInput = QueryTranslatorPhysicalSource.ownerInput sourceCtx rootElementType tableName (SqlExpr.Column(None, "Id")); KeyInput = ValueNone }
                buildFunc {| Vars = vars; Inner = inner.Query; Input = inner.SerializationInput; KeyInput = inner.KeyInput; TableName = tn |}

        buildLayer (layerCount - 1)
