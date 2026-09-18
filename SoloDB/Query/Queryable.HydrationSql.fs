namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq.Expressions
open System.Reflection
open System.Text
open Microsoft.Data.Sqlite
open SQLiteTools
open Utils
open SoloDatabase
open SoloDatabase.RelationsTypes
open SoloDatabase.RelationsSchemaBuilder
open SoloDatabase.RelationsSchemaValidator
open SoloDatabase.RelationsSchemaLinkTableDDL
open SoloDatabase.QueryableGroupByAliases
open SoloDatabase.SqlModel

/// Shared hydration SQL builders for both queryable and non-queryable paths.
/// Single canonical typed DU generator — no duplicated SQL string templates.
module internal HydrationSqlBuilder =
    open QueryableHelperPreprocess
    open QueryableHelperBase

    /// Maximum nesting depth for hydration correlated subqueries (matches batch load maxRecursiveDepth).
    let maxHydrationDepth = Utils.maxRelationDepth


    /// Build a hydrated Value expression by embedding correlated subqueries for DBRef properties.
    /// Each DBRef property that should be loaded gets a ScalarSubquery that returns
    /// jsonb_array(target.Id, target.Value) correlated on the FK in the owner's Value JSON.
    /// Multi-hop: recursive — the target's Value is itself hydrated for its own DBRef properties.
    /// Edge case SA-05: stops recursion at maxHydrationDepth.
    let rec buildHydrationValueExpr
        (connection: SqliteConnection)
        (ctx: QueryContext)
        (ownerType: Type)
        (ownerTable: string)
        (ownerValueExpr: SqlExpr)
        (depth: int)
        (prefix: string)
        (aliasCounter: byref<int>)
        : SqlExpr =

        if depth >= maxHydrationDepth then ownerValueExpr
        else

        // Reflected once per type; this function recurses over relation depth, so re-reflecting
        // here cost one full property scan per nesting level per translation.
        let descriptor = HydrationSqlMetadata.getRelationDescriptor ownerType

        if descriptor.SingleCount = 0 then ownerValueExpr
        else

        let args = ResizeArray<SqlExpr>()
        args.Add(ownerValueExpr)

        // Indexed over the cached descriptor: no per-call array, list or enumerable is built.
        for singleIndex in 0 .. descriptor.SingleCount - 1 do
            let prop = descriptor.Single singleIndex
            let path = if prefix = "" then prop.Name else prefix + "." + prop.Name

            if shouldLoadRelationPath ctx path then
                let targetType = (UtilsReflection.GenericTypeArgCache.Get prop.PropertyType).[0]
                match ctx.TryResolveRelationTarget(ownerTable, prop.Name) with
                | None ->
                    // Read-path hydration must not explode when downstream metadata
                    // is absent; the deserialized DBRef id remains usable.
                    ()
                | Some targetTable ->
                    aliasCounter <- aliasCounter + 1
                    let tAlias = sprintf "_ht%d" aliasCounter

                    let fkExpr =
                        SqlExpr.FunctionCall("jsonb_extract",
                            [ownerValueExpr; SqlExpr.Literal(SqlLiteral.String ("$." + prop.Name))])

                    let targetValueExpr =
                        buildHydrationValueExpr connection ctx targetType targetTable
                            (SqlExpr.Column(Some tAlias, "Value"))
                            (depth + 1) path &aliasCounter

                    let subqueryProjection =
                        SqlExpr.FunctionCall("jsonb_array",
                            [SqlExpr.Column(Some tAlias, "Id"); targetValueExpr])

                    let subqueryWhere =
                        SqlExpr.Binary(
                            SqlExpr.Column(Some tAlias, "Id"),
                            BinaryOperator.Eq,
                            fkExpr)

                    let subqueryCore =
                        { mkCore
                            [{ Alias = None; Expr = subqueryProjection }]
                            (Some (BaseTable(targetTable, Some tAlias)))
                          with Where = Some subqueryWhere }
                    let subquerySelect = { Ctes = []; Body = SingleSelect subqueryCore }

                    let coalesceExpr =
                        SqlExpr.Coalesce(
                            SqlExpr.ScalarSubquery subquerySelect,
                            [fkExpr])

                    args.Add(SqlExpr.Literal(SqlLiteral.String ("$." + prop.Name)))
                    args.Add(coalesceExpr)

        if args.Count <= 1 then
            ownerValueExpr
        else
            SqlExpr.FunctionCall("jsonb_set", args |> Seq.toList)

    /// Build a HydrationJSON projection expression for DBRefMany properties.
    let buildManyHydrationProjection
        (connection: SqliteConnection)
        (ctx: QueryContext)
        (ownerType: Type)
        (ownerTable: string)
        (ownerIdExpr: SqlExpr)
        (aliasCounter: byref<int>)
        : SqlExpr option =

        let descriptor = HydrationSqlMetadata.getRelationDescriptor ownerType

        if descriptor.ManyCount = 0 then None
        else

        let args = ResizeArray<SqlExpr>()

        for manyIndex in 0 .. descriptor.ManyCount - 1 do
            let prop = (descriptor.Many manyIndex).Property
            if shouldLoadRelationPath ctx prop.Name then
                let targetType = (UtilsReflection.GenericTypeArgCache.Get prop.PropertyType).[0]
                let linkTableOpt = ctx.TryResolveRelationLink(ownerTable, prop.Name)
                let targetTableOpt = ctx.TryResolveRelationTarget(ownerTable, prop.Name)
                match linkTableOpt, targetTableOpt with
                | Some linkTable, Some targetTable ->
                    let ownerUsesSource =
                        match ctx.TryResolveRelationOwnerUsesSource(ownerTable, prop.Name) with
                        | Some v -> v
                        | None -> true

                    let ownerColumn = if ownerUsesSource then "SourceId" else "TargetId"
                    let targetColumn = if ownerUsesSource then "TargetId" else "SourceId"

                    aliasCounter <- aliasCounter + 1
                    let lnkAlias = sprintf "_hlnk%d" aliasCounter
                    aliasCounter <- aliasCounter + 1
                    let tAlias = sprintf "_ht%d" aliasCounter

                    let subqueryProjection =
                        SqlExpr.FunctionCall("jsonb_group_array",
                            [SqlExpr.FunctionCall(jsonObjectFn,
                                [SqlExpr.Literal(SqlLiteral.String "Id"); SqlExpr.Column(Some tAlias, "Id")
                                 SqlExpr.Literal(SqlLiteral.String "Value"); SqlExpr.FunctionCall("json_quote", [SqlExpr.Column(Some tAlias, "Value")])])])

                    let joinOn =
                        SqlExpr.Binary(
                            SqlExpr.Column(Some tAlias, "Id"),
                            BinaryOperator.Eq,
                            SqlExpr.Column(Some lnkAlias, targetColumn))

                    let subqueryWhere =
                        SqlExpr.Binary(
                            SqlExpr.Column(Some lnkAlias, ownerColumn),
                            BinaryOperator.Eq,
                            ownerIdExpr)

                    let subqueryCore =
                        { mkCore
                            [{ Alias = None; Expr = subqueryProjection }]
                            (Some (BaseTable(linkTable, Some lnkAlias)))
                          with
                            Joins = [ConditionedJoin(JoinKind.Inner, BaseTable(targetTable, Some tAlias), joinOn)]
                            Where = Some subqueryWhere }
                    let subquerySelect = { Ctes = []; Body = SingleSelect subqueryCore }

                    let coalesceExpr =
                        SqlExpr.Coalesce(
                            SqlExpr.ScalarSubquery subquerySelect,
                            [SqlExpr.FunctionCall("jsonb", [SqlExpr.Literal(SqlLiteral.String "[]")])])

                    args.Add(SqlExpr.Literal(SqlLiteral.String prop.Name))
                    args.Add(coalesceExpr)
                | _ ->
                    match classifyMissingReadMetadata connection ownerTable prop.Name RelationKind.Many targetType with
                    | MissingReadMetadataState.AbsentNoEvidence ->
                        ()
                    | MissingReadMetadataState.PoisonedWithEvidence ->
                        raise (InvalidOperationException(
                            $"Error: Relation metadata not found for property {prop.Name} on collection {ownerTable}.\nReason: Existing relation evidence was found but automatic recovery is not safe.\nFix: Ensure the collection is initialized with Insert or GetCollection before querying, or call AsEnumerable() before accessing this relation."))

        if args.Count = 0 then None
        else Some (SqlExpr.FunctionCall(jsonObjectFn, args |> Seq.toList))

    /// Build a complete hydrated SELECT as SqlSelect DU for non-queryable GetById-style reads.
    /// Returns (sqlString, variables, hasSingleHydration, hasManyHydration).
    let buildHydratedGetByIdSql
        (connection: SqliteConnection)
        (tableName: string)
        (ownerType: Type)
        (whereExpr: SqlExpr)
        (initialVars: Dictionary<string, obj>)
        (addLimit: bool)
        : string * bool * bool =

        // Shape is computed first so a type that cannot hydrate any relation never allocates a
        // metadata source and never reaches the catalogs.
        let shape : HydrationSqlMetadata.RelationShapeInfo = HydrationSqlMetadata.getRelationShape ownerType
        let ctx =
            if shape.HasAny then
                { QueryContext.SingleSource(tableName) with MetadataSource = ValueSome (RelationMetadataSource connection) }
            else QueryContext.SingleSource(tableName)
        let hasSingle = shape.HasSingle
        let hasMany = shape.HasMany

        let mutable aliasCounter = 0
        let tblAlias = "o"

        // Build hydrated Value expression (DBRef jsonb_set enrichment).
        // Use qualified column references to avoid ambiguity with correlated subqueries.
        let valueExpr =
            if hasSingle then
                let hydrated =
                    buildHydrationValueExpr connection ctx ownerType tableName
                        (SqlExpr.Column(Some tblAlias, "Value")) 0 "" &aliasCounter
                if aliasCounter > 0 then
                    SqlExpr.FunctionCall("json_extract",
                        [hydrated; SqlExpr.Literal(SqlLiteral.String "$")])
                else
                    SqlExpr.FunctionCall("json_quote", [SqlExpr.Column(Some tblAlias, "Value")])
            else
                SqlExpr.FunctionCall("json_quote", [SqlExpr.Column(Some tblAlias, "Value")])

        // Build HydrationJSON (DBRefMany json_group_array).
        let manyHydrationOpt =
            if hasMany then
                buildManyHydrationProjection connection ctx ownerType tableName
                    (SqlExpr.Column(Some tblAlias, "Id")) &aliasCounter
            else
                None

        let singleHydrated = hasSingle && aliasCounter > 0
        let manyHydrated = hasMany && manyHydrationOpt.IsSome

        // Build projections with qualified column references.
        let projections =
            [{ Alias = None; Expr = SqlExpr.Column(Some tblAlias, "Id") }
             { Alias = Some "ValueJSON"; Expr = valueExpr }]
            @ (match manyHydrationOpt with
               | Some hydExpr -> [{ Alias = Some "HydrationJSON"; Expr = hydExpr }]
               | None -> [])

        // Build the full SELECT with table alias to prevent column ambiguity.
        let core =
            { mkCore projections (Some (BaseTable(tableName, Some tblAlias)))
              with
                Where = Some whereExpr
                Limit = if addLimit then Some (SqlExpr.Literal(SqlLiteral.Integer 1L)) else None }
        let select = wrapCore core

        // Emit to SQL string via optimizer pipeline.
        let modelTableNames = QueryableHelperBase.collectIndexModelTableNames tableName select
        let indexModel = RuntimeIndexModelCache.loadModelForTables connection modelTableNames
        let sql = emitSelectSql initialVars indexModel id select

        sql, singleHydrated, manyHydrated

    /// Build a mutation-prep SELECT with DBRefMany-only HydrationJSON (no DBRef enrichment).
    /// Returns (sqlString, hasManyHydration).
    let buildManyOnlyHydratedSql
        (connection: SqliteConnection)
        (tableName: string)
        (ownerType: Type)
        (whereExpr: SqlExpr)
        (initialVars: Dictionary<string, obj>)
        (addLimit: bool)
        : string * bool =

        // Many-only builder: a type with no DBRefMany property cannot hydrate here, so it
        // neither allocates a metadata source nor reaches the catalogs.
        let shape : HydrationSqlMetadata.RelationShapeInfo = HydrationSqlMetadata.getRelationShape ownerType
        let ctx =
            if shape.HasMany then
                { QueryContext.SingleSource(tableName) with MetadataSource = ValueSome (RelationMetadataSource connection) }
            else QueryContext.SingleSource(tableName)
        let hasMany = shape.HasMany

        let mutable aliasCounter = 0
        let tblAlias = "o"

        let manyHydrationOpt =
            if hasMany then
                buildManyHydrationProjection connection ctx ownerType tableName
                    (SqlExpr.Column(Some tblAlias, "Id")) &aliasCounter
            else
                None

        let manyHydrated = hasMany && manyHydrationOpt.IsSome

        let projections =
            [{ Alias = None; Expr = SqlExpr.Column(Some tblAlias, "Id") }
             { Alias = Some "ValueJSON"; Expr = SqlExpr.FunctionCall("json_quote", [SqlExpr.Column(Some tblAlias, "Value")]) }]
            @ (match manyHydrationOpt with
               | Some hydExpr -> [{ Alias = Some "HydrationJSON"; Expr = hydExpr }]
               | None -> [])

        let core =
            { mkCore projections (Some (BaseTable(tableName, Some tblAlias)))
              with
                Where = Some whereExpr
                Limit = if addLimit then Some (SqlExpr.Literal(SqlLiteral.Integer 1L)) else None }
        let select = wrapCore core

        let modelTableNames = QueryableHelperBase.collectIndexModelTableNames tableName select
        let indexModel = RuntimeIndexModelCache.loadModelForTables connection modelTableNames
        let sql = emitSelectSql initialVars indexModel id select

        sql, manyHydrated

    /// Strip source aliases from a SqlExpr (re-exported from Preprocess for use by SoloDBCore callers).
    let stripSourceAlias = QueryableHelperPreprocess.stripSourceAlias

    /// Emit a SqlExpr to a SQL string using the standalone EmitContext.
    /// Used for emitting DU-built expressions as SQL fragments for template composition.
    let emitExprToSql (expr: SqlExpr) : string =
        let ctx = EmitContext()
        ctx.InlineLiterals <- true
        let emitted = EmitExpr.emitExprWith EmitSelect.emitSelect ctx expr
        emitted.Sql

    /// Build a mutation-prep SELECT SQL with DBRefMany-only HydrationJSON.
    /// Uses table alias "o". The raw WHERE filter SQL is composed via SQL template
    /// (standard composition, not post-emission string surgery).
    /// Returns (sqlString, hasManyHydration).
    let buildManyOnlyHydratedSqlWithRawWhere
        (connection: SqliteConnection)
        (tableName: string)
        (ownerType: Type)
        (rawFilterSql: string)
        (addLimit: bool)
        : string * bool =

        // Many-only builder: a type with no DBRefMany property cannot hydrate here, so it
        // neither allocates a metadata source nor reaches the catalogs.
        let shape : HydrationSqlMetadata.RelationShapeInfo = HydrationSqlMetadata.getRelationShape ownerType
        let ctx =
            if shape.HasMany then
                { QueryContext.SingleSource(tableName) with MetadataSource = ValueSome (RelationMetadataSource connection) }
            else QueryContext.SingleSource(tableName)
        let limitClause = if addLimit then " LIMIT 1" else ""
        let qTable = "\"" + tableName + "\""

        if not shape.HasMany then
            $"SELECT Id, json_quote(Value) as ValueJSON FROM {qTable} WHERE {rawFilterSql}{limitClause}", false
        else

        let mutable aliasCounter = 0
        // Use table-name-qualified Id for correlated subquery correlation.
        // No table alias: the raw filter SQL references columns by table name (e.g., "Article"."Value").
        let ownerIdExpr = SqlExpr.Column(Some qTable, "Id")
        match buildManyHydrationProjection connection ctx ownerType tableName ownerIdExpr &aliasCounter with
        | None ->
            $"SELECT Id, json_quote(Value) as ValueJSON FROM {qTable} WHERE {rawFilterSql}{limitClause}", false
        | Some hydExpr ->
            // Emit the HydrationJSON expression to a SQL fragment via standalone emitter.
            let hydSql = emitExprToSql hydExpr
            // Compose full SQL via template — standard composition, no surgery.
            $"SELECT {qTable}.Id, json_quote({qTable}.Value) AS \"ValueJSON\", {hydSql} AS \"HydrationJSON\" FROM {qTable} WHERE {rawFilterSql}{limitClause}", true
