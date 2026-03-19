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
open SoloDatabase.RelationsSchema
open SqlDu.Engine.C1.Spec

/// Shared hydration SQL builders for both queryable (R45-9) and non-queryable (R45-10) paths.
/// Single canonical typed DU generator — no duplicated SQL string templates.
module internal HydrationSqlBuilder =
    open QueryableHelperPreprocess
    open QueryableHelperBase

    /// Maximum nesting depth for hydration correlated subqueries (matches batch load maxRecursiveDepth).
    let maxHydrationDepth = 10

    [<Struct>]
    type internal RelationShapeInfo = {
        HasAny: bool
        HasSingle: bool
        HasMany: bool
    }

    let internal relationShapeCache = System.Collections.Concurrent.ConcurrentDictionary<Type, RelationShapeInfo>()

    let internal getRelationShape (t: Type) : RelationShapeInfo =
        relationShapeCache.GetOrAdd(t, Func<Type, RelationShapeInfo>(fun t ->
            let props = t.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
            let hasSingle = props |> Array.exists (fun p -> DBRefTypeHelpers.isDBRefType p.PropertyType)
            let hasMany = props |> Array.exists (fun p -> DBRefTypeHelpers.isDBRefManyType p.PropertyType)
            { HasAny = hasSingle || hasMany; HasSingle = hasSingle; HasMany = hasMany }
        ))

    let internal hasRelationProperties (t: Type) =
        (getRelationShape t).HasAny

    let internal tableExists (connection: SqliteConnection) (tableName: string) =
        connection.QueryFirst<int64>(
            "SELECT CASE WHEN EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name) THEN 1 ELSE 0 END",
            {| name = tableName |}) = 1L

    let internal preloadQueryContextMetadata (ctx: QueryContext) (connection: SqliteConnection) =
        let canonicalManyName (a: string) (b: string) =
            if StringComparer.Ordinal.Compare(a, b) <= 0 then $"{a}_{b}" else $"{b}_{a}"

        if tableExists connection "SoloDBRelation" then
            for relation in connection.Query<{|
                Name: string
                OwnerCollection: string
                PropertyName: string
                TargetCollection: string
                RefKind: string
            |}>("SELECT Name, OwnerCollection, PropertyName, TargetCollection, RefKind FROM SoloDBRelation;") do
                let name = relation.Name
                let ownerCollection = relation.OwnerCollection
                let propertyName = relation.PropertyName
                let targetCollection = relation.TargetCollection
                let refKind = relation.RefKind
                if not (isNull name || isNull ownerCollection || isNull propertyName || isNull targetCollection || isNull refKind) then
                    let relationKind = stringToRelationKind refKind
                    let canonicalName = canonicalManyName ownerCollection targetCollection
                    let canonicalLinkTable = "SoloDBRelLink_" + canonicalName
                    let defaultLinkTable = "SoloDBRelLink_" + name
                    let useSharedMany =
                        match relationKind with
                        | RelationKind.Many ->
                            tableExists connection canonicalLinkTable
                            && not (tableExists connection defaultLinkTable)
                        | RelationKind.Single -> false
                    let ownerUsesSource =
                        if useSharedMany then
                            StringComparer.Ordinal.Compare(ownerCollection, targetCollection) <= 0
                        else
                            true
                    let linkTable = if useSharedMany then canonicalLinkTable else defaultLinkTable
                    ctx.RegisterRelation(ownerCollection, propertyName, targetCollection, linkTable, ownerUsesSource)

        if tableExists connection "SoloDBTypeCollectionMap" then
            for mapping in connection.Query<{|
                TypeKey: string
                CollectionName: string
            |}>("SELECT TypeKey, CollectionName FROM SoloDBTypeCollectionMap;") do
                let typeKey = mapping.TypeKey
                let collectionName = mapping.CollectionName
                if not (isNull typeKey || isNull collectionName) then
                    ctx.RegisterTypeCollection(typeKey, collectionName)

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

        let singleProps =
            ownerType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
            |> Array.filter (fun p -> DBRefTypeHelpers.isDBRefType p.PropertyType)

        if singleProps.Length = 0 then ownerValueExpr
        else

        let args = ResizeArray<SqlExpr>()
        args.Add(ownerValueExpr)

        for prop in singleProps do
            let path = if prefix = "" then prop.Name else prefix + "." + prop.Name

            if shouldLoadRelationPath ctx path then
                let targetType = (GenericTypeArgCache.Get prop.PropertyType).[0]
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

        let manyProps =
            ownerType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
            |> Array.filter (fun p -> DBRefTypeHelpers.isDBRefManyType p.PropertyType)

        if manyProps.Length = 0 then None
        else

        let args = ResizeArray<SqlExpr>()

        for prop in manyProps do
            if shouldLoadRelationPath ctx prop.Name then
                let targetType = (GenericTypeArgCache.Get prop.PropertyType).[0]
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
                        SqlExpr.FunctionCall("json_group_array",
                            [SqlExpr.FunctionCall("json_object",
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
                            $"Error: relation metadata missing for '{ownerTable}.{prop.Name}'.\nReason: prior relation evidence exists and read-time auto-heal is not safe.\nFix: rebuild/repair relation metadata and link-table state before retrying."))

        if args.Count = 0 then None
        else Some (SqlExpr.FunctionCall("json_object", args |> Seq.toList))

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

        let ctx = QueryContext.SingleSource(tableName)
        preloadQueryContextMetadata ctx connection

        let shape : RelationShapeInfo = getRelationShape ownerType
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
        let sb = StringBuilder(256)
        let modelTableNames = QueryableHelperBase.collectIndexModelTableNames tableName select
        let indexModel = SoloDatabase.IndexModel.loadModelForTables connection modelTableNames
        emitSelectToSb sb initialVars indexModel select

        sb.ToString(), singleHydrated, manyHydrated

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

        let ctx = QueryContext.SingleSource(tableName)
        preloadQueryContextMetadata ctx connection

        let shape : RelationShapeInfo = getRelationShape ownerType
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

        let sb = StringBuilder(256)
        let modelTableNames = QueryableHelperBase.collectIndexModelTableNames tableName select
        let indexModel = SoloDatabase.IndexModel.loadModelForTables connection modelTableNames
        emitSelectToSb sb initialVars indexModel select

        sb.ToString(), manyHydrated

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

        let ctx = QueryContext.SingleSource(tableName)
        preloadQueryContextMetadata ctx connection

        let shape : RelationShapeInfo = getRelationShape ownerType
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
