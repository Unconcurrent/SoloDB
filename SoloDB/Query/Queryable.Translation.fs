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
open SqlDu.Engine.C1.Spec

module internal QueryableTranslationCore =
    open QueryableHelperPreprocess
    open QueryableLayerBuild
    open QueryableBuildQueryMain
    open QueryableHelperBase
    /// <summary>
    /// Determines if a given LINQ expression corresponds to a method that does not return the document ID (e.g., aggregate functions).
    /// </summary>
    /// <param name="expression">The expression to check.</param>
    /// <returns>True if the method is an aggregate that doesn't return an ID; otherwise, false.</returns>
    let internal doesNotReturnIdFn (expression: Expression) =
        match expression with
        | :? MethodCallExpression as mce ->
            match parseSupportedMethod mce.Method.Name with
            | None -> false
            | Some method ->
            match method with
            | SupportedLinqMethods.Sum
            | SupportedLinqMethods.Count
            | SupportedLinqMethods.CountBy
            | SupportedLinqMethods.LongCount
            | SupportedLinqMethods.All
            | SupportedLinqMethods.Any
            | SupportedLinqMethods.Contains
            | SupportedLinqMethods.Aggregate
                -> true
            | SupportedLinqMethods.Min
            | SupportedLinqMethods.Max
            | SupportedLinqMethods.MinBy
            | SupportedLinqMethods.MaxBy
            | SupportedLinqMethods.Average
            | SupportedLinqMethods.Distinct
            | SupportedLinqMethods.DistinctBy
            | SupportedLinqMethods.Where
            | SupportedLinqMethods.Select
            | SupportedLinqMethods.Join
            | SupportedLinqMethods.SelectMany
            | SupportedLinqMethods.ThenBy
            | SupportedLinqMethods.ThenByDescending
            | SupportedLinqMethods.OrderBy
            | SupportedLinqMethods.Order
            | SupportedLinqMethods.OrderDescending
            | SupportedLinqMethods.OrderByDescending
            | SupportedLinqMethods.Take
            | SupportedLinqMethods.Skip
            | SupportedLinqMethods.First
            | SupportedLinqMethods.FirstOrDefault
            | SupportedLinqMethods.DefaultIfEmpty
            | SupportedLinqMethods.Last
            | SupportedLinqMethods.LastOrDefault
            | SupportedLinqMethods.Single
            | SupportedLinqMethods.SingleOrDefault
            | SupportedLinqMethods.Append
            | SupportedLinqMethods.Concat
            | SupportedLinqMethods.GroupBy
            | SupportedLinqMethods.Except
            | SupportedLinqMethods.ExceptBy
            | SupportedLinqMethods.Intersect
            | SupportedLinqMethods.IntersectBy
            | SupportedLinqMethods.Cast
            | SupportedLinqMethods.OfType
            | SupportedLinqMethods.Include
            | SupportedLinqMethods.ThenInclude
            | SupportedLinqMethods.Exclude
            | SupportedLinqMethods.ThenExclude
                -> false
        | _other -> false

    /// <summary>
    /// Checks if an Aggregate expression is a special call to ExplainQueryPlan.
    /// </summary>
    /// <param name="expression">The expression to check.</param>
    /// <returns>True if it's an ExplainQueryPlan call, otherwise false.</returns>
    let internal isAggregateExplainQuery (expression: Expression) =
        match expression with
        | :? MethodCallExpression as expression ->
            expression.Arguments.Count > 2 
            && expression.Arguments.[1] :? ConstantExpression 
            && Object.ReferenceEquals((expression.Arguments.[1] :?> ConstantExpression).Value, (QueryPlan.ExplainQueryPlanReference :> obj)) 
            && expression.Arguments.[2].NodeType = ExpressionType.Quote
        | _ -> false

    /// <summary>
    /// Checks if an Aggregate expression is a special call to GetSQL.
    /// </summary>
    /// <param name="expression">The expression to check.</param>
    /// <returns>True if it's a GetSQL call, otherwise false.</returns>
    let internal isGetGeneratedSQLQuery (expression: Expression) =
        match expression with
        | :? MethodCallExpression as expression ->
            expression.Arguments.Count > 2 
            && expression.Arguments.[1] :? ConstantExpression 
            && Object.ReferenceEquals((expression.Arguments.[1] :?> ConstantExpression).Value, (QueryPlan.GetGeneratedSQLReference :> obj)) 
            && expression.Arguments.[2].NodeType = ExpressionType.Quote
        | _ -> false

    let internal tableExists (connection: SqliteConnection) (tableName: string) =
        connection.QueryFirst<int64>(
            "SELECT CASE WHEN EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name) THEN 1 ELSE 0 END",
            {| name = tableName |}) = 1L

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

    /// Context captured during query translation for post-query relation batch loading.
    [<Struct>]
    type internal BatchLoadContext = {
        OwnerTable: string
        OwnerType: Type
        ExcludedPaths: HashSet<string>
        IncludedPaths: HashSet<string>
        WhitelistMode: bool
        HasSingleRelations: bool
        HasManyRelations: bool
        /// When true, all DBRef single-relation properties are hydrated inline in the SQL
        /// via correlated subqueries. The provider must skip batchLoadDBRefProperties.
        SingleRelationsHydrated: bool
        /// When true, all DBRefMany collection properties are hydrated inline in the SQL
        /// via a HydrationJSON column. The provider must skip batchLoadDBRefManyProperties.
        ManyRelationsHydrated: bool
    }

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
                            // Only share if canonical table exists AND the current per-property
                            // link table does NOT exist (meaning bootstrap chose shared).
                            // This aligns with R43-3b: if bootstrap created a per-property table,
                            // use it; if bootstrap used the shared canonical table, share.
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

    /// Maximum nesting depth for hydration correlated subqueries (matches batch load maxRecursiveDepth).
    let private maxHydrationDepth = 5

    /// Build a hydrated Value expression by embedding correlated subqueries for DBRef properties.
    /// Each DBRef property that should be loaded gets a ScalarSubquery that returns
    /// jsonb_array(target.Id, target.Value) correlated on the FK in the owner's Value JSON.
    /// Multi-hop: recursive — the target's Value is itself hydrated for its own DBRef properties.
    /// Edge case SA-05: stops recursion at maxHydrationDepth.
    let rec private buildHydrationValueExpr
        (ctx: QueryContext)
        (ownerType: Type)
        (ownerTable: string)
        (ownerValueExpr: SqlExpr)
        (depth: int)
        (prefix: string)
        (aliasCounter: byref<int>)
        : SqlExpr =

        // Edge case SA-05: depth cap — stop nesting, leave FK ids as-is.
        if depth >= maxHydrationDepth then ownerValueExpr
        else

        let singleProps =
            ownerType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
            |> Array.filter (fun p -> DBRefTypeHelpers.isDBRefType p.PropertyType)

        if singleProps.Length = 0 then ownerValueExpr
        else

        // Collect jsonb_set arguments: (base, path1, value1, path2, value2, ...)
        let args = ResizeArray<SqlExpr>()
        args.Add(ownerValueExpr)

        for prop in singleProps do
            let path = if prefix = "" then prop.Name else prefix + "." + prop.Name

            // Edge case SA-03: excluded path — no subquery emitted.
            if QueryableHelperPreprocess.shouldLoadRelationPath ctx path then
                let targetType = (GenericTypeArgCache.Get prop.PropertyType).[0]

                // Resolve target table from relation metadata.
                // Edge case: missing metadata → deterministic reject (per AGORA contract).
                let targetTable =
                    match ctx.TryResolveRelationTarget(ownerTable, prop.Name) with
                    | Some t -> t
                    | None ->
                        raise (NotSupportedException(
                            $"Error: Missing relation metadata for '{ownerType.Name}.{prop.Name}'.\nReason: Cannot hydrate DBRef without relation target mapping.\nFix: Ensure collection is properly initialized before querying."))

                aliasCounter <- aliasCounter + 1
                let tAlias = sprintf "_ht%d" aliasCounter

                // FK extraction from owner: jsonb_extract(ownerValue, '$.PropName')
                let fkExpr =
                    SqlExpr.FunctionCall("jsonb_extract",
                        [ownerValueExpr; SqlExpr.Literal(SqlLiteral.String ("$." + prop.Name))])

                // Recursive multi-hop: hydrate target type's DBRef properties within the subquery.
                let targetValueExpr =
                    buildHydrationValueExpr ctx targetType targetTable
                        (SqlExpr.Column(Some tAlias, "Value"))
                        (depth + 1) path &aliasCounter

                // Subquery projection: jsonb_array(t.Id, <hydrated_target_value>)
                // Deserializer recognizes [id, entityJson] as DBRef.Loaded(id, entity).
                let subqueryProjection =
                    SqlExpr.FunctionCall("jsonb_array",
                        [SqlExpr.Column(Some tAlias, "Id"); targetValueExpr])

                // Correlated WHERE: t.Id = jsonb_extract(owner.Value, '$.PropName')
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

                // COALESCE(subquery, raw_fk) — preserves raw FK when target missing/null.
                // When FK is null (DBRef.None): subquery returns NULL, COALESCE returns NULL → deserialized as None.
                // When FK is valid: subquery returns [id, entity] → deserialized as Loaded.
                // When FK is dangling: subquery returns NULL, COALESCE returns raw int → deserialized as Unloaded.
                let coalesceExpr =
                    SqlExpr.Coalesce(
                        SqlExpr.ScalarSubquery subquerySelect,
                        [fkExpr])

                // Add path/value pair to jsonb_set.
                args.Add(SqlExpr.Literal(SqlLiteral.String ("$." + prop.Name)))
                args.Add(coalesceExpr)

        // Edge case: all props excluded → no jsonb_set needed.
        if args.Count <= 1 then
            ownerValueExpr
        else
            SqlExpr.FunctionCall("jsonb_set", args |> Seq.toList)

    /// Build a HydrationJSON projection expression for DBRefMany properties.
    /// Returns Some(json_object('Prop1', COALESCE(subquery, jsonb('[]')), ...)) if any many-relations
    /// should be loaded, or None if all are excluded.
    /// Each subquery: SELECT json_group_array(json_object('Id', t.Id, 'Value', json_quote(t.Value)))
    ///   FROM "LinkTable" lnk INNER JOIN "Target" t ON t.Id = lnk.TargetColumn
    ///   WHERE lnk.OwnerColumn = o.Id
    let private buildManyHydrationProjection
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
            if QueryableHelperPreprocess.shouldLoadRelationPath ctx prop.Name then
                let targetType = (GenericTypeArgCache.Get prop.PropertyType).[0]

                // Resolve link table and target table from relation metadata.
                let linkTable =
                    match ctx.TryResolveRelationLink(ownerTable, prop.Name) with
                    | Some t -> t
                    | None ->
                        raise (NotSupportedException(
                            $"Error: Missing relation link metadata for '{ownerType.Name}.{prop.Name}'.\nReason: Cannot hydrate DBRefMany without link table mapping.\nFix: Ensure collection is properly initialized before querying."))

                let targetTable =
                    match ctx.TryResolveRelationTarget(ownerTable, prop.Name) with
                    | Some t -> t
                    | None ->
                        raise (NotSupportedException(
                            $"Error: Missing relation target metadata for '{ownerType.Name}.{prop.Name}'.\nReason: Cannot hydrate DBRefMany without target table mapping.\nFix: Ensure collection is properly initialized before querying."))

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

                // Subquery: SELECT json_group_array(json_object('Id', t.Id, 'Value', json_quote(t.Value)))
                //   FROM "LinkTable" AS _hlnk INNER JOIN "Target" AS _ht ON _ht.Id = _hlnk.TargetColumn
                //   WHERE _hlnk.OwnerColumn = o.Id
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

                // COALESCE(subquery, jsonb('[]')) — empty array when no linked targets.
                let coalesceExpr =
                    SqlExpr.Coalesce(
                        SqlExpr.ScalarSubquery subquerySelect,
                        [SqlExpr.FunctionCall("jsonb", [SqlExpr.Literal(SqlLiteral.String "[]")])])

                // Add property name + value to json_object args.
                args.Add(SqlExpr.Literal(SqlLiteral.String prop.Name))
                args.Add(coalesceExpr)

        if args.Count = 0 then None
        else Some (SqlExpr.FunctionCall("json_object", args |> Seq.toList))

    /// <summary>
    /// Initiates the translation of a LINQ expression tree to an SQL query string and a dictionary of parameters.
    /// </summary>
    /// <param name="source">The source collection of the query.</param>
    /// <param name="expression">The LINQ expression to translate.</param>
    /// <returns>A tuple containing the generated SQL string and the dictionary of parameters.</returns>
    let internal startTranslationCore (metadataConnection: SqliteConnection) (source: ISoloDBCollection<'T>) (expression: Expression) =
        let variables = Dictionary<string, obj>(16)

        let struct (isExplainQueryPlan, expression) =
            match expression with
            | :? MethodCallExpression as expression when isAggregateExplainQuery expression ->
                struct (true, expression.Arguments.[0])
            | :? MethodCallExpression as expression when isGetGeneratedSQLQuery expression ->
                struct (false, expression.Arguments.[0])
            | _ -> struct (false, expression)

        // Compute valueDecodedType AFTER unwrapping GetSQL/ExplainQueryPlan so the
        // hydration path sees the real element type, not the Aggregate wrapper type.
        let valueDecodedType =
            if typedefof<IQueryable>.IsAssignableFrom expression.Type then
                GenericTypeArgCache.Get expression.Type |> Array.head
            else
                expression.Type

        let ctx = QueryContext.SingleSource(source.Name)
        let hasRelations = hasRelationProperties typeof<'T>
        if hasRelations then
            let relationTx: Relations.RelationTxContext = {
                Connection = metadataConnection
                OwnerTable = source.Name
                OwnerType = typeof<'T>
                InTransaction =
                    match metadataConnection with
                    | :? TransactionalConnection -> true
                    | :? CachingDbConnection as cc -> cc.InsideTransaction
                    | _ -> false
            }
            Relations.withRelationSqliteWrap "build" "startTranslation.ensureSchemaForOwnerType" (fun () ->
                Relations.ensureSchemaForOwnerType relationTx typeof<'T>
            )

        preloadQueryContextMetadata ctx metadataConnection

        // Build the inner query as a SqlSelect DU.
        let innerSelect = translateQuery<'T> ctx variables expression

        let shape = getRelationShape typeof<'T>
        let hasSingleRelations = hasRelations && shape.HasSingle
        let hasManyRelations = hasRelations && shape.HasMany

        // Build hydrated value expression for DBRef single-relation properties.
        // Correlated subqueries embed target entity data directly in the Value column,
        // eliminating N+1 batch-load queries for DBRef relations on the queryable path.
        let mutable hydrationAliasCounter = 0
        let singleRelationsHydrated =
            hasSingleRelations && not (QueryTranslator.isPrimitiveSQLiteType valueDecodedType)

        let effectiveValueDecodedExpr =
            if singleRelationsHydrated then
                let ownerValueExpr = SqlExpr.Column(Some "o", "Value")
                let hydratedValue =
                    buildHydrationValueExpr ctx typeof<'T> (source.Name)
                        ownerValueExpr 0 "" &hydrationAliasCounter
                if hydrationAliasCounter = 0 then
                    // No hydration happened (all excluded or no matching props).
                    extractValueAsJsonDu valueDecodedType
                else
                    // Wrap hydrated value with json_extract for ValueJSON output.
                    SqlExpr.FunctionCall("json_extract",
                        [hydratedValue; SqlExpr.Literal(SqlLiteral.String "$")])
            else
                extractValueAsJsonDu valueDecodedType

        // Build HydrationJSON projection for DBRefMany properties.
        // json_object('Prop', COALESCE(json_group_array(...), jsonb('[]')), ...)
        let mutable manyAliasCounter = hydrationAliasCounter
        let manyRelationsHydrated =
            hasManyRelations && not (QueryTranslator.isPrimitiveSQLiteType valueDecodedType)
        let manyHydrationProjection =
            if manyRelationsHydrated then
                let ownerIdExpr = SqlExpr.Column(Some "o", "Id")
                buildManyHydrationProjection ctx typeof<'T> (source.Name)
                    ownerIdExpr &manyAliasCounter
            else
                None

        // Build the outer wrapper: SELECT Id/(-1), valueDecoded as ValueJSON [, HydrationJSON] FROM (inner)
        let baseProjections =
            if doesNotReturnIdFn expression then
                [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                 { Alias = Some "ValueJSON"; Expr = effectiveValueDecodedExpr }]
            else
                [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }
                 { Alias = Some "ValueJSON"; Expr = effectiveValueDecodedExpr }]

        let outerProjections =
            match manyHydrationProjection with
            | Some hydExpr ->
                baseProjections @ [{ Alias = Some "HydrationJSON"; Expr = hydExpr }]
            | None ->
                baseProjections

        let outerCore = mkCore outerProjections (Some (DerivedTable(innerSelect, "o")))
        let outerSelect = wrapCore outerCore

        // Emit to string via the minimal emitter.
        let sb = StringBuilder(256)
        let modelTableNames = collectIndexModelTableNames source.Name outerSelect
        let indexModel = SoloDatabase.IndexModel.loadModelForTables metadataConnection modelTableNames
        // Edge case 18: ExplainQueryPlan prefix
        if isExplainQueryPlan then
            sb.Append "EXPLAIN QUERY PLAN " |> ignore
        emitSelectToSb sb variables indexModel outerSelect

        let actuallyHydrated = singleRelationsHydrated && hydrationAliasCounter > 0
        let actuallyManyHydrated = manyRelationsHydrated && manyHydrationProjection.IsSome

        let batchLoadContext =
            if hasSingleRelations || hasManyRelations then
                ValueSome {
                    OwnerTable = source.Name
                    OwnerType = typeof<'T>
                    ExcludedPaths = new HashSet<string>(ctx.ExcludedPaths, StringComparer.Ordinal)
                    IncludedPaths = new HashSet<string>(ctx.IncludedPaths, StringComparer.Ordinal)
                    WhitelistMode = ctx.WhitelistMode
                    HasSingleRelations = hasSingleRelations
                    HasManyRelations = hasManyRelations
                    SingleRelationsHydrated = actuallyHydrated
                    ManyRelationsHydrated = actuallyManyHydrated
                }
            else
                ValueNone

        sb.ToString(), variables, batchLoadContext

    let internal startTranslation (source: ISoloDBCollection<'T>) (expression: Expression) =
        use metadataConnection = source.GetInternalConnection()
        startTranslationCore metadataConnection source expression

    let internal startTranslationWithConnection (metadataConnection: SqliteConnection) (source: ISoloDBCollection<'T>) (expression: Expression) =
        startTranslationCore metadataConnection source expression
