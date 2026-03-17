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
            | SupportedLinqMethods.Exclude
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
        HasSingleRelations: bool
        HasManyRelations: bool
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

    /// <summary>
    /// Initiates the translation of a LINQ expression tree to an SQL query string and a dictionary of parameters.
    /// </summary>
    /// <param name="source">The source collection of the query.</param>
    /// <param name="expression">The LINQ expression to translate.</param>
    /// <returns>A tuple containing the generated SQL string and the dictionary of parameters.</returns>
    let internal startTranslationCore (metadataConnection: SqliteConnection) (source: ISoloDBCollection<'T>) (expression: Expression) =
        let variables = Dictionary<string, obj>(16)

        let valueDecodedType =
            if typedefof<IQueryable>.IsAssignableFrom expression.Type then
                GenericTypeArgCache.Get expression.Type |> Array.head
            else
                expression.Type

        let struct (isExplainQueryPlan, expression) =
            match expression with
            | :? MethodCallExpression as expression when isAggregateExplainQuery expression ->
                struct (true, expression.Arguments.[0])
            | :? MethodCallExpression as expression when isGetGeneratedSQLQuery expression ->
                struct (false, expression.Arguments.[0])
            | _ -> struct (false, expression)

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

        // Build the outer wrapper: SELECT Id/(-1), valueDecoded as ValueJSON FROM (inner)
        let valueDecodedExpr = extractValueAsJsonDu valueDecodedType
        let outerProjections =
            if doesNotReturnIdFn expression then
                [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                 { Alias = Some "ValueJSON"; Expr = valueDecodedExpr }]
            else
                [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }
                 { Alias = Some "ValueJSON"; Expr = valueDecodedExpr }]
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

        let shape = getRelationShape typeof<'T>
        let hasSingleRelations = hasRelations && shape.HasSingle
        let hasManyRelations = hasRelations && shape.HasMany

        let batchLoadContext =
            if hasSingleRelations || hasManyRelations then
                ValueSome {
                    OwnerTable = source.Name
                    OwnerType = typeof<'T>
                    ExcludedPaths = new HashSet<string>(ctx.ExcludedPaths, StringComparer.Ordinal)
                    IncludedPaths = new HashSet<string>(ctx.IncludedPaths, StringComparer.Ordinal)
                    HasSingleRelations = hasSingleRelations
                    HasManyRelations = hasManyRelations
                }
            else
                ValueNone

        sb.ToString(), variables, batchLoadContext

    let internal startTranslation (source: ISoloDBCollection<'T>) (expression: Expression) =
        use metadataConnection = source.GetInternalConnection()
        startTranslationCore metadataConnection source expression

    let internal startTranslationWithConnection (metadataConnection: SqliteConnection) (source: ISoloDBCollection<'T>) (expression: Expression) =
        startTranslationCore metadataConnection source expression
