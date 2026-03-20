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
            | SupportedLinqMethods.CountBy
            | SupportedLinqMethods.Distinct
            | SupportedLinqMethods.DistinctBy
            | SupportedLinqMethods.Where
            | SupportedLinqMethods.Select
            | SupportedLinqMethods.Join
            | SupportedLinqMethods.GroupJoin
            | SupportedLinqMethods.SelectMany
            | SupportedLinqMethods.ThenBy
            | SupportedLinqMethods.ThenByDescending
            | SupportedLinqMethods.OrderBy
            | SupportedLinqMethods.Order
            | SupportedLinqMethods.OrderDescending
            | SupportedLinqMethods.OrderByDescending
            | SupportedLinqMethods.Take
            | SupportedLinqMethods.Skip
            | SupportedLinqMethods.TakeWhile
            | SupportedLinqMethods.SkipWhile
            | SupportedLinqMethods.First
            | SupportedLinqMethods.FirstOrDefault
            | SupportedLinqMethods.ElementAt
            | SupportedLinqMethods.ElementAtOrDefault
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
            | SupportedLinqMethods.UnionBy
            | SupportedLinqMethods.Cast
            | SupportedLinqMethods.OfType
            | SupportedLinqMethods.Include
            | SupportedLinqMethods.ThenInclude
            | SupportedLinqMethods.Exclude
            | SupportedLinqMethods.ThenExclude
                -> false
        | _other -> false

    let rec private isIdentityOwnerRowsetExpr (expression: Expression) =
        let rec stripConvert (expr: Expression) =
            match expr with
            | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert || ue.NodeType = ExpressionType.ConvertChecked ->
                stripConvert ue.Operand
            | _ -> expr
        let expression = stripConvert expression
        match expression with
        | :? ConstantExpression -> true
        | :? MethodCallExpression as mce ->
            let source =
                if not (isNull mce.Object) then mce.Object
                elif mce.Arguments.Count > 0 then mce.Arguments.[0]
                else null
            match parseSupportedMethod mce.Method.Name with
            | Some SupportedLinqMethods.Include
            | Some SupportedLinqMethods.ThenInclude
            | Some SupportedLinqMethods.Exclude
            | Some SupportedLinqMethods.ThenExclude ->
                not (isNull source) && isIdentityOwnerRowsetExpr source
            | _ -> false
        | _ -> false

    let internal tableExists = HydrationSqlBuilder.tableExists

    type internal RelationShapeInfo = HydrationSqlBuilder.RelationShapeInfo
    let internal getRelationShape = HydrationSqlBuilder.getRelationShape
    let internal hasRelationProperties = HydrationSqlBuilder.hasRelationProperties

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

    // Shared hydration SQL builders and metadata preloader are in HydrationSqlBuilder module.
    let internal preloadQueryContextMetadata = HydrationSqlBuilder.preloadQueryContextMetadata

    // Shared hydration builders are in HydrationSqlBuilder module.
    // Inline wrappers needed because byref parameters can't be aliased as first-class values.

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
        let innerSelect = translateQuery<'T> (ref 0) ctx variables expression

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
                    HydrationSqlBuilder.buildHydrationValueExpr metadataConnection ctx typeof<'T> (source.Name)
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

        // Inline DBRefMany hydration is only admitted for identity owner rowsets.
        // Ordered/filtered owner queries stay on the batch-load path to preserve the
        // non-cartesian root SQL contract required by relation tests.
        let mutable manyAliasCounter = hydrationAliasCounter
        let manyRelationsHydrated =
            hasManyRelations
            && not (QueryTranslator.isPrimitiveSQLiteType valueDecodedType)
            && valueDecodedType = typeof<'T>
            && isIdentityOwnerRowsetExpr expression
        let manyHydrationProjection =
            if manyRelationsHydrated then
                let ownerIdExpr = SqlExpr.Column(Some "o", "Id")
                HydrationSqlBuilder.buildManyHydrationProjection metadataConnection ctx typeof<'T> (source.Name)
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
