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
open SoloDatabase.SqlModel

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
            | SupportedLinqMethods.Union
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
        | :? RelationQueryExpression as directive -> isIdentityOwnerRowsetExpr directive.Source
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

    /// Each delivered row selects one of its actual authored physical sources.
    type internal BatchLoadPlan = {
        Owners: BatchLoadContext array
    }

    // Shared hydration SQL builders and metadata preloader are in HydrationSqlBuilder module.

    // Shared hydration builders are in HydrationSqlBuilder module.
    // Inline wrappers needed because byref parameters can't be aliased as first-class values.

    /// <summary>
    /// Initiates the translation of a LINQ expression tree to an SQL query string and a dictionary of parameters.
    /// </summary>
    /// <param name="source">The source collection of the query.</param>
    /// <param name="expression">The LINQ expression to translate.</param>
    /// <returns>A tuple containing the generated SQL string and the dictionary of parameters.</returns>
    let private translateWithBindings (bindQueryValue: QueryValueBinder voption) generalPrefixMatch (metadataConnection: NativeSqliteConnection) (source: ISoloDBCollection<'T>) (expression: Expression) =
        let variables = Dictionary<string, obj>(16)

        let valueDecodedType =
            if typedefof<IQueryable>.IsAssignableFrom expression.Type then
                TypeMetadata.GenericTypeArgCache.Get expression.Type |> Array.head
            else
                expression.Type

        // No static gate here: translation discovers additional roots (Join, GroupJoin,
        // SelectMany, nested builders) while running and resolves them against this context,
        // so the root type does not bound which collections will be consulted. The resolver
        // costs nothing until something is actually asked for.
        let ordinaryBindings = bindQueryValue.IsNone
        let bindQueryValue, acquire =
            match bindQueryValue with
            | ValueSome binder -> ValueSome binder, id
            | ValueNone ->
                let binder, acquire = QueryableValueBindings.create variables
                ValueSome binder, acquire
        let ctx = { QueryContext.SingleSource(source.Name) with MetadataSource = ValueSome (RelationMetadataSource metadataConnection); BindQueryValue = bindQueryValue; GeneralPrefixMatch = generalPrefixMatch || ordinaryBindings }
        // Mark this as the OUTERMOST translation context. Cleared in CloneForSubquery
        // so nested cardinality emit sites can detect they are nested and emit bare
        // scalars (default(T) propagation) rather than typed exceptions.
        ctx.IsAtTopLevel <- true
        let hasRelations = HydrationSqlMetadata.hasRelationProperties typeof<'T>
        if hasRelations then
            let relationTx: RelationsTypes.RelationTxContext = {
                Connection = metadataConnection
                OwnerTable = source.Name
                OwnerType = typeof<'T>
                InTransaction =
                    match metadataConnection with
                    | :? NativeCachingDbConnection as cc -> cc.InsideTransaction
                    | _ -> false
                Preparation = RelationsTypes.RelationPreparation()
                CollectionFactory = None
            }
            RelationsCore.withRelationSqliteWrap "build" "startTranslation.ensureSchemaForOwnerType" (fun () ->
                RelationsCore.ensureSchemaForOwnerType relationTx typeof<'T>
            )

        // Build the inner query as a SqlSelect DU.
        let completed = translateQuery<'T> (ref 0) ctx variables expression
        let innerSelect = completed.Query |> acquire
        let ownerInput = QueryTranslatorSerialization.scopeSerializationInput (Some "o") OuterLayer completed.SerializationInput
        let owner = ownerInput.PhysicalOwner
        let ownerSources = owner |> ValueOption.map _.Sources |> ValueOption.defaultValue Array.empty
        let selectedOwner source =
            owner |> ValueOption.map (fun owner -> QueryTranslatorPhysicalSource.selectsSource owner source)
                  |> ValueOption.defaultValue (SqlExpr.Literal(SqlLiteral.Integer 0L))
        let mutable hydrationAliasCounter = 0
        let ownerContexts = ownerSources |> Array.map (fun source ->
            let shape = HydrationSqlMetadata.getRelationShape source.OwnerType
            { OwnerTable = source.TableName; OwnerType = source.OwnerType
              ExcludedPaths = new HashSet<string>(source.Loading.Excluded, StringComparer.Ordinal)
              IncludedPaths = new HashSet<string>(source.Loading.Included, StringComparer.Ordinal)
              WhitelistMode = source.Loading.Whitelist; HasSingleRelations = shape.HasSingle; HasManyRelations = shape.HasMany
              SingleRelationsHydrated = false; ManyRelationsHydrated = false })
        let mutable effectiveValueDecodedExpr = extractValueAsJsonDu ownerInput valueDecodedType
        for index = ownerContexts.Length - 1 downto 0 do
            let context = ownerContexts[index]
            if context.HasSingleRelations && not (QueryTranslatorBaseTypes.isPrimitiveSQLiteType valueDecodedType) && not (DBRefTypeHelpers.isFileRefType valueDecodedType) then
                let before = hydrationAliasCounter
                let ownerContext = { ctx with ExcludedPaths = context.ExcludedPaths; IncludedPaths = context.IncludedPaths; WhitelistMode = context.WhitelistMode }
                let hydrated = HydrationSqlBuilder.buildHydrationValueExpr metadataConnection ownerContext context.OwnerType context.OwnerTable
                                   (SqlExpr.Column(Some "o", "Value")) 0 "" &hydrationAliasCounter
                if before <> hydrationAliasCounter then
                    effectiveValueDecodedExpr <- SqlExpr.CaseExpr((selectedOwner ownerSources[index], SqlExpr.FunctionCall("json", [hydrated])), [], Some effectiveValueDecodedExpr)
                    ownerContexts[index] <- { context with SingleRelationsHydrated = true }
        let mutable manyAliasCounter = hydrationAliasCounter
        let manyHydrationProjection =
            if ownerContexts.Length = 1 && ownerContexts[0].HasManyRelations && ownerContexts[0].OwnerType.Equals(valueDecodedType)
               && not(QueryTranslatorBaseTypes.isPrimitiveSQLiteType valueDecodedType) && isIdentityOwnerRowsetExpr expression then
                let context = ownerContexts[0]
                let ownerContext = { ctx with ExcludedPaths = context.ExcludedPaths; IncludedPaths = context.IncludedPaths; WhitelistMode = context.WhitelistMode }
                let projection = HydrationSqlBuilder.buildManyHydrationProjection metadataConnection ownerContext context.OwnerType context.OwnerTable
                                     (SqlExpr.Column(Some "o", "Id")) &manyAliasCounter
                if projection.IsSome then ownerContexts[0] <- { context with ManyRelationsHydrated = true }
                projection
            else None
        let ownerPosition =
            Array.foldBack (fun (index, source) otherwise ->
                SqlExpr.CaseExpr((selectedOwner source, SqlExpr.Literal(SqlLiteral.Integer(int64 index))), [], Some otherwise))
                (Array.indexed ownerSources) (SqlExpr.Literal(SqlLiteral.Integer -1L))

        // Outer (Id, Value) projection. Translator emit sites for typed runtime
        // errors (Cast / cardinality / out-of-range) build their own complete
        // CASE wrappers on Id+Value inline at their callsite via addComplexFinal,
        // emitting Id=NULL+typed-payload Value when the error condition fires.
        // The materializer (JsonFunctions.fromSQLite) detects Id=NULL (rowid is
        // unforgeable — user data structurally cannot reach NULL) and dispatches
        // by the kind field on the JSON Value object.
        let rawIdExpr =
            if doesNotReturnIdFn expression then SqlExpr.Literal(SqlLiteral.Integer -1L)
            else SqlExpr.Column(None, "Id")
        let effectiveValueDecodedExpr =
            if not (doesNotReturnIdFn expression) && canExposeNullId innerSelect then
                preserveRuntimeErrorValue rawIdExpr (SqlExpr.Column(None, "Value")) effectiveValueDecodedExpr
            else effectiveValueDecodedExpr
        // Preserve the master alias shape for backward-compat with column-name
        // resolution: Alias=Some "Id" only when doesNotReturnIdFn=true (synthetic Id);
        // Alias=None for the natural Id column projection so DbObjectRow maps via
        // the implicit "Id" column name.
        let baseProjections =
            if doesNotReturnIdFn expression then
                [{ Alias = Some "Id"; Expr = rawIdExpr }
                 { Alias = Some "ValueJSON"; Expr = effectiveValueDecodedExpr }]
            else
                [{ Alias = None; Expr = rawIdExpr }
                 { Alias = Some "ValueJSON"; Expr = effectiveValueDecodedExpr }]

        let outerProjections =
            if ownerSources.Length = 0 then baseProjections
            else baseProjections @
                    [{ Alias = Some "HydrationJSON"; Expr = manyHydrationProjection |> Option.defaultValue (SqlExpr.Literal SqlLiteral.Null) }
                     { Alias = Some "_owner"; Expr = ownerPosition }]

        let outerCore = mkCore outerProjections (Some (DerivedTable(innerSelect, "o")))
        let outerSelect = wrapCore outerCore

        // Optimize and emit through the shared SELECT owner.
        let modelTableNames = collectIndexModelTableNames source.Name outerSelect
        let indexModel = RuntimeIndexModelCache.loadModelForTables metadataConnection modelTableNames
        let estimates = lazy (IndexModel.loadTableEstimates metadataConnection modelTableNames)
        let retainedPlan query =
            let counted =
                if doesNotReturnIdFn expression then QueryCountPlanning.plan source.Name indexModel estimates query
                else query
            QueryReadPlanning.plan indexModel estimates counted
        let sql = emitSelectSql variables indexModel retainedPlan outerSelect

        let batchLoadContext =
            if ownerContexts |> Array.exists (fun context -> context.HasSingleRelations || context.HasManyRelations) then
                ValueSome { Owners = ownerContexts }
            else ValueNone

        // The pre-optimizer SqlSelect is returned alongside the emitted text so callers can
        // inspect the structure the translator actually produced, before any pass rewrites it.
        sql, variables, batchLoadContext, outerSelect

    let internal startTranslationCore connection source expression =
        translateWithBindings ValueNone false connection source expression

    let internal startCompiledTranslation connection source expression bindQueryValue generalPrefixMatch =
        translateWithBindings (ValueSome bindQueryValue) generalPrefixMatch connection source expression

    let internal startTranslation (source: ISoloDBCollection<'T>) (expression: Expression) =
        use metadataConnection = (source :?> INativeConnectionSource).AcquireNativeConnection()
        startTranslationCore metadataConnection source expression

    let internal startTranslationWithConnection (metadataConnection: NativeSqliteConnection) (source: ISoloDBCollection<'T>) (expression: Expression) =
        startTranslationCore metadataConnection source expression
