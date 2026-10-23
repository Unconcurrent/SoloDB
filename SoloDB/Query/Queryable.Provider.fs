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


/// <summary>
/// An internal interface defining the contract for a SoloDB query provider.
/// </summary>
type internal ISoloDBCollectionQueryProvider =
    /// <summary>Gets the source collection object.</summary>
    abstract member Source: obj
    /// <summary>Gets additional data passed from the parent database instance.</summary>
    abstract member AdditionalData: obj
    /// <summary>Translates a LINQ expression to its SQL string without executing the query.</summary>
    abstract member TranslateToSQL: expression: Expression -> string
    /// <summary>Translates and executes EXPLAIN QUERY PLAN for a LINQ expression.</summary>
    abstract member GetExplainQueryPlan: expression: Expression -> string
    /// <summary>
    /// Returns a structural rendering of the SqlSelect the translator produced for this
    /// expression, before the optimizer pass pipeline rewrites it. Two translators that
    /// agree on emitted SQL can still disagree here, so this is the structure-level
    /// counterpart to <see cref="TranslateToSQL"/>.
    /// </summary>
    abstract member DescribeUnoptimizedSelect: expression: Expression -> string

module internal QueryableTranslation =
    let startFilterTranslationWithConnection (metadataConnection: NativeSqliteConnection) (source: ISoloDBCollection<'T>) (expression: Expression) =
        let query, variables, _, _ = QueryableTranslationCore.startTranslationWithConnection metadataConnection source expression
        query, variables

module internal QueryableExecution =
    let private openQuery (connection: NativeSqliteConnection) (query: NativeStatementSource) (parameters: obj) =
        let mutable statement = connection.OpenStatementSource(query)
        try
            match parameters with
            | :? IReadOnlyDictionary<string, obj> as values -> statement.BindValues(values)
            | _ -> statement.Bind(statement.Statement.Capture(parameters))
            statement
        with error -> statement.Abort(error); reraise()

    let private readDocument owned (statement: byref<NativeStatementScope>) : struct(Types.DbObjectRow * int * int) voption =
        try
            if statement.Step() then
                let row=statement.Row
                let storageClass=row.StorageClass(1)
                let owner = if owned then row.GetInt32 3 else -1
                ValueSome(struct(SqlRowValue.Document(row),storageClass,owner))
            else ValueNone
        with error -> statement.Abort(error); reraise()

    let private readEntity<'Elem> owned (statement: byref<NativeStatementScope>) =
        try
            if statement.Step() then
                let row = statement.Row
                let struct(document, entity) = JsonFunctions.fromSQLiteRow<'Elem> row
                let owner = if owned then row.GetInt32 3 else -1
                ValueSome(struct(document, entity, owner))
            else ValueNone
        with error -> statement.Abort(error); reraise()

    let private queryRows<'Elem> (connection: NativeSqliteConnection) query parameters owned = seq {
        let mutable statement = openQuery connection query parameters
        try
            let mutable reading = true
            while reading do
                match readEntity<'Elem> owned &statement with
                | ValueSome row -> yield row
                | ValueNone -> reading <- false
        finally statement.Dispose()
    }

    /// Both scalar and sequence delivery hydrate only the row's selected physical owner.
    let private hydrateOwners (connection: NativeSqliteConnection) inTransaction (context: QueryableTranslationCore.BatchLoadContext)
                              (entities: (int64 * obj) array) (hydration: Dictionary<int64, string>) =
        if entities.Length > 0 then
            if context.HasSingleRelations && not context.SingleRelationsHydrated then
                RelationsSync.batchLoadDBRefProperties connection context.OwnerTable context.OwnerType context.ExcludedPaths context.IncludedPaths context.WhitelistMode entities inTransaction
            if context.HasManyRelations && not context.ManyRelationsHydrated then
                RelationsSync.batchLoadDBRefManyProperties connection context.OwnerTable context.OwnerType context.ExcludedPaths context.IncludedPaths context.WhitelistMode entities inTransaction
            if context.ManyRelationsHydrated then
                RelationsFileSystemBatchLoad.batchLoadFileSystemProperties connection context.OwnerTable context.OwnerType context.ExcludedPaths context.IncludedPaths context.WhitelistMode entities RelationKind.Many ""
                HydrationManyPopulator.populateFromHydrationJson context.OwnerType entities hydration
            if context.SingleRelationsHydrated || context.ManyRelationsHydrated then
                RelationsSync.recurseLoadedRelationTargets connection context.OwnerTable context.OwnerType context.ExcludedPaths context.IncludedPaths context.WhitelistMode entities inTransaction

    let enumerate<'Source, 'Elem>
        (source: ISoloDBCollection<'Source>) (query: NativeStatementSource) (par: obj)
        (batchCtx: QueryableTranslationCore.BatchLoadPlan voption) : IEnumerable<'Elem> =
        seq {
            use connection = (source :?> INativeConnectionSource).AcquireNativeConnection()
            match batchCtx with
            | ValueSome plan ->
                let buffer = ResizeArray<'Elem>()
                let owners = Array.init plan.Owners.Length (fun _ -> ResizeArray<int64 * obj>())
                let hydration = Array.init plan.Owners.Length (fun _ -> Dictionary<int64, string>())
                let () =
                    try
                        for struct(row, entity, position) in queryRows<'Elem> connection query par batchCtx.IsSome do
                            buffer.Add entity
                            if position >= 0 && position < owners.Length && row.Id.HasValue && not(isNull(box entity))
                               && plan.Owners[position].OwnerType.IsInstanceOfType(box entity) then
                                owners[position].Add(row.Id.Value, box entity)
                                if not(isNull row.HydrationJSON) then hydration[position][row.Id.Value] <- row.HydrationJSON
                        for index = 0 to owners.Length - 1 do
                            hydrateOwners connection source.InTransaction plan.Owners[index] (owners[index].ToArray()) hydration[index]
                    with error ->
                        SQLiteToolsHandlerFaultState.tryRecordHandlerFault connection error
                        reraiseAnywhere error
                yield! buffer
            | _ ->
                for struct(_, entity, _) in queryRows<'Elem> connection query par batchCtx.IsSome do
                    yield entity
        }

    let terminalDefaultExpression (expression: Expression) =
        let isExpressionLikeArgument (expr: Expression) =
            match expr with
            | :? LambdaExpression -> true
            | _ -> typeof<Expression>.IsAssignableFrom expr.Type
        match expression with
        | :? MethodCallExpression as call when call.Method.Name = "FirstOrDefault" || call.Method.Name = "SingleOrDefault" ->
            match call.Arguments.Count with
            | 2 when not (isExpressionLikeArgument call.Arguments.[1]) -> Some call.Arguments.[1]
            | 3 when isExpressionLikeArgument call.Arguments.[1] -> Some call.Arguments.[2]
            | _ -> None
        | _ -> None

    let scalar<'Source, 'TResult>
        (source: ISoloDBCollection<'Source>) (query: NativeStatementSource) (variables: obj)
        (batchCtx: QueryableTranslationCore.BatchLoadPlan voption)
        (methodName: string) (getTerminalDefaultValue: unit -> 'TResult) : 'TResult =
        let batchLoadSingle (connection: NativeSqliteConnection) (row: Types.DbObjectRow) position (entity: 'TResult) =
            match batchCtx with
            | ValueSome plan when position >= 0 && position < plan.Owners.Length && not(isNull(box entity)) && row.Id.HasValue
                                  && plan.Owners[position].OwnerType.IsInstanceOfType(box entity) ->
                let hydration = Dictionary<int64, string>()
                if not(isNull row.HydrationJSON) then hydration[row.Id.Value] <- row.HydrationJSON
                hydrateOwners connection source.InTransaction plan.Owners[position] [|(row.Id.Value, box entity)|] hydration
            | _ -> ()
            entity

        use connection = (source :?> INativeConnectionSource).AcquireNativeConnection()
        let mutable statement = openQuery connection query variables
        try
            let row =
                try
                    let row = readDocument batchCtx.IsSome &statement
                    match methodName, row with
                    | ("Single" | "SingleOrDefault"), ValueSome _ ->
                        if (readDocument batchCtx.IsSome &statement).IsSome then
                            raise (InvalidOperationException("Sequence contains more than one element"))
                    | _ -> ()
                    statement.Dispose()
                    row
                with error -> statement.Abort(error); reraise()
            match row with
            | ValueSome struct(row,storageClass,position) ->
                let entity = JsonFunctions.fromSQLiteCaptured<'TResult> storageClass row
                batchLoadSingle connection row position entity
            | ValueNone when methodName.EndsWith("OrDefault", StringComparison.Ordinal) -> getTerminalDefaultValue()
            | ValueNone when methodName = "MinBy" || methodName = "MaxBy" ->
                if typeof<'TResult>.IsValueType && isNull (TypeMetadata.nullableUnderlying(typeof<'TResult>)) then
                    raise (InvalidOperationException("Sequence contains no elements"))
                Unchecked.defaultof<'TResult>
            | ValueNone -> raise (InvalidOperationException("Sequence contains no elements"))
        with error ->
            SQLiteToolsHandlerFaultState.tryRecordHandlerFault connection error
            statement.Abort(error)
            reraise()

type internal SoloDBCollectionQueryProvider<'T>(source: ISoloDBCollection<'T>, data: obj) =
    // Non-generic entry points retain the generic provider's construction and execution contracts.
    static member private CreateTypedQuery<'Elem>(provider: IQueryProvider, expression: Expression) : IQueryable =
        provider.CreateQuery<'Elem>(expression) :> IQueryable

    static member private ExecuteTypedResult<'Result>(provider: IQueryProvider, expression: Expression) : obj =
        box (provider.Execute<'Result>(expression))

    interface ISoloDBCollectionQueryProvider with
        override this.Source = source
        override this.AdditionalData = data
        override this.TranslateToSQL(expression: Expression) =
            let query, _, _, _ = QueryableTranslationCore.startTranslation source expression
            query
        override this.DescribeUnoptimizedSelect(expression: Expression) =
            let _, _, _, select = QueryableTranslationCore.startTranslation source expression
            sprintf "%A" select
        override this.GetExplainQueryPlan(expression: Expression) =
            let query, variables, _, _ = QueryableTranslationCore.startTranslation source expression
            let explainQuery = "EXPLAIN QUERY PLAN " + query
            use connection = (source :?> INativeConnectionSource).AcquireNativeConnection()
            let mutable statement = connection.OpenStatement(explainQuery)
            try
                statement.BindValues(variables)
                let details = System.Text.StringBuilder()
                while statement.Step() do
                    if details.Length > 0 then details.Append(";\n") |> ignore
                    details.Append(statement.Row.GetString(3)) |> ignore
                statement.Dispose()
                details.ToString()
            with error -> statement.Abort(error); reraise()

    interface SoloDBQueryProvider
    member internal _.ExecuteEnumetable<'Elem> (query: string) (par: obj) (batchCtx: QueryableTranslationCore.BatchLoadPlan voption) : IEnumerable<'Elem> =
        QueryableExecution.enumerate<'T, 'Elem> source (NativeStatementSource.Text query) par batchCtx

    interface IQueryProvider with
        member this.CreateQuery<'TResult>(expression: Expression) : IQueryable<'TResult> =
            SoloDBCollectionQueryable<'T, 'TResult>(this, expression)

        member this.CreateQuery(expression: Expression) : IQueryable =
            let elementType = expression.Type.GetGenericArguments().[0]
            let create = QueryProviderMetadata<SoloDBCollectionQueryProvider<'T>>.CreateQuery(elementType)
            create.Invoke(this, expression)

        member this.Execute(expression: Expression) : obj =
            let resultType =
                if typeof<IQueryable>.IsAssignableFrom expression.Type then
                    QueryProviderMetadata<SoloDBCollectionQueryProvider<'T>>.SequenceResultType((TypeMetadata.GenericTypeArgCache.Get expression.Type).[0])
                else expression.Type
            let execute = QueryProviderMetadata<SoloDBCollectionQueryProvider<'T>>.ExecuteResult(resultType)
            execute.Invoke(this, expression)

        member this.Execute<'TResult>(expression: Expression) : 'TResult =
            let query, variables, batchCtx, _ = QueryableTranslationCore.startTranslation source expression

            #if DEBUG
            if System.Diagnostics.Debugger.IsAttached then
                printfn "%s" query
            #endif

            try
                match typeof<'TResult> with
                | t when t.IsGenericType && typeof<IEnumerable<'T>>.Equals typeof<'TResult> ->
                    let result = this.ExecuteEnumetable<'T> query variables batchCtx
                    result :> obj :?> 'TResult
                | t when t.IsGenericType && typedefof<IEnumerable<_>>.Equals typedefof<'TResult> ->
                    let elemType = (TypeMetadata.GenericTypeArgCache.Get t).[0]
                    let execute = QueryProviderMetadata<SoloDBCollectionQueryProvider<'T>>.ExecuteSequence(elemType)
                    execute.Invoke(this, query, variables, batchCtx) :?> 'TResult
                | _other ->
                    let methodName =
                        match expression with
                        | :? MethodCallExpression as call -> call.Method.Name
                        | _ -> "Execute"
                    let getDefault() =
                        match QueryableExecution.terminalDefaultExpression expression with
                        | Some value -> ExpressionEvaluation.evaluateExpr<'TResult> value
                        | None -> Unchecked.defaultof<'TResult>
                    QueryableExecution.scalar source (NativeStatementSource.Text query) variables batchCtx methodName getDefault

            finally
                ()

/// <summary>
/// The internal implementation of <c>IQueryable</c> and <c>IOrderedQueryable</c> for SoloDB collections.
/// </summary>
/// <param name="provider">The query provider that created this queryable.</param>
/// <param name="expression">The expression tree representing the query.</param>
and internal SoloDBCollectionQueryable<'I, 'T>(provider: IQueryProvider, expression: Expression) =
    interface IOrderedQueryable<'T>

    interface IQueryable<'T> with
        member _.Provider = provider
        member _.Expression = expression
        member _.ElementType = typeof<'T>

    interface IEnumerable<'T> with
        member this.GetEnumerator() =
            let items = (provider.Execute<IEnumerable<'T>>(expression))
            items.GetEnumerator()

    interface IEnumerable with
        member this.GetEnumerator() =
            (this :> IEnumerable<'T>).GetEnumerator() :> IEnumerator

/// <summary>
/// A private, sealed class representing the root of a query, which is always a collection.
/// </summary>
/// <param name="c">The source collection.</param>
and [<Sealed>] private RootQueryable<'T>(c: ISoloDBCollection<'T>) =
    member val Source = c

    interface IRootQueryable with
        override this.SourceTableName = this.Source.Name

    interface IQueryable<'T> with
        member _.Provider = null
        member _.Expression = Expression.Constant(c)
        member _.ElementType = typeof<'T>

    interface IEnumerable<'T> with
        member this.GetEnumerator() =
            Enumerable.Empty<'T>().GetEnumerator()

    interface IEnumerable with
        member this.GetEnumerator() =
            Enumerable.Empty<'T>().GetEnumerator()
