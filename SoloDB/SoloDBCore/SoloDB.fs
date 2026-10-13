namespace SoloDatabase

open SoloDatabase.JsonSerializator
open System.Linq.Expressions
open System
open System.Collections.Generic
open System.Collections
open SoloDatabase.Types
open System.IO
open System.Text
open NativeSQLiteTools
open JsonFunctions
open FileStorage
open SoloDatabase.SqlModel
open Connections
open Utils
open System.Reflection
open System.Data
open System.Globalization
open System.Linq
open SoloDatabase.Attributes
open System.Collections.Concurrent

/// <summary>
/// Represents a collection of documents of type 'T stored in the database. Provides methods for CRUD operations, indexing, and LINQ querying.
/// </summary>
/// <param name="connection">The connection provider.</param>
/// <param name="name">The name of the collection (table name).</param>
/// <param name="connectionString">The database connection string.</param>
/// <param name="parentData">Data from the parent SoloDB instance.</param>
type internal Collection<'T>(connection: NativeConnection, name: string, connectionString: string, parentData: SoloDBToCollectionData) as this =
    let hasRelations = RelationsSchemaValidator.getReferenceMembers typeof<'T> |> Array.exists(fun property -> DBRefTypeHelpers.isAnyRelationRefType property.ReferenceType)
    let relationCollectionFactory (conn: NativeSqliteConnection) =
        let rec onActiveConnection source =
            match source with
            | NativeGuarded (guard, inner) -> NativeGuarded(guard, onActiveConnection inner)
            | _ -> NativeTransactional conn
        let targetConnection = onActiveConnection connection
        // One target collection per type and table for this transaction-scoped factory.
        let collections = Dictionary<struct (Type * string), obj>()
        { new RelationsTypes.IRelationCollectionFactory with
            member _.CreateCollection<'U>(collectionName: string) =
                let key = struct (typeof<'U>, collectionName)
                System.Threading.Monitor.Enter collections
                try
                    let mutable existing = null
                    if collections.TryGetValue(key, &existing) then
                        existing :?> ISoloDBCollection<'U>
                    else
                        let created = Collection<'U>(targetConnection, collectionName, connectionString, parentData) :> ISoloDBCollection<'U>
                        collections.Add(key, created)
                        created
                finally
                    System.Threading.Monitor.Exit collections }
    let scaffold = CollectionScaffold<'T>(connection, connectionString, name, hasRelations, relationCollectionFactory)

    member val private SoloDBQueryable = SoloDBCollectionQueryable<'T, 'T>(SoloDBCollectionQueryProvider(this, parentData), Expression.Constant(RootQueryable<'T>(this))) :> IOrderedQueryable<'T>
    member val private ConnectionString = connectionString
    /// <summary>Gets the name of the collection.</summary>
    member val Name = name
    /// <summary>Gets a value indicating whether the collection is operating within a transaction.</summary>
    member val InTransaction =
        match connection with
        | NativeTransactional _ -> true
        | NativeGuarded (_, inner) ->
            match inner with
            | NativeTransactional _ -> true
            | _ -> false
        | NativePooled _ -> false
    /// <summary>Gets a value indicating whether type information should be included during serialization for documents in this collection.</summary>
    member val IncludeType = TypeMetadata.requiresTypeInformationFor<'T>
    /// <summary>True when the document type declares DBRef/DBRefMany properties.</summary>
    member val internal HasRelations = hasRelations

    member private this.HasIncomingRelations(connection: NativeSqliteConnection) =
        scaffold.HasIncomingRelations(connection)

    member internal this.RequiresRelationDeleteHandling(connection: NativeSqliteConnection) =
        scaffold.RequiresRelationDeleteHandling(connection)

    member internal this.SelectMutationRows(connection: NativeSqliteConnection, filter: Expression<Func<'T, bool>>, takeOne: bool) =
        let filtered = Queryable.Where(this.SoloDBQueryable, filter)
        let expression =
            if takeOne then Queryable.Take(filtered, 1).Expression
            else filtered.Expression
        let query, variables = QueryableTranslation.startFilterTranslationWithConnection connection this expression
        let mutable statement = connection.OpenStatement(query)
        try
            statement.BindValues(variables)
            let rows = ResizeArray<DbObjectRow>()
            while statement.Step() do rows.Add(SqlRowValue.Document(statement.Row))
            statement.Dispose()
            rows.ToArray()
        with error -> statement.Abort(error); reraise()

    member internal this.ExecuteJsonUpdateManyByRows(connection: NativeSqliteConnection, rows: DbObjectRow array, expressions: ResizeArray<Expression<System.Action<'T>>>) =
        // No expressions or no selected rows means there is nothing to update, and the corridor
        // emits no statement at all rather than one with an empty id list.
        if expressions.Count = 0 || rows.Length = 0 then
            0
        else
            // One dictionary for the whole statement, filled in the order the SQL reads: every
            // assignment in source order, then the row ids. The names are observable, so the
            // order they are allocated in is part of the contract rather than an implementation
            // detail.
            let variables = Dictionary<string, obj>()

            let assignments =
                [ for expression in expressions do
                    yield! QueryTranslator.translateUpdateAssignments name expression variables ]

            let setValue =
                SqlExpr.FunctionCall("jsonb_set",
                    SqlExpr.Column(None, "Value")
                    :: [ for (path, value) in assignments do yield path; yield value ])

            let idParameters =
                [ for i in 0 .. rows.Length - 1 do
                    let key = $"id{i}"
                    variables.[key] <- rows.[i].Id.Value :> obj
                    yield SqlExpr.Parameter key ]

            let where =
                match idParameters with
                | head :: tail -> SqlExpr.InList(SqlExpr.Column(None, "Id"), head, tail)
                | [] -> SqlExpr.Literal(SqlLiteral.Boolean false)

            let statement =
                UpdateStmt
                    { TableName = name
                      SetClauses = [ ("Value", setValue) ]
                      Where = Some where }

            StatementExecution.execute connection statement variables

    /// <summary>Gets the event registration API for this collection.</summary>
    member val internal Events: ISoloDBCollectionEvents<'T> =
        let createDb = Collection<'T>.CreateEventDatabase(connectionString, parentData, Some name)

        CollectionEventSystem<'T>(
            name,
            parentData.EventSystem,
            createDb)

    static member internal CreateEventDatabase(connectionString, parentData, knownCollectionName) =
        let factory guardedConnection =
            { new IEventDbCollectionFactory with
                member _.CreateCollection<'U>(collectionName: string) =
                    Collection<'U>(guardedConnection, collectionName, connectionString, parentData) :> ISoloDBCollection<'U> }
        EventContextDatabase.create connectionString parentData knownCollectionName factory

    /// <summary>Gets the internal connection provider for this collection.</summary>
    member val internal Connection = connection

    member internal this.RefreshIndexModelSnapshot(conn: NativeSqliteConnection) =
        scaffold.RefreshIndexModelSnapshot(conn)

    member internal this.InvalidateIndexModelSnapshot() =
        scaffold.InvalidateIndexModelSnapshot()

    member internal this.GetIndexModelSnapshot() =
        scaffold.GetIndexModelSnapshot()

    /// Wraps an operation in a relation-safe auto-transaction when not already in one.
    /// Routes through Connection.WithTransaction (hybrid: BEGIN IMMEDIATE top-level, SAVEPOINT nested).
    /// Used by Insert, Update, Replace, and batch methods that need relation atomicity.
    member internal this.WithRelationAutoTx (f: Collection<'T> -> 'R) : 'R =
        connection.WithTransaction(fun conn ->
            let directConnection = NativeTransactional conn
            let transientCollection = Collection<'T>(directConnection, name, connectionString, parentData)
            f transientCollection)

    member private this.mkRelationTx (conn: NativeSqliteConnection) : RelationsTypes.RelationTxContext =
        scaffold.MkRelationTx(conn)

    static member internal mkRelationPathSets() =
        CollectionScaffold<'T>.MkRelationPathSets()

    member internal this.ensureRelationTx (conn: NativeSqliteConnection) : RelationsTypes.RelationTxContext =
        scaffold.EnsureRelationTx(conn)

    member internal this.tryEnsureRelationTx (conn: NativeSqliteConnection) : RelationsTypes.RelationTxContext voption =
        scaffold.TryEnsureRelationTx(conn)

    member internal this.setSerializedItem (variables: IDictionary<string, obj>) (item: 'T) =
        variables.["item"] <- box (toDocument this.IncludeType item)

    member internal this.tryLoadExistingOwnerForUpsert (conn: NativeSqliteConnection) (item: 'T) : obj voption * int64 voption =
        let oldRow =
            if EntityDocumentId<'T>.Value then
                let id = EntityDocumentId<'T>.Read item
                if id > 0L then
                    match readDocumentById conn name id with
                    | row when Object.ReferenceEquals(row, null) -> ValueNone
                    | row -> ValueSome row
                else
                    ValueNone
            else
                match CustomTypeId<'T>.Get() with
                | Some customId ->
                    // DU-built SELECT for upsert existence check.
                    let idValue = customId.GetId(item |> box)
                    let idProp = customId.Member
                    let vars = System.Collections.Generic.Dictionary<string, obj>()
                    let idParameter =
                        QueryableHelperBase.allocateNamedStoredValueParam
                            vars "_cid0" idProp.IdType idValue
                    let whereExpr =
                        SoloDatabase.SqlModel.SqlExpr.Binary(
                            StoredValueParameter.declaredExpression idProp.IdType
                                (SoloDatabase.SqlModel.SqlExpr.JsonExtractExpr(Some "o", "Value", SoloDatabase.SqlModel.JsonPathOps.ofList [idProp.Name])),
                            SoloDatabase.SqlModel.BinaryOperator.Eq,
                            idParameter)
                    let sql, _ =
                        HydrationSqlBuilder.buildManyOnlyHydratedSql conn name typeof<'T> whereExpr vars true
                    match readHydratedDocument conn sql vars with
                    | row when Object.ReferenceEquals(row, null) -> ValueNone
                    | row -> ValueSome row
                | None -> ValueNone

        match oldRow with
        | ValueSome row -> ValueSome (fromSQLite<'T> row |> box), ValueSome row.Id.Value
        | ValueNone -> ValueNone, ValueNone

    member this.Insert(item: 'T) =
        CollectionInstanceCrud.insert<'T> item this.HasRelations connection.WithTransaction this.ensureRelationTx (fun conn -> Helper.insertInner this.IncludeType item conn name this)
    member this.InsertOrReplace(item: 'T) =
        CollectionInstanceCrud.insertOrReplace<'T> item this.HasRelations name connection.WithTransaction this.ensureRelationTx (fun conn -> this.tryLoadExistingOwnerForUpsert conn item) (fun conn -> Helper.insertOrReplaceInner this.IncludeType item conn name this)
    member this.InsertBatch(items: 'T seq) =
        let items = CollectionInsertOps<'T>.ValidateBatchItems(items)
        this.WithRelationAutoTx(fun transientCollection ->
            let connection = transientCollection.Connection.Get()
            let tx = this.tryEnsureRelationTx connection
            CollectionInsertOps<'T>.InsertBatchCore items tx (fun item -> Helper.insertInner this.IncludeType item connection name transientCollection))
    member this.InsertOrReplaceBatch(items: 'T seq) =
        let items = CollectionInsertOps<'T>.ValidateBatchItems(items)
        this.WithRelationAutoTx(fun transientCollection ->
            let connection = transientCollection.Connection.Get()
            let tx = this.tryEnsureRelationTx connection
            CollectionInsertOps<'T>.InsertOrReplaceBatchCore
                items
                tx
                name
                (fun item -> this.tryLoadExistingOwnerForUpsert connection item)
                (fun item -> Helper.insertOrReplaceInner this.IncludeType item connection name transientCollection))
    member this.TryGetById(id: int64) =
        CollectionInstanceCrud.tryGetByIdInt64<'T> id name this.HasRelations (fun () -> connection.Get()) (fun conn entityId entity ->
            let excludedPaths, includedPaths = Collection<'T>.mkRelationPathSets()
            RelationsSync.batchLoadDBRefProperties conn name typeof<'T> excludedPaths includedPaths false [| (entityId, entity) |] this.InTransaction
            RelationsSync.batchLoadDBRefManyProperties conn name typeof<'T> excludedPaths includedPaths false [| (entityId, entity) |] this.InTransaction)
    member this.GetById(id: int64) =
        match this.TryGetById id with
        | Some value -> value
        | None ->
            use native = connection.Get()
            try CollectionInstanceCrud.requireByIdInt64<'T> id name None
            with error ->
                SQLiteToolsHandlerFaultState.tryRecordHandlerFault native error
                reraise()
    member this.DeleteById(id: int64) =
        CollectionInstanceCrud.deleteByIdInt64<'T> id name connection.WithTransaction this.RequiresRelationDeleteHandling this.ensureRelationTx (fun tx ownerId owner ->
            let deletePlan = RelationsPlan.prepareDeleteOwner tx ownerId owner
            RelationsSync.syncDeleteOwner tx deletePlan)
    member this.TryGetById<'IdType when 'IdType : equality>(id: 'IdType) : 'T option =
        let hydrateRelations (conn: NativeSqliteConnection) (entityId: int64) (entity: obj) =
            let excludedPaths, includedPaths = Collection<'T>.mkRelationPathSets()
            RelationsSync.batchLoadDBRefProperties conn name typeof<'T> excludedPaths includedPaths false [| (entityId, entity) |] this.InTransaction
            RelationsSync.batchLoadDBRefManyProperties conn name typeof<'T> excludedPaths includedPaths false [| (entityId, entity) |] this.InTransaction
        let tryGetByCustomId () =
            CollectionInstanceCrud.tryGetByCustomId<'T, 'IdType> id name this.HasRelations (fun () -> connection.Get()) hydrateRelations
        CollectionInstanceCrud.tryGetByIdWithFallback<'T, 'IdType> id (fun (id64: int64) -> this.TryGetById(id64)) tryGetByCustomId
    member this.GetById<'IdType when 'IdType : equality>(id: 'IdType) : 'T =
        match this.TryGetById id with
        | None ->
            use native = connection.Get()
            let error = KeyNotFoundException(sprintf "There is no element with id '%A' inside collection '%s'" id name)
            SQLiteToolsHandlerFaultState.tryRecordHandlerFault native error
            raise error
        | Some x -> x
    member this.DeleteById<'IdType when 'IdType : equality>(id: 'IdType) : int =
        let deleteByCustomId () =
            CollectionInstanceCrud.deleteByCustomId<'T, 'IdType> id name connection.WithTransaction this.RequiresRelationDeleteHandling this.ensureRelationTx (fun tx ownerId owner ->
                let deletePlan = RelationsPlan.prepareDeleteOwner tx ownerId owner
                RelationsSync.syncDeleteOwner tx deletePlan)
        CollectionInstanceCrud.deleteByIdWithFallback<'T, 'IdType> id (fun (id64: int64) -> this.DeleteById(id64)) deleteByCustomId
    member this.Update(item: 'T) =
        CollectionInstanceCrud.update<'T> item name this.HasRelations connection.WithTransaction this.ensureRelationTx Collection<'T>.mkRelationPathSets this.setSerializedItem
    member this.DeleteMany(filter: Expression<Func<'T, bool>>) =
        CollectionInstanceCrud.deleteMany<'T> filter name connection.WithTransaction this.RequiresRelationDeleteHandling this.ensureRelationTx (fun conn currentFilter takeOne -> this.SelectMutationRows(conn, currentFilter, takeOne))
    member this.DeleteOne(filter: Expression<Func<'T, bool>>) =
        CollectionInstanceCrud.deleteOne<'T> filter name connection.WithTransaction this.RequiresRelationDeleteHandling this.ensureRelationTx (fun conn currentFilter takeOne -> this.SelectMutationRows(conn, currentFilter, takeOne))
    member this.ReplaceMany(item: 'T)(filter: Expression<Func<'T, bool>>) =
        CollectionInstanceCrud.replaceMany<'T> item filter name this.HasRelations connection.WithTransaction this.ensureRelationTx Collection<'T>.mkRelationPathSets this.setSerializedItem
    member this.ReplaceOne(item: 'T)(filter: Expression<Func<'T, bool>>) =
        CollectionInstanceCrud.replaceOne<'T> item filter name this.HasRelations connection.WithTransaction this.ensureRelationTx Collection<'T>.mkRelationPathSets this.setSerializedItem
    member this.UpdateMany(transform: Expression<System.Action<'T>> array)(filter: Expression<Func<'T, bool>>) =
        CollectionInstanceCrud.updateMany<'T> transform filter name this.HasRelations (fun () -> connection.Get()) connection.WithTransaction this.ensureRelationTx (fun conn currentFilter takeOne -> this.SelectMutationRows(conn, currentFilter, takeOne)) (fun conn rows expressions -> this.ExecuteJsonUpdateManyByRows(conn, rows, expressions))
    member this.EnsureIndex<'R>(expression: Expression<System.Func<'T, 'R>>) =
        CollectionInstanceSurface.ensureIndex name (fun () -> connection.Get()) this.InvalidateIndexModelSnapshot expression
    member this.EnsureUniqueAndIndex<'R>(expression: Expression<System.Func<'T, 'R>>) =
        CollectionInstanceSurface.ensureUniqueAndIndex name (fun () -> connection.Get()) this.InvalidateIndexModelSnapshot expression
    member this.DropIndexIfExists<'R>(expression: Expression<System.Func<'T, 'R>>) =
        CollectionInstanceSurface.dropIndexIfExists name (fun () -> connection.Get()) this.InvalidateIndexModelSnapshot expression
    member this.EnsureAddedAttributeIndexes() =
        CollectionInstanceSurface.ensureAddedAttributeIndexes<'T> name (fun () -> connection.Get()) this.InvalidateIndexModelSnapshot

    member this.OnInserting(handler) = CollectionInstanceSurface.onInserting this.Events handler
    member this.OnDeleting(handler) = CollectionInstanceSurface.onDeleting this.Events handler
    member this.OnUpdating(handler) = CollectionInstanceSurface.onUpdating this.Events handler
    member this.OnInserted(handler) = CollectionInstanceSurface.onInserted this.Events handler
    member this.OnDeleted(handler) = CollectionInstanceSurface.onDeleted this.Events handler
    member this.OnUpdated(handler) = CollectionInstanceSurface.onUpdated this.Events handler
    member this.Unregister(handler: InsertingHandler<'T>) = CollectionInstanceSurface.unregisterInserting this.Events handler
    member this.Unregister(handler: DeletingHandler<'T>) = CollectionInstanceSurface.unregisterDeleting this.Events handler
    member this.Unregister(handler: UpdatingHandler<'T>) = CollectionInstanceSurface.unregisterUpdating this.Events handler
    member this.Unregister(handler: InsertedHandler<'T>) = CollectionInstanceSurface.unregisterInserted this.Events handler
    member this.Unregister(handler: DeletedHandler<'T>) = CollectionInstanceSurface.unregisterDeleted this.Events handler
    member this.Unregister(handler: UpdatedHandler<'T>) = CollectionInstanceSurface.unregisterUpdated this.Events handler

    override this.Equals(other) = 
        match other with
        | :? Collection<'T> as other ->
            (this :> IEquatable<Collection<'T>>).Equals other
        | other -> false

    override this.GetHashCode() = hash this

    interface IEquatable<Collection<'T>> with
        member this.Equals (other) =
            this.ConnectionString = other.ConnectionString && this.Name = other.Name
    interface IOrderedQueryable<'T>
    interface IQueryable<'T> with
        member this.Provider = this.SoloDBQueryable.Provider
        member this.Expression = this.SoloDBQueryable.Expression
        member this.ElementType = typeof<'T>
    interface IEnumerable<'T> with
        member this.GetEnumerator() =
            this.SoloDBQueryable.GetEnumerator()
    interface IEnumerable with
        member this.GetEnumerator() =
            (this :> IEnumerable<'T>).GetEnumerator() :> IEnumerator
    interface INativeConnectionSource with
        member this.AcquireNativeConnection() = this.Connection.Get()

    interface ISoloDBCollection<'T> with
        member this.Compile(query: Expression<Func<IQueryable<'T>, IQueryable<'R>>>) : Func<IEnumerable<'R>> =
            let argument = Expression.Parameter(typeof<unit>, "args")
            let run = CompiledQueries.compile<'T, unit, 'R> (this :> ISoloDBCollection<'T>) query argument [||]
            Func<IEnumerable<'R>>(fun () -> run ())
        member this.Compile(query: Expression<Func<IQueryable<'T>, IOrderedQueryable<'R>>>) : Func<IEnumerable<'R>> =
            let argument = Expression.Parameter(typeof<unit>, "args")
            let run = CompiledQueries.compile<'T, unit, 'R> (this :> ISoloDBCollection<'T>) query argument [||]
            Func<IEnumerable<'R>>(fun () -> run ())
        member this.Compile(query: Expression<Func<IQueryable<'T>, 'R>>, _marker: CompileOptions) : Func<'R> =
            let argument = Expression.Parameter(typeof<unit>, "args")
            let run = CompiledQueries.compileResult<'T, unit, 'R> (this :> ISoloDBCollection<'T>) query argument [||]
            Func<'R>(fun () -> run ())
        member this.Compile(query: Expression<Func<IQueryable<'T>, 'A, IQueryable<'R>>>) : Func<'A, IEnumerable<'R>> =
            let argument = Expression.Parameter(typeof<'A>, "args")
            let run = CompiledQueries.compile<'T, 'A, 'R> (this :> ISoloDBCollection<'T>) query argument [|argument|]
            Func<'A, IEnumerable<'R>>(run)
        member this.Compile(query: Expression<Func<IQueryable<'T>, 'A, IOrderedQueryable<'R>>>) : Func<'A, IEnumerable<'R>> =
            let argument = Expression.Parameter(typeof<'A>, "args")
            let run = CompiledQueries.compile<'T, 'A, 'R> (this :> ISoloDBCollection<'T>) query argument [|argument|]
            Func<'A, IEnumerable<'R>>(run)
        member this.Compile(query: Expression<Func<IQueryable<'T>, 'A, 'R>>, _marker: CompileOptions) : Func<'A, 'R> =
            let argument = Expression.Parameter(typeof<'A>, "args")
            let run = CompiledQueries.compileResult<'T, 'A, 'R> (this :> ISoloDBCollection<'T>) query argument [|argument|]
            Func<'A, 'R>(run)
        member this.Compile(query: Expression<Func<IQueryable<'T>, 'A, 'B, IQueryable<'R>>>) : Func<'A, 'B, IEnumerable<'R>> =
            let struct(argument, values) = CompiledQueryArguments.pairArguments<'A, 'B>()
            let run = CompiledQueries.compile<'T, struct ('A * 'B), 'R> (this :> ISoloDBCollection<'T>) query argument values
            Func<'A, 'B, IEnumerable<'R>>(fun a b -> run (struct (a, b)))
        member this.Compile(query: Expression<Func<IQueryable<'T>, 'A, 'B, IOrderedQueryable<'R>>>) : Func<'A, 'B, IEnumerable<'R>> =
            let struct(argument, values) = CompiledQueryArguments.pairArguments<'A, 'B>()
            let run = CompiledQueries.compile<'T, struct ('A * 'B), 'R> (this :> ISoloDBCollection<'T>) query argument values
            Func<'A, 'B, IEnumerable<'R>>(fun a b -> run (struct (a, b)))
        member this.Compile(query: Expression<Func<IQueryable<'T>, 'A, 'B, 'R>>, _marker: CompileOptions) : Func<'A, 'B, 'R> =
            let struct(argument, values) = CompiledQueryArguments.pairArguments<'A, 'B>()
            let run = CompiledQueries.compileResult<'T, struct ('A * 'B), 'R> (this :> ISoloDBCollection<'T>) query argument values
            Func<'A, 'B, 'R>(fun a b -> run (struct (a, b)))
        member this.Compile(query: Expression<Func<IQueryable<'T>, 'A, 'B, 'C, IQueryable<'R>>>) : Func<'A, 'B, 'C, IEnumerable<'R>> =
            let struct(argument, values) = CompiledQueryArguments.tripleArguments<'A, 'B, 'C>()
            let run = CompiledQueries.compile<'T, struct ('A * 'B * 'C), 'R> (this :> ISoloDBCollection<'T>) query argument values
            Func<'A, 'B, 'C, IEnumerable<'R>>(fun a b c -> run (struct (a, b, c)))
        member this.Compile(query: Expression<Func<IQueryable<'T>, 'A, 'B, 'C, IOrderedQueryable<'R>>>) : Func<'A, 'B, 'C, IEnumerable<'R>> =
            let struct(argument, values) = CompiledQueryArguments.tripleArguments<'A, 'B, 'C>()
            let run = CompiledQueries.compile<'T, struct ('A * 'B * 'C), 'R> (this :> ISoloDBCollection<'T>) query argument values
            Func<'A, 'B, 'C, IEnumerable<'R>>(fun a b c -> run (struct (a, b, c)))
        member this.Compile(query: Expression<Func<IQueryable<'T>, 'A, 'B, 'C, 'R>>, _marker: CompileOptions) : Func<'A, 'B, 'C, 'R> =
            let struct(argument, values) = CompiledQueryArguments.tripleArguments<'A, 'B, 'C>()
            let run = CompiledQueries.compileResult<'T, struct ('A * 'B * 'C), 'R> (this :> ISoloDBCollection<'T>) query argument values
            Func<'A, 'B, 'C, 'R>(fun a b c -> run (struct (a, b, c)))
        member this.CompileQuotation(query: Microsoft.FSharp.Quotations.Expr<IQueryable<'T> -> IQueryable<'R>>) : Func<IEnumerable<'R>> =
            let query = QueryQuotationSyntax.expression<Func<IQueryable<'T>, IQueryable<'R>>> query
            (this :> ISoloDBCollection<'T>).Compile(query)

        member this.CompileQuotation(query: Microsoft.FSharp.Quotations.Expr<IQueryable<'T> -> IOrderedQueryable<'R>>) : Func<IEnumerable<'R>> =
            let query = QueryQuotationSyntax.expression<Func<IQueryable<'T>, IOrderedQueryable<'R>>> query
            (this :> ISoloDBCollection<'T>).Compile(query)

        member this.CompileQuotation(query: Microsoft.FSharp.Quotations.Expr<IQueryable<'T> -> 'R>, _marker: CompileQuotationOptions) : Func<'R> =
            let query = QueryQuotationSyntax.expression<Func<IQueryable<'T>, 'R>> query
            (this :> ISoloDBCollection<'T>).Compile(query, null)

        member this.CompileQuotation(query: Microsoft.FSharp.Quotations.Expr<IQueryable<'T> -> 'A -> IQueryable<'R>>) : Func<'A, IEnumerable<'R>> =
            let query = QueryQuotationSyntax.expression<Func<IQueryable<'T>, 'A, IQueryable<'R>>> query
            (this :> ISoloDBCollection<'T>).Compile(query)

        member this.CompileQuotation(query: Microsoft.FSharp.Quotations.Expr<IQueryable<'T> -> 'A -> IOrderedQueryable<'R>>) : Func<'A, IEnumerable<'R>> =
            let query = QueryQuotationSyntax.expression<Func<IQueryable<'T>, 'A, IOrderedQueryable<'R>>> query
            (this :> ISoloDBCollection<'T>).Compile(query)

        member this.CompileQuotation(query: Microsoft.FSharp.Quotations.Expr<IQueryable<'T> -> 'A -> 'R>, _marker: CompileQuotationOptions1) : Func<'A, 'R> =
            let query = QueryQuotationSyntax.expression<Func<IQueryable<'T>, 'A, 'R>> query
            (this :> ISoloDBCollection<'T>).Compile(query, null)

        member this.CompileQuotation(query: Microsoft.FSharp.Quotations.Expr<IQueryable<'T> -> 'A -> 'B -> IQueryable<'R>>) : Func<'A, 'B, IEnumerable<'R>> =
            let query = QueryQuotationSyntax.expression<Func<IQueryable<'T>, 'A, 'B, IQueryable<'R>>> query
            (this :> ISoloDBCollection<'T>).Compile(query)

        member this.CompileQuotation(query: Microsoft.FSharp.Quotations.Expr<IQueryable<'T> -> 'A -> 'B -> IOrderedQueryable<'R>>) : Func<'A, 'B, IEnumerable<'R>> =
            let query = QueryQuotationSyntax.expression<Func<IQueryable<'T>, 'A, 'B, IOrderedQueryable<'R>>> query
            (this :> ISoloDBCollection<'T>).Compile(query)

        member this.CompileQuotation(query: Microsoft.FSharp.Quotations.Expr<IQueryable<'T> -> 'A -> 'B -> 'R>, _marker: CompileQuotationOptions2) : Func<'A, 'B, 'R> =
            let query = QueryQuotationSyntax.expression<Func<IQueryable<'T>, 'A, 'B, 'R>> query
            (this :> ISoloDBCollection<'T>).Compile(query, null)

        member this.CompileQuotation(query: Microsoft.FSharp.Quotations.Expr<IQueryable<'T> -> 'A -> 'B -> 'C -> IQueryable<'R>>) : Func<'A, 'B, 'C, IEnumerable<'R>> =
            let query = QueryQuotationSyntax.expression<Func<IQueryable<'T>, 'A, 'B, 'C, IQueryable<'R>>> query
            (this :> ISoloDBCollection<'T>).Compile(query)

        member this.CompileQuotation(query: Microsoft.FSharp.Quotations.Expr<IQueryable<'T> -> 'A -> 'B -> 'C -> IOrderedQueryable<'R>>) : Func<'A, 'B, 'C, IEnumerable<'R>> =
            let query = QueryQuotationSyntax.expression<Func<IQueryable<'T>, 'A, 'B, 'C, IOrderedQueryable<'R>>> query
            (this :> ISoloDBCollection<'T>).Compile(query)

        member this.CompileQuotation(query: Microsoft.FSharp.Quotations.Expr<IQueryable<'T> -> 'A -> 'B -> 'C -> 'R>, _marker: CompileQuotationOptions3) : Func<'A, 'B, 'C, 'R> =
            let query = QueryQuotationSyntax.expression<Func<IQueryable<'T>, 'A, 'B, 'C, 'R>> query
            (this :> ISoloDBCollection<'T>).Compile(query, null)

        member this.InTransaction = this.InTransaction
        member this.IncludeType = this.IncludeType 
        member this.Name = this.Name
        member this.DropIndexIfExists(expression) = this.DropIndexIfExists(expression)
        member this.EnsureAddedAttributeIndexes() = this.EnsureAddedAttributeIndexes()
        member this.EnsureIndex(expression) = this.EnsureIndex(expression)
        member this.EnsureUniqueAndIndex(expression) = this.EnsureUniqueAndIndex(expression)
        member this.GetById(id: int64) : 'T = this.GetById(id)
        member this.GetById<'IdType when 'IdType : equality>(id: 'IdType) : 'T = this.GetById<'IdType>(id)
        member this.Insert(item) = this.Insert(item)
        member this.InsertBatch(items) = this.InsertBatch(items)
        member this.InsertOrReplace(item) = this.InsertOrReplace(item)
        member this.InsertOrReplaceBatch(items) = this.InsertOrReplaceBatch(items)
        member this.TryGetById(id: int64) : 'T option = this.TryGetById(id)
        member this.TryGetById<'IdType when 'IdType : equality>(id: 'IdType) : 'T option = this.TryGetById<'IdType>(id)
        member this.GetInternalConnection () = this.Connection.GetPresentation()
        member this.Delete<'IdType when 'IdType : equality>(id: 'IdType) : int = this.DeleteById<'IdType>(id)
        member this.Delete(id: int64) : int = this.DeleteById(id)
        member this.Update(item) = this.Update(item)
        member this.DeleteMany(filter) = this.DeleteMany(filter)
        member this.DeleteOne(filter) = this.DeleteOne(filter)
        member this.ReplaceMany(item,filter) = this.ReplaceMany(filter)(item)
        member this.ReplaceOne(item,filter) = this.ReplaceOne(filter)(item)
        member this.UpdateMany(filter, t) = this.UpdateMany(t)(filter)
    interface ISoloDBCollectionEvents<'T> with
        member this.OnInserting handler = this.OnInserting handler
        member this.OnDeleting handler = this.OnDeleting handler
        member this.OnUpdating handler = this.OnUpdating handler
        member this.OnInserted handler = this.OnInserted handler
        member this.OnDeleted handler = this.OnDeleted handler
        member this.OnUpdated handler = this.OnUpdated handler
        member this.Unregister (handler: InsertingHandler<'T>) = this.Unregister handler
        member this.Unregister (handler: DeletingHandler<'T>) = this.Unregister handler
        member this.Unregister (handler: UpdatingHandler<'T>) = this.Unregister handler
        member this.Unregister (handler: InsertedHandler<'T>) = this.Unregister handler
        member this.Unregister (handler: DeletedHandler<'T>) = this.Unregister handler
        member this.Unregister (handler: UpdatedHandler<'T>) = this.Unregister handler
