namespace SoloDatabase

open SoloDatabase.JsonSerializator
open Microsoft.Data.Sqlite
open System.Linq.Expressions
open System
open System.Collections.Generic
open System.Collections
open SoloDatabase.Types
open System.IO
open System.Text
open SQLiteTools
open JsonFunctions
open FileStorage
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
type internal Collection<'T>(connection: Connection, name: string, connectionString: string, parentData: SoloDBToCollectionData) as this =
    let hasRelations =
        typeof<'T>.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
        |> Array.exists (fun p -> DBRefTypeHelpers.isAnyRelationRefType p.PropertyType)
    let scaffold = CollectionScaffold<'T>(connection, connectionString, name, hasRelations)

    member val private SoloDBQueryable = SoloDBCollectionQueryable<'T, 'T>(SoloDBCollectionQueryProvider(this, parentData), Expression.Constant(RootQueryable<'T>(this))) :> IOrderedQueryable<'T>
    member val private ConnectionString = connectionString
    /// <summary>Gets the name of the collection.</summary>
    member val Name = name
    /// <summary>Gets a value indicating whether the collection is operating within a transaction.</summary>
    member val InTransaction =
        match connection with
        | Transactional _ -> true
        | Guarded (_, inner) ->
            match inner with
            | Transactional _ -> true
            | _ -> false
        | Pooled _ -> false
    /// <summary>Gets a value indicating whether type information should be included during serialization for documents in this collection.</summary>
    member val IncludeType = mustIncludeTypeInformationInSerialization<'T>
    /// <summary>True when the document type declares DBRef/DBRefMany properties.</summary>
    member val internal HasRelations = hasRelations

    member private this.HasIncomingRelations(connection: SqliteConnection) =
        scaffold.HasIncomingRelations(connection)

    member internal this.RequiresRelationDeleteHandling(connection: SqliteConnection) =
        scaffold.RequiresRelationDeleteHandling(connection)

    member internal this.SelectMutationRows(connection: SqliteConnection, filter: Expression<Func<'T, bool>>, takeOne: bool) =
        let filtered = Queryable.Where(this.SoloDBQueryable, filter)
        let expression =
            if takeOne then Queryable.Take(filtered, 1).Expression
            else filtered.Expression
        let query, variables = QueryableTranslation.startFilterTranslationWithConnection connection this expression
        connection.Query<DbObjectRow>(query, variables) |> Seq.toArray

    member internal this.ExecuteJsonUpdateManyByRows(connection: SqliteConnection, rows: DbObjectRow array, expressions: ResizeArray<Expression<System.Action<'T>>>) =
        if expressions.Count = 0 || rows.Length = 0 then
            0
        else
            let variables = Dictionary<string, obj>()
            let fullSQL = StringBuilder()
            let inline append (txt: string) = ignore (fullSQL.Append txt)

            append "UPDATE \""
            append name
            append "\" SET Value = jsonb_set(Value, "

            for expression in expressions do
                QueryTranslator.translateUpdateMode name expression fullSQL variables

            fullSQL.Remove(fullSQL.Length - 1, 1) |> ignore
            append ") WHERE Id IN ("

            for i in 0 .. rows.Length - 1 do
                if i > 0 then append ","
                let key = $"id{i}"
                append "@"
                append key
                variables.[key] <- rows.[i].Id.Value :> obj

            append ")"
            connection.Execute(fullSQL.ToString(), variables)

    /// <summary>Gets the event registration API for this collection.</summary>
    member val internal Events: ISoloDBCollectionEvents<'T> =
        let createDb (directConnection: SqliteConnection) (guard: unit -> unit) : ISoloDB =
            let guardedConnection = Guarded(guard, Transactional directConnection)
            let collectionFactory =
                { new IEventDbCollectionFactory with
                    member _.CreateCollection<'U>(collectionName: string) =
                        Collection<'U>(guardedConnection, collectionName, connectionString, parentData) :> ISoloDBCollection<'U> }
            let cache = EventDbCache(connectionString, directConnection, guardedConnection, parentData, name, collectionFactory)
            let throwNestedTxInEventContext () =
                raise (NotSupportedException(
                    "Error: Nested transactions are not supported inside event handler contexts.\n" +
                    "Reason: Event handlers execute during active SQL statements where SQLite cannot open SAVEPOINTs.\n" +
                    "Fix: Perform transactional work outside event handlers, or use the event context directly without nesting."))

            { new ISoloDB with
                member _.ConnectionString = connectionString
                member _.FileSystem = cache.FileSystem
                member _.GetCollection<'U>() =
                    cache.GetCollection<'U>(Helper.collectionNameOf<'U>)
                member _.GetCollection<'U>(collectionName: string) =
                    cache.GetCollection<'U>(collectionName)
                member _.GetUntypedCollection(collectionName: string) =
                    cache.GetCollection<JsonSerializator.JsonValue>(collectionName)
                member _.CollectionExists(collectionName: string) =
                    cache.CollectionExists(collectionName)
                member _.CollectionExists<'U>() =
                    cache.CollectionExists(Helper.collectionNameOf<'U>)
                member _.DropCollectionIfExists(collectionName: string) =
                    cache.DropCollectionIfExists(collectionName)
                member _.DropCollectionIfExists<'U>() =
                    cache.DropCollectionIfExists(Helper.collectionNameOf<'U>)
                member _.DropCollection(collectionName: string) =
                    cache.DropCollection(collectionName)
                member _.DropCollection<'U>() =
                    cache.DropCollection(Helper.collectionNameOf<'U>)
                member _.ListCollectionNames() =
                    Helper.listCollectionNamesSnapshot directConnection :> seq<string>
                member _.Optimize() =
                    directConnection.Execute "PRAGMA optimize;" |> ignore
                member _.WithTransaction<'R>(_func: Func<ISoloDB, 'R>) : 'R =
                    throwNestedTxInEventContext()
                member _.WithTransaction(_func: Action<ISoloDB>) : unit =
                    throwNestedTxInEventContext()
                member _.WithTransactionAsync<'R>(_func: Func<ISoloDB, Threading.Tasks.Task<'R>>) : Threading.Tasks.Task<'R> =
                    throwNestedTxInEventContext()
                member _.WithTransactionAsync(_func: Func<ISoloDB, Threading.Tasks.Task>) : Threading.Tasks.Task =
                    throwNestedTxInEventContext()
                member _.Dispose() = ()
            }

        CollectionEventSystem<'T>(
            name,
            parentData.EventSystem,
            createDb)

    /// <summary>Gets the internal connection provider for this collection.</summary>
    member val internal Connection = connection

    member internal this.RefreshIndexModelSnapshot(conn: SqliteConnection) =
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
            let directConnection = Transactional conn
            let transientCollection = Collection<'T>(directConnection, name, connectionString, parentData)
            f transientCollection)

    member private this.mkRelationTx (conn: SqliteConnection) : Relations.RelationTxContext =
        scaffold.MkRelationTx(conn)

    static member internal mkRelationPathSets() =
        CollectionScaffold<'T>.MkRelationPathSets()

    member internal this.ensureRelationTx (conn: SqliteConnection) : Relations.RelationTxContext =
        scaffold.EnsureRelationTx(conn)

    member internal this.tryEnsureRelationTx (conn: SqliteConnection) : Relations.RelationTxContext voption =
        scaffold.TryEnsureRelationTx(conn)

    member internal this.setSerializedItem (variables: IDictionary<string, obj>) (item: 'T) =
        variables.["item"] <- if this.IncludeType then toTypedJson item else toJson item

    member internal this.tryLoadExistingOwnerForUpsert (conn: SqliteConnection) (item: 'T) : obj voption * int64 voption =
        let oldRow =
            if HasTypeId<'T>.Value then
                let id = HasTypeId<'T>.Read item
                if id > 0L then
                    match conn.QueryFirstOrDefault<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE Id = @id LIMIT 1", {| id = id |}) with
                    | row when Object.ReferenceEquals(row, null) -> ValueNone
                    | row -> ValueSome row
                else
                    ValueNone
            else
                match CustomTypeId<'T>.Value with
                | Some customId ->
                    // DU-built SELECT for upsert existence check.
                    let idValue = customId.GetId(item |> box)
                    let idProp = customId.Property
                    let vars = System.Collections.Generic.Dictionary<string, obj>()
                    vars.["_cid0"] <- idValue
                    let whereExpr =
                        SqlDu.Engine.C1.Spec.SqlExpr.Binary(
                            SqlDu.Engine.C1.Spec.SqlExpr.FunctionCall("jsonb_extract",
                                [SqlDu.Engine.C1.Spec.SqlExpr.Column(Some "o", "Value"); SqlDu.Engine.C1.Spec.SqlExpr.Literal(SqlDu.Engine.C1.Spec.SqlLiteral.String ("$." + idProp.Name))]),
                            SqlDu.Engine.C1.Spec.BinaryOperator.Eq,
                            SqlDu.Engine.C1.Spec.SqlExpr.Parameter "_cid0")
                    let sql, _ =
                        HydrationSqlBuilder.buildManyOnlyHydratedSql conn name typeof<'T> whereExpr vars true
                    QueryCommandInstrumentation.Increment()
                    match conn.QueryFirstOrDefault<DbObjectRow>(sql, vars) with
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
            Relations.batchLoadDBRefProperties conn name typeof<'T> excludedPaths includedPaths false [| (entityId, entity) |] this.InTransaction
            Relations.batchLoadDBRefManyProperties conn name typeof<'T> excludedPaths includedPaths false [| (entityId, entity) |] this.InTransaction
            Relations.captureRelationVersionForEntities conn name [| (entityId, entity) |])
    member this.GetById(id: int64) =
        CollectionInstanceCrud.requireByIdInt64<'T> id name (this.TryGetById id)
    member this.DeleteById(id: int64) =
        CollectionInstanceCrud.deleteByIdInt64<'T> id name connection.WithTransaction this.RequiresRelationDeleteHandling this.ensureRelationTx (fun tx ownerId owner ->
            let deletePlan = Relations.prepareDeleteOwner tx ownerId owner
            Relations.syncDeleteOwner tx deletePlan)
    member this.TryGetById<'IdType when 'IdType : equality>(id: 'IdType) : 'T option =
        let hydrateRelations (conn: SqliteConnection) (entityId: int64) (entity: obj) =
            let excludedPaths, includedPaths = Collection<'T>.mkRelationPathSets()
            Relations.batchLoadDBRefProperties conn name typeof<'T> excludedPaths includedPaths false [| (entityId, entity) |] this.InTransaction
            Relations.batchLoadDBRefManyProperties conn name typeof<'T> excludedPaths includedPaths false [| (entityId, entity) |] this.InTransaction
            Relations.captureRelationVersionForEntities conn name [| (entityId, entity) |]
        let tryGetByCustomId () =
            CollectionInstanceCrud.tryGetByCustomId<'T, 'IdType> id name this.HasRelations (fun () -> connection.Get()) hydrateRelations
        CollectionInstanceCrud.tryGetByIdWithFallback<'T, 'IdType> id (fun (id64: int64) -> this.TryGetById(id64)) tryGetByCustomId
    member this.GetById<'IdType when 'IdType : equality>(id: 'IdType) : 'T =
        match this.TryGetById id with
        | None -> raise (KeyNotFoundException(sprintf "There is no element with id '%A' inside collection '%s'" id name))
        | Some x -> x
    member this.DeleteById<'IdType when 'IdType : equality>(id: 'IdType) : int =
        let deleteByCustomId () =
            CollectionInstanceCrud.deleteByCustomId<'T, 'IdType> id name connection.WithTransaction this.RequiresRelationDeleteHandling this.ensureRelationTx (fun tx ownerId owner ->
                let deletePlan = Relations.prepareDeleteOwner tx ownerId owner
                Relations.syncDeleteOwner tx deletePlan)
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
    interface ISoloDBCollection<'T> with
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
        member this.GetInternalConnection () = this.Connection.Get()
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
