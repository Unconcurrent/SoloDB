namespace SoloDatabase

open Microsoft.Data.Sqlite
open System
open SoloDatabase.Types
open Microsoft.FSharp.NativeInterop
open SoloDatabase.JsonSerializator

// NativePtr operations for zero-copy JSON parsing from SQLite trigger callbacks
#nowarn "9"

type internal SoloDBItemEventContext<'T> internal (collectionName: string, createDb: SqliteConnection -> (unit -> unit) -> ISoloDB) =
    let mutable currentSqliteConnection = Unchecked.defaultof<SqliteConnection>
    let mutable currentDb = Unchecked.defaultof<ISoloDB | null>
    let mutable previousSession = -1L
    let mutable json = struct (NativePtr.nullPtr<byte>, 0)
    let mutable item: 'T = Unchecked.defaultof<'T>
    let mutable disposed = true
    let disposedMessage = "The event context database can only be used during the handler execution."

    member private this.ReadDB() =
        this.ThrowIfDisposed()
        if isNull currentDb then currentDb <- createDb currentSqliteConnection this.ThrowIfDisposed
        currentDb

    member private this.ThrowIfDisposed() =
        if disposed then
            raise (ObjectDisposedException("EventContextDatabase", disposedMessage))

    member this.Reset (connection: SqliteConnection, session: int64, jsonBytes: nativeptr<byte>, jsonSize: int) =
        disposed <- false
        if previousSession <> session then
            previousSession <- session
            currentDb <- null
            currentSqliteConnection <- connection
            json <- struct (jsonBytes, jsonSize)

    member this.MarkDisposed() =
        disposed <- true

    interface ISoloDBItemEventContext<'T> with
        member this.CollectionName: string =
            collectionName

        member this.Item with get(): 'T =
            this.ThrowIfDisposed()
            let struct (ptr, len) = json
            if len <> 0 then
                let span = ReadOnlySpan<byte>(NativePtr.toVoidPtr ptr, len)
                item <- JsonValue.ParseInto<'T> span
                json <- struct (NativePtr.nullPtr<byte>, 0)

            item

    interface ISoloDB with
        member this.ConnectionString: string = this.ReadDB().ConnectionString
        member this.FileSystem: IFileSystem = this.ReadDB().FileSystem
        member this.GetCollection<'A> () = this.ReadDB().GetCollection<'A> ()
        member this.GetCollection<'A> (name: string) = this.ReadDB().GetCollection<'A> (name)
        member this.GetUntypedCollection (name) = this.ReadDB().GetUntypedCollection(name)
        member this.CollectionExists (name) = this.ReadDB().CollectionExists (name)
        member this.CollectionExists<'A> () = this.ReadDB().CollectionExists<'A> ()
        member this.DropCollectionIfExists (name) = this.ReadDB().DropCollectionIfExists (name)
        member this.DropCollectionIfExists<'A> () = this.ReadDB().DropCollectionIfExists<'A> ()
        member this.DropCollection (name) = this.ReadDB().DropCollection (name)
        member this.DropCollection<'A> () = this.ReadDB().DropCollection<'A> ()
        member this.ListCollectionNames () = this.ReadDB().ListCollectionNames ()
        member this.Optimize () = this.ReadDB().Optimize ()
        member this.WithTransaction<'R> (func: Func<ISoloDB, 'R>) : 'R = this.ReadDB().WithTransaction<'R>(func)
        member this.WithTransaction (func: Action<ISoloDB>) : unit = this.ReadDB().WithTransaction(func)
        member this.WithTransactionAsync<'R> (func: Func<ISoloDB, Threading.Tasks.Task<'R>>) : Threading.Tasks.Task<'R> = this.ReadDB().WithTransactionAsync<'R>(func)
        member this.WithTransactionAsync (func: Func<ISoloDB, Threading.Tasks.Task>) : Threading.Tasks.Task = this.ReadDB().WithTransactionAsync(func)
        member this.Dispose () = this.ReadDB().Dispose ()


type internal SoloDBUpdatingEventContext<'T> internal (collectionName: string, createDb: SqliteConnection -> (unit -> unit) -> ISoloDB) =
    let mutable currentSqliteConnection = Unchecked.defaultof<SqliteConnection>
    let mutable currentDb = Unchecked.defaultof<ISoloDB | null>
    let mutable previousSession = -1L
    let mutable jsonOld = struct (NativePtr.nullPtr<byte>, 0)
    let mutable jsonNew = struct (NativePtr.nullPtr<byte>, 0)

    let mutable itemOld: 'T = Unchecked.defaultof<'T>
    let mutable itemNew: 'T = Unchecked.defaultof<'T>
    let mutable disposed = true
    let disposedMessage = "The event context database can only be used during the handler execution."

    member private this.ReadDB() =
        this.ThrowIfDisposed()
        if isNull currentDb then currentDb <- createDb currentSqliteConnection this.ThrowIfDisposed
        currentDb

    member private this.ThrowIfDisposed() =
        if disposed then
            raise (ObjectDisposedException("EventContextDatabase", disposedMessage))

    member this.Reset (connection: SqliteConnection, session: int64, jsonOldBytes: nativeptr<byte>, jsonOldSize: int, jsonNewBytes: nativeptr<byte>, jsonNewSize: int) =
        disposed <- false
        if previousSession <> session then
            previousSession <- session
            currentDb <- null
            currentSqliteConnection <- connection
            jsonOld <- struct (jsonOldBytes, jsonOldSize)
            jsonNew <- struct (jsonNewBytes, jsonNewSize)
            itemOld <- Unchecked.defaultof<'T>
            itemNew <- Unchecked.defaultof<'T>

    member this.MarkDisposed() =
        disposed <- true

    interface ISoloDBUpdatingEventContext<'T> with
        member this.CollectionName: string =
            collectionName

        member this.OldItem with get(): 'T =
            this.ThrowIfDisposed()
            let struct (ptr, len) = jsonOld
            if len <> 0 then
                let span = ReadOnlySpan<byte>(NativePtr.toVoidPtr ptr, len)
                itemOld <- JsonValue.ParseInto<'T> span
                jsonOld <- struct (NativePtr.nullPtr<byte>, 0)

            itemOld

        member this.Item with get(): 'T =
            this.ThrowIfDisposed()
            let struct (ptr, len) = jsonNew
            if len <> 0 then
                let span = ReadOnlySpan<byte>(NativePtr.toVoidPtr ptr, len)
                itemNew <- JsonValue.ParseInto<'T> span
                jsonNew <- struct (NativePtr.nullPtr<byte>, 0)

            itemNew

    interface ISoloDB with
        member this.ConnectionString: string = this.ReadDB().ConnectionString
        member this.FileSystem: IFileSystem = this.ReadDB().FileSystem
        member this.GetCollection<'A> () = this.ReadDB().GetCollection<'A> ()
        member this.GetCollection<'A> (name: string) = this.ReadDB().GetCollection<'A> (name)
        member this.GetUntypedCollection (name) = this.ReadDB().GetUntypedCollection(name)
        member this.CollectionExists (name) = this.ReadDB().CollectionExists (name)
        member this.CollectionExists<'A> () = this.ReadDB().CollectionExists<'A> ()
        member this.DropCollectionIfExists (name) = this.ReadDB().DropCollectionIfExists (name)
        member this.DropCollectionIfExists<'A> () = this.ReadDB().DropCollectionIfExists<'A> ()
        member this.DropCollection (name) = this.ReadDB().DropCollection (name)
        member this.DropCollection<'A> () = this.ReadDB().DropCollection<'A> ()
        member this.ListCollectionNames () = this.ReadDB().ListCollectionNames ()
        member this.Optimize () = this.ReadDB().Optimize ()
        member this.WithTransaction<'R> (func: Func<ISoloDB, 'R>) : 'R = this.ReadDB().WithTransaction<'R>(func)
        member this.WithTransaction (func: Action<ISoloDB>) : unit = this.ReadDB().WithTransaction(func)
        member this.WithTransactionAsync<'R> (func: Func<ISoloDB, Threading.Tasks.Task<'R>>) : Threading.Tasks.Task<'R> = this.ReadDB().WithTransactionAsync<'R>(func)
        member this.WithTransactionAsync (func: Func<ISoloDB, Threading.Tasks.Task>) : Threading.Tasks.Task = this.ReadDB().WithTransactionAsync(func)
        member this.Dispose () = this.ReadDB().Dispose ()
