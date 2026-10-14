namespace SoloDatabase

open System
open SoloDatabase.Types
open Microsoft.FSharp.NativeInterop
open SoloDatabase.JsonSerializator

// NativePtr operations for zero-copy JSON parsing from SQLite trigger callbacks
#nowarn "9"

[<AbstractClass>]
type internal EventContextBase(createDb: NativeSqliteConnection -> (unit -> unit) -> ISoloDB) =
    let mutable connection = Unchecked.defaultof<NativeSqliteConnection>
    let mutable session = -1L
    let mutable disposed = true
    let mutable database: ISoloDB = Unchecked.defaultof<_>
    member _.Check() =
        if System.Threading.Volatile.Read(&disposed) then
            raise (ObjectDisposedException("EventContextDatabase", "The event context database can only be used during the handler execution."))
    member this.ReadDB() =
        this.Check()
        if isNull (box database) then
            let invocation = session
            database <- createDb connection (fun () ->
                this.Check()
                if System.Threading.Volatile.Read(&session) <> invocation then raise (ObjectDisposedException("EventContextDatabase")))
        database
    member _.Connection = connection
    member _.Begin(connectionValue, invocation) =
        connection <- connectionValue
        database <- Unchecked.defaultof<_>
        System.Threading.Volatile.Write(&session, invocation)
        System.Threading.Volatile.Write(&disposed, false)
    abstract member ClearInvocation: unit -> unit
    member this.End() =
        System.Threading.Volatile.Write(&disposed, true)
        database <- Unchecked.defaultof<_>
        connection <- Unchecked.defaultof<_>
        this.ClearInvocation()
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


type internal SoloDBItemEventContext<'T> internal (collectionName: string, createDb: NativeSqliteConnection -> (unit -> unit) -> ISoloDB) =
    inherit EventContextBase(createDb)
    let mutable json = struct (NativePtr.nullPtr<byte>, 0)
    let mutable item: 'T = Unchecked.defaultof<'T>

    member this.Reset(connection: NativeSqliteConnection, session: int64, jsonBytes: nativeptr<byte>, jsonSize: int) =
        this.Begin(connection, session)
        json <- struct(jsonBytes, jsonSize)
        item <- Unchecked.defaultof<'T>

    override _.ClearInvocation() =
        json <- struct (NativePtr.nullPtr<byte>, 0)
        item <- Unchecked.defaultof<'T>

    interface ISoloDBItemEventContext<'T> with
        member this.CollectionName: string =
            collectionName

        member this.Item with get(): 'T =
            this.Check()
            let struct (ptr, len) = json
            if len <> 0 then
                let span = ReadOnlySpan<byte>(NativePtr.toVoidPtr ptr, len)
                item <- JsonValue.ParseStored<'T> span
                json <- struct (NativePtr.nullPtr<byte>, 0)

            item


type internal SoloDBUpdatingEventContext<'T> internal (collectionName: string, createDb: NativeSqliteConnection -> (unit -> unit) -> ISoloDB) =
    inherit EventContextBase(createDb)
    let mutable jsonOld = struct (NativePtr.nullPtr<byte>, 0)
    let mutable jsonNew = struct (NativePtr.nullPtr<byte>, 0)
    let mutable itemOld: 'T = Unchecked.defaultof<'T>
    let mutable itemNew: 'T = Unchecked.defaultof<'T>

    member this.Reset(connection: NativeSqliteConnection, session: int64, jsonOldBytes: nativeptr<byte>, jsonOldSize: int, jsonNewBytes: nativeptr<byte>, jsonNewSize: int) =
        this.Begin(connection, session)
        jsonOld <- struct(jsonOldBytes, jsonOldSize)
        jsonNew <- struct(jsonNewBytes, jsonNewSize)
        itemOld <- Unchecked.defaultof<'T>
        itemNew <- Unchecked.defaultof<'T>

    override _.ClearInvocation() =
        jsonOld <- struct (NativePtr.nullPtr<byte>, 0)
        jsonNew <- struct (NativePtr.nullPtr<byte>, 0)
        itemOld <- Unchecked.defaultof<'T>
        itemNew <- Unchecked.defaultof<'T>

    interface ISoloDBUpdatingEventContext<'T> with
        member this.CollectionName: string =
            collectionName

        member this.OldItem with get(): 'T =
            this.Check()
            let struct (ptr, len) = jsonOld
            if len <> 0 then
                let span = ReadOnlySpan<byte>(NativePtr.toVoidPtr ptr, len)
                itemOld <- JsonValue.ParseStored<'T> span
                jsonOld <- struct (NativePtr.nullPtr<byte>, 0)

            itemOld

        member this.Item with get(): 'T =
            this.Check()
            let struct (ptr, len) = jsonNew
            if len <> 0 then
                let span = ReadOnlySpan<byte>(NativePtr.toVoidPtr ptr, len)
                itemNew <- JsonValue.ParseStored<'T> span
                jsonNew <- struct (NativePtr.nullPtr<byte>, 0)

            itemNew
