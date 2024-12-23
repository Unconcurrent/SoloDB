namespace SoloDatabase

open Microsoft.Data.Sqlite
open System.Linq.Expressions
open System
open System.Collections.Generic
open System.Collections
open FSharp.Interop.Dynamic
open SoloDatabase.Types
open System.IO
open System.Text
open SQLiteTools
open JsonFunctions
open FileStorage
open Connections
open Utils
open System.Threading
open System.Runtime.CompilerServices

[<Extension>]
type CollectionExtensions =
    [<Extension>]
    static member Find<'a>(collection: SoloDatabase.Collection<'a>, filter: Expression<Func<'a, bool>>) =
        collection.Where filter
        
    [<Extension>]
    static member InsertOne<'a>(collection: SoloDatabase.Collection<'a>, document: 'a) =
        collection.Insert document

    [<Extension>]
    static member InsertMany<'a>(collection: SoloDatabase.Collection<'a>, documents: 'a seq) =
        collection.InsertBatch documents
        
    [<Extension>]
    static member ReplaceOne<'a>(collection: SoloDatabase.Collection<'a>, filter: Expression<Func<'a, bool>>, document: 'a) =
        collection.Replace(document).Where(filter).Limit(1UL).Execute()

    [<Extension>]
    static member DeleteOne<'a>(collection: SoloDatabase.Collection<'a>, filter: Expression<Func<'a, bool>>) =
        collection.Delete().Where(filter).Limit(1UL).Execute()

    [<Extension>]
    static member DeleteMany<'a>(collection: SoloDatabase.Collection<'a>, filter: Expression<Func<'a, bool>>) =
        collection.Delete().Where(filter).Execute()

    [<Extension>]
    static member UpdateOne<'a>(collection: SoloDatabase.Collection<'a>, filter: Expression<Func<'a, bool>>, [<ParamArray>] updates: Expression<System.Action<'a>> array) =
        collection.Update(updates).Where(filter).Limit(1UL).Execute()

    [<Extension>]
    static member UpdateMany<'a>(collection: SoloDatabase.Collection<'a>, filter: Expression<Func<'a, bool>>, [<ParamArray>] updates: Expression<System.Action<'a>> array) =
        collection.Update(updates).Where(filter).Execute()

    [<Extension>]
    static member AsQueryable<'a>(collection: SoloDatabase.Collection<'a>) =
        collection.Select()

type MongoDatabase internal(soloDB: SoloDB) =
    member this.GetCollection<'doc> (name: string) =
        soloDB.GetCollection<'doc> name

    member this.CreateCollection (name: string) =
        soloDB.GetUntypedCollection name

    member this.ListCollections () =
        soloDB.ListCollectionNames ()

    member this.Dispose () =
        soloDB.Dispose ()

    interface IDisposable with
        override this.Dispose (): unit = 
            this.Dispose ()

type internal MongoClientLocation =
| Disk of {| Directory: DirectoryInfo; LockingFile: FileStream |}
| Memory of string

type MongoClient(directoryDatabaseSource: string) =
    do if directoryDatabaseSource.StartsWith "mongodb://" then failwithf "SoloDB does not support mongo connections."
    let location = 
        if directoryDatabaseSource.StartsWith "memory:" then
            Memory directoryDatabaseSource
        else
            let directoryDatabasePath = Path.GetFullPath directoryDatabaseSource
            let directory = Directory.CreateDirectory directoryDatabasePath
            Disk {|
                Directory = directory
                LockingFile = File.Open(Path.Combine (directory.FullName, ".lock"), FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite)
            |}

    let connectedDatabases = ResizeArray<WeakReference<MongoDatabase>> ()
    let mutable disposed = false

    member this.GetDatabase(?name : string) =
        if disposed then raise (ObjectDisposedException(nameof(MongoClient)))

        let name = match name with Some n -> n | None -> "Master"
        let dbSource =
            match location with
            | Disk disk ->
                Path.Combine (disk.Directory.FullName, name)
            | Memory source ->
                source + "-" + name

        let db = new SoloDB (dbSource)
        let db = new MongoDatabase (db)

        let _remoteCount = connectedDatabases.RemoveAll(fun x -> match x.TryGetTarget() with false, _ -> true | true, _ -> false)
        connectedDatabases.Add (WeakReference<MongoDatabase> db)

        db


    interface IDisposable with
        override this.Dispose () = 
            disposed <- true
            for x in connectedDatabases do
                match x.TryGetTarget() with
                | true, soloDB -> soloDB.Dispose()
                | false, _ -> ()

            connectedDatabases.Clear()
            match location with
            | Disk disk ->
                disk.LockingFile.Dispose ()
            | _other -> ()

