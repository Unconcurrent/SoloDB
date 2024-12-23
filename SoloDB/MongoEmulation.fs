namespace SoloDatabase.MongoDB

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
open SoloDatabase.JsonFunctions
open SoloDatabase.FileStorage
open SoloDatabase.Connections
open SoloDatabase.Utils
open SoloDatabase
open System.Threading
open System.Runtime.CompilerServices



type InsertManyResult = {
    Ids: ResizeArray<int64>
    Count: int64
}

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
        let result =(collection.InsertBatch documents)
        {
            Ids = result
            Count = result.Count
        }
        
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

type FilterDefinitionBuilder<'T> internal () =
    let mutable filters =[]

    // Add an equality filter
    member this.Eq<'TField>(field: Expression<Func<'T, 'TField>>, value: 'TField) : FilterDefinitionBuilder<'T> =
        let parameter = field.Parameters.[0]
        let body = Expression.Equal(field.Body, Expression.Constant(value, typeof<'TField>))
        let lambda = Expression.Lambda<Func<'T, bool>>(body, parameter)
        filters <- lambda :: filters
        this

    // Add a greater-than filter
    member this.Gt<'TField when 'TField :> IComparable>(field: Expression<Func<'T, 'TField>>, value: 'TField) : FilterDefinitionBuilder<'T> =
        let parameter = field.Parameters.[0]
        let body = Expression.GreaterThan(field.Body, Expression.Constant(value, typeof<'TField>))
        let lambda = Expression.Lambda<Func<'T, bool>>(body, parameter)
        filters <- lambda :: filters
        this

    // Add a less-than filter
    member this.Lt<'TField when 'TField :> IComparable>(field: Expression<Func<'T, 'TField>>, value: 'TField) : FilterDefinitionBuilder<'T> =
        let parameter = field.Parameters.[0]
        let body = Expression.LessThan(field.Body, Expression.Constant(value, typeof<'TField>))
        let lambda = Expression.Lambda<Func<'T, bool>>(body, parameter)
        filters <- lambda :: filters
        this

    // Add a contains filter (for collections or strings)
    member this.In<'TField>(field: Expression<Func<'T, 'TField>>, values: IEnumerable<'TField>) : FilterDefinitionBuilder<'T> =
        let parameter = field.Parameters.[0]
        let expressions =
            values
            |> Seq.map (fun value ->
                Expression.Equal(field.Body, Expression.Constant(value, typeof<'TField>))
            )
            |> Seq.toList

        let combinedBody =
            expressions
            |> List.reduce (fun acc expr -> Expression.OrElse(acc, expr))

        let lambda = Expression.Lambda<Func<'T, bool>>(combinedBody, parameter)
        filters <- lambda :: filters
        this


    // Combine all filters into a single LINQ expression
    member this.Build() : Expression<Func<'T, bool>> =
        if filters.IsEmpty then
            Expression.Lambda<Func<'T, bool>>(Expression.Constant(true), Expression.Parameter(typeof<'T>, "x"))
        else
            let parameter = Expression.Parameter(typeof<'T>, "x")
            let combined = 
                filters
                |> List.reduce (fun acc filter -> 
                    Expression.AndAlso(acc.Body, Expression.Invoke(filter, parameter))
                    |> Expression.Lambda<Func<'T, bool>>)
            combined

    static member op_Implicit(builder: FilterDefinitionBuilder<'T>) : Expression<Func<'T, bool>> =
        builder.Build()

[<Sealed>]
type Builders<'T> =
    static member Filter with get() = FilterDefinitionBuilder<'T> ()

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

