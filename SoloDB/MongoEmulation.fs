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
open SoloDatabase.JsonSerializator
open System.Threading
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Dynamic
open System.Globalization

type BsonDocument (json: JsonValue) =
    new () = BsonDocument (JsonValue.New())
    new (objToSerialize: obj) = BsonDocument (JsonValue.Serialize objToSerialize)

    member this.Json = json

    // Method to add or update a key-value pair
    member this.Add (key: string, value: obj) =
        json.[key] <- JsonValue.Serialize value

    // Method to retrieve a value by key
    member this.GetValue (key: string) : JsonValue =
        match json.TryGetProperty(key) with
        | true, v -> v
        | false, _ -> JsonValue.Null

    // Method to remove a key-value pair
    member this.Remove (key: string) : bool =
        match json with
        | JsonValue.Object(map) -> map.Remove(key)
        | JsonValue.List(l) -> 
            let index = Int32.Parse(key, CultureInfo.InvariantCulture)
            if index < l.Count then
                l.RemoveAt(index)
                true
            else false

        | _ -> failwith "Invalid operation: the internal data structure is not an object."

    member this.ToObject<'T>() = json.ToObject<'T>()

    // Method to serialize this BsonDocument to a JSON string
    member this.ToJsonString () = json.ToJsonString ()

    // Method to deserialize a JSON string to update this BsonDocument
    static member Deserialize (jsonString: string) =
        BsonDocument (JsonValue.Parse (jsonString))

    // Override of ToString to return the JSON string representation of the document
    override this.ToString () = this.ToJsonString ()

    member this.Item
        with get (key: string) : BsonDocument = 
            json.[key] |> BsonDocument
        and set (key: string) (value: BsonDocument) =
            json.[key] <- value.Json

    interface IEnumerable<KeyValuePair<string, JsonValue>> with
        override this.GetEnumerator ()  =
            (json :> IEnumerable<KeyValuePair<string, JsonValue>>).GetEnumerator ()
        override this.GetEnumerator (): IEnumerator = 
            (this :> IEnumerable<KeyValuePair<string, JsonValue>>).GetEnumerator () :> IEnumerator

    interface IDynamicMetaObjectProvider with
        member this.GetMetaObject(expression: Linq.Expressions.Expression): DynamicMetaObject = 
            JsonValueMetaObject(expression, BindingRestrictions.Empty, json)


type InsertManyResult = {
    Ids: ResizeArray<int64>
    Count: int64
}

[<Extension>]
type CollectionExtensions =
    [<Extension>]
    static member FirstOrDefault(builder: SoloDatabase.FinalBuilder<'T, 'Q, 'R>) =
        builder.Enumerate() |> Seq.tryHead |> Option.defaultWith (fun() -> Unchecked.defaultof<'R>)

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

type FilterDefinitionBuilder<'T> () =
    let mutable filters = []

    // Helper to create an expression from a string field name
    let makeExpression (fieldName: string) : Expression<Func<'T, 'TField>> =
        let parameter = Expression.Parameter(typeof<'T>, "x")
        let propertyOrField =
            if typeof<'T>.GetField fieldName <> null || typeof<'T>.GetProperty fieldName <> null then
                Expression.PropertyOrField(parameter, fieldName) :> Expression
            else 
                let indexExpr =
                    Expression.MakeIndex(parameter, typeof<'T>.GetProperty "Item", [Expression.Constant fieldName]) :> Expression

                if typeof<'T> = typeof<BsonDocument> || typeof<'T> = typeof<JsonValue> then
                    Expression.Call(indexExpr, typeof<'T>.GetMethod("ToObject").MakeGenericMethod(typeof<'TField>))
                else 
                    indexExpr
        Expression.Lambda<Func<'T, 'TField>>(propertyOrField, parameter)

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

    // Overloaded method for string field names
    member this.Eq<'TField>(field: string, value: 'TField) : FilterDefinitionBuilder<'T> =
        this.Eq<'TField>(makeExpression (field), value)

    member this.Gt<'TField when 'TField :> IComparable>(field: string, value: 'TField) : FilterDefinitionBuilder<'T> =
        this.Gt<'TField>(makeExpression (field), value)

    member this.Lt<'TField when 'TField :> IComparable>(field: string, value: 'TField) : FilterDefinitionBuilder<'T> =
        this.Lt<'TField>(makeExpression (field), value)

    member this.In<'TField>(field: string, values: IEnumerable<'TField>) : FilterDefinitionBuilder<'T> =
        this.In<'TField>(makeExpression (field), values)

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

type UpdateDefinitionBuilder<'T> () =
    let mutable updates = []

    member this.Set<'TField, 'TFieldValue>(field: Expression<Func<'T, 'TField>>, value: 'TFieldValue) : UpdateDefinitionBuilder<'T> =
        let parameter = field.Parameters.[0]
        let body = Expression.Call(typeof<SoloDatabase.Extensions>.GetMethod("Set").MakeGenericMethod(typeof<'TField>, typeof<'TFieldValue>), [|field.Body; Expression.Constant(value)|])
        let lambda = Expression.Lambda<Action<'T>>(body, parameter)
        updates <- lambda :: updates
        this

    member this.UnSet<'TField>(field: Expression<Func<'T, 'TField>>) : UpdateDefinitionBuilder<'T> =
        this.Set<'TField, obj>(field, Unchecked.defaultof<obj>)

    member this.Inc<'TField>(field: Expression<Func<'T, 'TField>>, value: 'TField) : UpdateDefinitionBuilder<'T> =
        let parameter = field.Parameters.[0]
        let lambda = Expression.Lambda<Action<'T>>(
            Expression.Call(typeof<SoloDatabase.Extensions>.GetMethod("Set").MakeGenericMethod(typeof<'TField>, typeof<'TField>), [|
                field.Body;
                if typeof<BsonDocument>.IsAssignableFrom typeof<'T> then
                    Expression.Add(
                        Expression.Call(field.Body, typeof<BsonDocument>.GetMethod("ToObject").MakeGenericMethod(typeof<'TField>)),
                        Expression.Constant value)
                if typeof<JsonValue>.IsAssignableFrom typeof<'T> then
                    Expression.Add(
                        Expression.Call(field.Body, typeof<JsonValue>.GetMethod("ToObject").MakeGenericMethod(typeof<'TField>)),
                        Expression.Constant value)
                else
                    Expression.Add(field.Body, Expression.Constant value)
            |]),
            parameter
        )
        updates <- lambda :: updates
        this

    member this.Build() : Expression<Action<'T>> array =
        updates |> List.toArray

    static member op_Implicit(builder: UpdateDefinitionBuilder<'T>) : Expression<Action<'T>> array =
        builder.Build()

[<Sealed>]
type Builders<'T> =
    static member Filter with get() = FilterDefinitionBuilder<'T> ()
    static member Update with get() = UpdateDefinitionBuilder<'T> ()

type MongoDatabase internal (soloDB: SoloDB) =
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

    member this.GetDatabase( [<Optional; DefaultParameterValue(null: string)>] name : string) =
        if disposed then raise (ObjectDisposedException(nameof(MongoClient)))

        let name = match name with null -> "Master" | n -> n 
        let name = name + ".solodb"
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

