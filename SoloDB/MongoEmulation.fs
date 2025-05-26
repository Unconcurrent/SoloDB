namespace SoloDatabase.MongoDB

open System.Linq.Expressions
open System
open System.Collections.Generic
open System.Collections
open System.IO
open SoloDatabase
open SoloDatabase.JsonSerializator
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Dynamic
open System.Globalization
open System.Reflection
open System.Linq

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

    member this.Contains(item: obj) =
        let itemJson = JsonValue.Serialize item
        match json with
        | List l -> l.Contains itemJson
        | Object o -> o.Keys.Contains (itemJson.ToObject<string>())
        | other -> raise (InvalidOperationException (sprintf "Cannot call Contains(%A) on %A" itemJson other))

    member this.ToObject<'T>() = json.ToObject<'T>()

    // Method to serialize this BsonDocument to a JSON string
    member this.ToJsonString () = json.ToJsonString ()

    member this.AsBoolean = match json with JsonValue.Boolean b -> b | _ -> raise (InvalidCastException("Not a boolean"))
    member this.AsBsonArray = match json with JsonValue.List _l -> BsonDocument(json) | _ -> raise (InvalidCastException("Not a BSON array"))
    member this.AsBsonDocument = match json with JsonValue.Object _o -> BsonDocument(json) | _ -> raise (InvalidCastException("Not a BSON document"))
    member this.AsString = match json with JsonValue.String s -> s | _ -> raise (InvalidCastException("Not a string"))
    member this.AsInt32 = match json with JsonValue.Number n when n <= decimal Int32.MaxValue && n >= decimal Int32.MinValue -> int n | _ -> raise (InvalidCastException("Not an int32"))
    member this.AsInt64 = match json with JsonValue.Number n when n <= decimal Int64.MaxValue && n >= decimal Int64.MinValue -> int64 n | _ -> raise (InvalidCastException("Not an int64"))
    member this.AsDouble = match json with JsonValue.Number n -> double n | _ -> raise (InvalidCastException("Not a double"))
    member this.AsDecimal = match json with JsonValue.Number n -> n | _ -> raise (InvalidCastException("Not a decimal"))

    member this.IsBoolean = match json with JsonValue.Boolean _ -> true | _ -> false
    member this.IsBsonArray = match json with JsonValue.List _ -> true | _ -> false
    member this.IsBsonDocument = match json with JsonValue.Object _ -> true | _ -> false
    member this.IsString = match json with JsonValue.String _ -> true | _ -> false
    member this.IsInt32 = match json with JsonValue.Number n when n <= decimal Int32.MaxValue && n >= decimal Int32.MinValue -> true | _ -> false
    member this.IsInt64 = match json with JsonValue.Number n when n <= decimal Int64.MaxValue && n >= decimal Int64.MinValue -> true | _ -> false
    member this.IsDouble = match json with JsonValue.Number _ -> true | _ -> false

    member this.ToBoolean() =
        match json with
        | JsonValue.Boolean b -> b
        | JsonValue.String "" -> false
        | JsonValue.String _s -> true
        | JsonValue.Number n when n = decimal 0 -> false
        | JsonValue.Number _n -> true
        | JsonValue.List _l -> true
        | JsonValue.Object _o -> true
        | JsonValue.Null -> false

    member this.ToInt32() =
        match json with
        | JsonValue.Number n when n <= decimal Int32.MaxValue && n >= decimal Int32.MinValue -> int n
        | JsonValue.String s -> 
            match Int32.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture) with
            | true, i -> i
            | false, _ -> raise (InvalidCastException("Cannot convert to int32"))
        | _ -> raise (InvalidCastException("Cannot convert to int32"))

    member this.ToInt64() =
        match json with
        | JsonValue.Number n when n <= decimal Int64.MaxValue && n >= decimal Int64.MinValue -> int64 n
        | JsonValue.String s -> 
            match Int64.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture) with
            | true, i -> i
            | false, _ -> raise (InvalidCastException("Cannot convert to int64"))
        | _ -> raise (InvalidCastException("Cannot convert to int64"))

    member this.ToDouble() =
        match json with
        | JsonValue.Number n -> double n
        | JsonValue.String s -> 
            match Double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture) with
            | true, d -> d
            | false, _ -> raise (InvalidCastException("Cannot convert to double"))
        | _ -> raise (InvalidCastException("Cannot convert to double"))

    member this.ToDecimal() =
        match json with
        | JsonValue.Number n -> n
        | JsonValue.String s -> 
            match Decimal.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture) with
            | true, d -> d
            | false, _ -> raise (InvalidCastException("Cannot convert to decimal"))
        | _ -> raise (InvalidCastException("Cannot convert to decimal"))

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

    member internal this.GetPropertyForBinder(name: string) =
        match json.GetPropertyForBinder name with
        | :? JsonValue as v -> v |> BsonDocument |> box
        | other -> other

    interface IDynamicMetaObjectProvider with
        member this.GetMetaObject(expression: Linq.Expressions.Expression): DynamicMetaObject = 
            BsonDocumentMetaObject(expression, BindingRestrictions.Empty, this)

and internal BsonDocumentMetaObject(expression: Expression, restrictions: BindingRestrictions, value: BsonDocument) =
    inherit DynamicMetaObject(expression, restrictions, value)

    static member val private GetPropertyMethod = typeof<BsonDocument>.GetMethod("GetPropertyForBinder", BindingFlags.NonPublic ||| BindingFlags.Instance)
    static member val private SetPropertyMethod = typeof<BsonDocument>.GetMethod("set_Item")
    static member val private ToJsonMethod = typeof<BsonDocument>.GetMethod("ToJsonString")
    static member val private ToStringMethod = typeof<obj>.GetMethod("ToString")

    override this.BindGetMember(binder: GetMemberBinder) : DynamicMetaObject =
        let resultExpression = Expression.Call(
            Expression.Convert(this.Expression, typeof<BsonDocument>),
            BsonDocumentMetaObject.GetPropertyMethod,
            Expression.Constant(binder.Name)
        )
        DynamicMetaObject(resultExpression, BindingRestrictions.GetTypeRestriction(this.Expression, this.LimitType))

    override this.BindSetMember(binder: SetMemberBinder, value: DynamicMetaObject) : DynamicMetaObject =
        let setExpression = Expression.Call(
            Expression.Convert(this.Expression, typeof<BsonDocument>),
            BsonDocumentMetaObject.SetPropertyMethod,
            Expression.Constant(binder.Name),
            value.Expression
        )
        let returnExpression = Expression.Block(setExpression, value.Expression)
        DynamicMetaObject(returnExpression, BindingRestrictions.GetTypeRestriction(this.Expression, this.LimitType))

    override this.BindConvert(binder: ConvertBinder) : DynamicMetaObject =
        let convertExpression = Expression.Call(
            Expression.Convert(this.Expression, typeof<BsonDocument>),
            BsonDocumentMetaObject.ToJsonMethod
        )
        DynamicMetaObject(convertExpression, BindingRestrictions.GetTypeRestriction(this.Expression, this.LimitType))

    override this.BindGetIndex(binder: GetIndexBinder, indexes: DynamicMetaObject[]) : DynamicMetaObject =
        if indexes.Length <> 1 then
            failwithf "BSON does not support indexes length <> 1: %i" indexes.Length
        let indexExpr = indexes.[0].Expression
        let resultExpression = Expression.Call(
            Expression.Convert(this.Expression, typeof<BsonDocument>),
            BsonDocumentMetaObject.GetPropertyMethod,
            Expression.Call(indexExpr, BsonDocumentMetaObject.ToStringMethod)
        )
        DynamicMetaObject(resultExpression, BindingRestrictions.GetTypeRestriction(this.Expression, this.LimitType))

    override this.BindSetIndex(binder: SetIndexBinder, indexes: DynamicMetaObject[], value: DynamicMetaObject) : DynamicMetaObject =
        if indexes.Length <> 1 then
            failwithf "BSON does not support indexes length <> 1: %i" indexes.Length
        let indexExpr = indexes.[0].Expression
        let setExpression = Expression.Call(
            Expression.Convert(this.Expression, typeof<BsonDocument>),
            BsonDocumentMetaObject.SetPropertyMethod,
            Expression.Call(indexExpr, BsonDocumentMetaObject.ToStringMethod),
            value.Expression
        )
        let returnExpression = Expression.Block(setExpression, value.Expression)
        DynamicMetaObject(returnExpression, BindingRestrictions.GetTypeRestriction(this.Expression, this.LimitType))

    override this.GetDynamicMemberNames() : IEnumerable<string> =
        match value.Json with
        | JsonValue.Object o -> seq { for kv in o do yield kv.Key }
        | _ -> Seq.empty


type InsertManyResult = {
    Ids: IList<int64>
    Count: int64
}

/// The F# compiler cannot decide which method to call.
[<AbstractClass; Sealed>]
type internal ProxyRef =
    static member ReplaceOne (collection: SoloDatabase.ISoloDBCollection<'a>) (document: 'a) (filter: Expression<Func<'a, bool>>) =
        collection.ReplaceOne (filter, document)

    static member ReplaceMany (collection: SoloDatabase.ISoloDBCollection<'a>) (document: 'a) (filter: Expression<Func<'a, bool>>) =
        collection.ReplaceMany (filter, document)

[<Extension; AbstractClass; Sealed>]
type CollectionExtensions =
    [<Extension>]
    static member CountDocuments<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, filter: Expression<Func<'a, bool>>) : int64 =
        collection.Where(filter).LongCount()

    [<Extension>]
    static member CountDocuments<'a>(collection: SoloDatabase.ISoloDBCollection<'a>) : int64 =
        collection.LongCount()

    [<Extension>]
    static member Find<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, filter: Expression<Func<'a, bool>>) =
        collection.Where(filter)
        
    [<Extension>]
    static member InsertOne<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, document: 'a) =
        collection.Insert document

    [<Extension>]
    static member InsertMany<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, documents: 'a seq) =
        let result = (collection.InsertBatch documents)
        {
            Ids = result
            Count = result.Count
        }

    [<Extension>]
    static member ReplaceOne<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, filter: Expression<Func<'a, bool>>, document: 'a) =
        ProxyRef.ReplaceOne collection document filter

    [<Extension>]
    static member ReplaceMany<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, filter: Expression<Func<'a, bool>>, document: 'a) =
        ProxyRef.ReplaceMany collection document filter

    [<Extension>]
    static member DeleteOne<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, filter: Expression<Func<'a, bool>>) =
        collection.DeleteMany(filter)

    [<Extension>]
    static member DeleteMany<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, filter: Expression<Func<'a, bool>>) =
        collection.DeleteMany(filter)

module private Helper =
    /// Helper to create an expression from a string field name
    let internal getPropertyExpression (fieldPath: string) : Expression<Func<'T, 'TField>> =
        let parameter = Expression.Parameter(typeof<'T>, "x")
        let fields = fieldPath.Split('.')
        
        let rec buildExpression (expr: Expression) (fields: string list) : Expression =
            match fields with
            | [] -> expr
            | field :: rest -> // Handle intermediate fields
                let field = 
                    if field = "_id" then "Id"
                    else field

                let propertyOrField =
                    if expr.Type.GetField(field) <> null || expr.Type.GetProperty(field) <> null then
                        Expression.PropertyOrField(expr, field) :> Expression
                    else
                        let indexExpr =
                            Expression.MakeIndex(expr, expr.Type.GetProperty "Item", [Expression.Constant field]) :> Expression

                        if rest.IsEmpty && field <> "Id" && (expr.Type = typeof<BsonDocument> || expr.Type = typeof<JsonValue>) then
                            Expression.Call(indexExpr, expr.Type.GetMethod("ToObject").MakeGenericMethod(typeof<'TField>))
                        elif field = "Id" then
                            Expression.Convert(Expression.Convert(indexExpr, typeof<obj>), typeof<'TField>)
                        else
                            indexExpr

                buildExpression propertyOrField rest

        let finalExpr = buildExpression (parameter :> Expression) (fields |> List.ofArray)
        Expression.Lambda<Func<'T, 'TField>>(finalExpr, parameter)

type FilterDefinitionBuilder<'T> () =
    let mutable filters = []

    member this.Empty = Expression.Lambda<Func<'T, bool>>(Expression.Constant(true), Expression.Parameter(typeof<'T>, "x"))

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
        this.Eq<'TField>(Helper.getPropertyExpression (field), value)

    member this.Gt<'TField when 'TField :> IComparable>(field: string, value: 'TField) : FilterDefinitionBuilder<'T> =
        this.Gt<'TField>(Helper.getPropertyExpression (field), value)

    member this.Lt<'TField when 'TField :> IComparable>(field: string, value: 'TField) : FilterDefinitionBuilder<'T> =
        this.Lt<'TField>(Helper.getPropertyExpression (field), value)

    member this.In<'TField>(field: string, values: IEnumerable<'TField>) : FilterDefinitionBuilder<'T> =
        this.In<'TField>(Helper.getPropertyExpression (field), values)

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

type QueryDefinitionBuilder<'T>() =
    let filterBuilder = new FilterDefinitionBuilder<'T>()

    member this.Empty = filterBuilder.Empty

    member this.EQ<'TField>(field: Expression<Func<'T, 'TField>>, value: 'TField) : QueryDefinitionBuilder<'T> =
        filterBuilder.Eq<'TField>(field, value) |> ignore
        this

    member this.GT<'TField when 'TField :> IComparable>(field: Expression<Func<'T, 'TField>>, value: 'TField) : QueryDefinitionBuilder<'T> =
        filterBuilder.Gt<'TField>(field, value) |> ignore
        this

    member this.LT<'TField when 'TField :> IComparable>(field: Expression<Func<'T, 'TField>>, value: 'TField) : QueryDefinitionBuilder<'T> =
        filterBuilder.Lt<'TField>(field, value) |> ignore
        this

    member this.IN<'TField>(field: Expression<Func<'T, 'TField>>, values: IEnumerable<'TField>) : QueryDefinitionBuilder<'T> =
        filterBuilder.In<'TField>(field, values) |> ignore
        this

    member this.EQ<'TField>(field: string, value: 'TField) : QueryDefinitionBuilder<'T> =
        filterBuilder.Eq<'TField>(field, value) |> ignore
        this

    member this.GT<'TField when 'TField :> IComparable>(field: string, value: 'TField) : QueryDefinitionBuilder<'T> =
        filterBuilder.Gt<'TField>(field, value) |> ignore
        this

    member this.LT<'TField when 'TField :> IComparable>(field: string, value: 'TField) : QueryDefinitionBuilder<'T> =
        filterBuilder.Lt<'TField>(field, value) |> ignore
        this

    member this.IN<'TField>(field: string, values: IEnumerable<'TField>) : QueryDefinitionBuilder<'T> =
        filterBuilder.In<'TField>(field, values) |> ignore
        this

    // Method to combine all filters into a single LINQ expression and build the query.
    member this.Build() : Expression<Func<'T, bool>> =
        filterBuilder.Build()

    static member op_Implicit(builder: QueryDefinitionBuilder<'T>) : Expression<Func<'T, bool>> =
        builder.Build()

[<Sealed>]
type Builders<'T> =
    static member Filter with get() = FilterDefinitionBuilder<'T> ()

    static member Query with get() = QueryDefinitionBuilder<'T> ()

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

