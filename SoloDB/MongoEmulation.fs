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

/// <summary>
/// Represents a BSON document, which is a wrapper around a JsonValue to provide
/// a dynamic and versatile way to interact with JSON/BSON data structures.
/// It implements IDynamicMetaObjectProvider to allow for dynamic property access.
/// </summary>
/// <param name="json">The underlying JsonValue that this BsonDocument wraps.</param>
type BsonDocument (json: JsonValue) =
    /// <summary>
    /// Initializes a new, empty instance of the <see cref="BsonDocument"/> class, representing an empty JSON object.
    /// </summary>
    new () = BsonDocument (JsonValue.New())
    
    /// <summary>
    /// Initializes a new instance of the <see cref="BsonDocument"/> class by serializing a given object.
    /// </summary>
    /// <param name="objToSerialize">The object to serialize into the BsonDocument.</param>
    new (objToSerialize: obj) = BsonDocument (JsonValue.Serialize objToSerialize)

    /// <summary>
    /// Gets the underlying <see cref="JsonValue"/> of the BsonDocument.
    /// </summary>
    member this.Json = json

    /// <summary>
    /// Adds or updates a key-value pair in the BsonDocument. The value is serialized to a JsonValue.
    /// </summary>
    /// <param name="key">The key of the element to add or update.</param>
    /// <param name="value">The value of the element to add or update.</param>
    member this.Add (key: string, value: obj) =
        json.[key] <- JsonValue.Serialize value

    /// <summary>
    /// Retrieves a <see cref="JsonValue"/> associated with the specified key.
    /// </summary>
    /// <param name="key">The key of the value to retrieve.</param>
    /// <returns>The <see cref="JsonValue"/> for the given key, or <see cref="JsonValue.Null"/> if the key is not found.</returns>
    member this.GetValue (key: string) : JsonValue =
        match json.TryGetProperty(key) with
        | true, v -> v
        | false, _ -> JsonValue.Null

    /// <summary>
    /// Removes a key-value pair from the BsonDocument.
    /// If the underlying structure is a list, the key is parsed to an int32 and used as index.
    /// </summary>
    /// <param name="key">The key or index of the element to remove.</param>
    /// <returns><c>true</c> if the element is successfully removed; otherwise, <c>false</c>.</returns>
    /// <exception cref="System.InvalidOperationException">Thrown if the underlying data structure is not a JSON object or list.</exception>
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

    /// <summary>
    /// Determines whether the BsonDocument contains a specific item.
    /// For an object, it checks for key containment. For a list, it checks for value containment.
    /// </summary>
    /// <param name="item">The item to locate in the BsonDocument.</param>
    /// <returns><c>true</c> if the item is found; otherwise, <c>false</c>.</returns>
    /// <exception cref="System.InvalidOperationException">Thrown if the operation is not applicable to the underlying JSON type.</exception>
    member this.Contains(item: obj) =
        let itemJson = JsonValue.Serialize item
        match json with
        | List l -> l.Contains itemJson
        | Object o -> o.Keys.Contains (itemJson.ToObject<string>())
        | other -> raise (InvalidOperationException (sprintf "Cannot call Contains(%A) on %A" itemJson other))

    /// <summary>
    /// Deserializes the BsonDocument to an instance of the specified type.
    /// </summary>
    /// <typeparam name="T">The type to deserialize the document into.</typeparam>
    /// <returns>An instance of type <typeparamref name="T"/>.</returns>
    member this.ToObject<'T>() = json.ToObject<'T>()

    /// <summary>
    /// Serializes this BsonDocument to its JSON string representation.
    /// </summary>
    /// <returns>A JSON string that represents the current BsonDocument.</returns>
    member this.ToJsonString () = json.ToJsonString ()

    /// <summary>
    /// Gets the value of the BsonDocument as a <see cref="System.Boolean"/>.
    /// </summary>
    /// <exception cref="System.InvalidCastException">Thrown if the underlying value is not a boolean.</exception>
    member this.AsBoolean = match json with JsonValue.Boolean b -> b | _ -> raise (InvalidCastException("Not a boolean"))
    
    /// <summary>
    /// Gets the value of the BsonDocument as a BSON array (another BsonDocument wrapping a list).
    /// </summary>
    /// <exception cref="System.InvalidCastException">Thrown if the underlying value is not a JSON list.</exception>
    member this.AsBsonArray = match json with JsonValue.List _l -> BsonDocument(json) | _ -> raise (InvalidCastException("Not a BSON array"))

    /// <summary>
    /// Gets the value of the BsonDocument as a BSON document (another BsonDocument wrapping an object).
    /// </summary>
    /// <exception cref="System.InvalidCastException">Thrown if the underlying value is not a JSON object.</exception>
    member this.AsBsonDocument = match json with JsonValue.Object _o -> BsonDocument(json) | _ -> raise (InvalidCastException("Not a BSON document"))

    /// <summary>
    /// Gets the value of the BsonDocument as a <see cref="System.String"/>.
    /// </summary>
    /// <exception cref="System.InvalidCastException">Thrown if the underlying value is not a string.</exception>
    member this.AsString = match json with JsonValue.String s -> s | _ -> raise (InvalidCastException("Not a string"))

    /// <summary>
    /// Gets the value of the BsonDocument as a <see cref="System.Int32"/>.
    /// </summary>
    /// <exception cref="System.InvalidCastException">Thrown if the underlying value is not a number or is out of the Int32 range.</exception>
    member this.AsInt32 = match json with JsonValue.Number n when n <= decimal Int32.MaxValue && n >= decimal Int32.MinValue -> int n | _ -> raise (InvalidCastException("Not an int32"))

    /// <summary>
    /// Gets the value of the BsonDocument as a <see cref="System.Int64"/>.
    /// </summary>
    /// <exception cref="System.InvalidCastException">Thrown if the underlying value is not a number or is out of the Int64 range.</exception>
    member this.AsInt64 = match json with JsonValue.Number n when n <= decimal Int64.MaxValue && n >= decimal Int64.MinValue -> int64 n | _ -> raise (InvalidCastException("Not an int64"))

    /// <summary>
    /// Gets the value of the BsonDocument as a <see cref="System.Double"/>.
    /// </summary>
    /// <exception cref="System.InvalidCastException">Thrown if the underlying value is not a number.</exception>
    member this.AsDouble = match json with JsonValue.Number n -> double n | _ -> raise (InvalidCastException("Not a double"))

    /// <summary>
    /// Gets the value of the BsonDocument as a <see cref="System.Decimal"/>.
    /// </summary>
    /// <exception cref="System.InvalidCastException">Thrown if the underlying value is not a number.</exception>
    member this.AsDecimal = match json with JsonValue.Number n -> n | _ -> raise (InvalidCastException("Not a decimal"))

    /// <summary>
    /// Gets a value indicating whether the BsonDocument represents a boolean.
    /// </summary>
    member this.IsBoolean = match json with JsonValue.Boolean _ -> true | _ -> false
    
    /// <summary>
    /// Gets a value indicating whether the BsonDocument represents a BSON array (JSON list).
    /// </summary>
    member this.IsBsonArray = match json with JsonValue.List _ -> true | _ -> false
    
    /// <summary>
    /// Gets a value indicating whether the BsonDocument represents a BSON document (JSON object).
    /// </summary>
    member this.IsBsonDocument = match json with JsonValue.Object _ -> true | _ -> false

    /// <summary>
    /// Gets a value indicating whether the BsonDocument represents a string.
    /// </summary>
    member this.IsString = match json with JsonValue.String _ -> true | _ -> false
    
    /// <summary>
    /// Gets a value indicating whether the BsonDocument represents a number that fits within an <see cref="System.Int32"/>.
    /// </summary>
    member this.IsInt32 = match json with JsonValue.Number n when n <= decimal Int32.MaxValue && n >= decimal Int32.MinValue -> true | _ -> false
    
    /// <summary>
    /// Gets a value indicating whether the BsonDocument represents a number that fits within an <see cref="System.Int64"/>.
    /// </summary>
    member this.IsInt64 = match json with JsonValue.Number n when n <= decimal Int64.MaxValue && n >= decimal Int64.MinValue -> true | _ -> false
    
    /// <summary>
    /// Gets a value indicating whether the BsonDocument represents a number.
    /// </summary>
    member this.IsDouble = match json with JsonValue.Number _ -> true | _ -> false

    /// <summary>
    /// Converts the BsonDocument's value to a <see cref="System.Boolean"/> using lenient conversion rules.
    /// Non-empty strings, non-zero numbers, lists, and objects convert to <c>true</c>. Null, empty strings and zero convert to <c>false</c>.
    /// </summary>
    /// <returns>The boolean representation of the value.</returns>
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

    /// <summary>
    /// Converts the BsonDocument's value to an <see cref="System.Int32"/>.
    /// It can convert from numbers or numeric strings.
    /// </summary>
    /// <returns>The Int32 representation of the value.</returns>
    /// <exception cref="System.InvalidCastException">Thrown if the value cannot be converted to an Int32.</exception>
    member this.ToInt32() =
        match json with
        | JsonValue.Number n when n <= decimal Int32.MaxValue && n >= decimal Int32.MinValue -> int n
        | JsonValue.String s -> 
            match Int32.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture) with
            | true, i -> i
            | false, _ -> raise (InvalidCastException("Cannot convert to int32"))
        | _ -> raise (InvalidCastException("Cannot convert to int32"))

    /// <summary>
    /// Converts the BsonDocument's value to an <see cref="System.Int64"/>.
    /// It can convert from numbers or numeric strings.
    /// </summary>
    /// <returns>The Int64 representation of the value.</returns>
    /// <exception cref="System.InvalidCastException">Thrown if the value cannot be converted to an Int64.</exception>
    member this.ToInt64() =
        match json with
        | JsonValue.Number n when n <= decimal Int64.MaxValue && n >= decimal Int64.MinValue -> int64 n
        | JsonValue.String s -> 
            match Int64.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture) with
            | true, i -> i
            | false, _ -> raise (InvalidCastException("Cannot convert to int64"))
        | _ -> raise (InvalidCastException("Cannot convert to int64"))

    /// <summary>
    /// Converts the BsonDocument's value to a <see cref="System.Double"/>.
    /// It can convert from numbers or numeric strings.
    /// </summary>
    /// <returns>The double-precision floating-point representation of the value.</returns>
    /// <exception cref="System.InvalidCastException">Thrown if the value cannot be converted to a double.</exception>
    member this.ToDouble() =
        match json with
        | JsonValue.Number n -> double n
        | JsonValue.String s -> 
            match Double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture) with
            | true, d -> d
            | false, _ -> raise (InvalidCastException("Cannot convert to double"))
        | _ -> raise (InvalidCastException("Cannot convert to double"))

    /// <summary>
    /// Converts the BsonDocument's value to a <see cref="System.Decimal"/>.
    /// It can convert from numbers or numeric strings.
    /// </summary>
    /// <returns>The decimal representation of the value.</returns>
    /// <exception cref="System.InvalidCastException">Thrown if the value cannot be converted to a decimal.</exception>
    member this.ToDecimal() =
        match json with
        | JsonValue.Number n -> n
        | JsonValue.String s -> 
            match Decimal.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture) with
            | true, d -> d
            | false, _ -> raise (InvalidCastException("Cannot convert to decimal"))
        | _ -> raise (InvalidCastException("Cannot convert to decimal"))

    /// <summary>
    /// Deserializes a JSON string into a <see cref="BsonDocument"/>.
    /// </summary>
    /// <param name="jsonString">The JSON string to parse.</param>
    /// <returns>A new <see cref="BsonDocument"/> instance.</returns>
    static member Deserialize (jsonString: string) =
        BsonDocument (JsonValue.Parse (jsonString))

    /// <summary>
    /// Returns the JSON string representation of the BsonDocument.
    /// </summary>
    /// <returns>A JSON string that represents the current object.</returns>
    override this.ToString () = this.ToJsonString ()

    /// <summary>
    /// Gets or sets the value associated with the specified key.
    /// </summary>
    /// <param name="key">The key of the value to get or set.</param>
    /// <returns>A new <see cref="BsonDocument"/> wrapping the value.</returns>
    member this.Item
        with get (key: string) : BsonDocument = 
            json.[key] |> BsonDocument
        and set (key: string) (value: BsonDocument) =
            json.[key] <- value.Json

    /// <summary>
    /// Returns an enumerator that iterates through the key-value pairs of the BsonDocument.
    /// </summary>
    /// <returns>An enumerator for the collection.</returns>
    interface IEnumerable<KeyValuePair<string, JsonValue>> with
        override this.GetEnumerator () =
            (json :> IEnumerable<KeyValuePair<string, JsonValue>>).GetEnumerator ()
        
        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>An <see cref="System.Collections.IEnumerator"/> object that can be used to iterate through the collection.</returns>
        override this.GetEnumerator (): IEnumerator = 
            (this :> IEnumerable<KeyValuePair<string, JsonValue>>).GetEnumerator () :> IEnumerator

    /// <summary>
    /// Internal method used by the dynamic meta object for property binding.
    /// </summary>
    /// <param name="name">The name of the property to get.</param>
    /// <returns>The property value, wrapped in a <see cref="BsonDocument"/> if it's a JsonValue.</returns>
    member internal this.GetPropertyForBinder(name: string) =
        match json.GetPropertyForBinder name with
        | :? JsonValue as v -> v |> BsonDocument |> box
        | other -> other

    /// <summary>
    /// Provides the implementation for dynamic operations on this BsonDocument.
    /// </summary>
    /// <param name="expression">The expression representing this <see cref="System.Dynamic.IDynamicMetaObjectProvider"/> in the dynamic binding process.</param>
    /// <returns>A <see cref="System.Dynamic.DynamicMetaObject"/> to bind this dynamic operation.</returns>
    interface IDynamicMetaObjectProvider with
        member this.GetMetaObject(expression: Linq.Expressions.Expression): DynamicMetaObject = 
            BsonDocumentMetaObject(expression, BindingRestrictions.Empty, this)

/// <summary>
/// Internal class that provides the dynamic behavior for <see cref="BsonDocument"/>.
/// It intercepts dynamic member access, method calls, and conversions.
/// </summary>
and internal BsonDocumentMetaObject(expression: Expression, restrictions: BindingRestrictions, value: BsonDocument) =
    inherit DynamicMetaObject(expression, restrictions, value)

    /// <summary>Cached method info for GetPropertyForBinder.</summary>
    static member val private GetPropertyMethod = typeof<BsonDocument>.GetMethod("GetPropertyForBinder", BindingFlags.NonPublic ||| BindingFlags.Instance)
    /// <summary>Cached method info for the Item setter.</summary>
    static member val private SetPropertyMethod = typeof<BsonDocument>.GetMethod("set_Item")
    /// <summary>Cached method info for ToJsonString.</summary>
    static member val private ToJsonMethod = typeof<BsonDocument>.GetMethod("ToJsonString")
    /// <summary>Cached method info for Object.ToString.</summary>
    static member val private ToStringMethod = typeof<obj>.GetMethod("ToString")

    /// <summary>
    /// Binds the dynamic get member operation.
    /// </summary>
    override this.BindGetMember(binder: GetMemberBinder) : DynamicMetaObject =
        let resultExpression = Expression.Call(
            Expression.Convert(this.Expression, typeof<BsonDocument>),
            BsonDocumentMetaObject.GetPropertyMethod,
            Expression.Constant(binder.Name)
        )
        DynamicMetaObject(resultExpression, BindingRestrictions.GetTypeRestriction(this.Expression, this.LimitType))

    /// <summary>
    /// Binds the dynamic set member operation.
    /// </summary>
    override this.BindSetMember(binder: SetMemberBinder, value: DynamicMetaObject) : DynamicMetaObject =
        let setExpression = Expression.Call(
            Expression.Convert(this.Expression, typeof<BsonDocument>),
            BsonDocumentMetaObject.SetPropertyMethod,
            Expression.Constant(binder.Name),
            value.Expression
        )
        let returnExpression = Expression.Block(setExpression, value.Expression)
        DynamicMetaObject(returnExpression, BindingRestrictions.GetTypeRestriction(this.Expression, this.LimitType))

    /// <summary>
    /// Binds the dynamic convert operation.
    /// </summary>
    override this.BindConvert(binder: ConvertBinder) : DynamicMetaObject =
        let convertExpression = Expression.Call(
            Expression.Convert(this.Expression, typeof<BsonDocument>),
            BsonDocumentMetaObject.ToJsonMethod
        )
        DynamicMetaObject(convertExpression, BindingRestrictions.GetTypeRestriction(this.Expression, this.LimitType))

    /// <summary>
    /// Binds the dynamic get index operation.
    /// </summary>
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

    /// <summary>
    /// Binds the dynamic set index operation.
    /// </summary>
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

    /// <summary>
    /// Returns the enumeration of all dynamic member names.
    /// </summary>
    override this.GetDynamicMemberNames() : IEnumerable<string> =
        match value.Json with
        | JsonValue.Object o -> seq { for kv in o do yield kv.Key }
        | _ -> Seq.empty

/// <summary>
/// Represents the result of an InsertMany operation.
/// </summary>
type InsertManyResult = {
    /// <summary>
    /// The list of IDs of the inserted documents.
    /// </summary>
    Ids: IList<int64>
    /// <summary>
    /// The number of documents inserted.
    /// </summary>
    Count: int64
}

/// <summary>
/// An internal proxy class to resolve method overloading ambiguity for the F# compiler.
/// This specifically helps differentiate between ReplaceOne/ReplaceMany methods.
/// </summary>
[<AbstractClass; Sealed>]
type internal ProxyRef =
    /// <summary>
    /// Calls the ReplaceOne method on the collection with a specific parameter order.
    /// </summary>
    static member ReplaceOne (collection: SoloDatabase.ISoloDBCollection<'a>) (document: 'a) (filter: Expression<Func<'a, bool>>) =
        collection.ReplaceOne (filter, document)

    /// <summary>
    /// Calls the ReplaceMany method on the collection with a specific parameter order.
    /// </summary>
    static member ReplaceMany (collection: SoloDatabase.ISoloDBCollection<'a>) (document: 'a) (filter: Expression<Func<'a, bool>>) =
        collection.ReplaceMany (filter, document)

/// <summary>
/// Provides extension methods for <see cref="SoloDatabase.ISoloDBCollection{a}"/>
/// to offer a MongoDB-like driver API.
/// </summary>
[<Extension; AbstractClass; Sealed>]
type CollectionExtensions =
    /// <summary>
    /// Counts the number of documents matching the given filter.
    /// </summary>
    /// <typeparam name="a">The type of the document.</typeparam>
    /// <param name="collection">The collection to query.</param>
    /// <param name="filter">The LINQ expression filter.</param>
    /// <returns>The number of matching documents.</returns>
    [<Extension>]
    static member CountDocuments<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, filter: Expression<Func<'a, bool>>) : int64 =
        collection.Where(filter).LongCount()

    /// <summary>
    /// Counts all documents in the collection.
    /// </summary>
    /// <typeparam name="a">The type of the document.</typeparam>
    /// <param name="collection">The collection to query.</param>
    /// <returns>The total number of documents in the collection.</returns>
    [<Extension>]
    static member CountDocuments<'a>(collection: SoloDatabase.ISoloDBCollection<'a>) : int64 =
        collection.LongCount()

    /// <summary>
    /// Finds all documents in the collection that match the given filter.
    /// </summary>
    /// <typeparam name="a">The type of the document.</typeparam>
    /// <param name="collection">The collection to query.</param>
    /// <param name="filter">The LINQ expression filter.</param>
    /// <returns>An <see cref="System.Linq.IQueryable{a}"/> for the matching documents.</returns>
    [<Extension>]
    static member Find<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, filter: Expression<Func<'a, bool>>) =
        collection.Where(filter)
        
    /// <summary>
    /// Inserts a single document into the collection.
    /// </summary>
    /// <typeparam name="a">The type of the document.</typeparam>
    /// <param name="collection">The collection to insert into.</param>
    /// <param name="document">The document to insert.</param>
    /// <returns>The ID of the inserted document.</returns>
    [<Extension>]
    static member InsertOne<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, document: 'a) =
        collection.Insert document

    /// <summary>
    /// Inserts a sequence of documents into the collection.
    /// </summary>
    /// <typeparam name="a">The type of the document.</typeparam>
    /// <param name="collection">The collection to insert into.</param>
    /// <param name="documents">The documents to insert.</param>
    /// <returns>An <see cref="InsertManyResult"/> containing the IDs and count of inserted documents.</returns>
    [<Extension>]
    static member InsertMany<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, documents: 'a seq) =
        let result = (collection.InsertBatch documents)
        {
            Ids = result
            Count = result.Count
        }

    /// <summary>
    /// Replaces a single document that matches the filter.
    /// </summary>
    /// <typeparam name="a">The type of the document.</typeparam>
    /// <param name="collection">The collection to update.</param>
    /// <param name="filter">The filter to find the document to replace.</param>
    /// <param name="document">The new document.</param>
    /// <returns>The number of documents replaced (0 or 1).</returns>
    [<Extension>]
    static member ReplaceOne<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, filter: Expression<Func<'a, bool>>, document: 'a) =
        ProxyRef.ReplaceOne collection document filter

    /// <summary>
    /// Replaces all documents that match the filter.
    /// </summary>
    /// <typeparam name="a">The type of the document.</typeparam>
    /// <param name="collection">The collection to update.</param>
    /// <param name="filter">The filter to find the documents to replace.</param>
    /// <param name="document">The new document.</param>
    /// <returns>The number of documents replaced.</returns>
    [<Extension>]
    static member ReplaceMany<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, filter: Expression<Func<'a, bool>>, document: 'a) =
        ProxyRef.ReplaceMany collection document filter

    /// <summary>
    /// Deletes a single document that matches the filter. Note: This implementation calls DeleteMany and may delete more than one if the filter is not specific enough.
    /// </summary>
    /// <typeparam name="a">The type of the document.</typeparam>
    /// <param name="collection">The collection to delete from.</param>
    /// <param name="filter">The filter to find the document to delete.</param>
    /// <returns>The number of documents deleted.</returns>
    [<Extension>]
    static member DeleteOne<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, filter: Expression<Func<'a, bool>>) =
        collection.DeleteMany(filter)

    /// <summary>
    /// Deletes all documents that match the filter.
    /// </summary>
    /// <typeparam name="a">The type of the document.</typeparam>
    /// <param name="collection">The collection to delete from.</param>
    /// <param name="filter">The filter to find the documents to delete.</param>
    /// <returns>The number of documents deleted.</returns>
    [<Extension>]
    static member DeleteMany<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, filter: Expression<Func<'a, bool>>) =
        collection.DeleteMany(filter)

/// <summary>
/// A private module containing helper functions for building expressions.
/// </summary>
module private Helper =
    /// <summary>
    /// Creates a property access expression from a string field path (e.g., "Customer.Address.Street").
    /// Handles nested properties and indexer access for dynamic types like BsonDocument.
    /// Also handles mapping "_id" to "Id".
    /// </summary>
    /// <param name="fieldPath">The dot-separated path to the property.</param>
    /// <returns>An expression tree representing the property access.</returns>
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
                        // Fallback to indexer for dynamic types
                        let indexExpr =
                            Expression.MakeIndex(expr, expr.Type.GetProperty "Item", [Expression.Constant field]) :> Expression

                        // If this is the last part of the path, we might need to convert the result
                        if rest.IsEmpty && field <> "Id" && (expr.Type = typeof<BsonDocument> || expr.Type = typeof<JsonValue>) then
                            Expression.Call(indexExpr, expr.Type.GetMethod("ToObject").MakeGenericMethod(typeof<'TField>))
                        elif field = "Id" then
                            Expression.Convert(Expression.Convert(indexExpr, typeof<obj>), typeof<'TField>)
                        else
                            indexExpr

                buildExpression propertyOrField rest

        let finalExpr = buildExpression (parameter :> Expression) (fields |> List.ofArray)
        Expression.Lambda<Func<'T, 'TField>>(finalExpr, parameter)

/// <summary>
/// A fluent builder for creating LINQ filter expressions.
/// </summary>
/// <typeparam name="T">The type of the document to filter.</typeparam>
type FilterDefinitionBuilder<'T> () =
    /// <summary>
    /// Internal list of accumulated filter expressions.
    /// </summary>
    let mutable filters = []

    /// <summary>
    /// Gets an empty filter that matches all documents.
    /// </summary>
    member val Empty = Expression.Lambda<Func<'T, bool>>(Expression.Constant(true), Expression.Parameter(typeof<'T>, "x"))

    /// <summary>
    /// Adds an equality filter (field == value).
    /// </summary>
    /// <typeparam name="TField">The type of the field.</typeparam>
    /// <param name="field">An expression specifying the field.</param>
    /// <param name="value">The value to compare against.</param>
    /// <returns>The builder instance for chaining.</returns>
    member this.Eq<'TField>(field: Expression<Func<'T, 'TField>>, value: 'TField) : FilterDefinitionBuilder<'T> =
        let parameter = field.Parameters.[0]
        let body = Expression.Equal(field.Body, Expression.Constant(value, typeof<'TField>))
        let lambda = Expression.Lambda<Func<'T, bool>>(body, parameter)
        filters <- lambda :: filters
        this

    /// <summary>
    /// Adds a greater-than filter (field > value).
    /// </summary>
    /// <typeparam name="TField">The type of the field, which must be comparable.</typeparam>
    /// <param name="field">An expression specifying the field.</param>
    /// <param name="value">The value to compare against.</param>
    /// <returns>The builder instance for chaining.</returns>
    member this.Gt<'TField when 'TField :> IComparable>(field: Expression<Func<'T, 'TField>>, value: 'TField) : FilterDefinitionBuilder<'T> =
        let parameter = field.Parameters.[0]
        let body = Expression.GreaterThan(field.Body, Expression.Constant(value, typeof<'TField>))
        let lambda = Expression.Lambda<Func<'T, bool>>(body, parameter)
        filters <- lambda :: filters
        this

    /// <summary>
    /// Adds a less-than filter (field < value).
    /// </summary>
    /// <typeparam name="TField">The type of the field, which must be comparable.</typeparam>
    /// <param name="field">An expression specifying the field.</param>
    /// <param name="value">The value to compare against.</param>
    /// <returns>The builder instance for chaining.</returns>
    member this.Lt<'TField when 'TField :> IComparable>(field: Expression<Func<'T, 'TField>>, value: 'TField) : FilterDefinitionBuilder<'T> =
        let parameter = field.Parameters.[0]
        let body = Expression.LessThan(field.Body, Expression.Constant(value, typeof<'TField>))
        let lambda = Expression.Lambda<Func<'T, bool>>(body, parameter)
        filters <- lambda :: filters
        this

    /// <summary>
    /// Adds an "in" filter, which checks if a field's value is in a given set of values.
    /// </summary>
    /// <typeparam name="TField">The type of the field.</typeparam>
    /// <param name="field">An expression specifying the field.</param>
    /// <param name="values">The collection of values to check against.</param>
    /// <returns>The builder instance for chaining.</returns>
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

    /// <summary>
    /// Adds an equality filter using a string field name.
    /// </summary>
    /// <typeparam name="TField">The type of the field.</typeparam>
    /// <param name="field">The name of the field (can be a nested path).</param>
    /// <param name="value">The value to compare against.</param>
    /// <returns>The builder instance for chaining.</returns>
    member this.Eq<'TField>(field: string, value: 'TField) : FilterDefinitionBuilder<'T> =
        this.Eq<'TField>(Helper.getPropertyExpression (field), value)

    /// <summary>
    /// Adds a greater-than filter using a string field name.
    /// </summary>
    /// <typeparam name="TField">The type of the field, which must be comparable.</typeparam>
    /// <param name="field">The name of the field (can be a nested path).</param>
    /// <param name="value">The value to compare against.</param>
    /// <returns>The builder instance for chaining.</returns>
    member this.Gt<'TField when 'TField :> IComparable>(field: string, value: 'TField) : FilterDefinitionBuilder<'T> =
        this.Gt<'TField>(Helper.getPropertyExpression (field), value)

    /// <summary>
    /// Adds a less-than filter using a string field name.
    /// </summary>
    /// <typeparam name="TField">The type of the field, which must be comparable.</typeparam>
    /// <param name="field">The name of the field (can be a nested path).</param>
    /// <param name="value">The value to compare against.</param>
    /// <returns>The builder instance for chaining.</returns>
    member this.Lt<'TField when 'TField :> IComparable>(field: string, value: 'TField) : FilterDefinitionBuilder<'T> =
        this.Lt<'TField>(Helper.getPropertyExpression (field), value)

    /// <summary>
    /// Adds an "in" filter using a string field name.
    /// </summary>
    /// <typeparam name="TField">The type of the field.</typeparam>
    /// <param name="field">The name of the field (can be a nested path).</param>
    /// <param name="values">The collection of values to check against.</param>
    /// <returns>The builder instance for chaining.</returns>
    member this.In<'TField>(field: string, values: IEnumerable<'TField>) : FilterDefinitionBuilder<'T> =
        this.In<'TField>(Helper.getPropertyExpression (field), values)

    /// <summary>
    /// Combines all registered filters into a single LINQ expression using 'AndAlso'.
    /// </summary>
    /// <returns>A single <see cref="System.Linq.Expressions.Expression{Func{T, bool}}"/> representing the combined filters.</returns>
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

    /// <summary>
    /// Allows the builder to be implicitly converted to its resulting expression.
    /// </summary>
    /// <param name="builder">The builder instance.</param>
    /// <returns>The result of calling <see cref="Build"/>.</returns>
    static member op_Implicit(builder: FilterDefinitionBuilder<'T>) : Expression<Func<'T, bool>> =
        builder.Build()

/// <summary>
/// A fluent builder for creating query expressions. This is a wrapper around <see cref="FilterDefinitionBuilder{T}"/>.
/// </summary>
/// <typeparam name="T">The type of the document to query.</typeparam>
type QueryDefinitionBuilder<'T>() =
    /// <summary>
    /// The underlying filter builder instance.
    /// </summary>
    let filterBuilder = new FilterDefinitionBuilder<'T>()

    /// <summary>
    /// Gets an empty query that matches all documents.
    /// </summary>
    member this.Empty = filterBuilder.Empty

    /// <summary>
    /// Adds an equality query condition (field == value).
    /// </summary>
    member this.EQ<'TField>(field: Expression<Func<'T, 'TField>>, value: 'TField) : QueryDefinitionBuilder<'T> =
        filterBuilder.Eq<'TField>(field, value) |> ignore
        this

    /// <summary>
    /// Adds a greater-than query condition (field > value).
    /// </summary>
    member this.GT<'TField when 'TField :> IComparable>(field: Expression<Func<'T, 'TField>>, value: 'TField) : QueryDefinitionBuilder<'T> =
        filterBuilder.Gt<'TField>(field, value) |> ignore
        this

    /// <summary>
    /// Adds a less-than query condition (field < value).
    /// </summary>
    member this.LT<'TField when 'TField :> IComparable>(field: Expression<Func<'T, 'TField>>, value: 'TField) : QueryDefinitionBuilder<'T> =
        filterBuilder.Lt<'TField>(field, value) |> ignore
        this

    /// <summary>
    /// Adds an "in" query condition.
    /// </summary>
    member this.IN<'TField>(field: Expression<Func<'T, 'TField>>, values: IEnumerable<'TField>) : QueryDefinitionBuilder<'T> =
        filterBuilder.In<'TField>(field, values) |> ignore
        this

    /// <summary>
    /// Adds an equality query condition using a string field name.
    /// </summary>
    member this.EQ<'TField>(field: string, value: 'TField) : QueryDefinitionBuilder<'T> =
        filterBuilder.Eq<'TField>(field, value) |> ignore
        this

    /// <summary>
    /// Adds a greater-than query condition using a string field name.
    /// </summary>
    member this.GT<'TField when 'TField :> IComparable>(field: string, value: 'TField) : QueryDefinitionBuilder<'T> =
        filterBuilder.Gt<'TField>(field, value) |> ignore
        this

    /// <summary>
    /// Adds a less-than query condition using a string field name.
    /// </summary>
    member this.LT<'TField when 'TField :> IComparable>(field: string, value: 'TField) : QueryDefinitionBuilder<'T> =
        filterBuilder.Lt<'TField>(field, value) |> ignore
        this

    /// <summary>
    /// Adds an "in" query condition using a string field name.
    /// </summary>
    member this.IN<'TField>(field: string, values: IEnumerable<'TField>) : QueryDefinitionBuilder<'T> =
        filterBuilder.In<'TField>(field, values) |> ignore
        this

    /// <summary>
    /// Combines all query conditions into a single LINQ expression.
    /// </summary>
    /// <returns>A single <see cref="System.Linq.Expressions.Expression{Func{T, bool}}"/> representing the combined query.</returns>
    member this.Build() : Expression<Func<'T, bool>> =
        filterBuilder.Build()

    /// <summary>
    /// Allows the builder to be implicitly converted to its resulting expression.
    /// </summary>
    /// <param name="builder">The builder instance.</param>
    /// <returns>The result of calling <see cref="Build"/>.</returns>
    static member op_Implicit(builder: QueryDefinitionBuilder<'T>) : Expression<Func<'T, bool>> =
        builder.Build()

/// <summary>
/// Provides static access to filter and query builders, mimicking the MongoDB driver's `Builders` class.
/// </summary>
/// <typeparam name="T">The document type for which to build queries.</typeparam>
[<Sealed>]
type Builders<'T> =
    /// <summary>
    /// Gets a new <see cref="FilterDefinitionBuilder{T}"/>.
    /// </summary>
    static member Filter with get() = FilterDefinitionBuilder<'T> ()

    /// <summary>
    /// Gets a new <see cref="QueryDefinitionBuilder{T}"/>.
    /// </summary>
    static member Query with get() = QueryDefinitionBuilder<'T> ()

/// <summary>
/// Represents a database, providing access to its collections. This is an internal wrapper around a <see cref="SoloDB"/> instance.
/// </summary>
/// <param name="soloDB">The underlying SoloDB instance.</param>
type MongoDatabase internal (soloDB: SoloDB) =
    /// <summary>
    /// Gets a collection with a specific document type.
    /// </summary>
    /// <typeparam name="doc">The document type.</typeparam>
    /// <param name="name">The name of the collection.</param>
    /// <returns>An instance of <see cref="SoloDatabase.ISoloDBCollection{doc}"/>.</returns>
    member this.GetCollection<'doc> (name: string) =
        soloDB.GetCollection<'doc> name

    /// <summary>
    /// Creates or gets an untyped collection, which will work with <see cref="BsonDocument"/>.
    /// </summary>
    /// <param name="name">The name of the collection.</param>
    /// <returns>An untyped collection instance.</returns>
    member this.CreateCollection (name: string) =
        soloDB.GetUntypedCollection name

    /// <summary>
    /// Lists the names of all collections in the database.
    /// </summary>
    /// <returns>A sequence of collection names.</returns>
    member this.ListCollections () =
        soloDB.ListCollectionNames ()

    /// <summary>
    /// Disposes the underlying database connection and resources.
    /// </summary>
    member this.Dispose () =
        soloDB.Dispose ()

    /// <summary>
    /// Disposes the object.
    /// </summary>
    interface IDisposable with
        override this.Dispose (): unit = 
            this.Dispose ()

/// <summary>
/// Internal discriminated union to represent the storage location of a MongoClient.
/// </summary>
type internal MongoClientLocation =
/// <summary>An on-disk database with a directory and a lock file.</summary>
| Disk of {| Directory: DirectoryInfo; LockingFile: FileStream |}
/// <summary>An in-memory database identified by a source string.</summary>
| Memory of string

/// <summary>
/// The main client for connecting to a SoloDB data source with a MongoDB-like API.
/// It can connect to on-disk or in-memory databases.
/// </summary>
/// <param name="directoryDatabaseSource">
/// The data source. For on-disk, this is a directory path. For in-memory, it should start with "memory:".
/// </param>
/// <exception cref="System.Exception">Thrown if the source string starts with "mongodb://".</exception>
type MongoClient(directoryDatabaseSource: string) =
    do if directoryDatabaseSource.StartsWith "mongodb://" then failwithf "SoloDB does not support mongo connections."
    
    /// <summary>
    /// The determined location (Disk or Memory) of the database.
    /// </summary>
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

    /// <summary>
    /// A list of weak references to the databases created by this client.
    /// </summary>
    let connectedDatabases = ResizeArray<WeakReference<MongoDatabase>> ()
    /// <summary>
    /// A flag to indicate if the client has been disposed.
    /// </summary>
    let mutable disposed = false

    /// <summary>
    /// Gets a handle to a database.
    /// </summary>
    /// <param name="name">The name of the database. Defaults to "Master" if null.</param>
    /// <returns>A <see cref="MongoDatabase"/> instance.</returns>
    /// <exception cref="System.ObjectDisposedException">Thrown if the client has been disposed.</exception>
    member this.GetDatabase([<Optional; DefaultParameterValue(null: string)>] name : string) =
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


    /// <summary>
    /// Disposes the client, along with all databases it has created and releases any file locks.
    /// </summary>
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