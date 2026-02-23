namespace SoloDatabase.MongoDB

open System.Linq.Expressions
open System
open System.Collections.Generic
open System.Collections
open SoloDatabase.JsonSerializator
open System.Dynamic
open System.Globalization
open System.Reflection

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
    /// </summary>
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
    /// </summary>
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
    /// </summary>
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
    /// </summary>
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
    static member Deserialize (jsonString: string) =
        BsonDocument (JsonValue.Parse (jsonString))

    /// <summary>
    /// Returns the JSON string representation of the BsonDocument.
    /// </summary>
    override this.ToString () = this.ToJsonString ()

    /// <summary>
    /// Gets or sets the value associated with the specified key.
    /// </summary>
    member this.Item
        with get (key: string) : BsonDocument =
            json.[key] |> BsonDocument
        and set (key: string) (value: BsonDocument) =
            json.[key] <- value.Json

    /// <summary>
    /// Returns an enumerator that iterates through the key-value pairs of the BsonDocument.
    /// </summary>
    interface IEnumerable<KeyValuePair<string, JsonValue>> with
        override this.GetEnumerator () =
            (json :> IEnumerable<KeyValuePair<string, JsonValue>>).GetEnumerator ()

        override this.GetEnumerator (): IEnumerator =
            (this :> IEnumerable<KeyValuePair<string, JsonValue>>).GetEnumerator () :> IEnumerator

    /// <summary>
    /// Internal method used by the dynamic meta object for property binding.
    /// </summary>
    member internal this.GetPropertyForBinder(name: string) =
        match json.GetPropertyForBinder name with
        | :? JsonValue as v -> v |> BsonDocument |> box
        | other -> other

    /// <summary>
    /// Provides the implementation for dynamic operations on this BsonDocument.
    /// </summary>
    interface System.Dynamic.IDynamicMetaObjectProvider with
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
