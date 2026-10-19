namespace SoloDatabase

open SoloDatabase.Attributes
open System.Reflection
open System.Linq.Expressions
open System
open SoloDatabase
open Utils
open SoloDatabase.Types
open SoloDatabase.JsonSerializator
open JsonEqualitySupport
open System.Runtime.CompilerServices
open System.Numerics
open SoloDatabase.Numerics


/// Subject publication preserves original tokens; equality consumes the parsed native tree.
type internal TreeValueFormat = Subject = 0 | EqualityKey = 1

module internal JsonFunctions =
    let [<Literal>] treeValueFunction = "TREE_VALUE"
    /// Result types whose established materializer consumes SQLite scalar text directly.
    let internal consumesSqlScalar (resultType: Type) =
        typeof<string>.Equals(resultType)
        || resultType.IsPrimitive && not (typeof<bool>.Equals(resultType)) && not (typeof<char>.Equals(resultType))
        || typeof<decimal>.Equals(resultType)
        || typeof<BigInteger>.Equals(resultType)
        || typeof<Nullable<decimal>>.Equals(resultType)
        || typeof<Nullable<BigInteger>>.Equals(resultType)

    let toDocument<'T> includeType (value: 'T) =
        JsonValue.WriteBuffer<'T>(value,includeType,EntityDocumentId<'T>.Value)

    /// Conservative scalar fast path. Other types use the encoding flag returned by
    /// the canonical converter, so their storage representation is not duplicated in callers.
    let internal parameterMayNeedJson (valueType: Type) =
        let t = match TypeMetadata.nullableUnderlying valueType with null -> valueType | underlying -> underlying
        not (t.IsPrimitive || t.IsEnum || typeof<string>.Equals(t))

    let internal toSQLParameterForComparison comparison (item: obj) =
        match item with
        | null -> struct (null, false)

        // Strings and chars
        | :? string -> struct (item, false)
        | :? char as c -> struct (string c :> obj, false)

        // Boolean — SQLite stores as integer 0/1
        | :? bool as b -> struct ((if b then 1L :> obj else 0L :> obj), false)

        // Type → full name string
        | :? Type as t -> struct (t.FullName :> obj, false)

        // Signed integers — all pass through directly to SQLite INTEGER
        | :? int8 -> struct (item, false)
        | :? int16 -> struct (item, false)
        | :? int32 -> struct (item, false)
        | :? int64 -> struct (item, false)
        | :? nativeint -> struct (item, false)

        // Unsigned integers — pass through directly
        | :? uint8 -> struct (item, false)
        | :? uint16 -> struct (item, false)
        | :? uint32 -> struct (item, false)
        | :? uint64 as value -> struct (box(NumericKey.encode(ExactNumber.ofBigInteger(BigInteger value))),false)
        | :? unativeint as value when UIntPtr.Size=8 -> struct (box(NumericKey.encode(ExactNumber.ofBigInteger(BigInteger(uint64 value)))),false)
        | :? unativeint -> struct (item,false)

        // Floating point — pass through directly to SQLite REAL
        | :? float32 -> struct (item, false)
        | :? float -> struct (item, false)
        | :? decimal as x -> struct (box(NumericKey.encode(ExactNumber.ofDecimal x)), false)
        | :? BigInteger as x -> struct (box(NumericKey.encode(ExactNumber.ofBigInteger x)), false)

        // Date/time types — must match JsonSerializator storage format (all stored as numeric values).
        | :? DateOnly as x -> struct (x.DayNumber :> obj, false)
        | :? DateTime as x -> struct (x.ToBinary() :> obj, false)
        | :? DateTimeOffset as x -> struct (x.ToUnixTimeMilliseconds() :> obj, false)
        | :? TimeOnly as x -> struct ((int64 (x.ToTimeSpan().TotalMilliseconds)) :> obj, false)
        | :? TimeSpan as x -> struct ((int64 x.TotalMilliseconds) :> obj, false)

        // Guid — string representation
        | :? System.Guid as x -> struct (x.ToString("D") :> obj, false)

        // Enums — serialize as underlying integer value (matches SoloDB JSON storage)
        | _ when item.GetType().IsEnum -> struct (System.Convert.ToInt64(item) :> obj, false)

        // Structured values use the shared raw writer and scalar storage policy.
        // This path handles arrays, lists, custom objects, and any other structured types.
        | _other ->
            let value, json = JsonValue.StoredParameter(typeof<obj>,item)
            struct (value, json)


    /// The untyped parameter converter selects the actual value's writer.
    let internal parameterWriterType declaredType (value: obj) =
        if isNull value then declaredType else JsonValue.RuntimeWriterType(value, value.GetType())

    /// A closed leaf writer or sealed declared type cannot change with invocation values.
    let internal parameterWriterIsFixed (declaredType: Type) =
        declaredType.IsValueType || declaredType.IsSealed ||
        (match JsonValue.SerializedValueFacts(Utils.optionalPayloadType declaredType) with
         | JsonSerializationShape.Uninterpreted -> true
         | _ -> false)

    /// Whole explicit tokens reach query producers before native scalar sampling.
    let internal prepareQueryParameter comparison declaredType (value: obj) =
        let writer = parameterWriterType declaredType value
        if comparison && typeof<char>.Equals(Utils.optionalPayloadType declaredType) then
            let stored, encoded = JsonValue.StoredComparisonParameter(declaredType, value)
            struct(stored, encoded, writer)
        else
        match JsonValue.SerializedValueFacts(Utils.optionalPayloadType writer) with
        | JsonSerializationShape.Uninterpreted when not(isNull value) ->
            let text = JsonValue.WriteRuntimeBuffer(writer,value,false,false)
            struct(box(System.String(text.Array,text.Offset,text.Count)), true, writer)
        | _ ->
            let struct(stored, encoded) = toSQLParameterForComparison comparison value
            struct(stored, encoded, writer)

    // Declared exact numerical operands share the stored comparable representation.
    let internal toSQLParameter item = toSQLParameterForComparison false item

    let internal fromJson<'T> (json: JsonValue) =
        match json with
        | Null when typeof<float>.Equals(typeof<'T>) ->
            nan :> obj :?> 'T
        | Null when typeof<float32>.Equals(typeof<'T>) ->
            nanf :> obj :?> 'T
        | Null when typeof<'T>.IsValueType ->
            raise (InvalidOperationException "Invalid operation on a value type.")
        | json -> json.ToObject<'T>()

    /// Internal serialization helper. Not part of the public API.
    let fromIdJson<'T> (element: JsonValue) =
        let id = element.["Id"].ToObject<int64>()
        let value = element.GetProperty("Value") |> fromJson<'T>

        id, value

    // A null Id carries the translator-generated exception payload. Keep its
    // decoder outside successful row materialization.
    [<MethodImpl(MethodImplOptions.NoInlining)>]
    let private raiseRowError (row: DbObjectRow) : unit =
        match JsonValue.Parse row.ValueJSON with
        | Object o ->
            let kind =
                match o.TryGetValue "kind" with
                | true, String s -> s
                | _ -> ""
            let msg =
                match o.TryGetValue "msg" with
                | true, String s -> s
                | _ -> row.ValueJSON
            match kind with
            | "CardinalityError" -> raise (InvalidOperationException msg)
            | "CastError" -> raise (InvalidCastException msg)
            | "RangeError" -> raise (ArgumentOutOfRangeException(null, msg))
            | _ ->
                // Internal invariant violation: Id=NULL row carried an unrecognized
                // kind. All translator emit sites must use the closed
                // RuntimeErrorKind enum (CardinalityError | CastError | RangeError).
                raise (InvalidOperationException msg)
        | _ ->
            // Internal invariant violation: Id=NULL row payload is not a JSON object.
            raise (InvalidOperationException row.ValueJSON)


    let private needsPhysicalTreeId id isObject hasId = isObject && not hasId && id >= 0L

    /// Member identity shares the tree reader policy; original child tokens remain unconverted.
    let internal physicalTreeSubject (id: int64) (text: string) =
        if id < 0L then text
        else
            let mutable input = JsonReadContext(JsonLexer.fromString text, { Start = 0; Length = text.Length })
            JsonReadCursor.scan &input
            let isObject = not(isNull input.Properties)
            let hasId = isObject && input.Properties.ContainsKey "Id"
            if not(needsPhysicalTreeId id isObject hasId) then text
            else
                let output = System.Text.StringBuilder(text.Length + 32)
                output.Append('{') |> ignore
                for memberValue in input.Properties do
                    output.Append(JsonStringLiteral.property memberValue.Key) |> ignore
                    let range = memberValue.Value
                    output.Append(text,range.Start,range.Length).Append(',') |> ignore
                output.Append(JsonStringLiteral.property "Id").Append(JsonValue.Number(decimal id).ToJsonString()).Append('}') |> ignore
                output.ToString()

    let internal withPhysicalId<'R when 'R :> obj> (id: int64) (value: 'R) : 'R =
        if typeof<JsonValue>.Equals(typeof<'R>) then
            let json = value :> obj :?> JsonValue
            let isObject = json.JsonType = JsonValueType.Object
            if needsPhysicalTreeId id isObject (isObject && json.Contains "Id") then json.["Id"] <- id
            value
        elif not(isNull(box value)) && id <> -1 && EntityDocumentId<'R>.Value then
            EntityDocumentId<'R>.WithId value id
        else value

    let private fromSQLiteJson<'R when 'R :> obj> (row: DbObjectRow) : 'R =
        if typeof<JsonValue>.Equals(typeof<'R>) then
            // A requested JsonValue is the explicit tree boundary, including physical Id injection.
            let json = JsonValue.Parse row.ValueJSON
            withPhysicalId row.Id.Value (json :> obj :?> 'R)
        else
            withPhysicalId row.Id.Value (JsonValue.ParseStored<'R>(row.ValueJSON))

    [<MethodImpl(MethodImplOptions.NoInlining)>]
    let private fromSQLiteValue<'R when 'R :> obj> (row: DbObjectRow) : 'R =
        let inline genericReinterpret (a: 'a) =
            // No allocations.
            Unsafe.As<'a, 'R>(&Unsafe.AsRef(&a))

        let isNonNullableDateTimeLikeNull =
            isNull row.ValueJSON
            && isDateTimeLikeType typeof<'R>
            && typeof<'R>.Equals(unwrapNullableType typeof<'R>)

        match typeof<'R> with
        | _ when isNonNullableDateTimeLikeNull ->
            raise (InvalidOperationException "Invalid operation on a value type.")
        | OfType float when isNull row.ValueJSON -> (genericReinterpret Double.NaN)
        | OfType float32 when isNull row.ValueJSON -> (genericReinterpret Single.NaN)
        | OfType float when row.ValueJSON <> null -> (genericReinterpret << float) row.ValueJSON
        | OfType float32 when row.ValueJSON <> null -> (genericReinterpret << float32) row.ValueJSON
        | _ when isNull row.ValueJSON -> Unchecked.defaultof<'R>
        | OfType decimal -> (genericReinterpret << ExactNumber.toDecimal << NumericKey.decode) row.ValueJSON
        | _ when typeof<BigInteger>.Equals(typeof<'R>) -> genericReinterpret(ExactNumber.toBigInteger(NumericKey.decode row.ValueJSON))
        | _ when typeof<Nullable<decimal>>.Equals(typeof<'R>) -> genericReinterpret(Nullable(ExactNumber.toDecimal(NumericKey.decode row.ValueJSON)))
        | _ when typeof<Nullable<BigInteger>>.Equals(typeof<'R>) -> genericReinterpret(Nullable(ExactNumber.toBigInteger(NumericKey.decode row.ValueJSON)))

        | OfType int8 -> (genericReinterpret << int8) row.ValueJSON
        | OfType uint8 -> (genericReinterpret << uint8) row.ValueJSON
        | OfType int16 -> (genericReinterpret << int16) row.ValueJSON
        | OfType uint16 -> (genericReinterpret << uint16) row.ValueJSON
        | OfType int32 -> (genericReinterpret << int32) row.ValueJSON
        | OfType uint32 -> (genericReinterpret << uint32) row.ValueJSON
        | OfType int64 -> (genericReinterpret << int64) row.ValueJSON
        | OfType uint64 -> (genericReinterpret << uint64) row.ValueJSON
        | OfType nativeint -> (genericReinterpret << nativeint) row.ValueJSON
        | OfType unativeint -> (genericReinterpret << unativeint) row.ValueJSON

        | _ ->
        fromSQLiteJson<'R> row

    let internal fromSQLite<'R when 'R :> obj> (row: DbObjectRow) : 'R =
        if isNull (row :> obj) then Unchecked.defaultof<'R>
        else
            if not row.Id.HasValue then raiseRowError row
            if typeof<'R>.Equals(typeof<string>) then row.ValueJSON :> obj :?> 'R
            elif typeof<'R>.IsValueType then fromSQLiteValue<'R> row
            elif isNull row.ValueJSON && DBRefTypeHelpers.isFileRefManyType typeof<'R> then fromJson<'R> JsonValue.Null
            elif isNull row.ValueJSON then Unchecked.defaultof<'R>
            else fromSQLiteJson<'R> row

    let internal fromSQLiteCaptured<'R when 'R :> obj> storageClass (row: DbObjectRow): 'R =
        let resultType=typeof<'R>
        if row.Id.HasValue && not(isNull row.ValueJSON) &&
           (typeof<uint64>.Equals(resultType) || typeof<unativeint>.Equals(resultType)) &&
           storageClass=SQLitePCL.raw.SQLITE_TEXT then
            let exact=NumericKey.decode row.ValueJSON
            if typeof<uint64>.Equals(resultType) then
                let mutable unsigned=ExactNumber.toUInt64 exact
                Unsafe.As<uint64,'R>(&unsigned)
            else
                let mutable unsigned =
                    if UIntPtr.Size=4 then unativeint(ExactNumber.toUInt32 exact)
                    else unativeint(ExactNumber.toUInt64 exact)
                Unsafe.As<unativeint,'R>(&unsigned)
        else fromSQLite<'R> row

    /// Materializes inside a private producer; only owning values leave this call.
    let internal fromSQLiteRow<'R when 'R :> obj> (row: SqlRow) : struct(DbObjectRow * 'R) =
        // Deferred hydration keeps its established post-root callback order.
        let id = if row.IsDBNull(0) then Nullable() else Nullable(row.GetInt64(0))
        let hydration = if row.FieldCount > 2 && not(row.IsDBNull(2)) then row.GetString(2) else null
        let missing = row.IsDBNull(1)
        let resultType = typeof<'R>
        // These conversions deliberately retain the established text scalar policy.
        if not id.HasValue || consumesSqlScalar resultType || missing then
            let storageClass=row.StorageClass(1)
            let document =
                { Id = id
                  ValueJSON = if missing then null else row.GetString(1)
                  HydrationJSON = hydration }
            struct(document, fromSQLiteCaptured<'R> storageClass document)
        else
            // Stored materialization preserves the established UTF16 numeric policy.
            // SQLite may convert its text representation before any model callback.
            let value =
                if typeof<JsonValue>.Equals(resultType) then
                    let json = if row.CanBorrowUtf16 then JsonValue.Parse(row.TextUtf16(1)) else JsonValue.Parse(row.GetString(1))
                    json :> obj :?> 'R
                elif row.CanBorrowUtf16 then JsonValue.ParseStored<'R>(row.TextUtf16(1))
                else JsonValue.ParseStored<'R>(row.GetString(1))
            let document = { Id = id; ValueJSON = null; HydrationJSON = hydration }
            struct(document, withPhysicalId id.Value value)
