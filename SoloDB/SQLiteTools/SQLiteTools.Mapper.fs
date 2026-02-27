namespace SoloDatabase

open System
open System.Reflection
open System.Collections.Generic
open System.Data
open System.Linq.Expressions
open Microsoft.FSharp.Reflection
open System.Runtime.Serialization
open SoloDatabase.JsonSerializator
open Microsoft.Data.Sqlite
open System.Data.Common
open Utils
open SQLiteToolsParams

/// <summary>
/// Internal module containing the TypeMapper and query execution helpers for SQLiteTools.
/// </summary>
module internal SQLiteToolsMapper =
    /// <summary>
    /// A helper type that provides a cached, compiled function for mapping an IDataReader record to an instance of type 'T.
    /// </summary>
    /// <typeparam name="'T">The target type to map to.</typeparam>
    type internal TypeMapper<'T> =
        /// <summary>
        /// A statically cached, compiled function that maps a data reader row to an object of type 'T.
        /// </summary>
        static member val Map =
            match typeof<'T> with
            | OfType int8 ->
                // Box avoidance: cast the function, not the primitive to obj and then to 'T
                (fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) ->
                    reader.GetByte(startIndex)) :> obj :?> IDataReader -> int -> IDictionary<string, int> -> 'T
            | OfType uint8 ->
                (fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) ->
                    reader.GetInt16(startIndex) |> uint8) :> obj :?> IDataReader -> int -> IDictionary<string, int> -> 'T
            | OfType int16 ->
                (fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) ->
                    reader.GetInt16(startIndex)) :> obj :?> IDataReader -> int -> IDictionary<string, int> -> 'T
            | OfType uint16 ->
                (fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) ->
                    reader.GetInt32(startIndex) |> uint16) :> obj :?> IDataReader -> int -> IDictionary<string, int> -> 'T
            | OfType int32 ->
                (fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) ->
                    reader.GetInt32(startIndex)) :> obj :?> IDataReader -> int -> IDictionary<string, int> -> 'T
            | OfType uint32 ->
                (fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) ->
                    reader.GetInt64(startIndex) |> uint32) :> obj :?> IDataReader -> int -> IDictionary<string, int> -> 'T
            | OfType int64 ->
                (fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) ->
                    reader.GetInt64(startIndex)) :> obj :?> IDataReader -> int -> IDictionary<string, int> -> 'T
            | OfType uint64 ->
                (fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) ->
                    reader.GetInt64(startIndex) |> uint64) :> obj :?> IDataReader -> int -> IDictionary<string, int> -> 'T
            | OfType float32 ->
                (fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) ->
                    reader.GetFloat(startIndex)) :> obj :?> IDataReader -> int -> IDictionary<string, int> -> 'T
            | OfType double ->
                (fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) ->
                    reader.GetDouble(startIndex)) :> obj :?> IDataReader -> int -> IDictionary<string, int> -> 'T
            | OfType decimal ->
                (fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) ->
                    reader.GetDecimal(startIndex)) :> obj :?> IDataReader -> int -> IDictionary<string, int> -> 'T
            | OfType string ->
                (fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) ->
                    reader.GetString(startIndex)) :> obj :?> IDataReader -> int -> IDictionary<string, int> -> 'T
            | OfType bool ->
                (fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) ->
                    reader.GetBoolean(startIndex)) :> obj :?> IDataReader -> int -> IDictionary<string, int> -> 'T
            | OfType (id: byte array -> byte array) ->
                (fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) ->
                    reader.GetValue(startIndex) :?> 'T) :> obj :?> IDataReader -> int -> IDictionary<string, int> -> 'T
            | OfType (id: Guid -> Guid) ->
                (fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) ->
                    reader.GetGuid(startIndex)) :> obj :?> IDataReader -> int -> IDictionary<string, int> -> 'T
            | OfType DateTime ->
                (fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) ->
                    reader.GetDateTime(startIndex)) :> obj :?> IDataReader -> int -> IDictionary<string, int> -> 'T
            | OfType DateTimeOffset ->
                (fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) ->
                    reader.GetInt64(startIndex) |> DateTimeOffset.FromUnixTimeMilliseconds) :> obj :?> IDataReader -> int -> IDictionary<string, int> -> 'T
            | t when t = typeof<NativeArray.NativeArray> ->
                (fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) ->
                    match reader with
                    | :? SqliteDataReader as reader ->
                        let stream = reader.GetStream(startIndex) :?> SqliteBlob
                        streamToNativeArrayFuncDisposing stream
                    | _ ->
                        let byteArray = reader.GetValue(startIndex) :?> byte array
                        let array = NativeArray.NativeArray.Alloc byteArray.Length
                        byteArray.AsSpan().CopyTo(array.Span)
                        array
                    ) :> obj :?> IDataReader -> int -> IDictionary<string, int> -> 'T
            | t when t = typeof<obj> ->
                fun (reader: IDataReader) (startIndex: int) (columns: IDictionary<string, int>) ->
                    // Scalar object queries should preserve DB NULL as null and avoid object wrappers.
                    if columns.Count = 1 then
                        let first = columns.Values |> Seq.head
                        if startIndex <= first then
                            let scalar: obj =
                                if reader.IsDBNull(first) then
                                    null
                                else
                                    reader.GetValue(first)
                            JsonSerializator.JsonValue.Serialize<obj>(scalar).ToObject<'T>()
                        else
                            Unchecked.defaultof<'T>
                    else
                    let jsonObj = JsonSerializator.JsonValue.Object(Dictionary(columns.Count))
                    for key in columns.Keys do
                        if startIndex <= columns.[key] then
                            let value = reader.GetValue(columns.[key])
                            jsonObj.[key] <- JsonSerializator.JsonValue.Serialize<obj> value

                    jsonObj.ToObject<'T>()

            | t when t = typeof<JsonValue> ->
                fun (reader: IDataReader) (startIndex: int) (columns: IDictionary<string, int>) ->
                    if columns.Count = 1 && (columns.Keys |> Seq.head).StartsWith "json_object" then
                        JsonSerializator.JsonValue.Parse (reader.GetString(0)) :> obj :?> 'T
                    else

                    let jsonObj = JsonSerializator.JsonValue.Object(Dictionary(columns.Count))
                    for key in columns.Keys do
                        if startIndex <= columns.[key] then
                            let value = reader.GetValue(columns.[key])
                            jsonObj.[key] <- JsonSerializator.JsonValue.Serialize<obj> value

                    jsonObj :> obj :?> 'T

            | t when FSharpType.IsRecord(t, true) ->
                // Parameter declarations
                let readerParam = Expression.Parameter(typeof<IDataReader>, "reader")
                let startIndexParam = Expression.Parameter(typeof<int>, "startIndex")
                let columnsParam = Expression.Parameter(typeof<IDictionary<string, int>>, "columns")

                let columnVar = Expression.Variable typeof<int>

                let recordFields = FSharpType.GetRecordFields(t, true)
                let recordFieldsType = recordFields |> Array.map(_.PropertyType)
                let ctor =
                    t.GetConstructors(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)
                    |> Array.find(fun c -> c.GetParameters() |> Array.map(_.ParameterType) = recordFieldsType)


                // Create parameter expressions for constructor
                let parameterExprs = recordFields |> Array.map (fun prop ->
                    // Check
                    let hasPropertyExpr =
                        Expression.AndAlso(
                            Expression.Call(
                                columnsParam,
                                typeof<IDictionary<string, int>>.GetMethod("TryGetValue"),
                                [
                                    Expression.Constant(prop.Name) :> Expression;
                                    columnVar :> Expression
                                ]
                            ),
                            Expression.AndAlso(
                                Expression.GreaterThanOrEqual(
                                    columnVar,
                                    startIndexParam
                                ),
                                Expression.Equal(
                                    Expression.Call(
                                        readerParam,
                                        typeof<IDataRecord>.GetMethod("IsDBNull"),
                                        [| columnVar :> Expression |]
                                    ),
                                    Expression.Constant(false)
                                )
                            ))

                    let getValueAndDeserialize = matchMethodWithMemberType prop readerParam columnVar

                    // Default value if property not found
                    let defaultValue =
                        if prop.PropertyType.IsValueType then
                            Expression.Default(prop.PropertyType) :> Expression
                        else
                            Expression.Constant(null, prop.PropertyType) :> Expression

                    // Return value from condition
                    Expression.Condition(
                        hasPropertyExpr,
                        getValueAndDeserialize,
                        defaultValue
                    ) :> Expression
                )


                // Build the complete expression
                let body = Expression.Block(
                    [|columnVar|],
                    [|Expression.New(ctor, parameterExprs) :> Expression|])

                let lambda = Expression.Lambda<Func<IDataReader, int, IDictionary<string, int>, 'T>>(
                    body,
                    [| readerParam; startIndexParam; columnsParam |]
                )

                let fn = lambda.Compile()

                fun (reader: IDataReader) (startIndex: int) (columns: IDictionary<string, int>) ->
                    try let value = fn.Invoke(reader, startIndex, columns) in value
                    with _ex -> reraise()

            | t ->
                // Parameter declarations
                let readerParam = Expression.Parameter(typeof<IDataReader>, "reader")
                let startIndexParam = Expression.Parameter(typeof<int>, "startIndex")
                let columnsParam = Expression.Parameter(typeof<IDictionary<string, int>>, "columns")

                // Create appropriate mapper based on the type
                let expr =
                    // Handle class types with properties
                    let props: MemberInfo array =
                        let props = t.GetProperties() |> Array.filter (fun p -> p.CanWrite) |> Array.map (fun x -> x :> MemberInfo)
                        if t.IsValueType && props.Length = 0 then
                            t.GetFields(BindingFlags.NonPublic ||| BindingFlags.Instance) |> Array.filter (fun p -> not p.IsPrivate) |> Array.map (fun x -> x :> MemberInfo)
                        else
                            props

                    // Variables for the expression
                    let resultVar = Expression.Variable(t, "result")
                    let statements = ResizeArray<Expression>()

                    // Create a new instance
                    let ctor = t.GetConstructor([||])
                    let createInstanceExpr =
                        if ctor <> null then
                            Expression.New(ctor) :> Expression
                        elif t.IsValueType then
                            Expression.Default t
                        else
                            // Use FormatterServices.GetSafeUninitializedObject
                            let fn = Func<'T>(fun () ->
                                FormatterServices.GetSafeUninitializedObject(t) :?> 'T)
                            Expression.Call(
                                Expression.Constant(fn),
                                typeof<Func<'T>>.GetMethod("Invoke"),
                                [||]
                            ) :> Expression

                    statements.Add(Expression.Assign(resultVar, createInstanceExpr) :> Expression)

                    // Set each property from the reader
                    for prop in props do
                        let columnVar = Expression.Variable(typeof<int>, "columnIndex")

                        let propName =
                            // for F# structs.
                            if prop.Name.EndsWith "@" then prop.Name.TrimEnd '@' else prop.Name

                        let finalValueExpr = matchMethodWithMemberType prop readerParam columnVar

                        let propExpr = Expression.Block(
                            [| columnVar |],
                            [|
                                // Try to get column index
                                Expression.IfThen(
                                    Expression.Call(
                                        columnsParam,
                                        typeof<IDictionary<string, int>>.GetMethod("TryGetValue"),
                                        [
                                            Expression.Constant(propName) :> Expression;
                                            columnVar :> Expression
                                        ]
                                    ),

                                    // If column exists and index >= startIndex and not null
                                    Expression.IfThen(
                                        Expression.AndAlso(
                                            Expression.GreaterThanOrEqual(
                                                columnVar,
                                                startIndexParam
                                            ),
                                            Expression.Equal(
                                                Expression.Call(
                                                    readerParam,
                                                    typeof<IDataRecord>.GetMethod("IsDBNull"),
                                                    [| columnVar :> Expression |]
                                                ),
                                                Expression.Constant(false)
                                            )
                                        ),
                                        Expression.Assign(
                                            match prop with
                                            | :? PropertyInfo as p -> Expression.Property(resultVar, p)
                                            | :? FieldInfo as f -> Expression.Field(resultVar, f)
                                            | _ -> failwithf "Unknown member type."
                                            ,
                                            finalValueExpr
                                        )
                                    )) :> Expression
                            |]
                        )

                        statements.Add(propExpr)

                    // Return the result
                    statements.Add(resultVar :> Expression)

                    Expression.Block([| resultVar |], statements) :> Expression

                // Create and compile lambda
                let lambda = Expression.Lambda<Func<IDataReader, int, IDictionary<string, int>, 'T>>(
                    expr,
                    [| readerParam; startIndexParam; columnsParam |]
                )

                let fn = lambda.Compile()

                fun (reader: IDataReader) (startIndex: int) (columns: IDictionary<string, int>) ->
                    try fn.Invoke(reader, startIndex, columns)
                    with _ex -> reraise()

    /// <summary>
    /// Executes a command and maps the resulting data reader to a sequence of objects of type 'T.
    /// </summary>
    let internal queryCommand<'T> (command: IDbCommand) (nullableCachedDict: Dictionary<string, int>) = seq {
        use reader = command.ExecuteReader()
        let dict =
            if isNull nullableCachedDict then
                Dictionary<string, int>(reader.FieldCount)
            else
                nullableCachedDict

        if dict.Count = 0 then
            for i in 0..(reader.FieldCount - 1) do
                dict.Add(reader.GetName(i), i)


        while reader.Read() do
            yield TypeMapper<'T>.Map reader 0 dict
    }

    /// <summary>
    /// A helper function that creates a command and queries the database.
    /// </summary>
    let internal queryInner<'T> this (sql: string) (parameters: obj) = seq {
        use command = createCommand this sql parameters
        yield! queryCommand<'T> command null
    }
