namespace SoloDatabase

open System
open System.Reflection
open System.Collections
open System.Collections.Generic
open SoloDatabase
open System.Data
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open Utils
open System.Linq.Expressions
open Microsoft.FSharp.Reflection
open System.Runtime.Serialization
open SoloDatabase.JsonSerializator
open System.Collections.Concurrent
open Microsoft.Data.Sqlite
open System.IO
open System.Data.Common

/// <summary>
/// Provides utility functions and extension methods for interacting with SQLite,
/// including parameter processing, command creation, and object mapping.
/// </summary>
module SQLiteTools =
    /// <summary>
    /// Caches PropertyInfo for Nullable types' 'HasValue' and 'Value' properties for performance.
    /// </summary>
    let private nullablePropsCache = ConcurrentDictionary<Type, struct (PropertyInfo * PropertyInfo)>()

    /// <summary>
    /// Retrieves the 'HasValue' and 'Value' properties for a given Nullable type from the cache or via reflection.
    /// </summary>
    /// <param name="nullableType">The Nullable type.</param>
    /// <returns>A struct tuple containing the PropertyInfo for HasValue and Value.</returns>
    let private getNullableProperties (nullableType: Type) =
        nullablePropsCache.GetOrAdd(nullableType, fun t ->
            struct (t.GetProperty("HasValue"), t.GetProperty("Value")))

    /// <summary>
    /// A private struct to pass an array along with its effective length, for BLOB operations.
    /// </summary>
    [<Struct>]
    type TrimmedArray = {
        /// <summary>The underlying array.</summary>
        Array: Array
        /// <summary>The number of valid bytes in the array.</summary>
        TrimmedLen: int
    }
        

    /// <summary>
    /// Processes a parameter value before it's added to a DB command, handling nulls, DateTimeOffset, and Nullable types.
    /// </summary>
    /// <param name="value">The input parameter value.</param>
    /// <returns>A struct tuple containing the processed value and its size if applicable.</returns>
    let private processParameter (value: obj) =
        match value with
        | null -> 
            struct (null, -1)
        | :? DateTimeOffset as dto ->
            struct (dto.ToUnixTimeMilliseconds() |> box, sizeof<int64>)
        | :? TrimmedArray as ta ->
            struct (ta.Array, ta.TrimmedLen)
        | _ ->
            let valType = value.GetType()
            if not (valType.IsArray) && valType.Name.StartsWith "Nullable`" then
                let struct (hasValueProp, valueProp) = getNullableProperties valType
                if hasValueProp.GetValue value :?> bool then
                    struct (valueProp.GetValue value, -1)
                else
                    struct (null, -1)
            else
                struct (value, -1)

    /// <summary>
    /// Creates and adds a new IDbDataParameter to a command.
    /// </summary>
    /// <param name="command">The command to add the parameter to.</param>
    /// <param name="key">The name of the parameter.</param>
    /// <param name="value">The value of the parameter.</param>
    let private addParameter (command: IDbCommand) (key: string) (value: obj) =
        let struct (value, size) = processParameter value

        let par = command.CreateParameter()
        par.ParameterName <- key
        par.Value <- value

        if size > 0 then
            par.Size <- size

        command.Parameters.Add par |> ignore

    /// <summary>
    /// Updates an existing parameter or adds a new one if it doesn't exist. Used for cached commands.
    /// </summary>
    /// <param name="command">The command to update.</param>
    /// <param name="key">The name of the parameter.</param>
    /// <param name="value">The value of the parameter.</param>
    let private setOrAddParameter (command: IDbCommand) (key: string) (value: obj) =
        let struct (value, size) = processParameter value

        let par = 
            if command.Parameters.Contains key then 
                command.Parameters.[key] :?> IDbDataParameter 
            else 
                let p = command.CreateParameter()
                command.Parameters.Add p |> ignore
                p.ParameterName <- key
                p

        par.Value <-value

        if size > 0 then
            par.Size <- size

    /// <summary>
    /// Caches compiled lambda expressions for dynamically processing anonymous-type parameters.
    /// </summary>
    let private dynamicParameterCache = ConcurrentDictionary<Type, Action<IDbCommand, obj, Action<IDbCommand,string,obj>>>()
    
    /// <summary>
    /// Processes parameters from an object (either an IDictionary or an anonymous type) and adds them to a command.
    /// </summary>
    /// <param name="processFn">The function to use for adding/setting a parameter (e.g., addParameter).</param>
    /// <param name="command">The command to add parameters to.</param>
    /// <param name="parameters">The parameter object (IDictionary or anonymous type).</param>
    let private processParameters processFn (command: IDbCommand) (parameters: obj) =
        match parameters with
        | null -> ()
        | :? IDictionary as dict ->
            for key in dict.Keys do
                let value = dict.[key]
                let key = key :?> string
                processFn command key value

        | parameters ->
            let fn = dynamicParameterCache.GetOrAdd(parameters.GetType(), Func<Type, Action<IDbCommand, obj, Action<IDbCommand,string,obj>>>(
                fun t ->
                    let props = t.GetProperties() |> Array.filter(_.CanRead)
                    let dbCmdPar = Expression.Parameter typeof<IDbCommand>
                    let parametersPar = Expression.Parameter typeof<obj>
                    let actionPar = Expression.Parameter typeof<Action<IDbCommand,string,obj>>

                    let meth = typeof<Action<IDbCommand,string,obj>>.GetMethod "Invoke"

                    let l = Expression.Lambda<Action<IDbCommand, obj, Action<IDbCommand,string,obj>>>(
                                Expression.Block([|
                                    for p in props do
                                        Expression.Call(actionPar, meth, [|dbCmdPar :> Expression; Expression.Constant(p.Name); Expression.Convert(Expression.Property(Expression.Convert(parametersPar, t), p), typeof<obj>)|]) :> Expression
                                |]),
                                [|dbCmdPar; parametersPar; actionPar|])

                    l.Compile(false)
            ))
            fn.Invoke(command, parameters, processFn)

    /// <summary>
    /// Creates an IDbCommand with the given SQL and parameters.
    /// </summary>
    /// <param name="this">The connection to create the command on.</param>
    /// <param name="sql">The SQL command text.</param>
    /// <param name="parameters">The parameters for the command.</param>
    /// <returns>A new IDbCommand.</returns>
    let private createCommand (this: SqliteConnection) (sql: string) (parameters: obj) =
        let command = this.CreateCommand()
        command.CommandText <- sql
        processParameters addParameter command parameters

        command

    /// <summary>
    /// Lazily gets all methods from DbDataReader and its interfaces for later use in the TypeMapper.
    /// </summary>
    let private dataReaderMethods =
        let rec getMethods (t: Type) =
            let implements = t.GetInterfaces()
            [
                yield! t.GetMethods()
                for implements in implements do
                    yield! getMethods implements
            ]
        getMethods typeof<DbDataReader>

    /// <summary>
    /// A private helper type that provides a cached, compiled function for mapping an IDataReader record to an instance of type 'T.
    /// </summary>
    /// <typeparam name="'T">The target type to map to.</typeparam>
    type private TypeMapper<'T> =
        /// <summary>
        /// A statically cached, compiled function that maps a data reader row to an object of type 'T.
        /// This is the core of the object mapping functionality. For primitive types, it uses a direct,
        /// non-reflective function. For complex reference and value types (classes, records, structs),
        /// it dynamically builds and compiles a LINQ Expression tree on first use to create a highly
        /// optimized mapper. This mapper populates the properties and fields of a new 'T instance
        /// from the columns in the IDataReader.
        /// </summary>
        /// <typeparam name="'T">The target type to which the data reader row will be mapped.</typeparam>
        /// <returns>
        /// A compiled and cached function that takes an IDataReader, a starting column index, 
        /// and a column-to-index dictionary, and returns a populated instance of 'T.
        /// </returns>
        static member val Map =             
            /// <summary>
            /// Reads all bytes from a SqliteBlob stream into a new NativeArray.NativeArray,
            /// ensuring the blob stream is properly disposed of afterward.
            /// </summary>
            /// <param name="s">The SqliteBlob stream to read from. This stream will be disposed of by the function.</param>
            /// <returns>A new NativeArray.NativeArray containing all the bytes from the stream.</returns>
            /// <remarks>
            /// This function will throw an exception if the stream ends prematurely before the entire allocated array can be filled.
            /// </remarks>
            let streamToNativeArrayFuncDisposing (s: SqliteBlob) =
                use s = s
                let arr = NativeArray.NativeArray.Alloc (int s.Length)
                let mutable totalRead = 0

                while totalRead < arr.Length do
                    let read = s.Read(arr.Span)
                    if read = 0 && totalRead < arr.Length then
                        failwithf "Could not read the whole BLOB, readCount = %i, size = %i" totalRead arr.Length
                    totalRead <- totalRead + read
                arr
                            
            let streamToNativeArray = 
                let method = typeof<Func<SqliteBlob, NativeArray.NativeArray>>.GetMethod "Invoke"
                let stna = Expression.Constant (Func<SqliteBlob, NativeArray.NativeArray> streamToNativeArrayFuncDisposing)
                fun (e: Expression) ->
                    Expression.Call (stna, method, [|e|])

            
            /// <summary>
            /// Dynamically builds a LINQ Expression to read a value from an IDataReader for a specific member (property or field).
            /// It selects the appropriate 'Get' method (e.g., GetInt32, GetString) from the reader based on the member's type
            /// and applies any necessary type conversions.
            /// </summary>
            /// <param name="prop">The MemberInfo (PropertyInfo or FieldInfo) of the target object to be populated.</param>
            /// <param name="readerParam">The Expression representing the IDataReader parameter in the lambda.</param>
            /// <param name="columnVar">The Expression representing the variable that will hold the column index.</param>
            /// <returns>
            /// A LINQ Expression that, when compiled and executed, will read and correctly type the value 
            /// from the data reader for the specified member.
            /// </returns>
            let matchMethodWithMemberType (prop: MemberInfo) (readerParam: Expression) (columnVar: Expression) =
                // Get the appropriate method and conversion for each property type
                let (getMethodName, needsConversion, conversionFunc) = 
                    let t = match prop with | :? PropertyInfo as p -> p.PropertyType | :? FieldInfo as p -> p.FieldType | _ -> failwithf "Unknown member type."
                    match t with
                    | t when t = typeof<byte> || t = typeof<int8> -> 
                        "GetByte", false, None
                    | t when t = typeof<uint8> -> 
                        "GetByte", true, Some (fun (expr: Expression) -> Expression.Convert(expr, typeof<uint8>) :> Expression)
                    | t when t = typeof<int16> -> 
                        "GetInt16", false, None
                    | t when t = typeof<uint16> -> 
                        "GetInt32", true, Some (fun expr -> 
                            Expression.Convert(Expression.Call(
                                null, 
                                typeof<uint16>.GetMethod("op_Explicit", [|typeof<int32>|]),
                                expr), 
                                typeof<uint16>))
                    | t when t = typeof<int32> -> 
                        "GetInt32", false, None
                    | t when t = typeof<uint32> -> 
                        "GetInt64", true, Some (fun expr -> 
                            Expression.Convert(Expression.Call(
                                null, 
                                typeof<uint32>.GetMethod("op_Explicit", [|typeof<int64>|]),
                                expr), 
                                typeof<uint32>))
                    | t when t = typeof<int64> -> 
                        "GetInt64", false, None
                    | t when t = typeof<uint64> -> 
                        "GetInt64", true, Some (fun expr -> 
                            Expression.Convert(Expression.Call(
                                null, 
                                typeof<uint64>.GetMethod("op_Explicit", [|typeof<int64>|]),
                                expr), 
                                typeof<uint64>))
                    | t when t = typeof<float32> || t = typeof<float> -> 
                        "GetFloat", false, None
                    | t when t = typeof<double> -> 
                        "GetDouble", false, None
                    | t when t = typeof<decimal> -> 
                        "GetDecimal", false, None
                    | t when t = typeof<string> -> 
                        "GetString", false, None
                    | t when t = typeof<bool> -> 
                        "GetBoolean", false, None
                    | t when t = typeof<NativeArray.NativeArray> -> 
                        "GetStream", true, Some (fun expr -> streamToNativeArray (Expression.TypeAs(expr, typeof<SqliteBlob>)))
                    | t when t = typeof<byte[]> -> 
                        "GetValue", true, Some (fun expr -> Expression.TypeAs(expr, typeof<byte[]>))
                    | t when t = typeof<Guid> -> 
                        "GetGuid", false, None
                    | t when t = typeof<DateTime> -> 
                        "GetDateTime", false, None
                    | t when t = typeof<DateTimeOffset> -> 
                        "GetInt64", true, Some (fun (expr: Expression) -> Expression.Call(typeof<DateTimeOffset>.GetMethod("FromUnixTimeMilliseconds", [|typeof<int64>|]), expr))
                    | _ -> 
                        "GetValue", true, Some (fun expr -> Expression.Convert(expr, t))


                let method = dataReaderMethods |> List.find(fun m -> m.Name = getMethodName)
                let readerParam = Expression.TypeAs(readerParam, typeof<DbDataReader>)

                // Call the appropriate method
                let valueExpr = Expression.Call(
                    readerParam,
                    method,
                    [| columnVar |]
                )

                // Apply conversion if needed
                let finalValueExpr = 
                    match needsConversion, conversionFunc with
                    | true, Some convFunc -> convFunc(valueExpr)
                    | _ -> valueExpr

                finalValueExpr


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

            | t when FSharpType.IsRecord t ->
                // Parameter declarations
                let readerParam = Expression.Parameter(typeof<IDataReader>, "reader")
                let startIndexParam = Expression.Parameter(typeof<int>, "startIndex")
                let columnsParam = Expression.Parameter(typeof<IDictionary<string, int>>, "columns")

                let columnVar = Expression.Variable typeof<int>

                let recordFields = FSharpType.GetRecordFields t
                let recordFieldsType = recordFields |> Array.map(_.PropertyType)
                let ctor = t.GetConstructors() |> Array.find(fun c -> c.GetParameters() |> Array.map(_.ParameterType) = recordFieldsType)
                

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
    /// <typeparam name="'T">The type to map the results to.</typeparam>
    /// <param name="command">The command to execute.</param>
    /// <param name="nullableCachedDict">An optional dictionary to cache column ordinals.</param>
    /// <returns>A sequence of 'T objects.</returns>
    let private queryCommand<'T> (command: IDbCommand) (nullableCachedDict: Dictionary<string, int>) = seq {
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
    /// A private helper function that creates a command and queries the database.
    /// </summary>
    /// <typeparam name="'T">The type to map the results to.</typeparam>
    /// <param name="this">The connection to use.</param>
    /// <param name="sql">The SQL query string.</param>
    /// <param name="parameters">The parameters for the query.</param>
    /// <returns>A sequence of 'T objects.</returns>
    let private queryInner<'T> this (sql: string) (parameters: obj) = seq {
        use command = createCommand this sql parameters
        yield! queryCommand<'T> command null
    }

    /// <summary>
    /// Internal interface to allow temporary disabling of the Dispose method on a connection.
    /// </summary>
    type internal IDisableDispose =
        /// <summary>
        /// Disables disposal and returns an IDisposable that re-enables it when disposed.
        /// </summary>
        abstract DisableDispose: unit -> IDisposable
    
    /// <summary>
    /// A sealed wrapper around SqliteConnection that adds command caching capabilities.
    /// </summary>
    /// <param name="connectionStr">The connection string.</param>
    /// <param name="onDispose">A callback function to execute on disposal.</param>
    /// <param name="config">The database configuration.</param>
    type [<Sealed>] CachingDbConnection internal (connectionStr: string, onDispose, config: Types.SoloDBConfiguration) = 
        inherit SqliteConnection(connectionStr)
        let mutable disposingDisabled = false
        let mutable preparedCache = Dictionary<string, {| Command: SqliteCommand; ColumnDict: Dictionary<string, int>; CallCount: int64 ref; InUse : bool ref |}>()
        let maxCacheSize = 1000

        let tryCachedCommand (this: CachingDbConnection) (sql: string) (parameters: obj) =
            // @VAR variable names are randomly generated, so caching them is not possible.
            if sql.Contains "@VAR" then ValueNone else
            if not config.CachingEnabled then
                ValueNone 
            else

            // Delete from cache 1/4 of the least used commands.
            if preparedCache.Count >= maxCacheSize then
                let arr = preparedCache |> Seq.toArray 
                arr |> Array.sortInPlaceBy (fun (KeyValue(_sql, item)) -> !item.CallCount)

                for i in 1..(maxCacheSize / 4) do
                    preparedCache.Remove (arr.[i].Key) |> ignore
                    arr.[i].Value.Command.Dispose()
                

            let item = 
                match preparedCache.TryGetValue sql with
                | true, x -> x
                | false, _ ->
                    let command = this.CreateCommand()
                    command.CommandText <- sql
                    processParameters addParameter command parameters
                    command.Prepare()

                    let item = {| Command = command; ColumnDict = Dictionary<string, int>(); CallCount = ref 0L; InUse = ref false |}
                    preparedCache.[sql] <- item
                    item

            if !item.InUse then ValueNone else

            item.CallCount := !item.CallCount + 1L
            item.InUse := true

            processParameters setOrAddParameter item.Command parameters
            struct (item.Command, item.ColumnDict, item.InUse) |> ValueSome

        /// <summary>The underlying SqliteConnection.</summary>
        member internal this.Inner = this :> SqliteConnection
        /// <summary>Indicates if the connection is currently part of a transaction.</summary>
        member val InsideTransaction = false with get, set

        /// <summary>
        /// Clears the prepared statement cache, waiting for any in-use commands to be released.
        /// </summary>
        member this.ClearCache() =
            if preparedCache.Count = 0 then () else

            let oldCache = preparedCache 
            preparedCache <- Dictionary<string, {| Command: SqliteCommand; ColumnDict: Dictionary<string, int>; CallCount: int64 ref; InUse : bool ref |}>()

            while oldCache.Count > 0 do
                for KeyValue(k, v) in oldCache |> Seq.toArray do
                    if (not !v.InUse) then
                        v.Command.Dispose()
                        ignore (oldCache.Remove k)

        /// <summary>Executes a non-query SQL command, utilizing the cache if possible.</summary>
        /// <param name="sql">The SQL command text.</param>
        /// <param name="parameters">The parameters for the command.</param>
        /// <returns>The number of rows affected.</returns>
        member this.Execute(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            match tryCachedCommand this sql parameters with
            | ValueSome struct (command, _columnDict, inUse) ->
                try command.ExecuteNonQuery()
                finally inUse := false
            | ValueNone ->

            use command = createCommand this sql parameters
            command.Prepare() // To throw all errors, not silently fail them.
            command.ExecuteNonQuery()

        /// <summary>Opens a data reader, utilizing the cache if possible.</summary>
        /// <param name="sql">The SQL query text.</param>
        /// <param name="outReader">The output SqliteDataReader.</param>
        /// <param name="parameters">The parameters for the query.</param>
        /// <returns>An IDisposable to manage the lifetime of the reader and command.</returns>
        member this.OpenReader(sql: string, outReader: outref<SqliteDataReader>, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            match tryCachedCommand this sql parameters with
            | ValueSome struct (command, _columnDict, inUse) ->
                try
                    let reader = command.ExecuteReader()
                    outReader <- reader
                    { new IDisposable with
                        member _.Dispose() = reader.Dispose() }
                finally inUse := false
            | ValueNone ->
                let command = createCommand this sql parameters
                command.Prepare()
                let reader = command.ExecuteReader()
                outReader <- reader
                { new IDisposable with
                    member _.Dispose() = 
                        reader.Dispose()
                        command.Dispose()
                }

        /// <summary>Executes a query and maps the results to a sequence of 'T, utilizing the cache if possible.</summary>
        /// <typeparam name="'T">The type to map results to.</typeparam>
        /// <param name="sql">The SQL query text.</param>
        /// <param name="parameters">The parameters for the query.</param>
        /// <returns>A sequence of 'T objects.</returns>
        member this.Query<'T>(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = seq {
            match tryCachedCommand this sql parameters with
            | ValueSome struct (command, columnDict, inUse) ->
                try yield! queryCommand<'T> command columnDict
                finally inUse := false
            | ValueNone ->
            use command = createCommand this sql parameters
            yield! queryCommand<'T> command null
        }

        /// <summary>Executes a query and returns the first result, utilizing the cache if possible.</summary>
        /// <typeparam name="'T">The type to map the result to.</typeparam>
        /// <param name="sql">The SQL query text.</param>
        /// <param name="parameters">The parameters for the query.</param>
        /// <returns>The first 'T object from the result set.</returns>
        member this.QueryFirst<'T>(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = 
            match tryCachedCommand this sql parameters with
            | ValueSome struct (command, columnDict, inUse) ->
                try queryCommand<'T> command columnDict |> Seq.head
                finally inUse := false
            | ValueNone ->
            use command = createCommand this sql parameters
            queryCommand<'T> command null |> Seq.head

        /// <summary>Executes a query and returns the first result, or a default value if the sequence is empty, utilizing the cache if possible.</summary>
        /// <typeparam name="'T">The type to map the result to.</typeparam>
        /// <param name="sql">The SQL query text.</param>
        /// <param name="parameters">The parameters for the query.</param>
        /// <returns>The first 'T object from the result set, or default.</returns>
        member this.QueryFirstOrDefault<'T>(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = 
            match tryCachedCommand this sql parameters with
            | ValueSome struct (command, columnDict, inUse) ->
                try
                    match queryCommand<'T> command columnDict |> Seq.tryHead with
                    | Some x -> x
                    | None -> Unchecked.defaultof<'T>
                finally inUse := false

            | ValueNone ->
            use command = createCommand this sql parameters
            
            match queryCommand<'T> command null |> Seq.tryHead with
            | Some x -> x
            | None -> Unchecked.defaultof<'T>

        /// <summary>Executes a multi-mapping query, utilizing the cache if possible.</summary>
        /// <typeparam name="'T1">The type of the first object.</typeparam>
        /// <typeparam name="'T2">The type of the second object.</typeparam>
        /// <typeparam name="'TReturn">The return type after mapping.</typeparam>
        /// <param name="sql">The SQL query text.</param>
        /// <param name="map">The function to map the two objects to the return type.</param>
        /// <param name="parameters">The parameters for the query.</param>
        /// <param name="splitOn">The column name to split the results on.</param>
        /// <returns>A sequence of 'TReturn objects.</returns>
        member this.Query<'T1, 'T2, 'TReturn>(sql: string, map: Func<'T1, 'T2, 'TReturn>, parameters: obj, splitOn: string) = seq {
            let struct (command, dict, dispose, inUse) =
                match tryCachedCommand this sql parameters with
                | ValueSome struct (command, columnDict, inUse) ->
                    struct (command, columnDict, false, Some inUse)
                | ValueNone ->
                    struct (createCommand this sql parameters, Dictionary<string, int>(), true, None)
            try
                use reader = command.ExecuteReader()

                if dict.Count = 0 then
                    for i in 0..(reader.FieldCount - 1) do
                        dict.Add(reader.GetName(i), i)

                let splitIndex = reader.GetOrdinal(splitOn)

                while reader.Read() do
                    let t1 = TypeMapper<'T1>.Map reader 0 dict
                    let t2 = 
                        if reader.IsDBNull(splitIndex) then Unchecked.defaultof<'T2>
                        else TypeMapper<'T2>.Map reader splitIndex dict

                    yield map.Invoke (t1, t2)
            finally
                match inUse with
                | Some inUse -> inUse := false
                | _ -> ()
                if dispose then command.Dispose()
        }

        /// <summary>
        /// Performs the actual disposal of the base connection.
        /// </summary>
        member this.DisposeReal() =
            base.Dispose(true)

        interface IDisableDispose with
            member this.DisableDispose(): IDisposable = 
                disposingDisabled <- true
                { new IDisposable with
                    override _.Dispose() =
                        disposingDisabled <- false
                        ()}

        interface IDisposable with
            override this.Dispose (): unit = 
                if not disposingDisabled then
                    onDispose this

    /// <summary>
    /// Provides extension methods for IDbConnection for executing queries.
    /// </summary>
    [<Extension>]
    type IDbConnectionExtensions =
        /// <summary>
        /// Extension method to open a data reader.
        /// </summary>
        [<Extension>]
        static member OpenReader<'R>(this: SqliteConnection, sql: string, outReader: outref<DbDataReader>, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            match this with
            | :? CachingDbConnection as c -> c.OpenReader(sql, &outReader, parameters)
            | _ ->
                let command = createCommand this sql parameters
                command.Prepare()
                let reader = command.ExecuteReader()
                outReader <- reader
                { new IDisposable with
                    member _.Dispose() = 
                        reader.Dispose()
                        command.Dispose()
                }


        /// <summary>
        /// Extension method to execute a non-query command.
        /// </summary>
        [<Extension>]
        static member Execute(this: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            match this with
            | :? CachingDbConnection as c -> c.Execute(sql, parameters)
            | _ ->
            use command = createCommand this sql parameters
            command.Prepare() // To throw all errors, not silently fail them.
            command.ExecuteNonQuery()

        /// <summary>
        /// Extension method to execute a query and map the results to a sequence of 'T.
        /// </summary>
        [<Extension>]
        static member Query<'T>(this: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            match this with
            | :? CachingDbConnection as c -> c.Query<'T>(sql, parameters)
            | _ ->
            queryInner<'T> this sql parameters

        /// <summary>
        /// Extension method to execute a query and return the first result.
        /// </summary>
        [<Extension>]
        static member QueryFirst<'T>(this: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = 
            match this with
            | :? CachingDbConnection as c -> c.QueryFirst<'T>(sql, parameters)
            | _ ->
            queryInner<'T> this sql parameters |> Seq.head

        /// <summary>
        /// Extension method to execute a query and return the first result, or a default value if the sequence is empty.
        /// </summary>
        [<Extension>]
        static member QueryFirstOrDefault<'T>(this: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = 
            match this with
            | :? CachingDbConnection as c -> c.QueryFirstOrDefault<'T>(sql, parameters)
            | _ ->
            match queryInner<'T> this sql parameters |> Seq.tryHead with
            | Some x -> x
            | None -> Unchecked.defaultof<'T>

        /// <summary>
        /// Extension method for executing a multi-mapping query.
        /// </summary>
        [<Extension>]
        static member Query<'T1, 'T2, 'TReturn>(this: SqliteConnection, sql: string, map: Func<'T1, 'T2, 'TReturn>, parameters: obj, splitOn: string) = 
            match this with
            | :? CachingDbConnection as c -> c.Query<'T1, 'T2, 'TReturn>(sql, map, parameters, splitOn)
            | _ ->
            
            seq {
                use command = createCommand this sql parameters
                use reader = command.ExecuteReader()

                let dict = Dictionary<string, int>(reader.FieldCount)

                for i in 0..(reader.FieldCount - 1) do
                    dict.Add(reader.GetName(i), i)

                let splitIndex = reader.GetOrdinal(splitOn)

                while reader.Read() do
                    let t1 = TypeMapper<'T1>.Map reader 0 dict
                    let t2 = 
                        if reader.IsDBNull(splitIndex) then Unchecked.defaultof<'T2>
                        else TypeMapper<'T2>.Map reader splitIndex dict

                    yield map.Invoke (t1, t2)
            }
