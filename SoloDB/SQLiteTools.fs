﻿namespace SoloDatabase

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

module SQLiteTools =
    let private nullablePropsCache = ConcurrentDictionary<Type, struct (PropertyInfo * PropertyInfo)>()

    let private getNullableProperties (nullableType: Type) =
        nullablePropsCache.GetOrAdd(nullableType, fun t ->
            struct (t.GetProperty("HasValue"), t.GetProperty("Value")))

    [<Struct>]
    type TrimmedArray = {
        Array: Array
        TrimmedLen: int
    }
        

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
            if valType.Name.StartsWith "Nullable`" then
                let struct (hasValueProp, valueProp) = getNullableProperties valType
                if hasValueProp.GetValue value :?> bool then
                    struct (valueProp.GetValue value, -1)
                else
                    struct (null, -1)
            else
                struct (value, -1)

    let private addParameter (command: IDbCommand) (key: string) (value: obj) =
        let struct (value, size) = processParameter value

        let par = command.CreateParameter()
        par.ParameterName <- key
        par.Value <- value

        if size > 0 then
            par.Size <- size

        command.Parameters.Add par |> ignore

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

    let private dynamicParameterCache = ConcurrentDictionary<Type, Action<IDbCommand, obj, Action<IDbCommand,string,obj>>>()
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

    let private createCommand (this: SqliteConnection) (sql: string) (parameters: obj) =
        let command = this.CreateCommand()
        command.CommandText <- sql
        processParameters addParameter command parameters

        command

    let private dataReaderMethods =
        let rec getMethods (t: Type) =
            let implements = t.GetInterfaces()
            [
                yield! t.GetMethods()
                for implements in implements do
                    yield! getMethods implements
            ]
        getMethods typeof<DbDataReader>

    type private TypeMapper<'T> =
        static member val Map = 
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

            let streamToNativeArray = Func<SqliteBlob, NativeArray.NativeArray> streamToNativeArrayFuncDisposing
            let streamToNativeArray = Expression.Constant streamToNativeArray
            let streamToNativeArray = 
                let method = typeof<Func<SqliteBlob, NativeArray.NativeArray>>.GetMethod "Invoke"
                fun (e: Expression) ->
                    Expression.Call (streamToNativeArray, method, [|e|])

            

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

    let private queryInner<'T> this (sql: string) (parameters: obj) = seq {
        use command = createCommand this sql parameters
        yield! queryCommand<'T> command null
    }

    type internal IDisableDispose =
        abstract DisableDispose: unit -> IDisposable
    
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

        member internal this.Inner = this :> SqliteConnection
        member val InsideTransaction = false with get, set

        /// Clears the cache while waiting the all cached connections to not be in use.
        member this.ClearCache() =
            if preparedCache.Count = 0 then () else

            let oldCache = preparedCache 
            preparedCache <- Dictionary<string, {| Command: SqliteCommand; ColumnDict: Dictionary<string, int>; CallCount: int64 ref; InUse : bool ref |}>()

            while oldCache.Count > 0 do
                for KeyValue(k, v) in oldCache |> Seq.toArray do
                    if (not !v.InUse) then
                        v.Command.Dispose()
                        ignore (oldCache.Remove k)

        member this.Execute(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            match tryCachedCommand this sql parameters with
            | ValueSome struct (command, _columnDict, inUse) ->
                try command.ExecuteNonQuery()
                finally inUse := false
            | ValueNone ->

            use command = createCommand this sql parameters
            command.Prepare() // To throw all errors, not silently fail them.
            command.ExecuteNonQuery()

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

        member this.Query<'T>(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = seq {
            match tryCachedCommand this sql parameters with
            | ValueSome struct (command, columnDict, inUse) ->
                try yield! queryCommand<'T> command columnDict
                finally inUse := false
            | ValueNone ->
            use command = createCommand this sql parameters
            yield! queryCommand<'T> command null
        }

        member this.QueryFirst<'T>(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = 
            match tryCachedCommand this sql parameters with
            | ValueSome struct (command, columnDict, inUse) ->
                try queryCommand<'T> command columnDict |> Seq.head
                finally inUse := false
            | ValueNone ->
            use command = createCommand this sql parameters
            queryCommand<'T> command null |> Seq.head

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

    [<Extension>]
    type IDbConnectionExtensions =
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


        [<Extension>]
        static member Execute(this: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            match this with
            | :? CachingDbConnection as c -> c.Execute(sql, parameters)
            | _ ->
            use command = createCommand this sql parameters
            command.Prepare() // To throw all errors, not silently fail them.
            command.ExecuteNonQuery()

        [<Extension>]
        static member Query<'T>(this: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            match this with
            | :? CachingDbConnection as c -> c.Query<'T>(sql, parameters)
            | _ ->
            queryInner<'T> this sql parameters

        [<Extension>]
        static member QueryFirst<'T>(this: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = 
            match this with
            | :? CachingDbConnection as c -> c.QueryFirst<'T>(sql, parameters)
            | _ ->
            queryInner<'T> this sql parameters |> Seq.head

        [<Extension>]
        static member QueryFirstOrDefault<'T>(this: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = 
            match this with
            | :? CachingDbConnection as c -> c.QueryFirstOrDefault<'T>(sql, parameters)
            | _ ->
            match queryInner<'T> this sql parameters |> Seq.tryHead with
            | Some x -> x
            | None -> Unchecked.defaultof<'T>

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