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

module SQLiteTools =
    let private addParameter (command: IDbCommand) (key: string) (value: obj) =
        let struct (value, size) = 
            if value = null then 
                struct (null, -1)
            else
            let valType = value.GetType()
            match value with
            | :? DateTimeOffset as dto ->
                 struct (dto.ToUnixTimeMilliseconds() |> box, sizeof<int64>)
            | _ ->
                if valType.Name.StartsWith "Nullable`" then
                    if valType.GetProperty("HasValue").GetValue value = true then
                        struct (valType.GetProperty("Value").GetValue value, -1)
                    else struct (null, -1)
                else
                    struct (value, -1)

        let par = command.CreateParameter()
        par.ParameterName <- key
        par.Value <- value

        if size > 0 then
            par.Size <- size

        command.Parameters.Add par |> ignore

    let private createCommand(this: IDbConnection)(sql: string)(parameters: obj option) =
        let command = this.CreateCommand()
        command.CommandText <- sql
        match parameters with
        | Some parameters when parameters <> null ->        
            match parameters with
            | :? IDictionary as dict ->
                for key in dict.Keys do
                    let value = dict.[key]
                    let key = key :?> string
                    addParameter command key value

            | parameters ->
            for p in parameters.GetType().GetProperties() do
                if p.CanRead then
                    addParameter command p.Name (p.GetValue parameters)
        | _ -> ()

        command

    type private TypeMapper<'T> =
        static member val Map = 
            let matchMethodWithPropertyType (prop: PropertyInfo) (readerParam: Expression) (columnVar: Expression) =
                // Get the appropriate method and conversion for each property type
                let (getMethodName, needsConversion, conversionFunc) = 
                    match prop.PropertyType with
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
                    | t when t = typeof<byte[]> -> 
                        "GetValue", true, Some (fun expr -> Expression.TypeAs(expr, typeof<byte[]>))
                    | t when t = typeof<Guid> -> 
                        "GetGuid", false, None
                    | t when t = typeof<DateTime> -> 
                        "GetDateTime", false, None
                    | t when t = typeof<DateTimeOffset> -> 
                        "GetInt64", true, Some (fun (expr: Expression) -> Expression.Call(typeof<DateTimeOffset>.GetMethod("FromUnixTimeMilliseconds", [|typeof<int64>|]), expr))
                    | _ -> 
                        "GetValue", true, Some (fun expr -> Expression.Convert(expr, prop.PropertyType))


                // Call the appropriate method
                let valueExpr = Expression.Call(
                    readerParam,
                    typeof<IDataRecord>.GetMethod(getMethodName),
                    [| columnVar |]
                )

                // Apply conversion if needed
                let finalValueExpr = 
                    match needsConversion, conversionFunc with
                    | true, Some convFunc -> convFunc(valueExpr)
                    | _ -> valueExpr

                finalValueExpr


            match typeof<'T> with
            | OfType int8 -> fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) -> 
                reader.GetByte(startIndex) :> obj :?> 'T
            | OfType uint8 -> fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) -> 
                reader.GetInt16(startIndex) |> uint8 :> obj :?> 'T
            | OfType int16 -> fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) -> 
                reader.GetInt16(startIndex) :> obj :?> 'T
            | OfType uint16 -> fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) -> 
                reader.GetInt32(startIndex) |> uint16 :> obj :?> 'T
            | OfType int32 -> fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) -> 
                reader.GetInt32(startIndex) :> obj :?> 'T
            | OfType uint32 -> fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) -> 
                reader.GetInt64(startIndex) |> uint32 :> obj :?> 'T
            | OfType int64 -> fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) -> 
                reader.GetInt64(startIndex) :> obj :?> 'T
            | OfType uint64 -> fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) -> 
                reader.GetInt64(startIndex) |> uint64 :> obj :?> 'T
            | OfType float -> fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) -> 
                reader.GetDouble(startIndex) |> float :> obj :?> 'T
            | OfType double -> fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) -> 
                reader.GetDouble(startIndex) :> obj :?> 'T
            | OfType decimal -> fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) -> 
                reader.GetDecimal(startIndex) :> obj :?> 'T
            | OfType string -> fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) -> 
                reader.GetString(startIndex) :> obj :?> 'T
            | OfType bool -> fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) -> 
                reader.GetBoolean(startIndex) :> obj :?> 'T
            | OfType (id: byte array -> byte array) -> fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) -> 
                reader.GetValue(startIndex) :?> 'T
            | OfType (id: Guid -> Guid) -> fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) -> 
                reader.GetGuid(startIndex) :> obj :?> 'T
            | OfType DateTime -> fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) -> 
                reader.GetDateTime(startIndex) :> obj :?> 'T
            | OfType DateTimeOffset -> fun (reader: IDataReader) (startIndex: int) (_columns: IDictionary<string, int>) -> 
                reader.GetInt64(startIndex) |> DateTimeOffset.FromUnixTimeMilliseconds :> obj :?> 'T
            | t when t = typeof<obj> ->
                fun (reader: IDataReader) (startIndex: int) (columns: IDictionary<string, int>) -> 
                    let jsonObj = JsonSerializator.JsonValue.Object(Dictionary(columns.Count))
                    for key in columns.Keys do
                        if startIndex <= columns.[key] then
                            let value = reader.GetValue(columns.[key])
                            jsonObj.[key] <- JsonSerializator.JsonValue.Serialize<obj> value

                    jsonObj.ToObject<'T>()
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
                                        
                    let getValueAndDeserialize = matchMethodWithPropertyType prop readerParam columnVar
                    
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
                    let props = t.GetProperties() |> Array.filter (fun p -> p.CanWrite)
                        
                    // Variables for the expression
                    let resultVar = Expression.Variable(t, "result")
                    let statements = ResizeArray<Expression>()
                        
                    // Create a new instance
                    let ctor = t.GetConstructor([||])
                    let createInstanceExpr =
                        if ctor <> null then
                            Expression.New(ctor) :> Expression
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
                    for i, prop in props |> Array.indexed do
                        let columnVar = Expression.Variable(typeof<int>, "columnIndex")

                        let finalValueExpr = matchMethodWithPropertyType prop readerParam columnVar

                        let propExpr = Expression.Block(
                            [| columnVar |],
                            [|
                                Expression.DebugInfo(Expression.SymbolDocument(prop.Name), i + 1, 1, i + 1, 2) :> Expression;
                                // Try to get column index
                                Expression.IfThen(
                                    Expression.Call(
                                        columnsParam,
                                        typeof<IDictionary<string, int>>.GetMethod("TryGetValue"),
                                        [
                                            Expression.Constant(prop.Name) :> Expression;
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
                                            Expression.Property(resultVar, prop),
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


    let private queryInner<'T> this (sql: string) (parameters: obj option) = seq {
           use command = createCommand this sql parameters
           use reader = command.ExecuteReader()
           let dict = Dictionary<string, int>()

           for i in 0..(reader.FieldCount - 1) do
               dict.Add(reader.GetName(i), i)

           while reader.Read() do
               yield TypeMapper<'T>.Map reader 0 dict
       }
    

    [<Extension>]
    type IDbConnectionExtensions =
        [<Extension>]
        static member Execute(this: IDbConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            use command = createCommand this sql (Option.ofObj parameters)
            command.Prepare() // To throw all errors, not silently fail them.
            command.ExecuteNonQuery()

        [<Extension>]
        static member Query<'T>(this: IDbConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            queryInner<'T> this sql (Option.ofObj parameters)

        [<Extension>]
        static member QueryFirst<'T>(this: IDbConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = 
            queryInner<'T> this sql (Option.ofObj parameters) |> Seq.head

        [<Extension>]
        static member QueryFirstOrDefault<'T>(this: IDbConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = 
            match queryInner<'T> this sql (Option.ofObj parameters) |> Seq.tryHead with
            | Some x -> x
            | None -> Unchecked.defaultof<'T>

        [<Extension>]
        static member Query<'T1, 'T2, 'TReturn>(this: IDbConnection, sql: string, map: Func<'T1, 'T2, 'TReturn>, parameters: obj, splitOn: string) = seq {
            use command = createCommand this sql (Some parameters)
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