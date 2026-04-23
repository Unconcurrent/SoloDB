namespace SoloDatabase

open System
open System.Reflection
open System.Collections
open System.Collections.Generic
open System.Data
open System.Runtime.InteropServices
open System.Linq.Expressions
open System.Collections.Concurrent
open Microsoft.Data.Sqlite
open System.Data.Common

/// <summary>
/// Internal helpers for SQLiteTools: parameter processing, command creation, and type-mapper building blocks.
/// </summary>
module internal SQLiteToolsParams =
    type internal DateTimeFamilyReaderSpec = {
        ClrType: Type
        BuildExpression: Expression -> Expression
        MapInt64: int64 -> obj
    }

    let private dateTimeFamilyReaderSpecs =
        [|
            { ClrType = typeof<DateTime>
              BuildExpression = fun expr ->
                  Expression.Call(typeof<DateTime>.GetMethod("FromBinary", [| typeof<int64> |]), expr) :> Expression
              MapInt64 = fun value -> DateTime.FromBinary value :> obj }
            { ClrType = typeof<DateTimeOffset>
              BuildExpression = fun expr ->
                  Expression.Call(typeof<DateTimeOffset>.GetMethod("FromUnixTimeMilliseconds", [| typeof<int64> |]), expr) :> Expression
              MapInt64 = fun value -> DateTimeOffset.FromUnixTimeMilliseconds value :> obj }
            { ClrType = typeof<DateOnly>
              BuildExpression = fun expr ->
                  Expression.Call(typeof<DateOnly>.GetMethod("FromDayNumber", [| typeof<int> |]), Expression.Convert(expr, typeof<int>)) :> Expression
              MapInt64 = fun value -> DateOnly.FromDayNumber(int value) :> obj }
            { ClrType = typeof<TimeOnly>
              BuildExpression = fun expr ->
                  let ms = Expression.Convert(expr, typeof<float>)
                  let ts = Expression.Call(typeof<TimeSpan>.GetMethod("FromMilliseconds", [| typeof<float> |]), ms)
                  Expression.Call(typeof<TimeOnly>.GetMethod("FromTimeSpan", [| typeof<TimeSpan> |]), ts) :> Expression
              MapInt64 = fun value -> value |> float |> TimeSpan.FromMilliseconds |> TimeOnly.FromTimeSpan :> obj }
            { ClrType = typeof<TimeSpan>
              BuildExpression = fun expr ->
                  let ms = Expression.Convert(expr, typeof<float>)
                  Expression.Call(typeof<TimeSpan>.GetMethod("FromMilliseconds", [| typeof<float> |]), ms) :> Expression
              MapInt64 = fun value -> value |> float |> TimeSpan.FromMilliseconds :> obj }
        |]

    let internal tryGetDateTimeFamilyReaderSpec (t: Type) =
        dateTimeFamilyReaderSpecs
        |> Array.tryFind (fun spec -> spec.ClrType = t)

    /// Optional SQL trace callback for corpus capture and diagnostics.
    /// Set by test harnesses to intercept all SQL at the execution boundary.
    let mutable internal sqlTraceCallback: Action<string> voption = ValueNone

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
            struct (DBNull.Value :> obj, -1)
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
                    struct (DBNull.Value :> obj, -1)
            else
                struct (value, -1)

    /// <summary>
    /// Creates and adds a new IDbDataParameter to a command.
    /// </summary>
    /// <param name="command">The command to add the parameter to.</param>
    /// <param name="key">The name of the parameter.</param>
    /// <param name="value">The value of the parameter.</param>
    let internal addParameter (command: IDbCommand) (key: string) (value: obj) =
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
    let internal setOrAddParameter (command: IDbCommand) (key: string) (value: obj) =
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
    let internal processParameters processFn (command: IDbCommand) (parameters: obj) =
        match parameters with
        | null -> ()
        | :? IDictionary<string, obj> as dict ->
            for KeyValue(key, value) in dict do
                processFn command key value
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
    let internal createCommand (this: SqliteConnection) (sql: string) (parameters: obj) =
        match sqlTraceCallback with ValueSome cb -> cb.Invoke(sql) | ValueNone -> ()
        let command = this.CreateCommand()
        command.CommandText <- sql
        processParameters addParameter command parameters

        command

    /// <summary>
    /// Lazily gets all methods from DbDataReader and its interfaces for later use in the TypeMapper.
    /// </summary>
    let internal dataReaderMethods =
        let rec getMethods (t: Type) =
            let implements = t.GetInterfaces()
            [
                yield! t.GetMethods()
                for implements in implements do
                    yield! getMethods implements
            ]
        getMethods typeof<DbDataReader>

    /// <summary>
    /// Reads all bytes from a SqliteBlob stream into a new NativeArray.NativeArray,
    /// ensuring the blob stream is properly disposed of afterward.
    /// </summary>
    let internal streamToNativeArrayFuncDisposing (s: SqliteBlob) =
        use s = s
        let arr = NativeArray.NativeArray.Alloc (int s.Length)
        let mutable totalRead = 0

        while totalRead < arr.Length do
            let read = s.Read(arr.Span)
            if read = 0 && totalRead < arr.Length then
                failwithf "Could not read the whole BLOB, readCount = %i, size = %i" totalRead arr.Length
            totalRead <- totalRead + read
        arr

    /// <summary>
    /// Creates an Expression that calls streamToNativeArrayFuncDisposing on a SqliteBlob expression.
    /// </summary>
    let internal streamToNativeArray =
        let method = typeof<Func<SqliteBlob, NativeArray.NativeArray>>.GetMethod "Invoke"
        let stna = Expression.Constant (Func<SqliteBlob, NativeArray.NativeArray> streamToNativeArrayFuncDisposing)
        fun (e: Expression) ->
            Expression.Call (stna, method, [|e|])

    /// <summary>
    /// Resolves the IDataReader method, conversion flag, and optional conversion function for a given target type.
    /// Shared logic used by both member-based and type-based expression builders.
    /// </summary>
    let private resolveReaderMethod (t: Type) : string * bool * (Expression -> Expression) option =
        match tryGetDateTimeFamilyReaderSpec t with
        | Some spec ->
            "GetInt64", true, Some spec.BuildExpression
        | None ->
            match t with
            | t when t = typeof<byte> || t = typeof<int8> ->
                "GetByte", false, None
            | t when t = typeof<uint8> ->
                "GetByte", true, Some (fun (expr: Expression) -> Expression.Convert(expr, typeof<uint8>) :> Expression)
            | t when t = typeof<int16> ->
                "GetInt16", false, None
            | t when t = typeof<uint16> ->
                "GetInt32", true, Some (fun (expr: Expression) ->
                    Expression.Convert(Expression.Call(
                        null,
                        typeof<uint16>.GetMethod("op_Explicit", [|typeof<int32>|]),
                        expr),
                        typeof<uint16>) :> Expression)
            | t when t = typeof<int32> ->
                "GetInt32", false, None
            | t when t = typeof<uint32> ->
                "GetInt64", true, Some (fun (expr: Expression) ->
                    Expression.Convert(Expression.Call(
                        null,
                        typeof<uint32>.GetMethod("op_Explicit", [|typeof<int64>|]),
                        expr),
                        typeof<uint32>) :> Expression)
            | t when t = typeof<int64> ->
                "GetInt64", false, None
            | t when t = typeof<uint64> ->
                "GetInt64", true, Some (fun (expr: Expression) ->
                    Expression.Convert(Expression.Call(
                        null,
                        typeof<uint64>.GetMethod("op_Explicit", [|typeof<int64>|]),
                        expr),
                        typeof<uint64>) :> Expression)
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
                "GetStream", true, Some (fun (expr: Expression) -> streamToNativeArray (Expression.TypeAs(expr, typeof<SqliteBlob>)) :> Expression)
            | t when t = typeof<byte[]> ->
                "GetValue", true, Some (fun (expr: Expression) -> Expression.TypeAs(expr, typeof<byte[]>) :> Expression)
            | t when t = typeof<Guid> ->
                "GetGuid", false, None
            | _ ->
                "GetValue", true, Some (fun (expr: Expression) -> Expression.Convert(expr, t) :> Expression)

    /// <summary>
    /// Builds the final reader call expression from the resolved method and conversion.
    /// </summary>
    let private buildReaderExpr (readerParam: Expression) (columnVar: Expression) (getMethodName: string, needsConversion: bool, conversionFunc: (Expression -> Expression) option) =
        let method = dataReaderMethods |> List.find(fun m -> m.Name = getMethodName)
        let readerParam = Expression.TypeAs(readerParam, typeof<DbDataReader>)

        let valueExpr: Expression =
            Expression.Call(
                readerParam,
                method,
                [| columnVar |]
            )

        match needsConversion, conversionFunc with
        | true, Some convFunc -> convFunc(valueExpr)
        | _ -> valueExpr

    /// <summary>
    /// Dynamically builds a LINQ Expression to read a value from an IDataReader for a given target type.
    /// Used by the tuple mapper for ordinal-based element reads.
    /// </summary>
    let internal matchMethodWithType (t: Type) (readerParam: Expression) (columnVar: Expression) =
        resolveReaderMethod t |> buildReaderExpr readerParam columnVar

    /// <summary>
    /// Dynamically builds a LINQ Expression to read a value from an IDataReader for a specific member (property or field).
    /// </summary>
    let internal matchMethodWithMemberType (prop: MemberInfo) (readerParam: Expression) (columnVar: Expression) =
        let (getMethodName, needsConversion, conversionFunc) =
            let t = match prop with | :? PropertyInfo as p -> p.PropertyType | :? FieldInfo as p -> p.FieldType | _ -> failwithf "Unknown member type."
            match tryGetDateTimeFamilyReaderSpec t with
            | Some spec ->
                "GetInt64", true, Some spec.BuildExpression
            | None ->
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
                | _ ->
                    "GetValue", true, Some (fun expr -> Expression.Convert(expr, t))


        let method = dataReaderMethods |> List.find(fun m -> m.Name = getMethodName)
        let readerParam = Expression.TypeAs(readerParam, typeof<DbDataReader>)

        let valueExpr = Expression.Call(
            readerParam,
            method,
            [| columnVar |]
        )

        let finalValueExpr =
            match needsConversion, conversionFunc with
            | true, Some convFunc -> convFunc(valueExpr)
            | _ -> valueExpr

        finalValueExpr
