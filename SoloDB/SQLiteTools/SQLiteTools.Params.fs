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
    // Names and constants belong to the retained plan. Values belong to one
    // invocation and remain alive with its enumerable; binding only reads them.
    type ParameterValues = {
        Constants: KeyValuePair<string, obj> array
        Names: string array
        Values: obj array
    }

    type private DateTimeFamilyReaderSpec = {
        ClrType: Type
        BuildExpression: Expression -> Expression
        BuildTextExpression: Expression -> Expression
    }

    let private parseInvariant (t: Type) (text: Expression) =
        Expression.Call(
            t.GetMethod("Parse", [| typeof<string>; typeof<IFormatProvider> |]),
            text, Expression.Constant(Globalization.CultureInfo.InvariantCulture, typeof<IFormatProvider>)) :> Expression

    let private dateTimeFamilyReaderSpecs =
        [|
            { ClrType = typeof<DateTime>
              BuildTextExpression = parseInvariant typeof<DateTime>
              BuildExpression = fun expr ->
                  Expression.Call(typeof<DateTime>.GetMethod("FromBinary", [| typeof<int64> |]), expr) :> Expression }
            { ClrType = typeof<DateTimeOffset>
              BuildTextExpression = parseInvariant typeof<DateTimeOffset>
              BuildExpression = fun expr ->
                  Expression.Call(typeof<DateTimeOffset>.GetMethod("FromUnixTimeMilliseconds", [| typeof<int64> |]), expr) :> Expression }
            { ClrType = typeof<DateOnly>
              BuildTextExpression = parseInvariant typeof<DateOnly>
              BuildExpression = fun expr ->
                  Expression.Call(typeof<DateOnly>.GetMethod("FromDayNumber", [| typeof<int> |]), Expression.Convert(expr, typeof<int>)) :> Expression }
            { ClrType = typeof<TimeOnly>
              BuildTextExpression = parseInvariant typeof<TimeOnly>
              BuildExpression = fun expr ->
                  let ms = Expression.Convert(expr, typeof<float>)
                  let ts = Expression.Call(typeof<TimeSpan>.GetMethod("FromMilliseconds", [| typeof<float> |]), ms)
                  Expression.Call(typeof<TimeOnly>.GetMethod("FromTimeSpan", [| typeof<TimeSpan> |]), ts) :> Expression }
            { ClrType = typeof<TimeSpan>
              BuildTextExpression = parseInvariant typeof<TimeSpan>
              BuildExpression = fun expr ->
                  let ms = Expression.Convert(expr, typeof<float>)
                  Expression.Call(typeof<TimeSpan>.GetMethod("FromMilliseconds", [| typeof<float> |]), ms) :> Expression }
        |]

    let private tryGetDateTimeFamilyReaderSpec (t: Type) =
        dateTimeFamilyReaderSpecs
        |> Array.tryFind (fun spec -> spec.ClrType = t)

    /// Optional SQL trace callback for corpus capture and diagnostics.
    /// Set by test harnesses to intercept all SQL at the execution boundary.
    let mutable internal sqlTraceCallback: Action<string> voption = ValueNone

    /// Optional callback reporting a statement together with the parameters actually bound to it.
    ///
    /// The trace above reports SQL text only, which cannot show what a statement was bound to, so
    /// it cannot distinguish a value carried as a parameter from one written into the text, nor
    /// establish the names and order a statement allocates. This fires after binding and reports
    /// both.
    let mutable internal sqlBoundTraceCallback: Action<string, IReadOnlyList<KeyValuePair<string, obj>>> voption = ValueNone

    /// <summary>Caches PropertyInfo for Nullable types' 'HasValue' and 'Value' properties for performance.</summary>
    let private nullablePropsCache = ConcurrentDictionary<Type, struct (PropertyInfo * PropertyInfo)>()

    /// <summary>Retrieves the 'HasValue' and 'Value' properties for a given Nullable type from the cache or via reflection.</summary>
    /// <returns>A struct tuple containing the PropertyInfo for HasValue and Value.</returns>
    let private getNullableProperties (nullableType: Type) =
        nullablePropsCache.GetOrAdd(nullableType, fun t ->
            struct (t.GetProperty("HasValue"), t.GetProperty("Value")))

    /// <summary>A private struct to pass an array along with its effective length, for BLOB operations.</summary>
    [<Struct>]
    type TrimmedArray = {
        /// <summary>The underlying array.</summary>
        Array: Array
        /// <summary>The number of valid bytes in the array.</summary>
        TrimmedLen: int
    }


    /// <summary>Processes a parameter value before it's added to a DB command, handling nulls, DateTimeOffset, and Nullable types.</summary>
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

    let internal setRetainedParameterValue (parameter: SqliteParameter) (value: obj) =
        let struct (value, size) = processParameter value
        parameter.Value <- value
        parameter.Size <- size

    /// <summary>Creates and adds a new IDbDataParameter to a command.</summary>
    let internal addParameter (command: IDbCommand) (key: string) (value: obj) =
        let struct (value, size) = processParameter value

        let par = command.CreateParameter()
        par.ParameterName <- key
        par.Value <- value

        par.Size <- size

        command.Parameters.Add par |> ignore

    /// <summary>Updates an existing parameter or adds a new one if it doesn't exist. Used for cached commands.</summary>
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

        par.Size <- size

    /// <summary>Caches compiled lambda expressions for dynamically processing anonymous-type parameters.</summary>
    let private dynamicParameterCache = ConcurrentDictionary<Type, struct(Action<IDbCommand, obj, Action<IDbCommand,string,obj>> * int)>()

    /// <summary>Binds retained, dictionary or anonymous-object parameters through the supplied command writer.</summary>
    let internal processParameters processFn (command: IDbCommand) (parameters: obj) =
        match parameters with
        | null -> 0
        | :? ParameterValues as packet ->
            for pair in packet.Constants do
                processFn command pair.Key pair.Value
            for i = 0 to packet.Names.Length - 1 do
                processFn command packet.Names.[i] packet.Values.[i]
            packet.Constants.Length + packet.Names.Length
        | :? IDictionary<string, obj> as dict ->
            let mutable count = 0
            for KeyValue(key, value) in dict do
                processFn command key value
                count <- count + 1
            count
        | :? IDictionary as dict ->
            let mutable count = 0
            for key in dict.Keys do
                let value = dict.[key]
                let key = key :?> string
                processFn command key value
                count <- count + 1
            count

        | parameters ->
            let struct(fn, count) = dynamicParameterCache.GetOrAdd(parameters.GetType(), Func<Type, struct(Action<IDbCommand, obj, Action<IDbCommand,string,obj>> * int)>(
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

                    struct(l.Compile(false), props.Length)
            ))
            fn.Invoke(command, parameters, processFn)
            count

    /// <summary>Creates an IDbCommand with the given SQL and parameters.</summary>
    /// <returns>A new IDbCommand.</returns>
    let internal createCommand (this: SqliteConnection) (sql: string) (parameters: obj) =
        match sqlTraceCallback with ValueSome cb -> cb.Invoke(sql) | ValueNone -> ()
        let command = this.CreateCommand()
        try
            command.CommandText <- sql
            processParameters addParameter command parameters |> ignore

            match sqlBoundTraceCallback with
            | ValueSome cb ->
                let bound = ResizeArray<KeyValuePair<string, obj>>(command.Parameters.Count)
                for i in 0 .. command.Parameters.Count - 1 do
                    let p = command.Parameters.[i]
                    bound.Add(KeyValuePair(p.ParameterName, p.Value))
                cb.Invoke(sql, bound :> IReadOnlyList<KeyValuePair<string, obj>>)
            | ValueNone -> ()

            command
        with _ ->
            command.Dispose()
            reraise()

    /// <summary>Lazily gets all methods from DbDataReader and its interfaces for later use in the TypeMapper.</summary>
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

    /// <summary>Creates an Expression that calls streamToNativeArrayFuncDisposing on a SqliteBlob expression.</summary>
    let internal streamToNativeArray =
        let method = typeof<Func<SqliteBlob, NativeArray.NativeArray>>.GetMethod "Invoke"
        let stna = Expression.Constant (Func<SqliteBlob, NativeArray.NativeArray> streamToNativeArrayFuncDisposing)
        fun (e: Expression) ->
            Expression.Call (stna, method, [|e|])

    /// <summary>
    /// Resolves the IDataReader method, conversion flag, and optional conversion function for a given target type.
    /// Shared logic used by both member-based and type-based expression builders.
    /// </summary>
    let rec private tryResolveReaderMethod (t: Type) : (string * bool * (Expression -> Expression) option) option =
        let direct name = Some (name, false, None)
        let convert name conversion = Some (name, true, Some conversion)
        match tryGetDateTimeFamilyReaderSpec t with
        | Some spec -> convert "GetInt64" spec.BuildExpression
        | None when t.IsEnum ->
            let underlying = Enum.GetUnderlyingType t
            if underlying = typeof<int64> then
                // Existing Int64 enum members read the native storage value strictly.
                convert "GetValue" (fun expr -> Expression.Convert(expr, t) :> Expression)
            else
                tryResolveReaderMethod underlying
                |> Option.map (fun (methodName, needsConversion, conversion) ->
                    methodName, true, Some (fun expr ->
                        let value =
                            match needsConversion, conversion with
                            | true, Some convertValue -> convertValue expr
                            | _ -> expr
                        Expression.Convert(value, t) :> Expression))
        | None ->
            match t with
            | t when t = typeof<byte> -> direct "GetByte"
            | t when t = typeof<int8> ->
                convert "GetInt16" (fun expr -> Expression.ConvertChecked(expr, t) :> Expression)
            | t when t = typeof<int16> -> direct "GetInt16"
            | t when t = typeof<uint16> ->
                convert "GetInt32" (fun expr -> Expression.Convert(expr, t) :> Expression)
            | t when t = typeof<int32> -> direct "GetInt32"
            | t when t = typeof<uint32> ->
                convert "GetInt64" (fun expr -> Expression.Convert(expr, t) :> Expression)
            | t when t = typeof<int64> -> direct "GetInt64"
            | t when t = typeof<uint64> ->
                convert "GetInt64" (fun expr -> Expression.Convert(expr, t) :> Expression)
            | t when t = typeof<float32> -> direct "GetFloat"
            | t when t = typeof<double> -> direct "GetDouble"
            | t when t = typeof<decimal> -> direct "GetDecimal"
            | t when t = typeof<string> -> direct "GetString"
            | t when t = typeof<char> -> direct "GetChar"
            | t when t = typeof<bool> -> direct "GetBoolean"
            | t when t = typeof<NativeArray.NativeArray> ->
                convert "GetStream" (fun expr -> streamToNativeArray (Expression.TypeAs(expr, typeof<SqliteBlob>)) :> Expression)
            | t when t = typeof<byte[]> ->
                convert "GetValue" (fun expr -> Expression.TypeAs(expr, typeof<byte[]>) :> Expression)
            | t when t = typeof<Guid> -> direct "GetGuid"
            | _ -> None

    /// <summary>Builds the final reader call expression from the resolved method and conversion.</summary>
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
    let rec internal matchMethodWithType (t: Type) (readerParam: Expression) (columnVar: Expression) : Expression =
        if not (isNull (Nullable.GetUnderlyingType t)) then
            readNullableValue t readerParam columnVar
        else
            let spec =
                tryResolveReaderMethod t
                |> Option.defaultWith (fun () ->
                    "GetValue", true, Some (fun expr -> Expression.Convert(expr, t) :> Expression))
            let read = buildReaderExpr readerParam columnVar spec
            match tryGetDateTimeFamilyReaderSpec t with
            | Some temporal ->
                // Keep numeric document encodings, while accepting native ADO TEXT parameters.
                let storedType = Expression.Call(readerParam, typeof<IDataRecord>.GetMethod("GetFieldType"), columnVar)
                let textRead =
                    if t = typeof<DateTime> then
                        buildReaderExpr readerParam columnVar ("GetDateTime", false, None)
                    else
                        buildReaderExpr readerParam columnVar ("GetString", true, Some temporal.BuildTextExpression)
                Expression.Condition(
                    Expression.Equal(storedType, Expression.Constant(typeof<string>, typeof<Type>)),
                    textRead, read) :> Expression
            | None -> read

    /// Nullable values share the underlying reader's storage conversions.
    and internal readNullableValue (t: Type) (readerParam: Expression) (columnVar: Expression) : Expression =
        let underlying = Nullable.GetUnderlyingType t
        if underlying = typeof<int64> || underlying = typeof<double> then
            // These native SQLite storage classes already supported strict unboxing.
            let value = Expression.Variable(typeof<obj>, "nullableValue")
            let read = buildReaderExpr readerParam columnVar ("GetValue", false, None)
            Expression.Block(
                [| value |],
                [| Expression.Assign(value, read) :> Expression
                   Expression.Condition(
                       Expression.ReferenceEqual(value, Expression.Constant(DBNull.Value, typeof<obj>)),
                       Expression.Default(t),
                       Expression.Convert(value, t)) :> Expression |]) :> Expression
        else
            let read = matchMethodWithType underlying readerParam columnVar
            Expression.Condition(
                Expression.Call(readerParam, typeof<IDataRecord>.GetMethod("IsDBNull"), columnVar),
                Expression.Default(t),
                Expression.New(t.GetConstructor([|underlying|]), read)) :> Expression

    /// Scalar primitives use the same reader as fields and tuple slots.
    let internal tryBuildScalarRead (t: Type) readerParam columnVar =
        if not (isNull (Nullable.GetUnderlyingType t))
           || (t <> typeof<NativeArray.NativeArray> && t <> typeof<byte[]> && (tryResolveReaderMethod t).IsSome) then
            Some (matchMethodWithType t readerParam columnVar)
        else None

    /// Resolve member and ordinal reads through the same storage-type rules.
    let internal matchMethodWithMemberType (prop: MemberInfo) (readerParam: Expression) (columnVar: Expression) =
        let t =
            match prop with
            | :? PropertyInfo as p -> p.PropertyType
            | :? FieldInfo as p -> p.FieldType
            | _ -> failwithf "Unknown member type."
        matchMethodWithType t readerParam columnVar
