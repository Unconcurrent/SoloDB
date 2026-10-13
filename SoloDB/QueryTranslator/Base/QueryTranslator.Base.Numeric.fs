namespace SoloDatabase

open System
open System.Linq
open System.Linq.Expressions
open System.Numerics
open System.Reflection
open SoloDatabase.Metadata.Generation
open SoloDatabase.Numerics
open SoloDatabase.SqlModel

/// Original CLR signatures establish numerical roles before SQL loses type information.
module internal QueryTranslatorNumeric =
    let underlying (target: Type) =
        let nullable = Nullable.GetUnderlyingType target
        if isNull nullable then target else nullable

    let unsigned64 target =
        let target=underlying target
        typeof<uint64>.Equals(target) || UIntPtr.Size=8 && typeof<unativeint>.Equals(target)

    let domain target =
        let target = underlying target
        if typeof<decimal>.Equals(target) then ValueSome NumericKind.Decimal
        elif typeof<BigInteger>.Equals(target) || unsigned64 target then ValueSome NumericKind.ArbitraryInteger
        else ValueNone

    let call kind name arguments =
        let operation = ExactNumericNames.operation name
        SqlExpr.FunctionCall(ExactNumericNames.scalar kind,SqlExpr.Literal(SqlLiteral.Integer(int64(int operation))) :: arguments)

    let private binaryName target operation =
        match domain target with
        | ValueNone -> ValueNone
        | ValueSome kind ->
            let name =
                match operation with
                | System.Linq.Expressions.ExpressionType.Add | System.Linq.Expressions.ExpressionType.AddChecked -> "ADD"
                | System.Linq.Expressions.ExpressionType.Subtract | System.Linq.Expressions.ExpressionType.SubtractChecked -> "SUB"
                | System.Linq.Expressions.ExpressionType.Multiply | System.Linq.Expressions.ExpressionType.MultiplyChecked -> "MUL"
                | System.Linq.Expressions.ExpressionType.Divide -> "DIV"
                | System.Linq.Expressions.ExpressionType.Modulo -> "MOD"
                | _ -> null
            if isNull name then ValueNone else ValueSome(kind,name)

    let binary target operation left right =
        match binaryName target operation with
        | ValueNone -> ValueNone
        | ValueSome(kind,name) ->
            let arguments =
                if unsigned64 target then
                    let checkedResult =
                        operation=ExpressionType.AddChecked || operation=ExpressionType.SubtractChecked ||
                        operation=ExpressionType.MultiplyChecked
                    [left;right;SqlExpr.Literal(SqlLiteral.Integer(int64(int ExactNumericOperation.ToUInt64)));
                     SqlExpr.Literal(SqlLiteral.Integer(if checkedResult then 1L else 0L))]
                else [left;right]
            ValueSome(call kind name arguments)

    let negate target argument =
        match domain target with
        | ValueSome kind -> call kind "NEG" [argument]
        | ValueNone -> SqlExpr.Unary(UnaryOperator.Neg,argument)

    let private nativeOperation target =
        let native = underlying target
        let core = TypeSupport.NumericCore native.FullName |> Option.defaultValue native.FullName
        match core with
        | "System.Double" -> "TO_DOUBLE" | "System.Single" -> "TO_SINGLE"
        | "System.Int64" -> "TO_INT64" | "System.Int32" -> "TO_INT32"
        | "System.Int16" -> "TO_INT16" | "System.SByte" -> "TO_SBYTE"
        | "System.UInt64" -> "TO_UINT64" | "System.UInt32" -> "TO_UINT32"
        | "System.UInt16" -> "TO_UINT16" | "System.Byte" -> "TO_BYTE"
        | "System.IntPtr" -> if IntPtr.Size=8 then "TO_INT64" else "TO_INT32"
        | "System.UIntPtr" -> if IntPtr.Size=8 then "TO_UINT64" else "TO_UINT32"
        | _ -> null

    let private jsonText value =
        match value with
        | SqlExpr.JsonExtractExpr(alias,column,path) ->
            SqlExpr.Binary(SqlExpr.Column(alias,column),BinaryOperator.JsonText,
                           SqlExpr.Literal(SqlLiteral.String(JsonPathOps.text(JsonPathOps.toList path))))
        | SqlExpr.JsonRootExtract(alias,column) ->
            SqlExpr.Binary(SqlExpr.Column(alias,column),BinaryOperator.JsonText,SqlExpr.Literal(SqlLiteral.String "$"))
        | SqlExpr.FunctionCall("jsonb_extract",[source;path]) -> SqlExpr.Binary(source,BinaryOperator.JsonText,path)
        | _ -> value

    let storedValue target argument =
        match argument with
        | SqlExpr.JsonExtractExpr _ | SqlExpr.JsonRootExtract _ | SqlExpr.FunctionCall("jsonb_extract",_) ->
            let operation=nativeOperation target
            if isNull operation then argument
            else
                let input=if unsigned64 target then jsonText argument else argument
                call NumericKind.Decimal "NATIVE_VALUE" [SqlExpr.Literal(SqlLiteral.Integer(int64(int(ExactNumericNames.operation operation))));input]
        | _ -> argument

    let private jsonConversion operation target arguments =
        let native = nativeOperation target
        let targetOperation, kind =
            match domain target with
            | ValueSome kind when not(unsigned64 target) -> ValueSome ExactNumericOperation.Cast, kind
            | _ when not(isNull native) -> ValueSome(ExactNumericNames.operation native), NumericKind.Decimal
            | _ -> ValueNone, NumericKind.Decimal
        targetOperation |> ValueOption.map (fun targetOperation ->
            call kind operation (SqlExpr.Literal(SqlLiteral.Integer(int64(int targetOperation))) :: arguments))

    /// The original extraction publishes JSON text before a native scalar can lose digits.
    let private jsonInput argument =
        match argument with
        | SqlExpr.JsonExtractExpr _ | SqlExpr.JsonRootExtract _ | SqlExpr.FunctionCall("jsonb_extract", _) -> jsonText argument
        | _ -> SqlExpr.Binary(argument, BinaryOperator.JsonText, SqlExpr.Literal(SqlLiteral.String "$"))

    /// An authored extraction already exposes SQLite's native scalar contract.
    let nativeExtraction argument =
        match argument with
        | SqlExpr.FunctionCall("jsonb", [SqlExpr.Binary(source, BinaryOperator.JsonText, path)]) ->
            let source = match source with SqlExpr.FunctionCall("jsonb", [source]) -> source | _ -> source
            SqlExpr.FunctionCall("jsonb_extract", [source; path])
        | _ -> argument

    let nativeScalar argument =
        let extracted = nativeExtraction argument
        match extracted with
        | SqlExpr.JsonExtractExpr _ | SqlExpr.JsonRootExtract _ | SqlExpr.FunctionCall("jsonb_extract", _) -> extracted
        | _ -> SqlExpr.FunctionCall("jsonb_extract", [argument; SqlExpr.Literal(SqlLiteral.String "$")])

    let convertStored target argument = jsonConversion "STORED_VALUE" target [jsonInput argument]
    let convertNativeJson target argument = jsonConversion "NATIVE_JSON_VALUE" target [jsonInput argument]
    let convertWriter target argument descriptor =
        jsonConversion "WRITER_VALUE" target [jsonInput argument; SqlExpr.FunctionCall("json", [descriptor])]

    let storedUnsigned target argument =
        if unsigned64 target then
            let checkedValue=call NumericKind.ArbitraryInteger "TO_UINT64" [argument]
            SqlExpr.FunctionCall("jsonb",[call NumericKind.ArbitraryInteger "TEXT" [checkedValue]])
        else argument

    let cast checkedConversion target source argument =
        match domain target,domain source with
        | ValueSome output,ValueSome input ->
            if unsigned64 target && not(unsigned64 source) then ValueSome(call input "TO_UINT64" [argument])
            elif output = input then ValueSome argument
            else ValueSome(call output "FROM_NUMERIC" [argument])
        | ValueSome output,ValueNone ->
            let converted=call output "CAST" [argument]
            ValueSome(if checkedConversion && unsigned64 target then call output "TO_UINT64" [converted] else converted)
        | ValueNone,ValueSome input ->
            let suffix = nativeOperation target
            if isNull suffix then ValueNone else ValueSome(call input suffix [argument])
        | _ -> ValueNone

    let nativeCastType target =
        let native=underlying target
        if typeof<double>.Equals(native) || typeof<single>.Equals(native) then ValueSome "REAL"
        elif typeof<bool>.Equals(native) || typeof<sbyte>.Equals(native) || typeof<byte>.Equals(native) ||
             typeof<int16>.Equals(native) || typeof<uint16>.Equals(native) || typeof<int>.Equals(native) ||
             typeof<uint32>.Equals(native) || typeof<int64>.Equals(native) || typeof<uint64>.Equals(native) ||
             typeof<nativeint>.Equals(native) || typeof<unativeint>.Equals(native) then ValueSome "INTEGER"
        else ValueNone

    let conversion checkedConversion target source argument =
        match cast checkedConversion target source argument with
        | ValueSome result -> result
        | ValueNone ->
            match nativeCastType target with
            | ValueSome sqlType -> SqlExpr.Cast(argument,sqlType)
            | ValueNone -> argument

    let zero target =
        match domain target with
        | ValueSome kind -> call kind "CAST" [SqlExpr.Literal(SqlLiteral.Integer 0L)]
        | ValueNone -> SqlExpr.Literal(SqlLiteral.Integer 0L)

    let kind = function
        | "SUM" -> AggregateKind.Sum
        | "AVG" -> AggregateKind.Avg
        | "MIN" -> AggregateKind.Min
        | "MAX" -> AggregateKind.Max
        | name -> invalidArg "name" ("Unsupported aggregate: " + name)

    let rejectsEmpty (target: Type) = target.IsValueType && isNull(Nullable.GetUnderlyingType target)

    let aggregate (method: MethodInfo) (inputType: Type) kind argument distinct =
        let owner = method.DeclaringType
        let exact =
            if typeof<Enumerable>.Equals(owner) || typeof<Queryable>.Equals(owner) then
                match domain inputType,domain method.ReturnType,kind with
                | ValueSome input,ValueSome output,AggregateKind.Sum when input = output -> ValueSome(DecimalAggregateKind.Sum,output)
                | ValueSome input,ValueSome output,AggregateKind.Avg when input = output -> ValueSome(DecimalAggregateKind.Average,output)
                | ValueSome input,ValueSome output,AggregateKind.Min when input = output -> ValueSome(DecimalAggregateKind.Minimum,output)
                | ValueSome input,ValueSome output,AggregateKind.Max when input = output -> ValueSome(DecimalAggregateKind.Maximum,output)
                | _ -> ValueNone
            else ValueNone
        let kind = match exact with ValueSome(kind,domain) -> AggregateKind.ExactNumeric(kind,domain) | ValueNone -> kind
        SqlExpr.AggregateCall(kind,Some argument,distinct,None)

    let aggregateInput (method: MethodInfo) =
        let parameters = method.GetParameters()
        let source = parameters[0].ParameterType
        if parameters.Length > 1 then
            let selector = parameters[1].ParameterType
            let selector =
                if selector.IsGenericType && typedefof<System.Linq.Expressions.Expression<_>>.Equals(selector.GetGenericTypeDefinition()) then
                    selector.GetGenericArguments()[0]
                else selector
            if selector.IsGenericType && typedefof<Func<_,_>>.Equals(selector.GetGenericTypeDefinition()) then selector.GetGenericArguments()[1]
            else source.GetGenericArguments()[0]
        else source.GetGenericArguments()[0]
