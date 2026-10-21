namespace SoloDatabase

open System
open System.Linq.Expressions
open System.Reflection
open SoloDatabase.RawSqliteFunctions
open SoloDatabase.SqlModel

/// Character operations consume UTF16 units, separately from ordinary SQL text.
module internal QueryTranslatorCharacters =
    type Operation = Index = 0 | Contains = 1 | StartsWith = 2 | EndsWith = 3 | Replace = 4 | IndexOf = 5 | Concat = 6 | Comparable = 7 | Trim = 8 | TrimStart = 9 | TrimEnd = 10 | OrdinalKey = 11

    let private nullValue = SqlExpr.Literal SqlLiteral.Null
    let private call operation source first second =
        SqlExpr.FunctionCall("CHARACTER_VALUE", [SqlExpr.Literal(SqlLiteral.Integer(int64 operation)); source; first; second])

    let ordinalKey source = call Operation.OrdinalKey source nullValue nullValue

    let ordinalBytes (text: string) =
        if isNull text then null else
        let bytes = Array.zeroCreate<byte> (text.Length * 2)
        for index in 0 .. text.Length - 1 do
            let unit = uint16 text[index]
            bytes[index * 2] <- byte (unit >>> 8)
            bytes[index * 2 + 1] <- byte unit
        bytes

    let hasLegacyIndex (originalIndex: Expression) =
        match originalIndex with
        | :? ConstantExpression as literal when literal.Value :? int && unbox<int> literal.Value >= 0 -> false
        | _ -> true

    let index (originalIndex: Expression) source position =
        let unit = call Operation.Index source position nullValue
        if not(hasLegacyIndex originalIndex) then unit
        else
            let legacy = SqlExpr.FunctionCall("SUBSTR", [source; SqlExpr.Binary(position, BinaryOperator.Add, SqlExpr.Literal(SqlLiteral.Integer 1L)); SqlExpr.Literal(SqlLiteral.Integer 1L)])
            SqlExpr.CaseExpr((SqlExpr.Binary(position, BinaryOperator.Lt, SqlExpr.Literal(SqlLiteral.Integer -1L)), legacy), [], Some unit)

    let requiresComparable target (input: QuerySerializationInput) =
        input.LegacyCharacterText && typeof<char>.Equals(Utils.optionalPayloadType target)

    let comparable target value input =
        if not(typeof<char>.Equals(Utils.optionalPayloadType target)) then value else
        match value with
        | SqlExpr.Literal(SqlLiteral.Integer _) | SqlExpr.Parameter _ -> value
        | _ -> call Operation.Comparable value nullValue nullValue

    let concat (components: (SqlExpr * SqlExpr) array) =
        SqlExpr.FunctionCall("CHARACTER_VALUE", SqlExpr.Literal(SqlLiteral.Integer(int64 Operation.Concat)) ::
            (components |> Array.collect (fun (value, character) -> [|character; value|]) |> Array.toList))

    let indexArguments (method: MethodInfo) =
        if method.ContainsGenericParameters || not(typeof<char>.Equals method.ReturnType) then false else
        let parameters = method.GetParameters()
        if not method.IsStatic then
            typeof<string>.Equals method.DeclaringType && method.Name = "get_Chars" &&
            parameters.Length = 1 && typeof<int>.Equals parameters[0].ParameterType
        else
            method.DeclaringType.Assembly = typeof<Microsoft.FSharp.Core.Unit>.Assembly &&
            method.DeclaringType.FullName = "Microsoft.FSharp.Core.LanguagePrimitives+IntrinsicFunctions" &&
            method.Name = "GetString" && parameters.Length = 2 &&
            typeof<string>.Equals parameters[0].ParameterType && typeof<int>.Equals parameters[1].ParameterType

    let stringOperation (method: MethodInfo) =
        if method.IsStatic || method.ContainsGenericParameters || not(typeof<string>.Equals method.DeclaringType) then ValueNone else
        let parameters = method.GetParameters()
        let charFirst = parameters.Length > 0 && typeof<char>.Equals parameters[0].ParameterType
        let comparison = parameters.Length = 2 && typeof<StringComparison>.Equals parameters[1].ParameterType
        match method.Name with
        | "Contains" when charFirst && (parameters.Length = 1 || comparison) && typeof<bool>.Equals method.ReturnType -> ValueSome Operation.Contains
        | "StartsWith" when charFirst && parameters.Length = 1 && typeof<bool>.Equals method.ReturnType -> ValueSome Operation.StartsWith
        | "EndsWith" when charFirst && parameters.Length = 1 && typeof<bool>.Equals method.ReturnType -> ValueSome Operation.EndsWith
        | "Replace" when charFirst && parameters.Length = 2 && typeof<char>.Equals parameters[1].ParameterType && typeof<string>.Equals method.ReturnType -> ValueSome Operation.Replace
        | "IndexOf" when charFirst && comparison && typeof<int>.Equals method.ReturnType -> ValueSome Operation.IndexOf
        | _ -> ValueNone

    type TrimmingInput = Parameterless | Character | Characters | EmptySpan

    let private readOnlyCharactersType =
        typeof<MemoryExtensions>.GetMethod("AsSpan", [|typeof<string>|]).ReturnType

    let trimmingOperation (expression: MethodCallExpression) =
        let method = expression.Method
        if method.IsStatic || method.ContainsGenericParameters || isNull expression.Object ||
           not(typeof<string>.Equals method.DeclaringType) || not(typeof<string>.Equals method.ReturnType) then ValueNone else
        let operation =
            match method.Name with
            | "Trim" -> ValueSome Operation.Trim
            | "TrimStart" -> ValueSome Operation.TrimStart
            | "TrimEnd" -> ValueSome Operation.TrimEnd
            | _ -> ValueNone
        let parameters = method.GetParameters()
        let input =
            if parameters.Length <> expression.Arguments.Count then ValueNone
            elif parameters.Length = 0 then ValueSome Parameterless
            elif parameters.Length <> 1 then ValueNone
            elif typeof<char>.Equals parameters[0].ParameterType then ValueSome Character
            elif typeof<char[]>.Equals parameters[0].ParameterType then ValueSome Characters
            elif readOnlyCharactersType.Equals parameters[0].ParameterType &&
                 expression.Arguments[0].NodeType = ExpressionType.Default then ValueSome EmptySpan
            else ValueNone
        match operation, input with
        | ValueSome operation, ValueSome input -> ValueSome(operation,input)
        | _ -> ValueNone

    let trimming kind input source value =
        match input with
        | Parameterless ->
            let name = if kind = Operation.Trim then "TRIM" elif kind = Operation.TrimStart then "LTRIM" else "RTRIM"
            SqlExpr.FunctionCall(name,[source])
        | _ ->
            let array = input = Characters || input = EmptySpan
            let value = if array then SqlExpr.FunctionCall("json",[value]) else value
            call kind source value (SqlExpr.Literal(SqlLiteral.Integer(if array then 1L else 0L)))

    let operation kind source first second = call kind source first second

    /// Character comparison accepts an original unit or a one-unit legacy text carrier.
    let unitValue (value: SqliteRawValue) =
        if value.IsNull then ValueNone
        elif value.IsInteger then ValueSome(char(value.GetInt32()))
        else
            let text = value.CopyToString()
            if text.Length = 1 then ValueSome text[0] else ValueNone

    let register (lower: string -> string) (connection: NativeSqliteConnection) =
        connection.RegisterScalar("CHARACTER_VALUE", -1, RawScalarFunc(fun context arguments ->
            let operation = enum<Operation>(arguments[0].GetInt32())
            let characterText (value: SqliteRawValue) =
                if value.IsInteger then string(char(value.GetInt32())) else value.CopyToString()
            if operation = Operation.OrdinalKey then
                if arguments[1].IsNull then context.SetNull()
                elif arguments[1].IsBlob then context.SetBlob(arguments[1].GetBlobSpan())
                else context.SetBlob(ordinalBytes(arguments[1].CopyToString()))
            elif operation = Operation.Comparable then
                let value = arguments[1]
                if value.IsNull then context.SetNull()
                elif value.IsInteger then context.SetInt64(value.GetInt64())
                elif value.IsFloat then context.SetDouble(value.GetDouble())
                else
                    let text = value.CopyToString()
                    if text.Length = 1 then context.SetInt32(int text[0])
                    else context.SetText(text)
            elif operation = Operation.Concat then
                let output = System.Text.StringBuilder()
                for position in 1 .. 2 .. arguments.Length - 2 do
                    let value = arguments[position + 1]
                    if not value.IsNull then
                        let role = arguments[position]
                        let character =
                            if role.IsInteger then role.GetInt32() = 1
                            else
                                let descriptor = JsonSerializator.JsonValue.Parse(role.CopyToString())
                                let writer = QuerySerializationDescriptor.resolveInputDescriptor descriptor
                                typeof<char>.Equals(Utils.optionalPayloadType writer)
                        if character then output.Append(characterText value) |> ignore
                        else output.Append(value.CopyToString()) |> ignore
                context.SetText(output.ToString())
            elif operation = Operation.Trim || operation = Operation.TrimStart || operation = Operation.TrimEnd then
                if arguments[1].IsNull then context.SetNull()
                else
                    let source = arguments[1].CopyToString()
                    let array = arguments[3].GetInt32() <> 0
                    if not array && arguments[2].IsNull then context.SetNull()
                    else
                        let characters =
                            if array then
                                if arguments[2].IsNull then null
                                else JsonSerializator.JsonValue.ParseInto<char[]>(arguments[2].CopyToString())
                            else (characterText arguments[2]).ToCharArray()
                        // Empty legacy scalar extraction is an empty text set, not
                        // the authored null/empty array switch to whitespace trimming.
                        if not array && characters.Length = 0 then context.SetText(source)
                        else
                            let result =
                                if operation = Operation.Trim then source.Trim(characters)
                                elif operation = Operation.TrimStart then source.TrimStart(characters)
                                else source.TrimEnd(characters)
                            context.SetText(result)
            elif (operation = Operation.Contains || operation = Operation.IndexOf) && arguments[3].GetInt32() <> 0 && arguments[2].IsNull then
                context.SetText(lower null)
            elif arguments[1].IsNull || arguments[2].IsNull || operation = Operation.Replace && arguments[3].IsNull then context.SetNull()
            else
                let source = arguments[1].CopyToString()
                if operation = Operation.Index then
                    let position = arguments[2].GetInt64()
                    if position >= 0L && position < int64 source.Length then context.SetInt32(int source[int position])
                    else context.SetText("")
                else
                    let unit = arguments[2].IsInteger
                    let first = characterText arguments[2]
                    let ignoreCase = (operation = Operation.Contains || operation = Operation.IndexOf) && arguments[3].GetInt32() <> 0
                    let first = if ignoreCase then lower first else first
                    match operation with
                    | Operation.Contains -> context.SetInt32(if source.IndexOf(first, StringComparison.Ordinal) >= 0 then 1 else 0)
                    | Operation.StartsWith ->
                        context.SetInt32(if (unit || first.IndexOf('\u0000') < 0) && source.StartsWith(first, StringComparison.Ordinal) then 1 else 0)
                    | Operation.EndsWith ->
                        let matches =
                            if unit then source.EndsWith(first, StringComparison.Ordinal)
                            else
                                let endPosition = source.IndexOf('\u0000')
                                let legacySource = if endPosition < 0 then source else source.Substring(0, endPosition)
                                if first.Length = 0 then legacySource.Length = 0
                                else first.IndexOf('\u0000') < 0 && legacySource.EndsWith(first, StringComparison.Ordinal)
                        context.SetInt32(if matches then 1 else 0)
                    | Operation.IndexOf ->
                        let position = source.IndexOf(first, StringComparison.Ordinal)
                        if unit || position < 0 then context.SetInt32(position)
                        else
                            // Legacy text search positions count SQL characters, not UTF16 units.
                            let mutable units, characters = 0, 0
                            while units < position do
                                let pair = units + 1 < position && Char.IsSurrogatePair(source, units)
                                units <- units + (if pair then 2 else 1)
                                characters <- characters + 1
                            context.SetInt32(characters)
                    | Operation.Replace ->
                        if first.Length = 0 then context.SetText(source)
                        else context.SetText(source.Replace(first, characterText arguments[3]))
                    | _ -> invalidArg "operation" "The character operation is not supported."), SqliteFunctionFlags.Deterministic)
