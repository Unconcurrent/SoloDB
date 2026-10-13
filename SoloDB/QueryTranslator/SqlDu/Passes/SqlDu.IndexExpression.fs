module internal SoloDatabase.IndexExpression

open System
open System.Collections.Generic
open System.Text
open System.Text.RegularExpressions
open SoloDatabase.SqlModel
open SoloDatabase.Numerics

// Recognizes emitted index terms without interpreting foreign SQL.
let private recIsWrappedByOuterParens (s: string) : bool =
    if String.IsNullOrWhiteSpace s then false
    else
        let t = s.Trim()
        if t.Length < 2 || t.[0] <> '(' || t.[t.Length - 1] <> ')' then
            false
        else
            let mutable depth = 0
            let mutable inSingle = false
            let mutable inDouble = false
            let mutable wraps = true
            let mutable i = 0
            while i < t.Length && wraps do
                let c = t.[i]
                if inSingle then
                    if c = '\'' then inSingle <- false
                elif inDouble then
                    if c = '"' then inDouble <- false
                else
                    if c = '\'' then inSingle <- true
                    elif c = '"' then inDouble <- true
                    elif c = '(' then depth <- depth + 1
                    elif c = ')' then
                        depth <- depth - 1
                        if depth = 0 && i <> t.Length - 1 then
                            wraps <- false
                i <- i + 1
            wraps && depth = 0 && not inSingle && not inDouble

let private stripOuterParens (s: string) : string =
    let mutable current = s.Trim()
    let mutable changed = true
    while changed do
        if recIsWrappedByOuterParens current then
            current <- current.Substring(1, current.Length - 2).Trim()
        else
            changed <- false
    current

let private splitTopLevelCommaTerms (s: string) : string list =
    let result = ResizeArray<string>()
    let mutable depth = 0
    let mutable inSingle = false
    let mutable inDouble = false
    let current = StringBuilder()

    let flush () =
        let term = current.ToString().Trim()
        current.Clear() |> ignore
        if term <> "" then result.Add term

    for c in s do
        if inSingle then
            current.Append(c) |> ignore
            if c = '\'' then inSingle <- false
        elif inDouble then
            current.Append(c) |> ignore
            if c = '"' then inDouble <- false
        else
            match c with
            | '\'' ->
                inSingle <- true
                current.Append(c) |> ignore
            | '"' ->
                inDouble <- true
                current.Append(c) |> ignore
            | '(' ->
                depth <- depth + 1
                current.Append(c) |> ignore
            | ')' ->
                depth <- depth - 1
                current.Append(c) |> ignore
            | ',' when depth = 0 ->
                flush ()
            | _ ->
                current.Append(c) |> ignore

    flush ()
    result |> Seq.toList

let tryExtractIndexTerms (indexSql: string) : string list option =
    let sql = indexSql.Trim()
    let openParen = sql.IndexOf '('
    if openParen < 0 then None
    else
        let mutable depth = 0
        let mutable inSingle = false
        let mutable inDouble = false
        let mutable closeParen = -1
        let mutable i = openParen
        while i < sql.Length && closeParen < 0 do
            let c = sql.[i]
            if inSingle then
                if c = '\'' then inSingle <- false
            elif inDouble then
                if c = '"' then inDouble <- false
            else
                if c = '\'' then inSingle <- true
                elif c = '"' then inDouble <- true
                elif c = '(' then depth <- depth + 1
                elif c = ')' then
                    depth <- depth - 1
                    if depth = 0 then closeParen <- i
            i <- i + 1

        if closeParen < 0 then None
        else
            let inner = sql.Substring(openParen + 1, closeParen - openParen - 1).Trim()
            match splitTopLevelCommaTerms inner with
            | [] -> None
            | terms -> Some terms

let private columnIdentifierRegex = Regex(@"^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled)
let private pathLiteralPattern = @"'(?<path>\$(?:(?:\.|\[)(?:[^']|'')*)?)'"
let private jsonExtractRegex =
    Regex(
        @"^\s*jsonb_extract\s*\(\s*(?:""Value""|Value)\s*,\s*" + pathLiteralPattern + @"\s*\)\s*$",
        RegexOptions.Compiled ||| RegexOptions.IgnoreCase)
let private castRegex =
    Regex(
        @"^\s*cast\s*\(\s*(?<inner>.+)\s+as\s+(?<type>[A-Za-z0-9_]+)\s*\)\s*$",
        RegexOptions.Compiled ||| RegexOptions.IgnoreCase ||| RegexOptions.Singleline)
let private numericalExpressionRegex =
    Regex(@"^\s*(?<function>DECIMAL|BIGINTEGER|CHARACTER_VALUE)\s*\((?<arguments>.+)\)\s*$", RegexOptions.Compiled ||| RegexOptions.IgnoreCase ||| RegexOptions.Singleline)
let private jsonTextRegex =
    Regex(@"^\s*(?:""Value""|Value)\s*->\s*" + pathLiteralPattern + @"\s*$", RegexOptions.Compiled ||| RegexOptions.IgnoreCase)

let private stringLiteralRegex = Regex(@"^'(?<text>(?:[^']|'')*)'$", RegexOptions.Compiled ||| RegexOptions.Singleline)
let private jsonFunctionRegex = Regex(@"^(?<name>json|jsonb|jsonb_array|jsonb_extract)\s*\((?<arguments>.*)\)$", RegexOptions.Compiled ||| RegexOptions.IgnoreCase ||| RegexOptions.Singleline)
let private producedJsonTextRegex = Regex(@"^(?<source>.+)\s*->\s*" + pathLiteralPattern + @"$", RegexOptions.Compiled ||| RegexOptions.Singleline)
let private presentHeaderRegex = Regex(@"^(?<header>.+)\s+IS\s+NOT\s+NULL$", RegexOptions.Compiled ||| RegexOptions.IgnoreCase ||| RegexOptions.Singleline)

/// Retain the emitted JSON access and writer-header grammar, including full literals.
/// Parameters, other columns, executable functions and subqueries are not index facts.
let rec private tryNormalizeJsonOperand (text: string) : SqlExpr option =
    let text = stripOuterParens text
    let literal = stringLiteralRegex.Match text
    if literal.Success then Some(Literal(SqlLiteral.String(literal.Groups.["text"].Value.Replace("''", "'"))))
    elif text.Equals("NULL", StringComparison.OrdinalIgnoreCase) then Some(Literal SqlLiteral.Null)
    elif text = "Value" || text = "\"Value\"" then Some(Column(None, "Value"))
    else
        match Int64.TryParse(text, Globalization.NumberStyles.Integer, Globalization.CultureInfo.InvariantCulture) with
        | true, value -> Some(Literal(SqlLiteral.Integer value))
        | _ ->
            let header = presentHeaderRegex.Match text
            let access = producedJsonTextRegex.Match text
            let call = jsonFunctionRegex.Match text
            if header.Success then
                tryNormalizeJsonOperand header.Groups.["header"].Value
                |> Option.map (fun value -> Unary(UnaryOperator.IsNotNull, value))
            elif access.Success then
                let path = access.Groups.["path"].Value.Replace("''", "'")
                tryNormalizeJsonOperand access.Groups.["source"].Value
                |> Option.map (fun source -> Binary(source, BinaryOperator.JsonText, Literal(SqlLiteral.String path)))
            elif call.Success then
                let name = call.Groups.["name"].Value.ToLowerInvariant()
                let arguments = splitTopLevelCommaTerms call.Groups.["arguments"].Value |> List.map tryNormalizeJsonOperand
                if arguments |> List.exists Option.isNone then None else
                let arguments = arguments |> List.choose id
                match name, arguments with
                | ("json" | "jsonb"), [_] | "jsonb_array", _ -> Some(FunctionCall(name, arguments))
                | "jsonb_extract", [_; Literal(SqlLiteral.String path)] when path.StartsWith("$", StringComparison.Ordinal) ->
                    Some(FunctionCall(name, arguments))
                | _ -> None
            else None

let private unquoteIdentifier (identifier: string) : string =
    let t = identifier.Trim()
    if t.Length >= 2 && ((t.[0] = '"' && t.[t.Length - 1] = '"') || (t.[0] = '`' && t.[t.Length - 1] = '`') || (t.[0] = '[' && t.[t.Length - 1] = ']')) then
        t.Substring(1, t.Length - 2)
    else t

let tryNormalizeSingleKeyColumnExpr (columnName: string) : SqlExpr option =
    let normalized = unquoteIdentifier columnName
    if columnIdentifierRegex.IsMatch normalized then
        Some(Column(None, normalized))
    else
        None

let private tryNormalizeJsonExtractExpr (expr: string) : SqlExpr option =
    let m = jsonExtractRegex.Match(stripOuterParens expr)
    if not m.Success then None
    else
        // Keep the produced literal opaque; quoted member dots are not path separators.
        let path = m.Groups.["path"].Value.Replace("''", "'")
        Some(FunctionCall("jsonb_extract", [Column(None, "Value"); Literal(SqlLiteral.String path)]))

let rec private tryNormalizeStoredExpression (expr: string) : SqlExpr option =
    let expr = stripOuterParens expr
    match tryNormalizeJsonExtractExpr expr with
    | Some value -> Some value
    | None ->
        let call = jsonFunctionRegex.Match expr
        if call.Success && call.Groups.["name"].Value.Equals("jsonb_extract", StringComparison.OrdinalIgnoreCase) then
            match splitTopLevelCommaTerms call.Groups.["arguments"].Value with
            | [source; path] ->
                let path = stringLiteralRegex.Match(path.Trim())
                if not path.Success then None else
                let path = path.Groups.["text"].Value.Replace("''", "'")
                if not(path.StartsWith("$", StringComparison.Ordinal)) then None else
                let input =
                    match tryNormalizeJsonOperand source with
                    | Some value when SqlExpr.exists (function Column(None, "Value") -> true | _ -> false) value -> Some value
                    | _ when path = "$" ->
                        match tryNormalizeNumericalExpr (stripOuterParens source) with
                        | Some(FunctionCall(_, Literal(SqlLiteral.Integer operation) :: _) as value)
                            when operation = int64(int ExactNumericOperation.NativeJsonValue)
                                 || operation = int64(int ExactNumericOperation.WriterValue) -> Some value
                        | _ -> None
                    | _ -> None
                input |> Option.map (fun value -> FunctionCall("jsonb_extract", [value; Literal(SqlLiteral.String path)]))
            | _ -> None
        else tryNormalizeCastOrNumericalExpr expr

and private tryNormalizeCastOrNumericalExpr (expr: string) : SqlExpr option =
    let m = castRegex.Match(stripOuterParens expr)
    if not m.Success then tryNormalizeNumericalExpr expr
    else
        match tryNormalizeStoredExpression (m.Groups.["inner"].Value) with
        | Some inner ->
            let sqlType = m.Groups.["type"].Value.ToUpperInvariant()
            Some(Cast(inner, sqlType))
        | None -> None

and private tryNormalizeNumericalExpr (expr: string) : SqlExpr option =
    let m = numericalExpressionRegex.Match expr
    if not m.Success then None else
    match splitTopLevelCommaTerms m.Groups.["arguments"].Value with
    | [operation; operand; first; second] when m.Groups.["function"].Value.Equals("CHARACTER_VALUE",StringComparison.OrdinalIgnoreCase) &&
         operation.Trim() = "7" && first.Trim().Equals("NULL",StringComparison.OrdinalIgnoreCase) && second.Trim().Equals("NULL",StringComparison.OrdinalIgnoreCase) ->
        tryNormalizeJsonExtractExpr operand |> Option.map (fun operand ->
            FunctionCall("CHARACTER_VALUE",[Literal(SqlLiteral.Integer 7L);operand;Literal SqlLiteral.Null;Literal SqlLiteral.Null]))
    | operation :: target :: operand :: descriptors when descriptors.Length <= 1 &&
        not(m.Groups.["function"].Value.Equals("CHARACTER_VALUE",StringComparison.OrdinalIgnoreCase)) ->
        let integer (text: string) =
            match Int32.TryParse(text.Trim(), Globalization.NumberStyles.Integer, Globalization.CultureInfo.InvariantCulture) with
            | true, value -> Some value
            | _ -> None
        match integer operation, integer target with
        | Some operation, Some target ->
            let target = enum<ExactNumericOperation> target
            let stored = operation = int ExactNumericOperation.StoredValue
            let nativeValue = operation = int ExactNumericOperation.NativeValue
            let nativeJson = operation = int ExactNumericOperation.NativeJsonValue
            let writer = operation = int ExactNumericOperation.WriterValue
            let functionName = m.Groups.["function"].Value.ToUpperInvariant()
            let native =
                match target with
                | ExactNumericOperation.ToDouble | ExactNumericOperation.ToSingle
                | ExactNumericOperation.ToInt64 | ExactNumericOperation.ToInt32
                | ExactNumericOperation.ToInt16 | ExactNumericOperation.ToSByte
                | ExactNumericOperation.ToUInt64 | ExactNumericOperation.ToUInt32
                | ExactNumericOperation.ToUInt16 | ExactNumericOperation.ToByte -> true
                | _ -> false
            let text = jsonTextRegex.Match(stripOuterParens operand)
            let input =
                if (stored || target = ExactNumericOperation.ToUInt64) && text.Success then
                    let path = text.Groups.["path"].Value.Replace("''", "'")
                    Some(Binary(Column(None,"Value"), BinaryOperator.JsonText, Literal(SqlLiteral.String path)))
                elif nativeValue && target <> ExactNumericOperation.ToUInt64 then tryNormalizeJsonExtractExpr operand
                elif nativeJson || writer then
                    match tryNormalizeJsonOperand operand with
                    | Some(Binary(_, BinaryOperator.JsonText, Literal(SqlLiteral.String _)) as value) -> Some value
                    | _ -> None
                else None
            let admitted =
                if stored || nativeJson || writer then (target = ExactNumericOperation.Cast || native && functionName = "DECIMAL")
                else nativeValue && native && functionName = "DECIMAL"
            if not admitted then None else
            let descriptor =
                match writer, descriptors with
                | false, [] -> Some []
                | true, [descriptor] ->
                    match tryNormalizeJsonOperand descriptor with
                    | Some(FunctionCall("json", [value]) as descriptor) ->
                        match value with
                        | FunctionCall("jsonb_array", _) -> Some [descriptor]
                        | _ -> None
                    | _ -> None
                | _ -> None
            match input, descriptor with
            | Some input, Some descriptor -> Some(
                FunctionCall(functionName,
                    [Literal(SqlLiteral.Integer(int64 operation)); Literal(SqlLiteral.Integer(int64(int target))); input] @ descriptor))
            | _ -> None
        | _ -> None
    | _ -> None

let private termDirectionRegex = Regex(@"\s+(?:ASC|DESC)\s*$", RegexOptions.IgnoreCase)
let private termCollationRegex =
    Regex(@"\s+COLLATE\s+(?:""[^""]+""|[A-Za-z_][A-Za-z0-9_]*)\s*$", RegexOptions.IgnoreCase)

let tryNormalizeIndexExpression (term: string) : SqlExpr option =
    let expression = termCollationRegex.Replace(termDirectionRegex.Replace(term, ""), "")
    tryNormalizeStoredExpression expression
