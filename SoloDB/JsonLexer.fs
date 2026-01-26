namespace SoloDatabase.JsonSerializator

open System
open System.Collections.Generic
open System.Globalization
open System.Runtime.CompilerServices
open System.Text

/// <summary>
/// Represents the different types of tokens that can be found in a JSON string.
/// </summary>
type internal Token =
    /// <summary>Represents a JSON null literal.</summary>
    | NullToken
    /// <summary>Represents an opening brace '{'.</summary>
    | OpenBrace
    /// <summary>Represents a closing brace '}'.</summary>
    | CloseBrace
    /// <summary>Represents an opening bracket '['.</summary>
    | OpenBracket
    /// <summary>Represents a closing bracket ']'.</summary>
    | CloseBracket
    /// <summary>Represents a comma ','.</summary>
    | Comma
    /// <summary>Represents a colon ':'.</summary>
    | Colon
    /// <summary>Represents a string literal.</summary>
    | StringToken of string
    /// <summary>Represents a numeric value.</summary>
    | NumberToken of decimal
    /// <summary>Represents a boolean literal.</summary>
    | BooleanToken of bool
    /// <summary>Represents the end of the input string.</summary>
    | EndOfInput

/// The "constructor surface" the tokenizer needs from whatever JSON value representation you use.
type internal JsonValueParserApi<'T> =
    {
        Null   : unit -> 'T
        Bool   : bool -> 'T
        Str    : string -> 'T
        Num    : decimal -> 'T
        Arr    : IList<'T> -> 'T
        Obj    : IDictionary<string, 'T> -> 'T
    }

/// Tokenizer state + the value factory API; no dependency on JsonValue anymore.
[<Struct; IsByRefLike>]
type internal ParserContext<'T> =
    {
        input : string
        mutable index : int
        sb : StringBuilder
        api : JsonValueParserApi<'T>
    }

type internal ParserContextFactoryStr<'T> = delegate of string -> ParserContext<'T>

module internal JsonParser =

    let inline fromString (api: JsonValueParserApi<'T>) : ParserContextFactoryStr<'T> =
        ParserContextFactoryStr<'T>(fun (source: string) ->
            { input = source; index = 0; sb = StringBuilder(); api = api })

    let rec private readNext (ctx: byref<ParserContext<'T>>) : Token =
        let inline isDigitOrMinus c = Char.IsDigit c || c = '-'
        let inline isInitialIdentifierChar c = Char.IsLetter c || c = '_'
        let inline isIdentifierChar c = isInitialIdentifierChar c || Char.IsDigit c

        RuntimeHelpers.EnsureSufficientExecutionStack()

        let input = ctx.input

        if ctx.index < input.Length then
            let currentChar = input.[ctx.index]
            match currentChar with
            | '{' ->
                ctx.index <- ctx.index + 1
                OpenBrace
            | '}' ->
                ctx.index <- ctx.index + 1
                CloseBrace
            | '[' ->
                ctx.index <- ctx.index + 1
                OpenBracket
            | ']' ->
                ctx.index <- ctx.index + 1
                CloseBracket
            | ',' ->
                ctx.index <- ctx.index + 1
                Comma
            | ':' ->
                ctx.index <- ctx.index + 1
                Colon
            | '"' | '\'' as quote ->
                let sb = ctx.sb.Clear()
                let mutable i = ctx.index + 1
                while i < input.Length && input.[i] <> quote do
                    if input.[i] = '\\' then
                        i <- i + 1
                        match input.[i] with
                        | '\\' -> sb.Append('\\') |> ignore
                        | '\"' -> sb.Append('"') |> ignore
                        | '\'' -> sb.Append('\'') |> ignore
                        | '/'  -> sb.Append('/') |> ignore
                        | 'b'  -> sb.Append('\b') |> ignore
                        | 'f'  -> sb.Append('\f') |> ignore
                        | 'n'  -> sb.Append('\n') |> ignore
                        | 'r'  -> sb.Append('\r') |> ignore
                        | 't'  -> sb.Append('\t') |> ignore
                        | 'u'  ->
                            if i + 4 < input.Length then
                                let code1 = JsonHelper.parseHex (input.AsSpan(i + 1, 4))
                                let ch1 = char code1
                                i <- i + 4

                                if Char.IsHighSurrogate(ch1) then
                                    if i + 6 < input.Length && input.[i + 1] = '\\' && input.[i + 2] = 'u' then
                                        let code2 = JsonHelper.parseHex (input.AsSpan(i + 3, 4))
                                        let ch2 = char code2
                                        if Char.IsLowSurrogate(ch2) then
                                            sb.Append(ch1) |> ignore
                                            sb.Append(ch2) |> ignore
                                            i <- i + 6
                                        else
                                            failwith "Invalid low surrogate"
                                    else
                                        failwith "Expected low surrogate after high surrogate"
                                else
                                    sb.Append(ch1) |> ignore
                            else
                                failwith "Invalid Unicode escape sequence"
                        | other -> failwithf "Invalid escape sequence: '%c'" other
                    else
                        sb.Append(input.[i]) |> ignore
                    i <- i + 1

                if i >= input.Length then failwith "Unterminated string"
                ctx.index <- i + 1
                StringToken(sb.ToString())

            | c when isDigitOrMinus c ->
                let mutable i = ctx.index
                let mutable length = 0

                while i < input.Length &&
                      (isDigitOrMinus input.[i] ||
                       input.[i] = '.' ||
                       input.[i] = 'e' ||
                       input.[i] = 'E' ||
                       input.[i] = '+') do
                    length <- length + 1
                    i <- i + 1

                #if NETSTANDARD2_1
                let s = input.AsSpan(ctx.index, length)
                #else
                let s = input.Substring(ctx.index, length)
                #endif

                ctx.index <- i

                NumberToken(
                    try Decimal.Parse(s, NumberStyles.Any, CultureInfo.InvariantCulture)
                    with :? OverflowException ->
                        if s.[0] = '-' then Decimal.MinValue else Decimal.MaxValue
                )

            | c when isInitialIdentifierChar c ->
                let inline fallback (input: string) (index: int) (sb: StringBuilder) =
                    let sb = sb.Clear()
                    let mutable i = index
                    while i < input.Length && isIdentifierChar input.[i] do
                        sb.Append(input.[i]) |> ignore
                        i <- i + 1

                    let txt = sb.ToString()

                    struct (
                        match txt with
                        | "null" | "NULL" -> NullToken
                        | "true" -> BooleanToken true
                        | "false" -> BooleanToken false
                        // Non-standard: treat as bare identifier string token
                        | other -> StringToken other
                        , i
                    )

                if ctx.index + 4 <= input.Length then
                    match input.AsSpan(ctx.index, 4) with
                    | x when x.Equals("null".AsSpan(), StringComparison.Ordinal)
                             && (ctx.index + 4 = input.Length || (not << isIdentifierChar) input.[ctx.index + 4]) ->
                        ctx.index <- ctx.index + 4
                        NullToken
                    | x when x.Equals("NULL".AsSpan(), StringComparison.Ordinal)
                             && (ctx.index + 4 = input.Length || (not << isIdentifierChar) input.[ctx.index + 4]) ->
                        ctx.index <- ctx.index + 4
                        NullToken
                    | x when x.Equals("true".AsSpan(), StringComparison.Ordinal)
                             && (ctx.index + 4 = input.Length || (not << isIdentifierChar) input.[ctx.index + 4]) ->
                        ctx.index <- ctx.index + 4
                        BooleanToken true
                    | x when x.Equals("fals".AsSpan(), StringComparison.Ordinal)
                             && ctx.index + 4 < input.Length
                             && input.[ctx.index + 4] = 'e'
                             && (ctx.index + 5 = input.Length || (not << isIdentifierChar) input.[ctx.index + 5]) ->
                        ctx.index <- ctx.index + 5
                        BooleanToken false
                    | _ ->
                        match fallback input ctx.index ctx.sb with
                        | struct (tok, i) -> ctx.index <- i; tok
                else
                    match fallback input ctx.index ctx.sb with
                    | struct (tok, i) -> ctx.index <- i; tok

            | c when Char.IsWhiteSpace c ->
                ctx.index <- ctx.index + 1
                readNext &ctx

            | c ->
                failwithf "Unexpected JSON character: %c, index = %i" c ctx.index
        else
            EndOfInput

    let inline peek (ctx: byref<ParserContext<'T>>) : Token =
        let savedIndex = ctx.index
        try readNext &ctx
        finally ctx.index <- savedIndex

    let rec private parseMembers (ctx: byref<ParserContext<'T>>) (dict: Dictionary<string, 'T>) =
        match readNext &ctx with
        | CloseBrace -> dict :> IDictionary<string, 'T>
        | StringToken key ->
            let colon = readNext &ctx
            if colon <> Colon then failwith "Malformed json."

            let value = parse &ctx
            dict.[key] <- value
            parseMembers &ctx dict
        | Comma ->
            parseMembers &ctx dict
        | _ ->
            failwith "Invalid object syntax"

    and private parseObject (ctx: byref<ParserContext<'T>>) : IDictionary<string, 'T> =
        let dict = Dictionary<string, 'T>()
        parseMembers &ctx dict

    and private parseElements (ctx: byref<ParserContext<'T>>) (items: List<'T>) : IList<'T> =
        let itemOpt =
            parseTokensOr &ctx (fun t ->
                match t with
                | CloseBracket -> None
                | other -> failwithf "Invalid list token: %A" other
            )

        match itemOpt with
        | None -> items :> IList<'T>
        | Some item ->
            items.Add item
            match readNext &ctx with
            | CloseBracket -> items :> IList<'T>
            | Comma -> parseElements &ctx items
            | _ -> failwith "Malformed json."

    and private parseArray (ctx: byref<ParserContext<'T>>) : IList<'T> =
        let items = List<'T>()
        parseElements &ctx items

    and private parseTokensOr (ctx: byref<ParserContext<'T>>) (fallback: Token -> 'T option) : 'T option =
        match readNext &ctx with
        | OpenBrace ->
            let obj = parseObject &ctx
            ctx.api.Obj obj |> Some
        | OpenBracket ->
            let arr = parseArray &ctx
            ctx.api.Arr arr |> Some
        | StringToken str ->
            ctx.api.Str str |> Some
        | NumberToken num ->
            ctx.api.Num num |> Some
        | BooleanToken b ->
            ctx.api.Bool b |> Some
        | NullToken ->
            ctx.api.Null() |> Some
        | other ->
            fallback other

    and private parseTokens (ctx: byref<ParserContext<'T>>) : 'T =
        match parseTokensOr &ctx (fun _ -> None) with
        | Some x -> x
        | None -> failwithf "Could not parse the token at index = %i." ctx.index

    and parse (ctx: byref<ParserContext<'T>>) : 'T =
        parseTokens &ctx
