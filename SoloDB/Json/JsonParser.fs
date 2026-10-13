namespace SoloDatabase.JsonSerializator

open System
open System.Collections.Generic
open System.Globalization
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Text

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

/// Output policies share the same structural grammar without boxing encoding or output state.
type internal IJsonParserSink<'Value,'Object,'Array,'R when 'R : struct and 'R :> IJsonReader> =
    abstract StringValues: bool
    abstract NeedsNames: 'Object -> bool
    abstract ObjectStart: int -> 'Object
    abstract ObjectSet: 'Object * string * 'Value -> unit
    abstract ObjectEnd: int * int * 'Object -> 'Value
    abstract ArrayStart: int -> 'Array
    abstract ArrayAdd: 'Array * 'Value -> unit
    abstract ArrayEnd: int * int * 'Array -> 'Value
    abstract Scalar: int * Token * byref<ParserContext<'R>> -> 'Value

[<Struct>]
type internal JsonSourceRange = { Start: int; Length: int }

module internal JsonGrammar =
    open JsonLexer

    let inline private asciiAt<'R when 'R : struct and 'R :> IJsonReader> (ctx: byref<ParserContext<'R>>) index =
        ctx.reader.TryPeekAsciiByte(ctx.input,index)

    let inline private digitAt<'R when 'R : struct and 'R :> IJsonReader> (ctx: byref<ParserContext<'R>>) index =
        match asciiAt &ctx index with ValueSome b -> NumberParsing.isDigitByte b | ValueNone -> false

    let validateNumber<'R when 'R : struct and 'R :> IJsonReader> (ctx: byref<ParserContext<'R>>) literal =
        match literal with
        | InfinityLiteral _ | NaNLiteral | HexLiteral _ -> ()
        | DecimalLiteral(start,length) ->
            let finish = start + length
            let mutable i = start
            if asciiAt &ctx i = ValueSome '+'B || asciiAt &ctx i = ValueSome '-'B then i <- i + 1
            let first = i
            while i < finish && digitAt &ctx i do i <- i + 1
            let mutable digits = i - first
            if i < finish && asciiAt &ctx i = ValueSome '.'B then
                i <- i + 1
                let first = i
                while i < finish && digitAt &ctx i do i <- i + 1
                digits <- digits + i - first
            if digits = 0 then raise (FormatException "Invalid number")
            if i < finish && (asciiAt &ctx i = ValueSome 'e'B || asciiAt &ctx i = ValueSome 'E'B) then
                i <- i + 1
                if i < finish && (asciiAt &ctx i = ValueSome '+'B || asciiAt &ctx i = ValueSome '-'B) then i <- i + 1
                let first = i
                while i < finish && digitAt &ctx i do i <- i + 1
                if first = i then raise (FormatException "Invalid number")
            if i <> finish then raise (FormatException "Invalid number")

    let treeNumber<'R when 'R : struct and 'R :> IJsonReader> (ctx: byref<ParserContext<'R>>) literal =
        match literal with
        | InfinityLiteral negative -> if negative then Decimal.MinValue else Decimal.MaxValue
        | NaNLiteral -> Decimal.Zero
        | HexLiteral(magnitude,negative,overflow) ->
            let value = if overflow then Decimal.MaxValue else decimal magnitude
            if negative then -value else value
        | DecimalLiteral(startIndex,length) ->
            try ctx.reader.ParseNumber(ctx.input,startIndex,length)
            with :? OverflowException ->
                // Overflow can precede an invalid exponent; only valid numbers saturate.
                validateNumber &ctx literal
                if ctx.reader.TryPeekAsciiByte(ctx.input,startIndex) = ValueSome '-'B then Decimal.MinValue else Decimal.MaxValue

    let propertyTokenName token =
        match token with
        | StringToken value -> value
        | NullToken -> "null"
        | BooleanToken true -> "true"
        | BooleanToken false -> "false"
        | _ -> failwith "Invalid object syntax"

    let completePropertyName<'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'R>>) =
        match readNext &ctx with Colon -> () | _ -> failwith "Malformed json."

    let propertyName<'R when 'R : struct and 'R :> IJsonReader> (ctx: byref<ParserContext<'R>>) token =
        let name = propertyTokenName token
        completePropertyName &ctx
        name

    // Shared header admission stops before the name; conversion owns the following colon.
    let beginPropertyName<'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'R>>) first =
        let mutable ended = false
        if not first then
            match readNext &ctx with
            | CloseBrace -> ended <- true
            | Comma -> ()
            | _ -> failwith "Malformed json."
        if ended then -2
        else
            let point = nextSignificantCodePoint &ctx
            if point = int '}' then ctx.codeUnitIndex <- ctx.codeUnitIndex + 1; -2
            else point

    // The caller has consumed the opening delimiter and each preceding value.
    let nextProperty<'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'R>>) first decodeNames =
        ctx.decodeStrings <- decodeNames
        if beginPropertyName &ctx first = -2 then ValueNone
        else ValueSome(propertyName &ctx (readNext &ctx))

    let nextElement<'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'R>>) first =
        ctx.decodeStrings <- false
        let ended =
            if first then false
            else
                match readNext &ctx with
                | CloseBracket -> true
                | Comma -> false
                | _ -> failwith "Malformed json."
        if ended then false
        else
            if nextSignificantCodePoint &ctx = int ']' then
                ctx.codeUnitIndex <- ctx.codeUnitIndex + 1
                false
            else true

    let rec private members<'V,'O,'A,'R,'S when 'R : struct and 'R :> IJsonReader and 'S : struct and 'S :> IJsonParserSink<'V,'O,'A,'R>>
        (sink: byref<'S>) (ctx: byref<ParserContext<'R>>) (start: int) (values: 'O) : 'V =
        let mutable first = true
        let mutable finished = false
        while not finished do
            match nextProperty &ctx first (sink.NeedsNames values) with
            | ValueNone -> finished <- true
            | ValueSome key ->
                let value = parse<'V,'O,'A,'R,'S> &sink &ctx
                sink.ObjectSet(values,key,value)
                first <- false
        sink.ObjectEnd(start,ctx.codeUnitIndex,values)

    and private elements<'V,'O,'A,'R,'S when 'R : struct and 'R :> IJsonReader and 'S : struct and 'S :> IJsonParserSink<'V,'O,'A,'R>>
        (sink: byref<'S>) (ctx: byref<ParserContext<'R>>) (start: int) (values: 'A) : 'V =
        let mutable first = true
        while nextElement &ctx first do
            let elementStart = ctx.codeUnitIndex
            ctx.decodeStrings <- sink.StringValues
            let token = readNext &ctx
            let value = fromToken<'V,'O,'A,'R,'S> &sink &ctx elementStart token true
            sink.ArrayAdd(values,value)
            first <- false
        sink.ArrayEnd(start,ctx.codeUnitIndex,values)

    and internal fromToken<'V,'O,'A,'R,'S when 'R : struct and 'R :> IJsonReader and 'S : struct and 'S :> IJsonParserSink<'V,'O,'A,'R>>
        (sink: byref<'S>) (ctx: byref<ParserContext<'R>>) (start: int) (token: Token) (arrayElement: bool) : 'V =
        match token with
        | OpenBrace ->
            let values: 'O = sink.ObjectStart start
            members<'V,'O,'A,'R,'S> &sink &ctx start values
        | OpenBracket ->
            let values: 'A = sink.ArrayStart start
            elements<'V,'O,'A,'R,'S> &sink &ctx start values
        | StringToken _ | NumberToken _ | BooleanToken _ | NullToken -> (sink.Scalar(start,token,&ctx) : 'V)
        | CloseBrace | CloseBracket | Comma | Colon | EndOfInput ->
            if arrayElement then failwithf "Invalid list token: %A" token
            else failwithf "Could not parse the token at index = %i." ctx.codeUnitIndex

    and parse<'V,'O,'A,'R,'S when 'R : struct and 'R :> IJsonReader and 'S : struct and 'S :> IJsonParserSink<'V,'O,'A,'R>>
        (sink: byref<'S>) (ctx: byref<ParserContext<'R>>) : 'V =
        let start = ctx.codeUnitIndex
        ctx.decodeStrings <- sink.StringValues
        let token = readNext &ctx
        fromToken<'V,'O,'A,'R,'S> &sink &ctx start token false

[<Struct>]
type private JsonTreeSink<'T,'R when 'R : struct and 'R :> IJsonReader> =
    val api: JsonValueParserApi<'T>
    new(api) = { api = api }
    interface IJsonParserSink<'T,IDictionary<string,'T>,List<'T>,'R> with
        member _.StringValues = true
        member _.NeedsNames _ = true
        member _.ObjectStart _ = JsonHelper.newDefaultDict<'T>()
        member _.ObjectSet(values,key,value) = values[key] <- value
        member this.ObjectEnd(_,_,values) = this.api.Obj values
        member _.ArrayStart _ = List<'T>()
        member _.ArrayAdd(values,value) = values.Add value
        member this.ArrayEnd(_,_,values) = this.api.Arr values
        member this.Scalar(_,token,ctx) =
            match token with
            | StringToken value -> this.api.Str value
            | NumberToken value -> this.api.Num(JsonGrammar.treeNumber &ctx value)
            | BooleanToken value -> this.api.Bool value
            | NullToken -> this.api.Null()
            | OpenBrace | CloseBrace | OpenBracket | CloseBracket | Comma | Colon | EndOfInput -> invalidOp "Expected a scalar JSON token."

[<Struct>]
type internal JsonRangeSink<'R when 'R : struct and 'R :> IJsonReader> =
    val root: int
    val mutable properties: Dictionary<string,JsonSourceRange>
    val mutable elements: List<JsonSourceRange>
    new(root) = { root = root; properties = null; elements = null }
    interface IJsonParserSink<JsonSourceRange,Dictionary<string,JsonSourceRange>,List<JsonSourceRange>,'R> with
        member _.StringValues = false
        member _.NeedsNames values = not(isNull values)
        member this.ObjectStart start =
            if start = this.root then
                let values = Dictionary<string,JsonSourceRange>(StringComparer.Ordinal)
                this.properties <- values
                values
            else null
        member _.ObjectSet(values,key,value) = if not(isNull values) then values[key] <- value
        member _.ObjectEnd(start,finish,_) = { Start = start; Length = finish - start }
        member this.ArrayStart start =
            if start = this.root then
                let values = List<JsonSourceRange>()
                this.elements <- values
                values
            else null
        member _.ArrayAdd(values,value) = if not(isNull values) then values.Add value
        member _.ArrayEnd(start,finish,_) = { Start = start; Length = finish - start }
        member _.Scalar(start,token,ctx) =
            match token with
            | NumberToken number -> JsonGrammar.validateNumber &ctx number
            | StringToken _ | BooleanToken _ | NullToken -> ()
            | OpenBrace | CloseBrace | OpenBracket | CloseBracket | Comma | Colon | EndOfInput -> invalidOp "Expected a scalar JSON token."
            { Start = start; Length = ctx.codeUnitIndex - start }

module internal JsonParser =
    let parse<'T,'R when 'R : struct and 'R :> IJsonReader> (api: JsonValueParserApi<'T>) (ctx: byref<ParserContext<'R>>) =
        let mutable sink = JsonTreeSink<'T,'R>(api)
        JsonGrammar.parse<'T,IDictionary<string,'T>,List<'T>,'R,JsonTreeSink<'T,'R>> &sink &ctx

[<Struct>]
type internal JsonDiscardSink<'R when 'R : struct and 'R :> IJsonReader> =
    interface IJsonParserSink<int,int,int,'R> with
        member _.StringValues = false
        member _.NeedsNames _ = false
        member _.ObjectStart _ = 0
        member _.ObjectSet(_,_,_) = ()
        member _.ObjectEnd(_,_,_) = 0
        member _.ArrayStart _ = 0
        member _.ArrayAdd(_,_) = ()
        member _.ArrayEnd(_,_,_) = 0
        member _.Scalar(_,token,ctx) =
            match token with
            | NumberToken literal -> JsonGrammar.validateNumber &ctx literal
            | StringToken _ | BooleanToken _ | NullToken -> ()
            | OpenBrace | CloseBrace | OpenBracket | CloseBracket | Comma | Colon | EndOfInput -> invalidOp "Expected a scalar JSON token."
            0
