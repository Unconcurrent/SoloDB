namespace SoloDatabase.JsonSerializator

/// Shared ASCII identifier facts for the lexer and concrete generated input operations.
module internal JsonIdentifierSyntax =
    [<Literal>]
    let Upper = 65
    [<Literal>]
    let Lower = 97
    [<Literal>]
    let LetterRange = 25
    [<Literal>]
    let Digit = 48
    [<Literal>]
    let DigitRange = 9
    [<Literal>]
    let Underscore = 95
    [<Literal>]
    let Dollar = 36

    let inline isStart point =
        uint32(point - Upper) <= uint32 LetterRange || uint32(point - Lower) <= uint32 LetterRange ||
        point = Underscore || point = Dollar

    [<Literal>]
    let NullWord = "null"
    [<Literal>]
    let NullUpperWord = "NULL"
    [<Literal>]
    let TrueWord = "true"
    [<Literal>]
    let FalseWord = "false"
    [<Literal>]
    let InfinityWord = "Infinity"
    [<Literal>]
    let NaNWord = "NaN"

    let classify text =
        match text with
        | NullWord | NullUpperWord -> 0
        | TrueWord -> 1
        | FalseWord -> 2
        | InfinityWord -> 3
        | NaNWord -> 4
        | _ -> -1

    // These generation facts share the lexer's exact keyword spellings and roles.
    let keywords() = [|NullWord,0;NullUpperWord,0;TrueWord,1;FalseWord,2;InfinityWord,3;NaNWord,4|]
