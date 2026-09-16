namespace SoloDatabase.JsonSerializator

open System
open SoloDatabase.Utils

module internal JsonEqualitySupport =
    [<Struct>]
    type PathSegment =
        | Member of name: string
        | Index of index: int

    // JSON numbers and comparison parameters share SQLite's INTEGER/REAL boundary.
    let numberToSQLValue (number: decimal) : obj =
        if Decimal.IsInteger number && number >= decimal Int64.MinValue && number <= decimal Int64.MaxValue then
            box (int64 number)
        else box (float number)

    let appendMember (path: string) (key: string) =
        if key.Length = 0 || key |> Seq.exists (fun c -> not (Char.IsLetterOrDigit c || c = '_' || c = '$')) then
            let escaped = key.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\000", "\\u0000")
            sprintf "%s.\"%s\"" path escaped
        else sprintf "%s.%s" path key

    // Traversal prepends segments; rendering applies them from the root outwards.
    let renderPath segments =
        List.foldBack (fun segment path ->
            match segment with
            | Member key -> appendMember path key
            | Index index -> sprintf "%s[%d]" path index) segments "$"
