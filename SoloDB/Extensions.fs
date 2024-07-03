module SoloDBExtensions

open System
open System.Text.RegularExpressions
open SoloDBTypes


type System.Object with
    member this.Set(value: obj) =
        failwithf "This is a dummy function for the SQL builder."

    member this.Like(pattern: string) =
        let regexPattern = 
            "^" + Regex.Escape(pattern).Replace("\\%", ".*").Replace("\\_", ".") + "$"
        Regex.IsMatch(this.ToString(), regexPattern, RegexOptions.IgnoreCase)

    member this.Contains(pattern: string) =
        failwithf "This is a dummy function for the SQL builder."

type Array with
    member this.Add(value: obj) =
        failwithf "This is a dummy function for the SQL builder."

    member this.SetAt(index: int, value: obj) =
        failwithf "This is a dummy function for the SQL builder."

    member this.RemoveAt(index: int) =
        failwithf "This is a dummy function for the SQL builder."

    member this.AnyInEach(condition: InnerExpr) = 
        failwithf "This is a dummy function for the SQL builder."
        bool()

// This operator allow for multiple operations in the Update method,
// else it will throw 'Could not convert the following F# Quotation to a LINQ Expression Tree',
// imagine it as a ';'.
let (|+|) a b = ()