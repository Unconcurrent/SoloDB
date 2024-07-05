namespace SoloDatabase

open System
open System.Collections.Generic
open System.Text.RegularExpressions
open SoloDatabase.Types
open System.Runtime.CompilerServices
open System.Linq.Expressions

[<Extension>]
type Extensions =
    [<Extension>]
    static member Set<'T>(this: 'T, value: 'T) : unit =
        failwithf "This is a dummy function for the SQL builder."

    [<Extension>]
    static member Like(this: string, pattern: string) =
        let regexPattern = 
            "^" + Regex.Escape(pattern).Replace("\\%", ".*").Replace("\\_", ".") + "$"
        Regex.IsMatch(this.ToString(), regexPattern, RegexOptions.IgnoreCase)

    [<Extension>]
    static member AddToEnd<'T>(this: IEnumerable<'T>, value: 'T) : unit = // For arrays
        failwithf "This is a dummy function for the SQL builder."

    [<Extension>]
    static member SetAt<'T>(this: ICollection<'T>, index: int, value: 'T) : unit =
        failwithf "This is a dummy function for the SQL builder."

    [<Extension>]
    static member RemoveAt<'T>(this: ICollection<'T>, index: int) : unit =
        failwithf "This is a dummy function for the SQL builder."

    [<Extension>]
    static member AnyInEach<'T>(this: ICollection<'T>, condition: Expression<Func<'T, bool>>) = 
        failwithf "This is a dummy function for the SQL builder."
        bool()