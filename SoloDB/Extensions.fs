namespace SoloDatabase

open System
open System.Collections.Generic
open System.Collections
open System.Text.RegularExpressions
open SoloDatabase.Types
open System.Runtime.CompilerServices
open System.Linq.Expressions
open FSharp.Interop.Dynamic

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
    static member AddToEnd(this: IEnumerable, value: obj) : unit = // For arrays
        failwithf "This is a dummy function for the SQL builder."

    [<Extension>]
    static member SetAt(this: ICollection, index: int, value: obj) : unit =
        failwithf "This is a dummy function for the SQL builder."

    [<Extension>]
    static member RemoveAt(this: ICollection, index: int) : unit =
        failwithf "This is a dummy function for the SQL builder."

    [<Extension>]
    static member AnyInEach<'T>(this: ICollection<'T>, condition: Expression<Func<'T, bool>>) = 
        failwithf "This is a dummy function for the SQL builder."
        bool()

    [<Extension>]
    static member Dyn<'T>(this: obj, property: string) : 'T = // LINQ Expressions do not support C# dynamic.
        this |> Dyn.get<'T> property

    [<Extension>]
    static member Dyn(this: obj, property: string) : obj =
        this |> Dyn.get property