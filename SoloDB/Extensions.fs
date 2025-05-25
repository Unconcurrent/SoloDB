namespace SoloDatabase

open System
open System.Collections.Generic
open System.Collections
open System.Text.RegularExpressions
open System.Runtime.CompilerServices
open System.Linq.Expressions
open FSharp.Interop.Dynamic

[<Extension; AbstractClass; Sealed>]
type Extensions =
    [<Extension>]
    static member Like(this: string, pattern: string) =
        let regexPattern = 
            "^" + Regex.Escape(pattern).Replace("\\%", ".*").Replace("\\_", ".") + "$"
        Regex.IsMatch(this, regexPattern, RegexOptions.IgnoreCase)

    [<Extension>]
    static member Any<'T>(this: ICollection<'T>, condition: Expression<Func<'T, bool>>) =
        let f = condition.Compile true
        this |> Seq.exists f.Invoke

    [<Extension>]
    static member Dyn<'T>(this: obj, property: string) : 'T = // LINQ Expressions do not support C# dynamic.
        this |> Dyn.get<'T> property

    [<Extension>]
    static member Dyn(this: obj, property: string) : obj =
        this |> Dyn.get property

    [<Extension>]
    static member CastTo<'T>(this: obj) : 'T =
        this :?> 'T