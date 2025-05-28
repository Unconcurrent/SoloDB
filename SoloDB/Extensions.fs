namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq
open System.Text.RegularExpressions
open System.Runtime.CompilerServices
open System.Linq.Expressions
open System.Reflection

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

    // LINQ Expressions do not support C# dynamic.

    /// <summary>
    /// Dynamically retrieves a property value from an object and casts it to the specified generic type.
    /// </summary>
    /// <param name="this">The source object.</param>
    /// <param name="property">The name of the property to retrieve.</param>
    /// <returns>The value of the property cast to type 'T.</returns>
    [<Extension>]
    static member Dyn<'T>(this: obj, propertyName: string) : 'T =
        let prop = this.GetType().GetProperty(propertyName, BindingFlags.Public ||| BindingFlags.Instance)
        if prop <> null && prop.CanRead then
            prop.GetValue(this) :?> 'T
        else
            let msg = sprintf "Property '%s' not found on type '%s' or is not readable." propertyName (this.GetType().FullName)
            raise (ArgumentException(msg))

    /// <summary>
    /// Retrieves a property value from an object using a PropertyInfo object and casts it to the specified generic type.
    /// </summary>
    /// <param name="this">The source object.</param>
    /// <param name="property">The PropertyInfo object representing the property to retrieve.</param>
    /// <returns>The value of the property cast to type 'T.</returns>
    [<Extension>]
    static member Dyn<'T>(this: obj, property: PropertyInfo) : 'T =
        try
            property.GetValue(this) :?> 'T
        with
        | :? InvalidCastException as ex ->
            let msg = sprintf "Cannot cast property '%s' value to type '%s'." property.Name (typeof<'T>.FullName)
            raise (InvalidCastException(msg, ex))

    /// <summary>
    /// Dynamically retrieves a property value from an object.
    /// </summary>
    /// <param name="this">The source object.</param>
    /// <param name="property">The name of the property to retrieve.</param>
    /// <returns>The value of the property as an obj.</returns>
    [<Extension>]
    static member Dyn(this: obj, propertyName: string) : obj =
        let prop = this.GetType().GetProperty(propertyName, BindingFlags.Public ||| BindingFlags.Instance)
        if prop <> null && prop.CanRead then
            prop.GetValue(this)
        else
            let msg = sprintf "Property '%s' not found on type '%s' or is not readable." propertyName (this.GetType().FullName)
            raise (ArgumentException(msg))


    [<Extension>]
    static member CastTo<'T>(this: obj) : 'T =
        this :?> 'T