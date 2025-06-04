namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq
open System.Text.RegularExpressions
open System.Runtime.CompilerServices
open System.Linq.Expressions
open System.Reflection

/// These extensions are supported by the Linq Expression to SQLite translator.
/// Linq Expressions do not support C# dynamic, therefore all the Dyn method are
/// made to simulate that. This class also contains SQL native methods: 
/// Like and Any that propagate into (LIKE) and (json_each() with a WHERE filter).
[<Extension; AbstractClass; Sealed>]
type Extensions =
    /// The SQL LIKE operator, to be used in queries.
    [<Extension>]
    static member Like(this: string, pattern: string) =
        let regexPattern = 
            "^" + Regex.Escape(pattern).Replace("\\%", ".*").Replace("\\_", ".") + "$"
        Regex.IsMatch(this, regexPattern, RegexOptions.IgnoreCase)

    /// <summary>
    /// This method will be translated directly into an EXISTS subquery with the `json_each` table-valued function.
    /// To be used inside queries with items that contain arrays or other supported collections.
    /// </summary>
    /// <remarks>
    /// Example: collection.Where(x => x.Numbers.Any(x => x > 10)).LongCount();
    /// </remarks>
    [<Extension>]
    static member Any<'T>(this: ICollection<'T>, condition: Expression<Func<'T, bool>>) =
        let f = condition.Compile true
        this |> Seq.exists f.Invoke

    /// <summary>
    /// Dynamically retrieves a property value from an object and casts it to the specified generic type, to be used in queries.
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
    /// Retrieves a property value from an object using a PropertyInfo object and casts it to the specified generic type, to be used in queries.
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
    /// Dynamically retrieves a property value from an object, to be used in queries.
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

    /// To be used in queries.
    [<Extension>]
    static member CastTo<'T>(this: obj) : 'T =
        this :?> 'T
