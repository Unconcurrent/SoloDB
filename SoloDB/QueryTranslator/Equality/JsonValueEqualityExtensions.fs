namespace SoloDatabase

open System.Runtime.CompilerServices
open SoloDatabase.JsonSerializator

/// Runtime comparison using the same directional matching rules as object query predicates.
[<Extension; AbstractClass; Sealed>]
type JsonValueEqualityExtensions =
    /// <summary>
    /// Tests whether this stored value matches the argument as an object query predicate would.
    /// Extra stored object fields are allowed; missing fields compare as null, and missing
    /// type discriminators are accepted. The argument uses query serialization, with JsonValue passthrough.
    /// Use Eq for symmetric equality of complete JSON structures.
    /// </summary>
    /// <remarks>
    /// Property names follow SQLite JSON lookup: ordinal comparison, terminating at the first NUL.
    /// Names sharing that prefix can alias; colliding names can prevent Eq from implying EqLoose.
    /// Fractional numbers and integers outside Int64 retain SQLite REAL comparison precision.
    /// </remarks>
    [<Extension>]
    static member EqLoose(stored: JsonValue, other: obj) = EqualityRules.matches stored other
