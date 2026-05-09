namespace SoloDatabase

open System

/// Single source of truth for the SQL → CLR error sentinel prefix.
/// Translator emit paths embed this prefix into a SQL-level string sentinel
/// when an aggregate or correlated subquery should raise a runtime error
/// (e.g. cardinality guard violations, index-out-of-range). The hydration
/// layer in JsonFunctions detects the prefix and translates the suffix
/// into an InvalidOperationException whose message matches the .NET LINQ
/// standard wording (e.g. "Sequence contains no elements").
module internal ErrorTag =

    [<Literal>]
    let internal Prefix = "__solodb_error__:"

    /// If the input string carries the error-sentinel prefix, returns the
    /// suffix (the .NET-style error message) wrapped in ValueSome; otherwise
    /// ValueNone. Used by the JSON hydration layer to decide whether to
    /// translate the value into an InvalidOperationException.
    let internal tryDetect (s: string) : string voption =
        if isNull s then ValueNone
        elif s.StartsWith(Prefix, StringComparison.Ordinal) then
            ValueSome (s.Substring(Prefix.Length))
        else
            ValueNone
