namespace SoloDatabase

/// Mutable emission context responsible for:
/// 1. Parameter naming/allocation (deterministic, sequential @p0, @p1, ...)
/// 2. Identifier quoting policy (SQLite double-quote style)
/// 3. JSON path quoting policy (single-quote with $ prefix)
/// 4. Literal emission policy (inline vs parameterized)
type EmitContext() =
    let mutable paramCounter = 0

    /// When true, Integer/Float/String literals are emitted inline in SQL.
    /// When false (default), they are parameterized via AllocParam.
    /// Product adapter sets this to true to match legacy MinimalEmit behavior.
    member val InlineLiterals = false with get, set

    /// Allocate the next parameter name and register the value.
    /// Returns Emitted with placeholder and single param.
    member _.AllocParam(value: obj) : Emitted =
        let name = sprintf "@p%d" paramCounter
        paramCounter <- paramCounter + 1
        let ps = ResizeArray<string * obj>(1)
        ps.Add(name, value)
        { Sql = name; Parameters = ps }

    /// Quote an identifier using SQLite double-quote convention.
    member _.QuoteIdent(name: string) : string =
        sprintf "\"%s\"" (name.Replace("\"", "\"\""))

    /// Format a JSON path for use in json_extract/jsonb_extract.
    /// JsonPath is a list of path segments; result is '$.<seg1>.<seg2>...'
    /// Handles array-index bracket notation and escapes single quotes.
    member _.FormatJsonPath(segments: string list) : string =
        match segments with
        | [] -> "'$'"
        | segs ->
            let path = System.String.Join(".", segs)
            let path = path.Replace(".[", "[")
            let escaped = path.Replace("'", "''").Replace("\0", "")
            sprintf "'$.%s'" escaped

    /// Current parameter count (for determinism verification).
    member _.ParamCount = paramCounter

    /// Reset context for a fresh emission (used in testing).
    member _.Reset() = paramCounter <- 0
