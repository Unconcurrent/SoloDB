namespace SoloDatabase

/// Result payload for emission; parameter lists are immutable after construction.
/// Sql contains the emitted SQL text with named parameter placeholders.
/// Parameters contains the ordered list of (name, value) pairs.
type internal Emitted = {
    Sql: string
    Parameters: ResizeArray<string * obj>
}

module internal Emitted =
    let emptyParameters () = ResizeArray<string * obj>()

    let private collect (parameters: 'T -> ResizeArray<string * obj>) (parts: 'T list) =
        let mutable combined: ResizeArray<string * obj> = null
        let mutable ownsCombined = false
        for part in parts do
            let values = parameters part
            if isNull combined || combined.Count = 0 then
                combined <- values
            elif values.Count > 0 then
                if not ownsCombined then
                    combined <- ResizeArray(combined)
                    ownsCombined <- true
                combined.AddRange(values)
        if isNull combined then emptyParameters () else combined

    let concatParameterSets parts = collect id parts

    let collectParameters (parts: Emitted list) = collect (fun part -> part.Parameters) parts

    /// Empty emission result — used as identity for combining.
    let empty = { Sql = ""; Parameters = emptyParameters () }

    /// Combine two emission results by concatenating SQL with a separator
    /// and merging parameter lists in order.
    let combine (sep: string) (a: Emitted) (b: Emitted) =
        { Sql = a.Sql + sep + b.Sql
          Parameters = concatParameterSets [ a.Parameters; b.Parameters ] }

    /// Wrap emission SQL in parentheses, sharing parameters (immutable after construction).
    let parens (e: Emitted) =
        { Sql = "(" + e.Sql + ")"; Parameters = e.Parameters }

    /// Prefix SQL text, sharing parameters (immutable after construction).
    let prefix (p: string) (e: Emitted) =
        { Sql = p + e.Sql; Parameters = e.Parameters }

    /// Suffix SQL text, sharing parameters (immutable after construction).
    let suffix (s: string) (e: Emitted) =
        { Sql = e.Sql + s; Parameters = e.Parameters }
