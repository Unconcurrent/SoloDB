namespace SoloDatabase

/// Mutable result payload for all emission operations.
/// Sql contains the emitted SQL text with named parameter placeholders.
/// Parameters contains the ordered list of (name, value) pairs.
type Emitted = {
    Sql: string
    Parameters: ResizeArray<string * obj>
}

module Emitted =
    let emptyParameters () = ResizeArray<string * obj>()

    let concatParameterSets (parts: seq<ResizeArray<string * obj>>) =
        let parts = parts |> Seq.toArray
        let total = parts |> Array.sumBy (fun ps -> ps.Count)
        let combined = ResizeArray<string * obj>(total)
        for ps in parts do
            combined.AddRange(ps)
        combined

    let collectParameters (parts: seq<Emitted>) =
        parts |> Seq.map (fun e -> e.Parameters) |> concatParameterSets

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
