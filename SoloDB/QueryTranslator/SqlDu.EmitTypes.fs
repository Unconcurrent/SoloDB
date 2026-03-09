namespace SoloDatabase

/// Immutable result payload for all emission operations.
/// Sql contains the emitted SQL text with named parameter placeholders.
/// Parameters contains the ordered list of (name, value) pairs.
type Emitted = {
    Sql: string
    Parameters: (string * obj) list
}

module Emitted =
    /// Empty emission result — used as identity for combining.
    let empty = { Sql = ""; Parameters = [] }

    /// Combine two emission results by concatenating SQL with a separator
    /// and merging parameter lists in order.
    let combine (sep: string) (a: Emitted) (b: Emitted) =
        { Sql = a.Sql + sep + b.Sql
          Parameters = a.Parameters @ b.Parameters }

    /// Wrap emission SQL in parentheses, preserving parameters.
    let parens (e: Emitted) =
        { Sql = "(" + e.Sql + ")"; Parameters = e.Parameters }

    /// Prefix SQL text, preserving parameters.
    let prefix (p: string) (e: Emitted) =
        { Sql = p + e.Sql; Parameters = e.Parameters }

    /// Suffix SQL text, preserving parameters.
    let suffix (s: string) (e: Emitted) =
        { Sql = e.Sql + s; Parameters = e.Parameters }
