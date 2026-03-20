namespace SoloDatabase

open System.Linq.Expressions
open SqlDu.Engine.C1.Spec

/// Shared types for DBRefMany query translation.
module internal DBRefManyDescriptor =

    /// Resolved owner reference for a DBRefMany property.
    type DBRefManyOwnerRef = {
        OwnerCollection: string
        OwnerAliasSql: string
        PropertyExpr: System.Linq.Expressions.MemberExpression
    }

    /// Terminal operation that consumes the DBRefMany chain.
    [<RequireQualifiedAccess>]
    type Terminal =
        | Any of predicate: Expression option
        | All of predicate: Expression
        | Count
        | LongCount
        | Sum of selector: Expression
        | SumProjected
        | Min of selector: Expression
        | MinProjected
        | Max of selector: Expression
        | MaxProjected
        | Average of selector: Expression
        | AverageProjected
        | Select of projection: Expression
        | Contains of value: Expression
        | Exists  // bare Any() without predicate
        // L4a element-access terminals.
        | First of predicate: Expression option
        | FirstOrDefault of predicate: Expression option
        | Last of predicate: Expression option
        | LastOrDefault of predicate: Expression option
        // Single/SingleOrDefault: emits json_group_array LIMIT 2, materializer enforces cardinality.
        | Single of predicate: Expression option
        | SingleOrDefault of predicate: Expression option
        // R55: MinBy/MaxBy — return element with min/max key. Emits ORDER BY key + LIMIT 1.
        | MinBy of keySelector: Expression
        | MaxBy of keySelector: Expression
        | DistinctBy of keySelector: Expression
        | ElementAt of index: Expression
        | ElementAtOrDefault of index: Expression
        | CountBy of keySelector: Expression

    /// Set operation shape.
    [<RequireQualifiedAccess>]
    type SetOperation =
        | DistinctBy of keySelector: Expression
        | Intersect of rightSource: Expression
        | Except of rightSource: Expression
        | Union of rightSource: Expression
        | Concat of rightSource: Expression
        // R56: By-key set operations — carry both the right source and the key selector.
        | IntersectBy of rightKeys: Expression * keySelector: Expression
        | ExceptBy of rightKeys: Expression * keySelector: Expression
        | UnionBy of rightKeys: Expression * keySelector: Expression

    /// The full query descriptor for a DBRefMany operator chain.
    type QueryDescriptor = {
        /// The innermost DBRefMany source expression.
        Source: Expression
        /// OfType<T> type name, if present.
        OfTypeName: string option
        /// Cast<T> type name, if present. Preserves rows and encodes mismatches as errors.
        CastTypeName: string option
        /// Where predicates applied BEFORE Take/Skip (inner scope).
        WherePredicates: Expression list
        /// OrderBy/ThenBy sort keys applied BEFORE Take/Skip (inner scope).
        SortKeys: (Expression * SortDirection) list
        /// Take limit expression (innermost bounding).
        Limit: Expression option
        /// Skip offset expression (innermost bounding).
        Offset: Expression option
        /// Where predicates applied AFTER Take/Skip (outer scope, post-bound).
        /// When non-empty, the builder wraps the core in a DerivedTable and applies these on the outer layer.
        PostBoundWherePredicates: Expression list
        /// OrderBy sort keys applied AFTER Take/Skip (outer scope, post-bound).
        PostBoundSortKeys: (Expression * SortDirection) list
        /// Outer Take after inner bounding (e.g., Skip(1).Take(3)).
        PostBoundLimit: Expression option
        /// Outer Skip after inner bounding (e.g., Take(3).Skip(1)).
        PostBoundOffset: Expression option
        /// TakeWhile/SkipWhile predicate and direction.
        TakeWhileInfo: (LambdaExpression * bool) option  // (pred, isTakeWhile)
        /// GroupBy key selector.
        GroupByKey: LambdaExpression option
        /// Whether Distinct was applied (after Select).
        Distinct: bool
        /// Select projection (before Distinct, after Where/OrderBy/Take).
        SelectProjection: LambdaExpression option
        /// Set operation (Intersect/Except/Union/Concat with right operand).
        SetOp: SetOperation option
        /// Terminal operation.
        Terminal: Terminal
        /// GroupBy HAVING predicate (for GroupBy.Any/All terminals).
        GroupByHavingPredicate: Expression option
        /// DefaultIfEmpty on base entity rowset (before Select projection).
        /// None = not applied. Some None = DefaultIfEmpty(). Some(Some expr) = DefaultIfEmpty(value).
        DefaultIfEmpty: Expression option option
        /// DefaultIfEmpty on projected rowset (after Select projection).
        PostSelectDefaultIfEmpty: Expression option option
    }
