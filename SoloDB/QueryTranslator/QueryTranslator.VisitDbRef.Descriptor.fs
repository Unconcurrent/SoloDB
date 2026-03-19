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
        | Min of selector: Expression
        | Max of selector: Expression
        | Average of selector: Expression
        | Select of projection: Expression
        | Contains of value: Expression
        | Exists  // bare Any() without predicate

    /// Set operation shape.
    [<RequireQualifiedAccess>]
    type SetOperation =
        | Intersect of rightSource: Expression
        | Except of rightSource: Expression
        | Union of rightSource: Expression
        | Concat of rightSource: Expression

    /// The full query descriptor for a DBRefMany operator chain.
    type QueryDescriptor = {
        /// The innermost DBRefMany source expression.
        Source: Expression
        /// OfType<T> type name, if present.
        OfTypeName: string option
        /// Where predicates (accumulated, any order).
        WherePredicates: Expression list
        /// OrderBy/ThenBy sort keys with directions.
        SortKeys: (Expression * SortDirection) list
        /// Take limit expression.
        Limit: Expression option
        /// Skip offset expression.
        Offset: Expression option
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
    }
