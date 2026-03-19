namespace SoloDatabase

open System.Linq
open System.Collections
open System.Collections.Generic
open System.Linq.Expressions
open System
open SoloDatabase
open System.Runtime.CompilerServices
open SQLiteTools
open System.Reflection
open System.Text
open JsonFunctions
open Utils
open Connections
open SoloDatabase.RelationsTypes
open SoloDatabase.JsonSerializator
open Microsoft.Data.Sqlite
open SqlDu.Engine.C1.Spec
open SoloDatabase.QueryTranslatorBaseTypes

/// <summary>
/// An internal discriminated union that enumerates the LINQ methods supported by the query translator.
/// </summary>
[<RequireQualifiedAccess>]
type internal SupportedLinqMethods =
| Sum
| Average
| Min
| Max
| MinBy
| MaxBy
| Distinct
| DistinctBy
| Count
| CountBy
| LongCount
| Where
| Select
| SelectMany
| ThenBy
| ThenByDescending
| Order
| OrderDescending
| OrderBy
| OrderByDescending
| Take
| Skip
| TakeWhile
| SkipWhile
| First
| FirstOrDefault
| DefaultIfEmpty
| Last
| LastOrDefault
| Single
| SingleOrDefault
| All
| Any
| Contains
| Join
| Append
| Concat
| GroupBy
| Except
| ExceptBy
| Intersect
| IntersectBy
| Cast
| OfType
| Aggregate
| Include
| ThenInclude
| Exclude
| ThenExclude

// Translation validation helpers.
/// <summary>A marker interface for the SoloDB query provider.</summary>
type internal SoloDBQueryProvider = interface end
/// <summary>A marker interface for the root of a queryable expression, identifying the source table.</summary>
type internal IRootQueryable =
    /// <summary>Gets the name of the source database table for the query.</summary>
    abstract member SourceTableName: string

/// <summary>
/// Represents a preprocessed query where the expression tree has been flattened
/// into a simple chain of method calls starting at the root queryable.
/// </summary>
type internal PreprocessedQuery =
| Method of {|
    Value: SupportedLinqMethods;
    OriginalMethod: MethodInfo;
    Expressions: Expression array |}
| RootQuery of IRootQueryable

/// <summary>
/// Represents an ORDER BY statement captured during preprocessing.
/// </summary>
type internal PreprocessedOrder = {
    OrderingRule: Expression;
    mutable Descending: bool
    /// When set, used as DU expression instead of translating OrderingRule.
    RawExpr: SqlExpr option
}

type internal SQLSelector =
| Expression of Expression
| KeyProjection of Expression
| DuSelector of (string -> Dictionary<string, obj> -> Projection list)

type internal UsedSQLStatements = 
    {
        /// May be more than 1, they will be concatinated together with (...) AND (...) SQL structure.
        Filters: Expression ResizeArray
        Orders: PreprocessedOrder ResizeArray
        mutable Selector: SQLSelector option
        // These will account for the fact that in SQLite, the OFFSET clause cannot be used independently without the LIMIT clause.
        mutable Skip: Expression option
        mutable Take: Expression option
        UnionAll: (string -> Dictionary<string, obj> -> SelectCore) ResizeArray
        TableName: string
    }
    member this.IsEmptyWithTableName =
        this.Filters.Count = 0
        && this.Orders.Count = 0
        && this.Selector.IsNone
        && this.Skip.IsNone
        && this.Take.IsNone
        && this.UnionAll.Count = 0
        && this.TableName.Length > 0

type internal SQLSubquery =
    | Simple of UsedSQLStatements
    | ComplexDu of (struct {|Vars: Dictionary<string, obj>; Inner: SqlSelect; TableName: string|} -> SqlSelect)

type internal PredicateRole =
| WherePredicate
| AnyPredicate
| AllPredicate
| CountPredicate
| LongCountPredicate

type internal RelationAccessKind =
| NoRelationAccess
| HasRelationAccess

type internal LoweredPredicate = {
    Role: PredicateRole
    Predicate: Expression
    LayerPosition: LayerPosition
    RelationAccess: RelationAccessKind
    MaterializedPathsSnapshot: string array
}

type internal KeySelectorRole =
| DistinctByKey
| GroupByKey

type internal LoweredKeySelector = {
    Role: KeySelectorRole
    KeyExpression: Expression
    RelationAccess: RelationAccessKind
    Fingerprint: string
}

/// Describes a relation property hydrated via correlated subquery in the SQL projection.
/// Used to track which properties were embedded in the single SQL statement.
[<RequireQualifiedAccess>]
type internal HydrationProjection =
    /// DBRef single-relation: target entity embedded as [Id, Value] array via scalar subquery.
    | ScalarRelProjection of propertyName: string * targetType: Type
    /// DBRefMany collection-relation: targets embedded as json_group_array via scalar subquery (Slice B).
    | CollectionRelProjection of propertyName: string * targetType: Type

/// <summary>
/// A private, mutable builder used to construct an SQL query from a LINQ expression tree.
/// </summary>
/// <remarks>This type holds the state of the translation process, including the SQL command being built and any parameters.</remarks>
type internal QueryableBuilder =
    {
        /// A dictionary of parameters to be passed to the SQLite command.
        Variables: Dictionary<string, obj>
        /// This list will be using to know if it is necessary to create another SQLite subquery to finish the translation, by checking if all the available slots have been used.
        Subqueries: ResizeArray<SQLSubquery>
    }
