namespace SqlDu.Engine.C1.Spec

// ======================================================================
// DU Contract Substrate — Compile-Time Only
//
// Core SQL algebra types used by QueryTranslator and Queryable product
// code to construct typed DU values. These types form the compile-time
// contract substrate moved into SoloDB per Batch 0.5 (Path B).
//
// LIFECYCLE:
// - Batch 0.5: types move here for product compilation
// - Batch 1 (B1b): QueryTranslator constructs these types
// - Batch 2: Queryable constructs these types
// - Batch 4: full runtime/emitter/passes join them in SoloDB
//
// NO runtime behavior routes through these types yet.
// The adapter seam (Batch 0) converts DU values back to strings
// until the full engine move in Batch 4.
// ======================================================================

type JsonPath = JsonPath of string list

type SortDirection =
    | Asc
    | Desc

type JoinKind =
    | Inner
    | Left
    | Cross

type AggregateKind =
    | Count
    | Sum
    | Avg
    | Min
    | Max
    | GroupConcat
    | JsonGroupArray

type WindowFunctionKind =
    | RowNumber
    | DenseRank
    | Rank
    | NamedWindowFunction of string

type StatementKind =
    | Select
    | Insert
    | Update
    | Delete
    | Ddl

type RelationPattern =
    | NoRelations
    | DbRef
    | DbRefMany
    | MixedRelations

type SelectComposition =
    | SingleSelect
    | UnionAll

type CtePresence =
    | WithCte
    | NoCte

type AntiSemiForm =
    | NoAntiSemi
    | NotExists
    | NotIn

type JsonbOperationClass =
    | NoJsonb
    | JsonExtract
    | JsonSet
    | JsonAggregate
    | JsonEach
    | MixedJsonb

type Sargability =
    | Sargable
    | NonSargable
    | ContextDependent

type FlattenSafety =
    | FlattenSafe
    | FlattenUnsafe
    | FlattenRequiresProof

type IndexEligibility =
    | IndexEligible
    | IndexHostile
    | RequiresPlanEvidence

type SqlLiteral =
    | Null
    | Integer of int64
    | Float of float
    | String of string
    | Boolean of bool
    | Blob of byte[]

type BinaryOperator =
    | Eq
    | Ne
    | Lt
    | Le
    | Gt
    | Ge
    | And
    | Or
    | Add
    | Sub
    | Mul
    | Div
    | Mod
    | Like
    | Glob
    | Regexp
    | Is
    | IsNot
    | In
    | NotInOp

type UnaryOperator =
    | Not
    | Neg
    | IsNull
    | IsNotNull

type SqlExpr =
    | Column of sourceAlias: string option * column: string
    | Literal of SqlLiteral
    | Parameter of name: string
    | JsonExtractExpr of sourceAlias: string option * column: string * path: JsonPath
    | JsonSetExpr of target: SqlExpr * assignments: (JsonPath * SqlExpr) list
    | JsonArrayExpr of elements: SqlExpr list
    | JsonObjectExpr of properties: (string * SqlExpr) list
    | FunctionCall of name: string * arguments: SqlExpr list
    | AggregateCall of kind: AggregateKind * argument: SqlExpr option * distinct: bool * separator: SqlExpr option
    | WindowCall of WindowSpec
    | Unary of UnaryOperator * SqlExpr
    | Binary of SqlExpr * BinaryOperator * SqlExpr
    | Between of SqlExpr * lower: SqlExpr * upper: SqlExpr
    | InList of SqlExpr * SqlExpr list
    | InSubquery of SqlExpr * SqlSelect
    | Cast of SqlExpr * sqlType: string
    | Coalesce of SqlExpr list
    | Exists of SqlSelect
    | ScalarSubquery of SqlSelect
    | CaseExpr of branches: (SqlExpr * SqlExpr) list * elseExpr: SqlExpr option
    | UpdateFragment of path: SqlExpr * value: SqlExpr

and WindowSpec = {
    Kind: WindowFunctionKind
    Arguments: SqlExpr list
    PartitionBy: SqlExpr list
    OrderBy: (SqlExpr * SortDirection) list
}

and TableSource =
    | BaseTable of table: string * alias: string option
    | DerivedTable of query: SqlSelect * alias: string
    | FromJsonEach of valueExpr: SqlExpr * alias: string option

and Projection = {
    Alias: string option
    Expr: SqlExpr
}

and JoinShape = {
    Kind: JoinKind
    Source: TableSource
    On: SqlExpr option
}

and OrderBy = {
    Expr: SqlExpr
    Direction: SortDirection
}

and SelectCore = {
    Source: TableSource option
    Joins: JoinShape list
    Projections: Projection list
    Where: SqlExpr option
    GroupBy: SqlExpr list
    Having: SqlExpr option
    OrderBy: OrderBy list
    Limit: SqlExpr option
    Offset: SqlExpr option
    Distinct: bool
}

and SelectBody =
    | SingleSelect of SelectCore
    | UnionAllSelect of head: SelectCore * tail: SelectCore list

and CteBinding = {
    Name: string
    Query: SqlSelect
}

and SqlSelect = {
    Ctes: CteBinding list
    Body: SelectBody
}

type InsertConflictResolution =
    | NoConflictResolution
    | OrIgnore
    | OrReplace

type InsertStatement = {
    TableName: string
    Columns: string list
    Values: SqlExpr list list
    ConflictResolution: InsertConflictResolution
    Returning: SqlExpr list option
}

type UpdateStatement = {
    TableName: string
    SetClauses: (string * SqlExpr) list
    Where: SqlExpr option
}

type DeleteStatement = {
    TableName: string
    Where: SqlExpr option
}

type DdlStatement = {
    Sql: string
}

type SqlStatement =
    | SelectStmt of SqlSelect
    | InsertStmt of InsertStatement
    | UpdateStmt of UpdateStatement
    | DeleteStmt of DeleteStatement
    | DdlStmt of DdlStatement
