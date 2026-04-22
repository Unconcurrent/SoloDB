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
// - QueryTranslator constructs these types
// - Queryable constructs these types
// - Full runtime/emitter/pass layers join them in SoloDB
//
// NO runtime behavior routes through these types yet.
// The adapter seam (Batch 0) converts DU values back to strings
// until the full engine move in Batch 4.
// ======================================================================

type internal JsonPath = JsonPath of head: string * tail: string list

[<RequireQualifiedAccess>]
module internal JsonPathOps =
    let ofList (segments: string list) : JsonPath =
        match segments with
        | head :: tail -> JsonPath(head, tail)
        | [] -> invalidArg "segments" "JsonPath must be non-empty."

    let toList (JsonPath(head, tail): JsonPath) : string list =
        head :: tail

type internal SortDirection =
    | Asc
    | Desc

type internal JoinKind =
    | Inner
    | Left

type internal AggregateKind =
    | Count
    | Sum
    | Avg
    | Min
    | Max
    | GroupConcat
    | JsonGroupArray

type internal WindowFunctionKind =
    | RowNumber
    | DenseRank
    | Rank
    | NamedWindowFunction of string

type internal StatementKind =
    | Select
    | Insert
    | Update
    | Delete
    | Ddl

type internal RelationPattern =
    | NoRelations
    | DbRef
    | DbRefMany
    | MixedRelations

type internal CtePresence =
    | WithCte
    | NoCte

type internal AntiSemiForm =
    | NoAntiSemi
    | NotExists
    | NotIn

type internal JsonbOperationClass =
    | NoJsonb
    | JsonExtract
    | JsonSet
    | JsonAggregate
    | JsonEach
    | MixedJsonb

type internal Sargability =
    | Sargable
    | NonSargable
    | ContextDependent

type internal FlattenSafety =
    | FlattenSafe
    | FlattenUnsafe
    | FlattenRequiresProof

type internal IndexEligibility =
    | IndexEligible
    | IndexHostile
    | RequiresPlanEvidence

type internal SqlLiteral =
    | Null
    | Integer of int64
    | Float of float
    | String of string
    | Boolean of bool
    | Blob of byte[]

type internal BinaryOperator =
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
    | BitwiseAnd

type internal UnaryOperator =
    | Not
    | Neg
    | IsNull
    | IsNotNull
