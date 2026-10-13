namespace SoloDatabase.SqlModel

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

type internal JsonPath =
    | JsonPath of head: string * tail: string list
    | IndexedPath of head: SoloDatabase.JsonSerializator.JsonEqualitySupport.PathSegment * tail: SoloDatabase.JsonSerializator.JsonEqualitySupport.PathSegment list

[<RequireQualifiedAccess>]
module internal JsonPathOps =
    open SoloDatabase.JsonSerializator.JsonEqualitySupport
    let private memberDelimiters = [|'.'; '['; '\000'|]

    let ofList (segments: string list) : JsonPath =
        match segments with
        | head :: tail -> JsonPath(head, tail)
        | [] -> invalidArg "segments" "JsonPath must be non-empty."

    let ofSegments segments =
        match segments with
        | [] -> invalidArg "segments" "JsonPath must be non-empty."
        | head :: tail ->
            if segments |> List.forall (function Member _ -> true | Index _ -> false) then
                segments |> List.map (function Member name -> name | Index _ -> failwith "Expected member path.") |> ofList
            else IndexedPath(head, tail)

    let toList path =
        match path with
        | JsonPath(head, tail) -> (head :: tail) |> List.map Member
        | IndexedPath(head, tail) -> head :: tail

    let trySimpleMembers (literal: string) =
        if literal.StartsWith("$.", System.StringComparison.Ordinal) then
            let members = literal.Substring(2).Split('.')
            if members |> Array.forall (fun name ->
                name.Length > 0 && name |> Seq.forall (fun c -> System.Char.IsLetterOrDigit c || c = '_' || c = '$')) then
                Some(members |> Array.toList)
            else None
        else None

    let text segments =
        segments |> List.fold (fun path segment ->
            match segment with
            | Member name ->
                if name.Length > 0 && name.[0] <> '"' && name.IndexOfAny(memberDelimiters) < 0 then path + "." + name
                else appendMember path name
            | Index index -> path + "[" + index.ToString(System.Globalization.CultureInfo.InvariantCulture) + "]") "$"

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
    | ExactNumeric of SoloDatabase.Numerics.DecimalAggregateKind * SoloDatabase.Metadata.Generation.NumericKind

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
    | JsonText

type internal UnaryOperator =
    | Pos
    | Not
    | Neg
    | IsNull
    | IsNotNull
