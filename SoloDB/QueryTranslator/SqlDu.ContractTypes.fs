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

type SqlExpr with
    /// Pre-order fold over SqlExpr.
    /// Visits the current node before visiting children.
    static member fold (folder: 'State -> SqlExpr -> 'State) (state: 'State) (expr: SqlExpr) : 'State =
        let rec loop (acc: 'State) (node: SqlExpr) : 'State =
            let acc = folder acc node
            match node with
            | Column _ -> acc
            | Literal _ -> acc
            | Parameter _ -> acc
            | JsonExtractExpr _ -> acc
            | JsonSetExpr(target, assignments) ->
                let acc = loop acc target
                assignments |> List.fold (fun s (_, value) -> loop s value) acc
            | JsonArrayExpr(elements) ->
                elements |> List.fold loop acc
            | JsonObjectExpr(properties) ->
                properties |> List.fold (fun s (_, value) -> loop s value) acc
            | FunctionCall(_, arguments) ->
                arguments |> List.fold loop acc
            | AggregateCall(_, argument, _, separator) ->
                let acc =
                    match argument with
                    | Some arg -> loop acc arg
                    | None -> acc
                match separator with
                | Some sep -> loop acc sep
                | None -> acc
            | WindowCall(spec) ->
                let acc = spec.Arguments |> List.fold loop acc
                let acc = spec.PartitionBy |> List.fold loop acc
                spec.OrderBy |> List.fold (fun s (orderExpr, _) -> loop s orderExpr) acc
            | Unary(_, inner) ->
                loop acc inner
            | Binary(left, _, right) ->
                let acc = loop acc left
                loop acc right
            | Between(valueExpr, lower, upper) ->
                let acc = loop acc valueExpr
                let acc = loop acc lower
                loop acc upper
            | InList(valueExpr, values) ->
                let acc = loop acc valueExpr
                values |> List.fold loop acc
            | InSubquery(valueExpr, _) ->
                // Default boundary: do not cross into SqlSelect.
                loop acc valueExpr
            | Cast(inner, _) ->
                loop acc inner
            | Coalesce(expressions) ->
                expressions |> List.fold loop acc
            | Exists _ ->
                // Default boundary: do not cross into SqlSelect.
                acc
            | ScalarSubquery _ ->
                // Default boundary: do not cross into SqlSelect.
                acc
            | CaseExpr(branches, elseExpr) ->
                let acc =
                    branches
                    |> List.fold (fun s (condExpr, resultExpr) ->
                        let s = loop s condExpr
                        loop s resultExpr) acc
                match elseExpr with
                | Some elseNode -> loop acc elseNode
                | None -> acc
            | UpdateFragment(pathExpr, valueExpr) ->
                let acc = loop acc pathExpr
                loop acc valueExpr
        loop state expr

    /// Bottom-up map over SqlExpr.
    /// Rewrites children first, then applies `mapper` on the rebuilt node.
    static member map (mapper: SqlExpr -> SqlExpr) (expr: SqlExpr) : SqlExpr =
        let rec loop (node: SqlExpr) : SqlExpr =
            let mappedNode =
                match node with
                | Column _ -> node
                | Literal _ -> node
                | Parameter _ -> node
                | JsonExtractExpr _ -> node
                | JsonSetExpr(target, assignments) ->
                    JsonSetExpr(
                        loop target,
                        assignments |> List.map (fun (path, value) -> path, loop value))
                | JsonArrayExpr(elements) ->
                    JsonArrayExpr(elements |> List.map loop)
                | JsonObjectExpr(properties) ->
                    JsonObjectExpr(properties |> List.map (fun (key, value) -> key, loop value))
                | FunctionCall(name, arguments) ->
                    FunctionCall(name, arguments |> List.map loop)
                | AggregateCall(kind, argument, distinct, separator) ->
                    AggregateCall(
                        kind,
                        argument |> Option.map loop,
                        distinct,
                        separator |> Option.map loop)
                | WindowCall(spec) ->
                    WindowCall({
                        spec with
                            Arguments = spec.Arguments |> List.map loop
                            PartitionBy = spec.PartitionBy |> List.map loop
                            OrderBy = spec.OrderBy |> List.map (fun (orderExpr, dir) -> loop orderExpr, dir)
                    })
                | Unary(op, inner) ->
                    Unary(op, loop inner)
                | Binary(left, op, right) ->
                    Binary(loop left, op, loop right)
                | Between(valueExpr, lower, upper) ->
                    Between(loop valueExpr, loop lower, loop upper)
                | InList(valueExpr, values) ->
                    InList(loop valueExpr, values |> List.map loop)
                | InSubquery(valueExpr, query) ->
                    // Default boundary: do not cross into SqlSelect.
                    InSubquery(loop valueExpr, query)
                | Cast(inner, sqlType) ->
                    Cast(loop inner, sqlType)
                | Coalesce(expressions) ->
                    Coalesce(expressions |> List.map loop)
                | Exists query ->
                    // Default boundary: do not cross into SqlSelect.
                    Exists query
                | ScalarSubquery query ->
                    // Default boundary: do not cross into SqlSelect.
                    ScalarSubquery query
                | CaseExpr(branches, elseExpr) ->
                    CaseExpr(
                        branches |> List.map (fun (condExpr, resultExpr) -> loop condExpr, loop resultExpr),
                        elseExpr |> Option.map loop)
                | UpdateFragment(pathExpr, valueExpr) ->
                    UpdateFragment(loop pathExpr, loop valueExpr)
            mapper mappedNode
        loop expr

    /// Short-circuit predicate over SqlExpr.
    static member exists (predicate: SqlExpr -> bool) (expr: SqlExpr) : bool =
        let rec loop (node: SqlExpr) : bool =
            if predicate node then true
            else
                match node with
                | Column _ -> false
                | Literal _ -> false
                | Parameter _ -> false
                | JsonExtractExpr _ -> false
                | JsonSetExpr(target, assignments) ->
                    loop target || (assignments |> List.exists (fun (_, value) -> loop value))
                | JsonArrayExpr(elements) ->
                    elements |> List.exists loop
                | JsonObjectExpr(properties) ->
                    properties |> List.exists (fun (_, value) -> loop value)
                | FunctionCall(_, arguments) ->
                    arguments |> List.exists loop
                | AggregateCall(_, argument, _, separator) ->
                    (argument |> Option.map loop |> Option.defaultValue false)
                    || (separator |> Option.map loop |> Option.defaultValue false)
                | WindowCall(spec) ->
                    (spec.Arguments |> List.exists loop)
                    || (spec.PartitionBy |> List.exists loop)
                    || (spec.OrderBy |> List.exists (fun (orderExpr, _) -> loop orderExpr))
                | Unary(_, inner) ->
                    loop inner
                | Binary(left, _, right) ->
                    loop left || loop right
                | Between(valueExpr, lower, upper) ->
                    loop valueExpr || loop lower || loop upper
                | InList(valueExpr, values) ->
                    loop valueExpr || (values |> List.exists loop)
                | InSubquery(valueExpr, _) ->
                    // Default boundary: do not cross into SqlSelect.
                    loop valueExpr
                | Cast(inner, _) ->
                    loop inner
                | Coalesce(expressions) ->
                    expressions |> List.exists loop
                | Exists _ ->
                    // Default boundary: do not cross into SqlSelect.
                    false
                | ScalarSubquery _ ->
                    // Default boundary: do not cross into SqlSelect.
                    false
                | CaseExpr(branches, elseExpr) ->
                    (branches |> List.exists (fun (condExpr, resultExpr) -> loop condExpr || loop resultExpr))
                    || (elseExpr |> Option.map loop |> Option.defaultValue false)
                | UpdateFragment(pathExpr, valueExpr) ->
                    loop pathExpr || loop valueExpr
        loop expr

    /// Bottom-up optional rewrite.
    /// Returns Some(rewritten) when either children changed or `mapper` rewrites the rebuilt node.
    /// Returns None when no rewrite occurred.
    static member tryMap (mapper: SqlExpr -> SqlExpr option) (expr: SqlExpr) : SqlExpr option =
        let rec loop (node: SqlExpr) : SqlExpr option =
            let rebuiltNode, childChanged =
                match node with
                | Column _ -> node, false
                | Literal _ -> node, false
                | Parameter _ -> node, false
                | JsonExtractExpr _ -> node, false
                | JsonSetExpr(target, assignments) ->
                    let newTargetOpt = loop target
                    let newTarget = newTargetOpt |> Option.defaultValue target
                    let mutable changed = newTargetOpt.IsSome
                    let newAssignments =
                        assignments
                        |> List.map (fun (path, value) ->
                            let rewritten = loop value
                            if rewritten.IsSome then changed <- true
                            path, (rewritten |> Option.defaultValue value))
                    JsonSetExpr(newTarget, newAssignments), changed
                | JsonArrayExpr(elements) ->
                    let mutable changed = false
                    let newElements =
                        elements
                        |> List.map (fun element ->
                            let rewritten = loop element
                            if rewritten.IsSome then changed <- true
                            rewritten |> Option.defaultValue element)
                    JsonArrayExpr(newElements), changed
                | JsonObjectExpr(properties) ->
                    let mutable changed = false
                    let newProperties =
                        properties
                        |> List.map (fun (key, value) ->
                            let rewritten = loop value
                            if rewritten.IsSome then changed <- true
                            key, (rewritten |> Option.defaultValue value))
                    JsonObjectExpr(newProperties), changed
                | FunctionCall(name, arguments) ->
                    let mutable changed = false
                    let newArgs =
                        arguments
                        |> List.map (fun argument ->
                            let rewritten = loop argument
                            if rewritten.IsSome then changed <- true
                            rewritten |> Option.defaultValue argument)
                    FunctionCall(name, newArgs), changed
                | AggregateCall(kind, argument, distinct, separator) ->
                    let newArgument =
                        argument |> Option.bind loop
                    let newSeparator =
                        separator |> Option.bind loop
                    let changed = newArgument.IsSome || newSeparator.IsSome
                    let argumentValue =
                        match newArgument, argument with
                        | Some arg, _ -> Some arg
                        | None, original -> original
                    let separatorValue =
                        match newSeparator, separator with
                        | Some sep, _ -> Some sep
                        | None, original -> original
                    AggregateCall(
                        kind,
                        argumentValue,
                        distinct,
                        separatorValue), changed
                | WindowCall(spec) ->
                    let mutable changed = false
                    let newArguments =
                        spec.Arguments |> List.map (fun arg ->
                            let rewritten = loop arg
                            if rewritten.IsSome then changed <- true
                            rewritten |> Option.defaultValue arg)
                    let newPartitionBy =
                        spec.PartitionBy |> List.map (fun part ->
                            let rewritten = loop part
                            if rewritten.IsSome then changed <- true
                            rewritten |> Option.defaultValue part)
                    let newOrderBy =
                        spec.OrderBy |> List.map (fun (orderExpr, dir) ->
                            let rewritten = loop orderExpr
                            if rewritten.IsSome then changed <- true
                            (rewritten |> Option.defaultValue orderExpr), dir)
                    WindowCall({
                        spec with
                            Arguments = newArguments
                            PartitionBy = newPartitionBy
                            OrderBy = newOrderBy
                    }), changed
                | Unary(op, inner) ->
                    let innerOpt = loop inner
                    Unary(op, innerOpt |> Option.defaultValue inner), innerOpt.IsSome
                | Binary(left, op, right) ->
                    let leftOpt = loop left
                    let rightOpt = loop right
                    Binary(leftOpt |> Option.defaultValue left, op, rightOpt |> Option.defaultValue right), (leftOpt.IsSome || rightOpt.IsSome)
                | Between(valueExpr, lower, upper) ->
                    let valueOpt = loop valueExpr
                    let lowerOpt = loop lower
                    let upperOpt = loop upper
                    Between(
                        valueOpt |> Option.defaultValue valueExpr,
                        lowerOpt |> Option.defaultValue lower,
                        upperOpt |> Option.defaultValue upper), (valueOpt.IsSome || lowerOpt.IsSome || upperOpt.IsSome)
                | InList(valueExpr, values) ->
                    let valueOpt = loop valueExpr
                    let mutable changed = valueOpt.IsSome
                    let newValues =
                        values |> List.map (fun value ->
                            let rewritten = loop value
                            if rewritten.IsSome then changed <- true
                            rewritten |> Option.defaultValue value)
                    InList(valueOpt |> Option.defaultValue valueExpr, newValues), changed
                | InSubquery(valueExpr, query) ->
                    // Default boundary: do not cross into SqlSelect.
                    let valueOpt = loop valueExpr
                    InSubquery(valueOpt |> Option.defaultValue valueExpr, query), valueOpt.IsSome
                | Cast(inner, sqlType) ->
                    let innerOpt = loop inner
                    Cast(innerOpt |> Option.defaultValue inner, sqlType), innerOpt.IsSome
                | Coalesce(expressions) ->
                    let mutable changed = false
                    let newExpressions =
                        expressions |> List.map (fun value ->
                            let rewritten = loop value
                            if rewritten.IsSome then changed <- true
                            rewritten |> Option.defaultValue value)
                    Coalesce(newExpressions), changed
                | Exists query ->
                    // Default boundary: do not cross into SqlSelect.
                    Exists query, false
                | ScalarSubquery query ->
                    // Default boundary: do not cross into SqlSelect.
                    ScalarSubquery query, false
                | CaseExpr(branches, elseExpr) ->
                    let mutable changed = false
                    let newBranches =
                        branches |> List.map (fun (condExpr, resultExpr) ->
                            let condOpt = loop condExpr
                            let resultOpt = loop resultExpr
                            if condOpt.IsSome || resultOpt.IsSome then changed <- true
                            (condOpt |> Option.defaultValue condExpr), (resultOpt |> Option.defaultValue resultExpr))
                    let newElseOpt =
                        elseExpr |> Option.bind loop
                    if newElseOpt.IsSome then changed <- true
                    CaseExpr(newBranches, match newElseOpt, elseExpr with | Some e, _ -> Some e | None, original -> original), changed
                | UpdateFragment(pathExpr, valueExpr) ->
                    let pathOpt = loop pathExpr
                    let valueOpt = loop valueExpr
                    UpdateFragment(pathOpt |> Option.defaultValue pathExpr, valueOpt |> Option.defaultValue valueExpr), (pathOpt.IsSome || valueOpt.IsSome)

            match mapper rebuiltNode with
            | Some rewritten -> Some rewritten
            | None when childChanged -> Some rebuiltNode
            | None -> None

        loop expr
