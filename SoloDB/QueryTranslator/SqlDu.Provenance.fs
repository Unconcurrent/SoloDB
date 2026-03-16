module SoloDatabase.Provenance

open SqlDu.Engine.C1.Spec

/// A resolved column reference: either a base table column or an opaque (unresolvable) expression.
type ColumnSource =
    | BaseColumn of table: string * column: string
    | DerivedExpr of SqlExpr
    | Opaque

/// Provenance map: maps (derivedAlias, columnName) → ultimate source at each query level.
type ProvenanceMap = Map<string * string, ColumnSource>

/// Build a single-level alias map from a SelectCore's projections.
let private buildLocalMap (core: SelectCore) : Map<string, SqlExpr> =
    core.Projections
    |> ProjectionSetOps.toList
    |> List.choose (fun p ->
        match p.Alias with
        | Some alias -> Some(alias, p.Expr)
        | None ->
            match p.Expr with
            | Column(_, colName) -> Some(colName, p.Expr)
            | _ -> None)
    |> Map.ofList

/// Resolve an expression through a local alias map, transitively following Column references.
let rec resolveExpr (localMap: Map<string, SqlExpr>) (derivedAlias: string) (expr: SqlExpr) : ColumnSource =
    match expr with
    | Column(Some src, col) when src = derivedAlias ->
        match Map.tryFind col localMap with
        | Some innerExpr -> resolveInnerExpr innerExpr
        | None -> Opaque
    | Column(None, col) ->
        match Map.tryFind col localMap with
        | Some innerExpr -> resolveInnerExpr innerExpr
        | None -> Opaque
    | Column(Some src, col) -> BaseColumn(src, col)
    | _ -> DerivedExpr expr

and private resolveInnerExpr (expr: SqlExpr) : ColumnSource =
    match expr with
    | Column(Some src, col) -> BaseColumn(src, col)
    | JsonExtractExpr(Some src, col, _) -> BaseColumn(src, col)
    | JsonRootExtract(Some src, col) -> BaseColumn(src, col)
    | _ -> DerivedExpr expr

/// Build a provenance map for a single SelectCore with a DerivedTable source.
let buildForCore (core: SelectCore) : ProvenanceMap =
    match core.Source with
    | Some(DerivedTable(innerSel, derivedAlias)) ->
        match innerSel.Body with
        | SingleSelect innerCore ->
            let localMap = buildLocalMap innerCore
            core.Projections
            |> ProjectionSetOps.toList
            |> List.choose (fun p ->
                let name = match p.Alias with Some a -> Some a | None -> match p.Expr with Column(_, c) -> Some c | _ -> None
                match name with
                | Some n -> Some((derivedAlias, n), resolveExpr localMap derivedAlias p.Expr)
                | None -> None)
            |> Map.ofList
        | UnionAllSelect _ -> Map.empty
    | _ -> Map.empty

/// Check if a column reference at a given scope can be resolved to a base column.
let tryResolveColumn (prov: ProvenanceMap) (alias: string) (col: string) : ColumnSource option =
    Map.tryFind (alias, col) prov
