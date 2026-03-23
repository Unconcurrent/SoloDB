module internal SoloDatabase.Provenance

open System.Runtime.CompilerServices
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

/// Recursively resolve an expression through nested DerivedTable levels
/// until reaching a BaseTable column or an opaque/derived expression.
let private coreId (core: SelectCore) =
    RuntimeHelpers.GetHashCode core

let rec private resolveTransitive (visited: Set<int * string * string>) (expr: SqlExpr) (coreChain: SelectCore list) : ColumnSource =
    match expr with
    | Column(Some src, col) ->
        match coreChain with
        | currentCore :: deeperCores ->
            let visitKey = (coreId currentCore, src, col)
            if Set.contains visitKey visited then Opaque
            else
                let visited = Set.add visitKey visited
                match currentCore.Source with
                | Some(DerivedTable(innerSel, derivedAlias)) when derivedAlias = src ->
                    resolveIntoDerivedTable visited col innerSel deeperCores
                | Some(BaseTable(_, Some tableAlias)) when tableAlias = src ->
                    BaseColumn(tableAlias, col)
                | Some(BaseTable(tableName, None)) when tableName = src ->
                    BaseColumn(tableName, col)
                | _ ->
                    let joinMatch =
                        currentCore.Joins |> List.tryPick (fun j ->
                            match j with
                            | ConditionedJoin(_, DerivedTable(jSel, jAlias), _) when jAlias = src ->
                                Some(resolveIntoDerivedTable visited col jSel deeperCores)
                            | ConditionedJoin(_, BaseTable(_, Some tAlias), _) when tAlias = src ->
                                Some(BaseColumn(tAlias, col))
                            | ConditionedJoin(_, BaseTable(tbl, None), _) when tbl = src ->
                                Some(BaseColumn(tbl, col))
                            | CrossJoin(DerivedTable(jSel, jAlias)) when jAlias = src ->
                                Some(resolveIntoDerivedTable visited col jSel deeperCores)
                            | CrossJoin(BaseTable(_, Some tAlias)) when tAlias = src ->
                                Some(BaseColumn(tAlias, col))
                            | CrossJoin(BaseTable(tbl, None)) when tbl = src ->
                                Some(BaseColumn(tbl, col))
                            | _ -> None)
                    match joinMatch with
                    | Some resolved -> resolved
                    | None -> BaseColumn(src, col)
        | [] ->
            BaseColumn(src, col)
    | Column(None, col) ->
        match coreChain with
        | currentCore :: deeperCores ->
            let visitKey = (coreId currentCore, "", col)
            if Set.contains visitKey visited then Opaque
            else
                let visited = Set.add visitKey visited
                let localMap = buildLocalMap currentCore
                match Map.tryFind col localMap with
                | Some(Column(None, innerCol)) when innerCol = col ->
                    resolveFromSource visited currentCore deeperCores col
                | Some innerExpr ->
                    resolveTransitive visited innerExpr coreChain
                | None -> Opaque
        | [] -> Opaque
    | JsonExtractExpr(Some src, col, _) ->
        match coreChain with
        | _ :: _ -> resolveTransitive visited (Column(Some src, col)) coreChain
        | [] -> BaseColumn(src, col)
    | JsonRootExtract(Some src, col) ->
        match coreChain with
        | _ :: _ -> resolveTransitive visited (Column(Some src, col)) coreChain
        | [] -> BaseColumn(src, col)
    | _ -> DerivedExpr expr

and private resolveFromSource (visited: Set<int * string * string>) (core: SelectCore) (deeperCores: SelectCore list) (col: string) : ColumnSource =
    match core.Source with
    | Some(DerivedTable(innerSel, _)) ->
        resolveIntoDerivedTable visited col innerSel deeperCores
    | Some(BaseTable(_, Some alias)) when core.Joins.IsEmpty ->
        BaseColumn(alias, col)
    | Some(BaseTable(tableName, None)) when core.Joins.IsEmpty ->
        BaseColumn(tableName, col)
    | _ ->
        Opaque

/// Resolve a column name into a DerivedTable's inner select, following the chain deeper.
and private resolveIntoDerivedTable (visited: Set<int * string * string>) (col: string) (innerSel: SqlSelect) (deeperCores: SelectCore list) : ColumnSource =
    match innerSel.Body with
    | SingleSelect innerCore ->
        let localMap = buildLocalMap innerCore
        match Map.tryFind col localMap with
        | Some(Column(None, innerCol)) when innerCol = col ->
            resolveFromSource visited innerCore deeperCores col
        | Some innerExpr ->
            resolveTransitive visited innerExpr (innerCore :: deeperCores)
        | None -> Opaque
    | UnionAllSelect _ -> Opaque

let private resolveProjectionExpr (core: SelectCore) (expr: SqlExpr) : ColumnSource =
    match expr, core.Source with
    | Column(Some src, col), Some(DerivedTable(innerSel, derivedAlias)) when src = derivedAlias ->
        resolveIntoDerivedTable Set.empty col innerSel []
    | JsonExtractExpr(Some src, col, _), Some(DerivedTable(innerSel, derivedAlias)) when src = derivedAlias ->
        resolveIntoDerivedTable Set.empty col innerSel []
    | JsonRootExtract(Some src, col), Some(DerivedTable(innerSel, derivedAlias)) when src = derivedAlias ->
        resolveIntoDerivedTable Set.empty col innerSel []
    | Column(None, col), _ ->
        resolveFromSource Set.empty core [] col
    | _ ->
        resolveTransitive Set.empty expr [core]

/// Build a full transitive provenance map for a SelectCore.
/// Resolves every projected column through arbitrary DerivedTable nesting depth
/// until reaching BaseTable columns or opaque expressions.
let buildForCore (core: SelectCore) : ProvenanceMap =
    core.Projections
    |> ProjectionSetOps.toList
    |> List.choose (fun p ->
        let name =
            match p.Alias with
            | Some a -> Some a
            | None ->
                match p.Expr with
                | Column(_, c) -> Some c
                | _ -> None
        match name with
        | Some n ->
            let resolved = resolveProjectionExpr core p.Expr
            Some(("", n), resolved)
        | None -> None)
    |> Map.ofList

/// Build provenance map for a specific DerivedTable alias context.
let buildForDerivedTable (outer: SelectCore) (derivedAlias: string) : ProvenanceMap =
    outer.Projections
    |> ProjectionSetOps.toList
    |> List.choose (fun p ->
        let name =
            match p.Alias with
            | Some a -> Some a
            | None ->
                match p.Expr with
                | Column(_, c) -> Some c
                | _ -> None
        match name with
        | Some n ->
            let resolved = resolveProjectionExpr outer p.Expr
            Some((derivedAlias, n), resolved)
        | None -> None)
    |> Map.ofList

/// Check if a column reference at a given scope can be resolved to a base column.
let tryResolveColumn (prov: ProvenanceMap) (alias: string) (col: string) : ColumnSource option =
    Map.tryFind (alias, col) prov

/// Check if ALL projected columns in a core resolve to base columns (not opaque).
/// Provenance-backed replacement for hasBaseTableInnerSource.
let allProjectionsResolved (core: SelectCore) : bool =
    let prov = buildForCore core
    prov |> Map.forall (fun _ source ->
        match source with
        | BaseColumn _ -> true
        | DerivedExpr(Column(Some _, _)) -> true
        | _ -> false)
