namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Linq.Expressions
open System.Threading
open SoloDatabase.SqlModel
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.QueryableHelperBase
open SoloDatabase.QueryableHelperJoin
open SoloDatabase.QueryableGroupByAliases
open SoloDatabase.GroupByRebind

/// GroupBy chained-expression support — correlated subqueries for
/// group-item chains that go beyond simple aggregate whitelist.
/// Uses shared Terminal DU + walkChain extraction with per-context GroupBy building.


/// Builds the correlated subquery for each GroupBy terminal kind: aggregate, element, exists and
/// count, plus the cardinality wrapper they share.
module internal GroupByTerminals =
    let buildCorrelation (subAlias: string) (groupRowAlias: string) : SqlExpr =
        let outerKey = SqlExpr.Column(Some groupRowAlias, syntheticGroupKeyAlias)
        let innerKey = SqlExpr.Column(Some subAlias, syntheticGroupKeyAlias)
        SqlExpr.Binary(innerKey, BinaryOperator.Is, outerKey)

    /// Build a correlated subquery core for GroupBy chains.
    /// Selects from innerSelect (pre-grouping rows) correlated by group key.
    let buildCorrelatedCore
        (sourceCtx: QueryContext) (baseTableName: string) (innerSelect: SqlSelect) (groupRowAlias: string)
        (vars: Dictionary<string, obj>) (groupByExprs: Expression array) (desc: QueryDescriptor) (projections: Projection list) : string * SelectCore =

        let subAlias = GroupByAliases.nextSubquery sourceCtx

        // Null-safe group key correlation using the original key expression
        let correlation = buildCorrelation subAlias groupRowAlias

        // Translate Where predicates against subquery alias
        let wherePreds = desc.WherePredicates |> List.map (translateExprAgainst sourceCtx subAlias vars)

        let fullWhere =
            match wherePreds with
            | [] -> Some correlation
            | preds -> Some (preds |> List.fold (fun acc p -> SqlExpr.Binary(acc, BinaryOperator.And, p)) correlation)

        // Translate OrderBy sort keys
        let orderBy =
            desc.SortKeys
            |> List.map (fun (keyExpr, dir) ->
                { Expr = DateTimeFunctions.canonicalizeForCompareOrOrder keyExpr.Type (translateExprAgainst sourceCtx subAlias vars keyExpr); Direction = dir })

        // Default ordering by Id if no explicit order
        let orderBy =
            if orderBy.IsEmpty then [{ Expr = SqlExpr.Column(Some subAlias, "Id"); Direction = SortDirection.Asc }]
            else orderBy

        // Limit/Offset through the one owner of that policy. Mapping the two independently
        // emitted OFFSET with no LIMIT when a group was skipped without being taken, which
        // SQLite rejects; the sentinel that expresses "offset, unbounded" lives in ChainBounds.
        let limitDu, offsetDu = ChainBounds.buildLimitOffset desc.Limit desc.Offset

        let core =
            { Distinct = desc.Distinct && desc.SelectProjection.IsSome
              Projections = ProjectionSetOps.ofList projections
              Source = Some(DerivedTable(innerSelect, subAlias))
              Joins = []
              Where = fullWhere
              GroupBy = []
              Having = None
              OrderBy = orderBy
              Limit = limitDu
              Offset = offsetDu }

        subAlias, core

    /// Build a scalar subquery expression for an aggregate terminal on a group chain.
    let buildAggregateSubquery
        (sourceCtx: QueryContext) (baseTableName: string) (innerSelect: SqlSelect) (groupRowAlias: string) (vars: Dictionary<string, obj>)
        (groupByExprs: Expression array) (desc: QueryDescriptor) (aggKind: AggregateKind) (selectorOpt: Expression option) (coalesceZero: bool) : SqlExpr =

        let subAlias = GroupByAliases.nextAggregate sourceCtx

        let correlation = buildCorrelation subAlias groupRowAlias

        let wherePreds = desc.WherePredicates |> List.map (translateExprAgainst sourceCtx subAlias vars)

        let fullWhere =
            match wherePreds with
            | [] -> Some correlation
            | preds -> Some (preds |> List.fold (fun acc p -> SqlExpr.Binary(acc, BinaryOperator.And, p)) correlation)

        let aggArg =
            match selectorOpt with
            | Some sel -> Some (translateExprAgainst sourceCtx subAlias vars sel |> normalizeScalarExpr sel.Type)
            | None when desc.SelectProjection.IsSome ->
                match desc.SelectProjection with
                | Some proj -> Some (translateExprAgainst sourceCtx subAlias vars (proj :> Expression) |> normalizeScalarExpr proj.ReturnType)
                | None -> None
            | None when aggKind = AggregateKind.Count -> None
            | None -> None

        let aggExpr = SqlExpr.AggregateCall(aggKind, aggArg, false, None)
        let aggExpr = if coalesceZero then SqlExpr.Coalesce(aggExpr, [SqlExpr.Literal(SqlLiteral.Integer 0L)]) else aggExpr

        let core =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = aggExpr }]
              Source = Some(DerivedTable(innerSelect, subAlias))
              Joins = []
              Where = fullWhere
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = None
              Offset = None }

        SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }

    /// Element-access cardinality semantics. SingleLike is the strict variant:
    /// raises on >1 elements, and on 0 elements unless `orDefault` is true.
    type ElementCardinality =
        | FirstLike
        | SingleLike

    /// Wraps a value scalar-subquery in a CASE-WHEN cardinality guard against a
    /// sibling COUNT(LIMIT 2) subquery built over the same correlated core. The
    /// error sentinels use the json_quote-wrapped tag that JsonFunctions.fs
    /// hydration translates to InvalidOperationException with the .NET LINQ
    /// standard wording.
    let wrapCardinalityCase
        (sourceCtx: QueryContext) (baseCore: SelectCore)
        (orDefault: bool) (valueScalar: SqlExpr) : SqlExpr =
        let limitedSourceCore =
            { baseCore with
                Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 2L)) }
        let limitedSourceSel = { Ctes = []; Body = SingleSelect limitedSourceCore }
        let countAlias = GroupByAliases.nextCountDistinct sourceCtx
        let countOuterCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
              Source = Some(DerivedTable(limitedSourceSel, countAlias))
              Joins = []; Where = None; GroupBy = []; Having = None; OrderBy = []; Limit = None; Offset = None }
        let countExpr = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect countOuterCore }
        // Nested cardinality emit per SoloDB 1.2.2 contract: silent NULL → default(T).
        let errorTagJson (_message: string) = SqlExpr.Literal SqlLiteral.Null
        let manyArm =
            (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 2L)),
             errorTagJson "Sequence contains more than one element")
        if orDefault then
            SqlExpr.CaseExpr(manyArm, [], Some valueScalar)
        else
            let zeroArm =
                (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)),
                 errorTagJson "Sequence contains no elements")
            SqlExpr.CaseExpr(zeroArm, [manyArm], Some valueScalar)

    /// Build a scalar subquery for an element terminal (First, FirstOrDefault, Last, etc.)
    let buildElementSubquery
        (sourceCtx: QueryContext) (baseTableName: string) (innerSelect: SqlSelect) (groupRowAlias: string) (vars: Dictionary<string, obj>)
        (groupByExprs: Expression array) (desc: QueryDescriptor) (pickLast: bool) (orDefault: bool) (cardinality: ElementCardinality) : SqlExpr =

        // Build correlated core first — this determines the subquery alias
        let dummyProj = [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
        let subAlias, baseCore = buildCorrelatedCore sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc dummyProj

        // Now build projections using the SAME alias
        let projections =
            match desc.SelectProjection with
            | Some proj ->
                let selDu = translateExprAgainst sourceCtx subAlias vars (proj :> Expression) |> normalizeScalarExpr proj.ReturnType
                [{ Alias = Some "v"; Expr = selDu }]
            | None ->
                // Stitch the rowid Id into the Value JSON via jsonb_set so the
                // entity preserves its rowid through the GroupBy element pick.
                // Outer materializer hydrates from Value JSON; the embedded "$.Id"
                // becomes the entity's Id field. Without this, downstream consumers
                // see entity.Id=0 (default) instead of the rowid.
                [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some subAlias, "Id") }
                 { Alias = Some "Value"
                   Expr =
                     SqlExpr.FunctionCall("jsonb_set", [
                         SqlExpr.Column(Some subAlias, "Value")
                         SqlExpr.Literal(SqlLiteral.String "$.Id")
                         SqlExpr.Column(Some subAlias, "Id")
                     ]) }]

        let core = { baseCore with Projections = ProjectionSetOps.ofList projections }

        // Flip order for Last
        let core =
            if pickLast then
                { core with OrderBy = core.OrderBy |> List.map (fun ob -> { ob with Direction = if ob.Direction = SortDirection.Asc then SortDirection.Desc else SortDirection.Asc }) }
            else core

        // LIMIT 1 for element access (SingleLike still emits LIMIT 1 for the
        // value; cardinality is checked by a sibling COUNT subquery).
        let core = { core with Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L)) }

        // Wrap in scalar subquery
        let wrapAlias = GroupByAliases.nextElementWrap sourceCtx
        let resultCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Column(Some wrapAlias, if desc.SelectProjection.IsSome then "v" else "Value") }]
              Source = Some(DerivedTable({ Ctes = []; Body = SingleSelect core }, wrapAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
              Offset = None }

        let valueScalar = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect resultCore }

        match cardinality with
        | FirstLike -> valueScalar
        | SingleLike -> wrapCardinalityCase sourceCtx baseCore orDefault valueScalar

    /// Build an EXISTS subquery for Any/All terminals.
    let buildExistsSubquery
        (sourceCtx: QueryContext) (baseTableName: string) (innerSelect: SqlSelect) (groupRowAlias: string) (vars: Dictionary<string, obj>)
        (groupByExprs: Expression array) (desc: QueryDescriptor) (predOpt: Expression option) (negate: bool) : SqlExpr =

        let subAlias = GroupByAliases.nextElementSubquery sourceCtx
        let correlation = buildCorrelation subAlias groupRowAlias

        let wherePreds = desc.WherePredicates |> List.map (translateExprAgainst sourceCtx subAlias vars)

        let predExpr =
            match predOpt with
            | Some pred ->
                let predDu = translateExprAgainst sourceCtx subAlias vars pred
                if negate then [SqlExpr.Unary(UnaryOperator.Not, predDu)]
                else [predDu]
            | None -> []

        let allPreds = [correlation] @ wherePreds @ predExpr
        let fullWhere = allPreds |> List.reduce (fun a b -> SqlExpr.Binary(a, BinaryOperator.And, b))

        let core =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
              Source = Some(DerivedTable(innerSelect, subAlias))
              Joins = []
              Where = Some fullWhere
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
              Offset = None }

        let existsExpr = SqlExpr.Exists { Ctes = []; Body = SingleSelect core }
        if negate then SqlExpr.Unary(UnaryOperator.Not, existsExpr) else existsExpr

    /// Build a count subquery for Count terminal on a group chain.
    let buildCountSubquery
        (sourceCtx: QueryContext) (baseTableName: string) (innerSelect: SqlSelect) (groupRowAlias: string) (vars: Dictionary<string, obj>)
        (groupByExprs: Expression array) (desc: QueryDescriptor) : SqlExpr =

        let oneProj = [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
        let subAlias, core = buildCorrelatedCore sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc oneProj

        // If Distinct + SelectProjection, count distinct projected values
        let innerCore =
            if desc.GroupByKey.IsSome then
                let keyExpr = translateExprAgainst sourceCtx subAlias vars (desc.GroupByKey.Value :> Expression) |> normalizeScalarExpr desc.GroupByKey.Value.ReturnType
                { core with Distinct = true; Projections = ProjectionSetOps.ofList [{ Alias = Some "k"; Expr = keyExpr }] }
            elif desc.Distinct && desc.SelectProjection.IsSome then
                let proj = desc.SelectProjection.Value
                let projDu = translateExprAgainst sourceCtx subAlias vars (proj :> Expression) |> normalizeScalarExpr proj.ReturnType
                { core with Distinct = true; Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = projDu }] }
            else
                { core with Projections = ProjectionSetOps.ofList oneProj }

        let countAlias = GroupByAliases.nextCountWrap sourceCtx
        let countCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
              Source = Some(DerivedTable({ Ctes = []; Body = SingleSelect innerCore }, countAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = None
              Offset = None }

        SqlExpr.Coalesce(
            SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect countCore },
            [SqlExpr.Literal(SqlLiteral.Integer 0L)])

