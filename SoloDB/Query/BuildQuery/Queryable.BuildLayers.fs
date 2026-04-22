namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open System.Text
open System.Runtime.CompilerServices
open Microsoft.Data.Sqlite
open SQLiteTools
open Utils
open JsonFunctions
open Connections
open SoloDatabase
open SoloDatabase.JsonSerializator
open SoloDatabase.RelationsTypes
open SoloDatabase.QueryTranslatorBaseTypes
open SqlDu.Engine.C1.Spec

module internal QueryableLayerBuild =
    open QueryableHelperState
    open QueryableHelperPreprocess
    open QueryableHelperBase
    let internal buildLayersDu<'T>
        (sourceCtx: QueryContext)
        (vars: Dictionary<string, obj>)
        (layers: SQLSubquery ResizeArray)
        : SqlSelect =

        let layerCount = layers.Count

        let tableName =
            if layerCount > 0 then
                match layers.[0] with
                | Simple layer -> layer.TableName
                | ComplexDu _ -> ""
            else
                ""

        let rec buildLayer (i: int) : SqlSelect =
            let layer = layers.[i]
            match layer with
            // Edge case 1: Empty query (root table only)
            | Simple layer when layer.IsEmptyWithTableName ->
                let isTypePrimitive = QueryTranslator.isPrimitiveSQLiteType typeof<'T>
                if isTypePrimitive then
                    // Edge case 2: Primitive type extraction — jsonb_extract(Value, '$')
                    wrapCore (mkCore
                        [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }
                         { Alias = Some "Value"; Expr = SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$")]) }]
                        (Some (BaseTable(layer.TableName, None))))
                else
                    // Non-primitive: bare table reference (Id and Value are columns)
                    wrapCore (mkCore
                        [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }
                         { Alias = None; Expr = SqlExpr.Column(None, "Value") }]
                        (Some (BaseTable(layer.TableName, None))))

            | Simple layer ->
                let isLocalKeyProjection =
                    match layer.Selector with
                    | Some (KeyProjection _) -> true
                    | _ -> false
                let currentCtx =
                    if isLocalKeyProjection then cloneQueryContext sourceCtx
                    else sourceCtx
                let effectiveTableName = layer.TableName
                let hasTableName = not (String.IsNullOrEmpty effectiveTableName)
                let quotedTableName = "\"" + effectiveTableName + "\""
                let idColumnExpr = if hasTableName then SqlExpr.Column(Some quotedTableName, "Id") else SqlExpr.Column(None, "Id")
                let valueColumnExpr = if hasTableName then SqlExpr.Column(Some quotedTableName, "Value") else SqlExpr.Column(None, "Value")

                // Track whether the Value projection can be rewritten for JOIN materialization.
                let mutable needsValueMaterialization = false

                // Build projections based on selector.
                let projections =
                    match layer.Selector with
                    | Some (Expression selector) ->
                        let selectorExpr = translateExprDu sourceCtx layer.TableName selector vars
                        [{ Alias = None; Expr = idColumnExpr }
                         { Alias = Some "Value"; Expr = selectorExpr }]
                    | Some (KeyProjection selector) ->
                        let isTypePrimitive = QueryTranslator.isPrimitiveSQLiteType typeof<'T>
                        let keyExpr = translateExprDu currentCtx effectiveTableName selector vars
                        let selectorParameters =
                            match unwrapQuotedLambda selector with
                            | :? LambdaExpression as lambda -> Some lambda.Parameters
                            | _ -> None
                        let compoundSlotProjections =
                            match tryGetCompoundScalarKeyMembers selector with
                            | Some members ->
                                [ for i = 0 to Array.length members - 1 do
                                      let struct(_, argExpr) = Array.get members i
                                      let slotExpr =
                                          match selectorParameters, argExpr with
                                          | Some parameters, (:? LambdaExpression as lambdaExpr) ->
                                              lambdaExpr :> Expression
                                          | Some parameters, _ ->
                                              Expression.Lambda(argExpr, parameters) :> Expression
                                          | None, _ ->
                                              argExpr
                                      yield { Alias = Some (groupKeySlotName i); Expr = translateExprDu currentCtx effectiveTableName slotExpr vars } ]
                            | None -> []
                        if isTypePrimitive then
                            [{ Alias = None; Expr = idColumnExpr }
                             { Alias = Some "Value"; Expr = SqlExpr.FunctionCall("jsonb_extract", [valueColumnExpr; SqlExpr.Literal(SqlLiteral.String "$")]) }
                             { Alias = Some "__solodb_group_key"; Expr = keyExpr }]
                            @ compoundSlotProjections
                        else
                            needsValueMaterialization <- true
                            [{ Alias = None; Expr = idColumnExpr }
                             { Alias = Some "Value"; Expr = valueColumnExpr }
                             { Alias = Some "__solodb_group_key"; Expr = keyExpr }]
                            @ compoundSlotProjections
                    | Some (DuSelector buildProjections) ->
                        buildProjections layer.TableName vars
                    | None ->
                        let isTypePrimitive = QueryTranslator.isPrimitiveSQLiteType typeof<'T>
                        if isTypePrimitive then
                            [{ Alias = None; Expr = idColumnExpr }
                             { Alias = Some "Value"; Expr = SqlExpr.FunctionCall("jsonb_extract", [valueColumnExpr; SqlExpr.Literal(SqlLiteral.String "$")]) }]
                        else
                            needsValueMaterialization <- true
                            [{ Alias = None; Expr = idColumnExpr }
                             { Alias = None; Expr = valueColumnExpr }]

                // Build source.
                let isBaseTable = (i = 0)
                let source =
                    if isBaseTable then
                        Some (BaseTable(layer.TableName, None))
                    else
                        let innerSel = buildLayer (i - 1)
                        Some (DerivedTable(innerSel, "o"))

                // Build clauses (WHERE, ORDER BY, LIMIT, OFFSET, UNION ALL).
                // Side effect: expression translation discovers JOINs via QueryContext.
                let clauseCtx = if isLocalKeyProjection then currentCtx else sourceCtx
                let clauseTable = if isLocalKeyProjection then effectiveTableName else layer.TableName
                let struct (where, orderBy, limit, offset, unionAlls) =
                    buildClausesDu clauseCtx vars layer clauseTable

                // Edge case 8: JOIN materialization (DBRef) — discovered during clause translation.
                let mutable finalProjections = projections
                let mutable joins = []

                if isBaseTable && currentCtx.Joins.Count > 0 then
                    // Build JoinShape list from discovered JoinEdges.
                    joins <-
                        currentCtx.Joins
                        |> Seq.map (fun j ->
                            ConditionedJoin(
                                parseJoinKind j.JoinKind,
                                BaseTable(j.TargetTable, Some j.TargetAlias),
                                SqlExpr.Binary(
                                    SqlExpr.Column(Some j.TargetAlias, "Id"),
                                    BinaryOperator.Eq,
                                    SqlExpr.JsonExtractExpr(j.OnSourceAlias, "Value", JsonPath(j.OnPropertyName, [])))))
                        |> Seq.toList

                    // Rewrite Value projection for materialization (jsonb_set).
                    if needsValueMaterialization then
                        let materializedValueExpr = buildMaterializedValueExpr currentCtx quotedTableName valueColumnExpr
                        finalProjections <-
                            finalProjections |> List.map (fun p ->
                                // Replace bare Value column with materialized expression.
                                match p.Alias, p.Expr with
                                | None, SqlExpr.Column(_, "Value") ->
                                    { Alias = Some "Value"; Expr = materializedValueExpr }
                                | Some "Value", _ ->
                                    { Alias = Some "Value"; Expr = materializedValueExpr }
                                | _ -> p)

                // Assemble the SelectCore.
                let body =
                    match unionAlls with
                    | [] ->
                        let core =
                            { mkCore finalProjections source with
                                Joins = joins
                                Where = where
                                OrderBy = orderBy
                                Limit = limit
                                Offset = offset }
                        SingleSelect core
                    | _ ->
                        // Edge case 4: UNION ALL chains
                        let headCore =
                            { mkCore finalProjections source with
                                Joins = joins
                                Where = where }
                        UnionAllSelect(headCore, unionAlls)

                wrapCoreBody body orderBy limit offset

            | ComplexDu buildFunc ->
                let tn =
                    if i = 0 then tableName
                    else ""
                let innerSel =
                    if i > 0 then buildLayer (i - 1)
                    else
                        // No inner layers — provide an empty select (shouldn't happen in practice)
                        wrapCore (mkCore [] None)
                buildFunc {| Vars = vars; Inner = innerSel; TableName = tn |}

        buildLayer (layerCount - 1)
