namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBase
open SoloDatabase.QueryTranslatorSerialization

/// Existing collection assignments retain their ordered path and writer facts.
module internal QueryTranslatorCollectionMutation =
    let internal tryCall visitProduced visitDu visitStoredValueDu (m: MethodCallExpression) (qb: QueryBuilder) =
        match m with
        // Update methods collect target paths and stored values.
        | OfShape1 null null "Set" null (oldValue, newValue) when qb.UpdateMode ->
            let destination = visitProduced oldValue qb
            let pathExpr = QueryProduced.value qb.SourceContext.NextAlias destination
            let valueExpr = visitStoredValueDu destination.Input newValue qb
            qb.UpdateAssignments.Add(pathExpr, valueExpr)
            ValueSome(SqlExpr.Literal(SqlLiteral.Null))

        | OfShape1 null null "Append" null (array, newValue)
        | OfShape1 null null "Add" null (array, newValue) when qb.UpdateMode ->
            let destination = visitProduced array qb
            let arrayPathExpr = QueryProduced.value qb.SourceContext.NextAlias destination
            let modifiedPath =
                match arrayPathExpr with
                | SqlExpr.Literal(SqlLiteral.String path) -> SqlExpr.Literal(SqlLiteral.String $"{path}[#]")
                | other -> other
            let struct(element, _) = QueryTranslatorSerialization.serializationAccess destination.Input (StoredLabel(box 0))
            let valueExpr = visitStoredValueDu element newValue qb
            qb.UpdateAssignments.Add(modifiedPath, valueExpr)
            ValueSome(SqlExpr.Literal(SqlLiteral.Null))

        | OfShape2 null null "SetAt" null null (array, indexExpr, newValue) when qb.UpdateMode ->
            let destination = visitProduced array qb
            let arrayPathExpr = QueryProduced.value qb.SourceContext.NextAlias destination
            let indexVal = ExpressionEvaluation.evaluateExpr<obj> indexExpr
            let modifiedPath =
                match arrayPathExpr with
                | SqlExpr.Literal(SqlLiteral.String path) -> SqlExpr.Literal(SqlLiteral.String $"{path}[{indexVal}]")
                | other -> other
            let struct(element, _) = QueryTranslatorSerialization.serializationAccess destination.Input (StoredLabel(box 0))
            let valueExpr = visitStoredValueDu element newValue qb
            qb.UpdateAssignments.Add(modifiedPath, valueExpr)
            ValueSome(SqlExpr.Literal(SqlLiteral.Null))

        | OfShape1 null null "RemoveAt" null (array, indexExpr) when qb.UpdateMode ->
            let arrayPathExpr = visitDu array qb
            let indexVal = ExpressionEvaluation.evaluateExpr<obj> indexExpr
            let alias = qb.SourceAlias
            let valueExpr =
                SqlExpr.FunctionCall("jsonb_remove", [
                    SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(alias, "Value"); arrayPathExpr])
                    SqlExpr.Literal(SqlLiteral.String $"$[{indexVal}]")
                ])
            qb.UpdateAssignments.Add(arrayPathExpr, valueExpr)
            ValueSome(SqlExpr.Literal(SqlLiteral.Null))

        | OfShape2 null null "Insert" null null (array, indexExpr, newValue) when qb.UpdateMode ->
            // Shift-insert into a JSON array via json_each rebuild. Emit a
            // 3-arm UNION ALL (rows before idx, the new value at idx, rows from
            // idx onward shifted by +1) and re-aggregate via jsonb_group_array.
            // The intermediate SELECT applies ORDER BY k LIMIT -1 — a documented
            // SQLite idiom that preserves subquery row order through the outer
            // aggregate consumer.
            let destination = visitProduced array qb
            let arrayPathExpr = QueryProduced.value qb.SourceContext.NextAlias destination
            let indexVal = Convert.ToInt64(ExpressionEvaluation.evaluateExpr<obj> indexExpr)
            if indexVal < 0L then
                raise (ArgumentOutOfRangeException(
                    "indexExpr",
                    indexVal,
                    "Insert: negative index is not supported. Use a non-negative index."))
            let struct(element, _) = QueryTranslatorSerialization.serializationAccess destination.Input (StoredLabel(box 0))
            let newValueExpr = visitStoredValueDu element newValue qb
            let alias = qb.SourceAlias
            let extractArrayExpr =
                SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(alias, "Value"); arrayPathExpr])
            let beforeAlias = "_je_b"
            let afterAlias = "_je_a"
            let unionAlias = "_u"
            let orderedAlias = "_o"
            let mkCore source projections where =
                { Distinct = false
                  Projections = ProjectionSetOps.ofList projections
                  Source = source
                  Joins = []
                  Where = where
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            let beforeCore =
                mkCore
                    (Some (FromJsonEach(extractArrayExpr, Some beforeAlias)))
                    [ { Alias = Some "k"; Expr = SqlExpr.Column(Some beforeAlias, "key") }
                      { Alias = Some "v"; Expr = SqlExpr.Column(Some beforeAlias, "value") } ]
                    (Some (SqlExpr.Binary(
                        SqlExpr.Column(Some beforeAlias, "key"),
                        BinaryOperator.Lt,
                        SqlExpr.Literal(SqlLiteral.Integer indexVal))))
            let middleCore =
                mkCore
                    None
                    [ { Alias = Some "k"; Expr = SqlExpr.Literal(SqlLiteral.Integer indexVal) }
                      { Alias = Some "v"; Expr = newValueExpr } ]
                    None
            let afterCore =
                mkCore
                    (Some (FromJsonEach(extractArrayExpr, Some afterAlias)))
                    [ { Alias = Some "k"
                        Expr = SqlExpr.Binary(
                                  SqlExpr.Column(Some afterAlias, "key"),
                                  BinaryOperator.Add,
                                  SqlExpr.Literal(SqlLiteral.Integer 1L)) }
                      { Alias = Some "v"; Expr = SqlExpr.Column(Some afterAlias, "value") } ]
                    (Some (SqlExpr.Binary(
                        SqlExpr.Column(Some afterAlias, "key"),
                        BinaryOperator.Ge,
                        SqlExpr.Literal(SqlLiteral.Integer indexVal))))
            let unionSel = { Ctes = []; Body = UnionAllSelect(beforeCore, [middleCore; afterCore]) }
            let orderedCore =
                { Distinct = false
                  Projections = ProjectionSetOps.ofList
                      [ { Alias = Some "k"; Expr = SqlExpr.Column(Some unionAlias, "k") }
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some unionAlias, "v") } ]
                  Source = Some (DerivedTable(unionSel, unionAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = [ { Expr = SqlExpr.Column(Some unionAlias, "k"); Direction = SortDirection.Asc } ]
                  Limit = Some (SqlExpr.Literal(SqlLiteral.Integer -1L))
                  Offset = None }
            let orderedSel = { Ctes = []; Body = SingleSelect orderedCore }
            let outerCore =
                { Distinct = false
                  Projections = ProjectionSetOps.ofList
                      [ { Alias = None
                          Expr = SqlExpr.FunctionCall("jsonb_group_array", [SqlExpr.Column(Some orderedAlias, "v")]) } ]
                  Source = Some (DerivedTable(orderedSel, orderedAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            let outerSel = { Ctes = []; Body = SingleSelect outerCore }
            qb.UpdateAssignments.Add(arrayPathExpr, SqlExpr.ScalarSubquery outerSel)
            ValueSome(SqlExpr.Literal(SqlLiteral.Null))
        | _ -> ValueNone
