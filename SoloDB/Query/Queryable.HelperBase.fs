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

module internal QueryableHelperBase =
    /// Allocate a parameter in the shared Variables dict and return a SqlExpr referencing it.
    let internal allocateParam (variables: Dictionary<string, obj>) (value: obj) : SqlExpr =
        let value = match value with :? bool as b -> box (if b then 1 else 0) | _ -> value
        let jsonValue, shouldEncode = toSQLJson value
        let name = sprintf "dp%d" variables.Count
        variables.[name] <- jsonValue
        if shouldEncode then SqlExpr.FunctionCall("jsonb", [SqlExpr.Parameter name])
        else SqlExpr.Parameter name

    /// Translate a LINQ expression to SqlExpr DU via the QueryTranslator DU path.
    let internal translateExprDu (sourceCtx: QueryContext) (tableName: string) (expr: Expression) (vars: Dictionary<string, obj>) : SqlExpr =
        QueryTranslator.translateToSqlExpr sourceCtx tableName expr vars

    /// Return the DU expression for extracting Value as JSON if the type is not a primitive SQLite type.
    let internal extractValueAsJsonDu (x: Type) : SqlExpr =
        let isPrimitive = QueryTranslator.isPrimitiveSQLiteType x
        if isPrimitive then
            SqlExpr.Column(None, "Value")
        elif x = typeof<obj> || typeof<JsonValue>.IsAssignableFrom x then
            SqlExpr.CaseExpr(
                [(SqlExpr.Binary(
                    SqlExpr.FunctionCall("typeof", [SqlExpr.Column(None, "Value")]),
                    BinaryOperator.Eq,
                    SqlExpr.Literal(SqlLiteral.String "blob")),
                  SqlExpr.FunctionCall("json_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$")]))],
                Some(SqlExpr.Column(None, "Value")))
        else
            SqlExpr.FunctionCall("json_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$")])

    /// Emit a SqlSelect to a StringBuilder via the minimal emitter.
    let internal emitSelectToSb (sb: StringBuilder) (variables: Dictionary<string, obj>) (indexModel: SoloDatabase.IndexModel.IndexModel) (sel: SqlSelect) =
        let qb : QueryBuilder = {
            StringBuilder = sb
            Variables = variables
            AppendVariable = appendVariable sb variables
            RollBack = fun N -> sb.Remove(sb.Length - (int)N, (int)N) |> ignore
            UpdateMode = false
            TableNameDot = ""
            JsonExtractSelfValue = true
            Parameters = System.Collections.ObjectModel.ReadOnlyCollection(Array.empty)
            IdParameterIndex = -1
            SourceContext = QueryContext.SingleSource("")
            ParamCounter = ref 0
            DuHandlerResult = ref ValueNone
        }
        let pipelineResult =
            PassRunner.runPipeline [
                ConstantFoldPass.constantFold
                FlattenPass.subqueryFlatten
                PushdownPass.predicatePushdown
                ProjectionPass.projectionPushdown
                IndexPlanShapingPass.indexPlanShaping indexModel
                JsonbRewritePolicyPass.jsonbRewritePolicy indexModel
            ] (SelectStmt sel)
        match pipelineResult.Output with
        | SelectStmt outSel -> SqlDuMinimalEmit.emitSelect qb outSel
        | _ -> failwith "internal invariant violation: expected SelectStmt from optimizer pipeline"

    /// Helper to build a simple SelectCore with default empty fields.
    let internal mkCore projections source =
        { Distinct = false; Projections = projections; Source = source
          Joins = []; Where = None; GroupBy = []; Having = None
          OrderBy = []; Limit = None; Offset = None }

    /// Wrap a SelectCore in a SqlSelect.
    let internal wrapCore core = { Ctes = []; Body = SingleSelect core }

    /// Build a DerivedTable source from a SqlSelect with alias "o".
    let internal derivedO sel = DerivedTable(sel, "o")

    let internal collectIndexModelTableNames (sourceTableName: string) (select: SqlSelect) : string list =
        let tableNames = HashSet<string>(StringComparer.Ordinal)
        tableNames.Add(sourceTableName) |> ignore

        let addJoinSource = function
            | BaseTable(name, _) ->
                tableNames.Add(name) |> ignore
            | _ -> ()

        let addCoreJoinTables (core: SelectCore) =
            core.Joins |> List.iter (fun join -> addJoinSource join.Source)

        match select.Body with
        | SingleSelect core ->
            addCoreJoinTables core
        | UnionAllSelect(head, tail) ->
            addCoreJoinTables head
            tail |> List.iter addCoreJoinTables

        tableNames |> Seq.toList

    let internal unwrapQuotedLambda (expr: Expression) =
        match expr with
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Quote -> ue.Operand
        | _ -> expr

    /// Try to extract a relation JSON path from a lambda expression.
    /// For `o => o.Customer.Value.Rating`, returns Some "$.Customer[1].Rating".
    /// Only handles DBRef<T>.Value.Property single-hop patterns.
    /// Caller is responsible for guarding that the relation will be materialized.
    let internal tryExtractRelationJsonPath (expr: Expression) : string option =
        let unwrapped = unwrapQuotedLambda expr
        match unwrapped with
        | :? LambdaExpression as lambda ->
            // Pattern: param.RelProp.Value.ScalarProp
            match lambda.Body with
            | :? MemberExpression as outerMember ->
                let scalarProp = outerMember.Member.Name
                match outerMember.Expression with
                | :? MemberExpression as valueMember when valueMember.Member.Name = "Value" ->
                    match valueMember.Expression with
                    | :? MemberExpression as _relMember ->
                        let relProp = _relMember.Member.Name
                        Some (sprintf "$.%s[1].%s" relProp scalarProp)
                    | _ -> None
                | _ -> None
            | _ -> None
        | _ -> None

    let internal expressionFingerprint (expr: Expression) =
        let unwrapped = unwrapQuotedLambda expr
        match unwrapped with
        | :? LambdaExpression as lambda -> lambda.Body.ToString()
        | _ -> unwrapped.ToString()

    let rec internal detectRelationAccessNode (expr: Expression) =
        if isNull expr then false
        else
            match expr with
            | :? MemberExpression as m ->
                let declaring = m.Member.DeclaringType
                let hitsRelationMember =
                    not (isNull declaring)
                    && declaring.IsGenericType
                    && (
                        declaring.GetGenericTypeDefinition() = typedefof<DBRef<_>>
                        || declaring.GetGenericTypeDefinition() = typedefof<DBRefMany<_>>
                    )
                hitsRelationMember || detectRelationAccessNode m.Expression
            | :? MethodCallExpression as m ->
                let declaring = m.Method.DeclaringType
                let hitsRelationCall =
                    not (isNull declaring)
                    && declaring.IsGenericType
                    && declaring.GetGenericTypeDefinition() = typedefof<DBRefMany<_>>
                hitsRelationCall
                || (not (isNull m.Object) && detectRelationAccessNode m.Object)
                || (m.Arguments |> Seq.exists detectRelationAccessNode)
            | :? LambdaExpression as l ->
                detectRelationAccessNode l.Body
            | :? UnaryExpression as u ->
                detectRelationAccessNode u.Operand
            | :? BinaryExpression as b ->
                detectRelationAccessNode b.Left || detectRelationAccessNode b.Right
            | :? ConditionalExpression as c ->
                detectRelationAccessNode c.Test
                || detectRelationAccessNode c.IfTrue
                || detectRelationAccessNode c.IfFalse
            | :? InvocationExpression as i ->
                detectRelationAccessNode i.Expression || (i.Arguments |> Seq.exists detectRelationAccessNode)
            | :? NewArrayExpression as na ->
                na.Expressions |> Seq.exists detectRelationAccessNode
            | :? NewExpression as n ->
                n.Arguments |> Seq.exists detectRelationAccessNode
            | :? MemberInitExpression as mi ->
                detectRelationAccessNode mi.NewExpression
                || (mi.Bindings |> Seq.exists (function
                    | :? MemberAssignment as ma -> detectRelationAccessNode ma.Expression
                    | _ -> false))
            | :? ListInitExpression as li ->
                detectRelationAccessNode li.NewExpression
                || (li.Initializers |> Seq.exists (fun init -> init.Arguments |> Seq.exists detectRelationAccessNode))
            | _ -> false

    let internal detectRelationAccess (expr: Expression) =
        if detectRelationAccessNode expr then HasRelationAccess else NoRelationAccess

    /// <summary>
    /// Parses a method name string into a <c>SupportedLinqMethods</c> option.
    /// </summary>
    /// <param name="methodName">The name of the LINQ method.</param>
    /// <returns>A Some value if the method is supported, otherwise None.</returns>
    let internal parseSupportedMethod (methodName: string) : SupportedLinqMethods option =
        match methodName with
        | "Sum" -> Some SupportedLinqMethods.Sum
        | "Average" -> Some SupportedLinqMethods.Average
        | "Min" -> Some SupportedLinqMethods.Min
        | "Max" -> Some SupportedLinqMethods.Max
        | "MinBy" -> Some SupportedLinqMethods.MinBy
        | "MaxBy" -> Some SupportedLinqMethods.MaxBy
        | "Distinct" -> Some SupportedLinqMethods.Distinct
        | "DistinctBy" -> Some SupportedLinqMethods.DistinctBy
        | "Count" -> Some SupportedLinqMethods.Count
        | "CountBy" -> Some SupportedLinqMethods.CountBy
        | "LongCount" -> Some SupportedLinqMethods.LongCount
        | "Where" -> Some SupportedLinqMethods.Where
        | "Select" -> Some SupportedLinqMethods.Select
        | "SelectMany" -> Some SupportedLinqMethods.SelectMany
        | "ThenBy" -> Some SupportedLinqMethods.ThenBy
        | "ThenByDescending" -> Some SupportedLinqMethods.ThenByDescending
        | "OrderBy" -> Some SupportedLinqMethods.OrderBy
        | "Order" -> Some SupportedLinqMethods.Order
        | "OrderDescending" -> Some SupportedLinqMethods.OrderDescending
        | "OrderByDescending" -> Some SupportedLinqMethods.OrderByDescending
        | "Take" -> Some SupportedLinqMethods.Take
        | "Skip" -> Some SupportedLinqMethods.Skip
        | "First" -> Some SupportedLinqMethods.First
        | "FirstOrDefault" -> Some SupportedLinqMethods.FirstOrDefault
        | "DefaultIfEmpty" -> Some SupportedLinqMethods.DefaultIfEmpty
        | "Last" -> Some SupportedLinqMethods.Last
        | "LastOrDefault" -> Some SupportedLinqMethods.LastOrDefault
        | "Single" -> Some SupportedLinqMethods.Single
        | "SingleOrDefault" -> Some SupportedLinqMethods.SingleOrDefault
        | "All" -> Some SupportedLinqMethods.All
        | "Any" -> Some SupportedLinqMethods.Any
        | "Contains" -> Some SupportedLinqMethods.Contains
        | "Join" -> Some SupportedLinqMethods.Join
        | "Append" -> Some SupportedLinqMethods.Append
        | "Concat" -> Some SupportedLinqMethods.Concat
        | "GroupBy" -> Some SupportedLinqMethods.GroupBy
        | "Except" -> Some SupportedLinqMethods.Except
        | "ExceptBy" -> Some SupportedLinqMethods.ExceptBy
        | "Intersect" -> Some SupportedLinqMethods.Intersect
        | "IntersectBy" -> Some SupportedLinqMethods.IntersectBy
        | "Cast" -> Some SupportedLinqMethods.Cast
        | "OfType" -> Some SupportedLinqMethods.OfType
        | "Aggregate" -> Some SupportedLinqMethods.Aggregate
        | "Include" -> Some SupportedLinqMethods.Include
        | "Exclude" -> Some SupportedLinqMethods.Exclude
        | _ -> None

    /// <summary>
    /// Determines the appropriate SQL to select a value, extracting it from JSON if the type is not a primitive SQLite type.
    /// </summary>
    /// <param name="x">The .NET type of the value being selected.</param>
    /// <returns>An SQL string snippet for selecting the value.</returns>
    let internal extractValueAsJsonIfNecesary (x: Type) =
        let isPrimitive = QueryTranslator.isPrimitiveSQLiteType x

        if isPrimitive then
            "Value "
        else
            if (*unknown type*)
               x = typeof<obj> || typeof<JsonValue>.IsAssignableFrom x then
                "CASE WHEN typeof(Value) = 'blob' THEN json_extract(Value, '$') ELSE Value END "
            else
                "json_extract(Value, '$') "

    let rec internal isIdentityLambda (expr: Expression) =
        match expr with
        // Unwrap the quote node if present
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Quote ->
            isIdentityLambda ue.Operand
        // Check if it's a lambda
        | :? LambdaExpression as lambda ->
            match lambda.Body with
            | :? ParameterExpression as bodyParam ->
                Object.ReferenceEquals(bodyParam, lambda.Parameters.[0])
            | _ -> false
        | _ -> false

    let internal negatePredicateExpression (expr: Expression) =
        match expr with
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Quote ->
            match ue.Operand with
            | :? LambdaExpression as lambda ->
                let negated = Expression.Lambda(Expression.Not(lambda.Body), lambda.Parameters)
                Expression.Quote negated :> Expression
            | _ ->
                Expression.Not expr :> Expression
        | :? LambdaExpression as lambda ->
            Expression.Lambda(Expression.Not(lambda.Body), lambda.Parameters) :> Expression
        | _ ->
            Expression.Not expr :> Expression

    /// Null-safe negation for All() violation subquery.
    /// Produces NOT (predicate) OR (predicate) IS NULL so that rows with
    /// missing/null boolean fields are counted as violations instead of being
    /// silently excluded by SQL three-valued NOT NULL → NULL semantics.
    let internal negatePredicateForAllExpression (expr: Expression) =
        let wrapBody (body: Expression) (parameters: Collections.ObjectModel.ReadOnlyCollection<ParameterExpression>) =
            let negated = Expression.Not(body)
            let boxed = Expression.Convert(body, typeof<obj>)
            let isNull = Expression.Equal(boxed, Expression.Constant(null, typeof<obj>))
            let combined = Expression.OrElse(negated, isNull)
            Expression.Lambda(combined, parameters)
        match expr with
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Quote ->
            match ue.Operand with
            | :? LambdaExpression as lambda ->
                Expression.Quote (wrapBody lambda.Body lambda.Parameters) :> Expression
            | _ ->
                Expression.Not expr :> Expression
        | :? LambdaExpression as lambda ->
            wrapBody lambda.Body lambda.Parameters :> Expression
        | _ ->
            Expression.Not expr :> Expression

