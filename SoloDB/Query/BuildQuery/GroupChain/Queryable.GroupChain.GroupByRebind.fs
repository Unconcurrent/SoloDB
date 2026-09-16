namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Linq.Expressions
open System.Threading
open SoloDatabase.SqlModel
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryableHelperBase
open SoloDatabase.QueryableHelperJoin

/// GroupBy chained-expression support — correlated subqueries for
/// group-item chains that go beyond simple aggregate whitelist.
/// Uses shared Terminal DU + walkChain extraction with per-context GroupBy building.

/// Rebinding a group-element expression so it reads from a subquery alias, and normalising the
/// scalar shape the surrounding SQL expects.
module internal GroupByRebind =
    let tryFindSingleParameter (expr: Expression) =
        let seen = ResizeArray<ParameterExpression>()
        let rec visit (e: Expression) =
            if not (isNull e) then
                match e with
                | :? ParameterExpression as p ->
                    if not (seen |> Seq.exists (fun existing -> Object.ReferenceEquals(existing, p))) then
                        seen.Add(p)
                | :? LambdaExpression as lambda ->
                    visit lambda.Body
                | :? UnaryExpression as u ->
                    visit u.Operand
                | :? BinaryExpression as b ->
                    visit b.Left
                    visit b.Right
                    visit b.Conversion
                | :? MethodCallExpression as mc ->
                    visit mc.Object
                    mc.Arguments |> Seq.iter visit
                | :? MemberExpression as m ->
                    visit m.Expression
                | :? ConditionalExpression as c ->
                    visit c.Test
                    visit c.IfTrue
                    visit c.IfFalse
                | :? InvocationExpression as i ->
                    visit i.Expression
                    i.Arguments |> Seq.iter visit
                | :? NewExpression as n ->
                    n.Arguments |> Seq.iter visit
                | :? NewArrayExpression as na ->
                    na.Expressions |> Seq.iter visit
                | :? MemberInitExpression as mi ->
                    visit mi.NewExpression
                    mi.Bindings
                    |> Seq.iter (function
                        | :? MemberAssignment as ma -> visit ma.Expression
                        | _ -> ())
                | :? ListInitExpression as li ->
                    visit li.NewExpression
                    li.Initializers |> Seq.collect (fun init -> init.Arguments) |> Seq.iter visit
                | _ -> ()
        visit expr
        match seen |> Seq.toList with
        | [param] -> Some param
        | _ -> None

    /// Translate a group-chain expression against the correlated subquery alias.
    let translateExprAgainst (sourceCtx: QueryContext) (subAlias: string) (vars: Dictionary<string, obj>) (expr: Expression) : SqlExpr =
        match QueryTranslatorVisitPost.tryExtractLambdaExpression expr with
        | ValueSome lambda ->
            translateExprDu sourceCtx subAlias (lambda :> Expression) vars
        | ValueNone ->
            translateJoinSingleSourceExpression sourceCtx subAlias vars (tryFindSingleParameter expr) expr

    let normalizeScalarExpr (exprType: Type) (expr: SqlExpr) =
        if QueryTranslatorBaseTypes.isPrimitiveSQLiteType exprType then
            SqlExpr.CaseExpr(
                (SqlExpr.Binary(
                    SqlExpr.FunctionCall("typeof", [expr]),
                    BinaryOperator.Eq,
                    SqlExpr.Literal(SqlLiteral.String "blob")),
                 SqlExpr.FunctionCall("json_extract", [expr; SqlExpr.Literal(SqlLiteral.String "$")])),
                [],
                Some expr)
        else
            expr

    /// Build the null-safe correlation predicate.
    /// Translates the group key expression against the subquery alias and compares to the outer group key.
