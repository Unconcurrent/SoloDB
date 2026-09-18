namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryableHelperBase
open SoloDatabase.QueryableGroupByAliases
open SoloDatabase.QueryTranslatorBaseTypes

/// Supplies group correlation and expression binding to the shared ordered-chain owner.
/// Direct aggregate forms are selected by GroupByOps before reaching this adapter.
module internal QueryableBuildQueryGroupByChained =
    let tryTranslateGroupByChainedExpr
        (sourceCtx: QueryContext) (innerSelect: SqlSelect) (groupRowAlias: string)
        (groupParam: ParameterExpression) (vars: Dictionary<string, obj>) (expr: Expression) : SqlExpr option =
        let adapter: OrderedChainPlan.Adapter = {
            EntityMembershipById = false
            LambdaContext = "group"
            Validate = ignore
            IsRoot = fun e -> obj.ReferenceEquals(e, groupParam)
            IsValue = fun e -> QueryTranslatorBaseHelpers.isFullyConstant e || (sourceCtx.BindQueryValue |> ValueOption.exists (fun b -> b.IsValue e))
            Alias = fun () -> GroupByAliases.nextSubquery sourceCtx
            Value = fun e -> translateExprDu sourceCtx groupRowAlias e vars
            Translate = fun _ alias l -> translateExprDu sourceCtx alias (l :> Expression) vars, []
            Source = fun _ _ ->
                let a = GroupByAliases.nextSubquery sourceCtx
                let ps = [{Alias=Some "Id";Expr=SqlExpr.Column(Some a,"Id")}
                          {Alias=Some "Value";Expr=SqlExpr.Column(Some a,"Value")}
                          {Alias=Some "__ord";Expr=SqlExpr.Column(Some a,"Id")}]
                {Ctes=[];Body=SingleSelect {OrderedChainRows.core (Some(DerivedTable(innerSelect,a))) ps with
                                              Where=Some(SqlExpr.Binary(SqlExpr.Column(Some a, syntheticGroupKeyAlias), BinaryOperator.Is, SqlExpr.Column(Some groupRowAlias, syntheticGroupKeyAlias)))
                                              OrderBy=[{Expr=SqlExpr.Column(Some a,"Id");Direction=SortDirection.Asc}]}}
        }
        OrderedChain.tryBuild adapter expr
