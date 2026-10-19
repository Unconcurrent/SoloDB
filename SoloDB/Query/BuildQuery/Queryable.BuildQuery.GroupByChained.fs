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
    let private groupAdapter
        (sourceCtx: QueryContext) (innerSelect: SqlSelect) (groupRowAlias: string)
        (groupParam: ParameterExpression) (vars: Dictionary<string, obj>)
        (captures: Dictionary<ParameterExpression, ParameterSource>) producer =
        let itemInput =
            match sourceCtx.SerializationInput.Facts with
            | JsonSerializator.JsonSerializationShape.GroupingValues _ -> QueryTranslatorSerialization.serializationElement sourceCtx.SerializationInput
            | _ -> sourceCtx.SerializationInput
        let rowCtx = { sourceCtx with LayerPosition = OuterLayer; SerializationInput = itemInput }
        let adapter: OrderedChainPlan.Adapter = {
            MembershipContext = DefaultMembership
            LambdaContext = "group"
            Validate = ignore
            IsRoot = fun e -> obj.ReferenceEquals(e, groupParam)
            IsValue = fun e -> QueryTranslatorBaseHelpers.isFullyConstant e || (sourceCtx.BindQueryValue |> ValueOption.exists (fun b -> b.IsValue e))
            Alias = fun () -> GroupByAliases.nextSubquery sourceCtx
            Value = fun e -> QueryTranslator.translateScopedBody rowCtx groupRowAlias e
                                (System.Collections.ObjectModel.ReadOnlyCollection<ParameterExpression>(Array.empty))
                                (ValueSome captures) (ValueSome producer) false vars
            Comparison = fun e ->
                QueryTranslator.translateComparisonScopedBody rowCtx groupRowAlias e
                    (System.Collections.ObjectModel.ReadOnlyCollection<ParameterExpression>(Array.empty))
                    (ValueSome captures) (ValueSome producer) false vars
            Translate = fun _ input alias l handler ->
                let child = { rowCtx with SerializationInput = input; Joins = ResizeArray() }
                let producer expression =
                    handler |> ValueOption.bind (fun handler -> handler expression) |> ValueOption.orElseWith (fun () -> producer expression)
                let value = QueryTranslator.translateScopedBody child alias l.Body l.Parameters
                                (ValueSome captures) (ValueSome producer) false vars
                value, QueryTranslatorReferenceJoin.preparedJoinClauses child.Joins
            Source = fun _ _ ->
                let a = GroupByAliases.nextSubquery sourceCtx
                let ps = [{Alias=Some "Id";Expr=SqlExpr.Column(Some a,"Id")}
                          {Alias=Some "Value";Expr=SqlExpr.Literal SqlLiteral.Null}]
                let row =
                    { OrderedChainRows.core (Some(DerivedTable(innerSelect,a))) ps with
                        Where =
                            let keyType = groupParam.Type.GetGenericArguments().[0]
                            let keyInput = sourceCtx.SerializationInput.Children[0]
                            if QueryDefaultEquality.requiresPrepared keyType keyInput then
                                let leftInput = QueryTranslatorSerialization.scopeSerializationInput (Some a) OuterLayer keyInput
                                let actualKey = producer (Expression.Property(groupParam, "Key"))
                                                |> ValueOption.defaultWith (fun () -> invalidOp "A completed group requires its actual key producer.")
                                let left = QueryDefaultEquality.key sourceCtx.NextAlias keyType (SqlExpr.Column(Some a, syntheticGroupKeyAlias)) leftInput
                                Some(QueryProduced.consume sourceCtx.NextAlias (fun value input ->
                                    QueryDefaultEquality.equal left (QueryDefaultEquality.key sourceCtx.NextAlias keyType value input)) actualKey)
                            else Some(SqlExpr.Binary(SqlExpr.Column(Some a, syntheticGroupKeyAlias), BinaryOperator.Is,
                                                     SqlExpr.Column(Some groupRowAlias, syntheticGroupKeyAlias)))
                        OrderBy = [] }
                    |> QueryProduced.withPosition "__ord"
                let input = QueryTranslatorSerialization.scopeSerializationInput (Some a) OuterLayer itemInput
                let produced = QueryProduced.canonical sourceCtx.NextAlias (DirectValue(SqlExpr.Column(Some a, "Value"), input))
                QueryProduced.publishCore sourceCtx.NextAlias row produced false
        }
        adapter

    let tryTranslateGroupByChainedExpr sourceCtx innerSelect groupRowAlias groupParam vars captures producer expr =
        OrderedChain.tryBuild (groupAdapter sourceCtx innerSelect groupRowAlias groupParam vars captures producer) expr

    let collection sourceCtx innerSelect groupRowAlias groupParam vars captures producer resultType =
        OrderedChain.collection (groupAdapter sourceCtx innerSelect groupRowAlias groupParam vars captures producer)
            groupParam resultType
