namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq.Expressions
open Utils
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.SqlModel
open SoloDatabase.QueryableHelperBase

/// Joined selectors share admission and their actual scoped parameter producers.
module internal QueryableHelperJoinBindings =
    let internal unwrapLambdaExpressionOrThrow (operationName: string) (expr: Expression) : LambdaExpression =
        match unwrapQuotedLambda expr with
        | :? LambdaExpression as lambda -> lambda
        | _ ->
            raise (NotSupportedException(
                $"Error: {operationName} is not supported.
Reason: Expected a quoted lambda expression.
Fix: Rewrite the query to use direct lambda arguments or move it after AsEnumerable()."))

    let rec internal referencesParam (param: ParameterExpression) (expr: Expression) =
        if isNull expr then
            false
        elif obj.ReferenceEquals(expr, param) then
            true
        else
            match expr with
            | :? MethodCallExpression as mc ->
                (not (isNull mc.Object) && referencesParam param mc.Object)
                || (mc.Arguments |> Seq.exists (referencesParam param))
            | :? MemberExpression as me ->
                not (isNull me.Expression) && referencesParam param me.Expression
            | :? UnaryExpression as ue ->
                referencesParam param ue.Operand
            | :? BinaryExpression as be ->
                referencesParam param be.Left || referencesParam param be.Right
            | :? ConditionalExpression as ce ->
                referencesParam param ce.Test || referencesParam param ce.IfTrue || referencesParam param ce.IfFalse
            | :? BoundValueCallExpression as call -> call.Inputs |> Array.exists (referencesParam param)
            | :? BoundValueAccessExpression as access -> referencesParam param access.Parent
            | :? NewExpression as ne ->
                ne.Arguments |> Seq.exists (referencesParam param)
            | :? MemberInitExpression as mi ->
                referencesParam param mi.NewExpression
                || (mi.Bindings
                    |> Seq.exists (fun binding ->
                        match binding with
                        | :? MemberAssignment as ma -> referencesParam param ma.Expression
                        | _ -> false))
            | :? NewArrayExpression as na ->
                na.Expressions |> Seq.exists (referencesParam param)
            | :? InvocationExpression as ie ->
                referencesParam param ie.Expression
                || (ie.Arguments |> Seq.exists (referencesParam param))
            | :? ListInitExpression as li ->
                referencesParam param li.NewExpression
                || (li.Initializers |> Seq.collect (fun init -> init.Arguments) |> Seq.exists (referencesParam param))
            | _ ->
                false

    let internal isCompositeJoinKeyBody (expr: Expression) =
        match expr with
        | _ when DateTimeSql.isDateTimeLikeType expr.Type || (QueryTranslatorNumeric.domain expr.Type).IsSome -> false
        | :? BlockExpression when QueryConstructionExpressions.hasConstructionTail expr -> true
        | :? BoundValueCallExpression as call when not(isNull call.ProjectionNames) || isTuple call.Type -> true
        | :? NewExpression
        | :? MemberInitExpression
        | :? NewArrayExpression -> true
        | _ -> false

    let internal translateJoinSingleSourceProduced (ctx: QueryContext) (tableAlias: string) (vars: Dictionary<string, obj>) (parameter: ParameterExpression option) (expr: Expression) =
        let lambdaExpr =
            match parameter with
            | Some p -> Expression.Lambda(expr, p) :> Expression
            | None -> Expression.Lambda(expr) :> Expression
        QueryTranslator.translateProduced ctx tableAlias lambdaExpr vars

    let internal translateJoinSingleSourceExpression ctx tableAlias vars parameter expr =
        translateJoinSingleSourceProduced ctx tableAlias vars parameter expr
        |> QueryProduced.value ctx.NextAlias

    type internal JoinKeyShape =
        | AtomicKey of Type
        | ProductKey of Type * (string * JoinKeyShape) array
        | OptionalKey of Type * JoinKeyShape

    let private describedKey target =
        let rec describe = function
            | QueryStructuralShape.Leaf t -> AtomicKey t
            | QueryStructuralShape.Optional(t,child) -> OptionalKey(t,describe child)
            | QueryStructuralShape.Components(t,members) -> ProductKey(t,members |> Array.map(fun (name,child) -> name,describe child))
        match QueryStructuralShape.tryCreate target with
        | QueryStructuralShape.Available graph when not graph.Recursive -> describe graph.Root
        | _ -> AtomicKey target

    let private compilerProduct (target: Type) =
        target.IsDefined(typeof<System.Runtime.CompilerServices.CompilerGeneratedAttribute>,false) || TypeMetadata.standardComparison target

    let rec private keyShape (expression: Expression) =
        let expression = QueryTranslatorBaseHelpers.normalizeStoredConstruction expression
        match expression with
        | :? NewExpression as created when Utils.isTuple created.Type ->
            ProductKey(created.Type,created.Arguments |> Seq.mapi(fun i value -> (if i=7 then "Rest" else "Item"+string(i+1)),keyShape value) |> Seq.toArray)
        | :? NewExpression as created when compilerProduct created.Type ->
            match QueryConstructionExpressions.memberNames created with
            | ValueSome names -> ProductKey(created.Type,Array.map2 (fun name value -> name,keyShape value) names (Seq.toArray created.Arguments))
            | _ -> describedKey expression.Type
        | :? BoundValueCallExpression as created when not(isNull created.ProjectionNames) && compilerProduct created.Type ->
            ProductKey(created.Type,Array.map2 (fun name value -> name,keyShape value) created.ProjectionNames created.Inputs)
        | _ -> describedKey expression.Type

    let rec private mergeKeyShape left right =
        match left,right with
        | ProductKey(t,left),ProductKey(_,right) ->
            ProductKey(t,Array.map2(fun (name,left) (_,right) -> name,mergeKeyShape left right) left right)
        | OptionalKey(t,left),OptionalKey(_,right) -> OptionalKey(t,mergeKeyShape left right)
        | OptionalKey _,_ -> left
        | _,OptionalKey _ -> right
        | ProductKey _,_ -> left
        | _,ProductKey _ -> right
        | _ -> left

    let internal joinKeyShape (left: LambdaExpression) (right: LambdaExpression) =
        mergeKeyShape (keyShape left.Body) (keyShape right.Body)

    let internal keyColumns alias = function
        | [name] -> SqlExpr.Column(Some alias,name)
        | names -> SqlExpr.RowValueExpr(names |> List.map(fun name -> SqlExpr.Column(Some alias,name)))

    let internal keyExpression = function [value] -> value | values -> SqlExpr.RowValueExpr values

    /// Complete operands retain their actual input and are bound once before decomposition.
    let internal prepareJoinKey shape (context: QueryContext) variables produced =
        let presence root parent =
            let missing = QueryStructuralAccess.presence context.NextAlias parent
            if root then SqlExpr.CaseExpr((missing,SqlExpr.Literal SqlLiteral.Null),[],Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))
            else missing
        let rec fields shape root produced =
            let struct(values,children),parents = QueryProduced.bindTerms context.NextAlias (fun value input ->
                match shape with
                | AtomicKey target ->
                    let value,input =
                        if DateTimeSql.isDateTimeLikeType target then
                            DateTimeSql.canonicalizeForCompareOrOrder target (QueryProduced.comparisonValue target value input),
                            {input with Carrier=SqlScalar;Source=ValueNone}
                        else value,input
                    QueryDefaultEquality.key context.NextAlias target value input |> QueryDefaultEquality.components,[]
                | OptionalKey(_,child) ->
                    let parent = DirectValue(value,input)
                    let values,edges = QueryStructuralAccess.payload context.NextAlias parent |> fields child false
                    presence root parent :: values,edges
                | ProductKey(_,members) ->
                    let parent = DirectValue(value,input)
                    let selected = members |> Array.map(fun (name,shape) ->
                        let target = match shape with AtomicKey t | ProductKey(t,_) | OptionalKey(t,_) -> t
                        QueryStructuralAccess.child variables context.NextAlias name target parent |> fields shape false)
                    presence root parent :: (selected |> Array.toList |> List.collect fst), selected |> Array.toList |> List.collect snd) produced
            values,parents @ children
        match shape with
        | AtomicKey _ ->
            let value,joins = QueryProduced.bindTerms context.NextAlias (fun value _ -> value) produced
            [value],joins
        | _ -> fields shape true produced

    let internal translateJoinResultSelectorProduced
        (outerCtx: QueryContext) (innerCtx: QueryContext)
        (vars: Dictionary<string, obj>) (outerAlias: string) (innerAlias: string)
        (outerParam: ParameterExpression) (innerParam: ParameterExpression) (expr: Expression)
        (producedParameters: (QueryProducedValue * QueryProducedValue) voption) =
        let expr = QueryTranslatorBaseHelpers.normalizeStoredConstruction expr
        let sources = Dictionary<ParameterExpression, ParameterSource>()
        match producedParameters with
        | ValueSome(outerValue, innerValue) ->
            sources.Add(outerParam, { Alias = None; Context = outerCtx; Value = ProducedParameter outerValue })
            sources.Add(innerParam, { Alias = None; Context = innerCtx; Value = ProducedParameter innerValue })
        | ValueNone ->
            sources.Add(outerParam, { Alias = Some outerAlias; Context = outerCtx; Value = SourceParameter })
            sources.Add(innerParam, { Alias = Some innerAlias; Context = innerCtx; Value = SourceParameter })
        let parameters = System.Collections.ObjectModel.ReadOnlyCollection<ParameterExpression>([| outerParam; innerParam |])
        QueryTranslator.translateScopedBody outerCtx outerAlias expr parameters (ValueSome sources) ValueNone producedParameters.IsSome vars
