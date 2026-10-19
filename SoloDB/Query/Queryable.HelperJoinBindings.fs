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
    type internal JoinSourceBinding =
    | NoJoinSource
    | OuterJoinSource
    | InnerJoinSource
    | MixedJoinSource

    let internal combineJoinSourceBinding left right =
        match left, right with
        | MixedJoinSource, _
        | _, MixedJoinSource -> MixedJoinSource
        | NoJoinSource, other
        | other, NoJoinSource -> other
        | OuterJoinSource, OuterJoinSource -> OuterJoinSource
        | InnerJoinSource, InnerJoinSource -> InnerJoinSource
        | _ -> MixedJoinSource

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
        | :? BlockExpression when QueryConstructionExpressions.hasConstructionTail expr -> true
        | :? BoundValueCallExpression as call when not(isNull call.ProjectionNames) || isTuple call.Type -> true
        | :? NewExpression
        | :? MemberInitExpression
        | :? NewArrayExpression -> true
        | _ -> false

    let rec internal classifyJoinResultExpression (outerParam: ParameterExpression) (innerParam: ParameterExpression) (expr: Expression) =
        if isNull expr then
            NoJoinSource
        else
            match expr with
            | :? ParameterExpression as p ->
                if Object.ReferenceEquals(p, outerParam) then OuterJoinSource
                elif Object.ReferenceEquals(p, innerParam) then InnerJoinSource
                else NoJoinSource
            | :? MemberExpression as m ->
                classifyJoinResultExpression outerParam innerParam m.Expression
            | :? UnaryExpression as u ->
                classifyJoinResultExpression outerParam innerParam u.Operand
            | :? BinaryExpression as b ->
                combineJoinSourceBinding
                    (classifyJoinResultExpression outerParam innerParam b.Left)
                    (classifyJoinResultExpression outerParam innerParam b.Right)
            | :? ConditionalExpression as c ->
                [ classifyJoinResultExpression outerParam innerParam c.Test
                  classifyJoinResultExpression outerParam innerParam c.IfTrue
                  classifyJoinResultExpression outerParam innerParam c.IfFalse ]
                |> List.reduce combineJoinSourceBinding
            | :? MethodCallExpression as m ->
                let objectBinding =
                    if isNull m.Object then NoJoinSource
                    else classifyJoinResultExpression outerParam innerParam m.Object
                m.Arguments
                |> Seq.map (classifyJoinResultExpression outerParam innerParam)
                |> Seq.fold combineJoinSourceBinding objectBinding
            | :? BoundValueAccessExpression as access -> classifyJoinResultExpression outerParam innerParam access.Parent
            | :? BoundValueCallExpression as call ->
                call.Inputs
                |> Seq.map (classifyJoinResultExpression outerParam innerParam)
                |> Seq.fold combineJoinSourceBinding NoJoinSource
            | :? NewExpression as n ->
                n.Arguments
                |> Seq.map (classifyJoinResultExpression outerParam innerParam)
                |> Seq.fold combineJoinSourceBinding NoJoinSource
            | :? MemberInitExpression as mi ->
                let start = classifyJoinResultExpression outerParam innerParam mi.NewExpression
                mi.Bindings
                |> Seq.fold (fun acc binding ->
                    match binding with
                    | :? MemberAssignment as ma ->
                        combineJoinSourceBinding acc (classifyJoinResultExpression outerParam innerParam ma.Expression)
                    | _ -> MixedJoinSource) start
            | :? NewArrayExpression as na ->
                na.Expressions
                |> Seq.map (classifyJoinResultExpression outerParam innerParam)
                |> Seq.fold combineJoinSourceBinding NoJoinSource
            | :? InvocationExpression as i ->
                let exprBinding = classifyJoinResultExpression outerParam innerParam i.Expression
                i.Arguments
                |> Seq.map (classifyJoinResultExpression outerParam innerParam)
                |> Seq.fold combineJoinSourceBinding exprBinding
            | :? ListInitExpression as li ->
                let start = classifyJoinResultExpression outerParam innerParam li.NewExpression
                li.Initializers
                |> Seq.collect (fun init -> init.Arguments)
                |> Seq.map (classifyJoinResultExpression outerParam innerParam)
                |> Seq.fold combineJoinSourceBinding start
            | :? ConstantExpression -> NoJoinSource
            | _ -> MixedJoinSource

    let internal translateJoinSingleSourceProduced (ctx: QueryContext) (tableAlias: string) (vars: Dictionary<string, obj>) (parameter: ParameterExpression option) (expr: Expression) =
        let lambdaExpr =
            match parameter with
            | Some p -> Expression.Lambda(expr, p) :> Expression
            | None -> Expression.Lambda(expr) :> Expression
        QueryTranslator.translateProduced ctx tableAlias lambdaExpr vars

    let internal translateJoinSingleSourceExpression ctx tableAlias vars parameter expr =
        translateJoinSingleSourceProduced ctx tableAlias vars parameter expr
        |> QueryProduced.value ctx.NextAlias

    let internal translateJoinResultSelectorProduced
        (outerCtx: QueryContext) (innerCtx: QueryContext)
        (vars: Dictionary<string, obj>) (outerAlias: string) (innerAlias: string)
        (outerParam: ParameterExpression) (innerParam: ParameterExpression) (expr: Expression)
        (producedParameters: (QueryProducedValue * QueryProducedValue) voption) =
        let expr = QueryTranslatorBaseHelpers.normalizeStoredConstruction expr
        match classifyJoinResultExpression outerParam innerParam expr with
        | MixedJoinSource ->
            match expr with
            | :? NewExpression | :? MemberInitExpression -> ()
            | :? BlockExpression when QueryConstructionExpressions.hasConstructionTail expr -> ()
            | :? BoundValueCallExpression as call when isTuple call.Type || not(isNull call.ProjectionNames) -> ()
            | _ ->
                raise (NotSupportedException(
                    "Error: Join result selector is not supported.\nReason: Mixed-source selector expressions are limited to object and tuple construction here.\nFix: Project outer and inner members into an anonymous object or move the projection after AsEnumerable()."))
        | _ -> ()
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
