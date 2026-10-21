namespace SoloDatabase

open System
open System.Collections.Generic
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorSerialization

/// Structural consumers share actual stored children and their default/presence finishing.
module internal QueryStructuralAccess =
    let child variables nextName name target parent =
        let finish = function
            | DirectValue(raw,input) when typeof<float>.Equals target || typeof<single>.Equals target ->
                match input.Source with
                | ValueSome token ->
                    let fallback = QueryDefault.comparisonValue variables target |> QueryProduced.value nextName
                    let value = SqlExpr.CaseExpr((SqlExpr.Unary(UnaryOperator.IsNull,token),fallback),[],
                                                Some(QueryProduced.comparisonValue target raw input))
                    DirectValue(value,{ input with Carrier = SqlScalar; Source = ValueSome(jsonToken value) })
                | ValueNone -> DirectValue(raw,input)
            | selected -> selected
        QueryTranslatorVisitCoreMemberAccess.accessValueFinishing (QueryDefault.value variables) nextName finish (ClrMember(name,target)) target parent

    let payload nextName parent =
        QueryProduced.bind nextName (fun value input -> DirectValue(value,serializationPayload input)) parent

    let scalar (variables: Dictionary<string,obj>) nextName (target: Type) produced =
        let target = if target.IsEnum then TypeMetadata.enumUnderlying target else target
        QueryProduced.consume nextName (fun raw input ->
            let value = QueryProduced.comparisonValue target raw input
            if target.IsValueType && not(typeof<float>.Equals target || typeof<single>.Equals target) then
                let fallback = QueryDefault.comparisonValue variables target |> QueryProduced.value nextName
                SqlExpr.Coalesce(value,[fallback])
            else value) produced
