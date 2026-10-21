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

    let private storedScalar (variables: Dictionary<string,obj>) nextName (target: Type) produced =
        let target = if target.IsEnum then TypeMetadata.enumUnderlying target else target
        QueryProduced.consume nextName (fun raw input ->
            let value = QueryProduced.comparisonValue target raw input
            if target.IsValueType && not(typeof<float>.Equals target || typeof<single>.Equals target) then
                let fallback = QueryDefault.comparisonValue variables target |> QueryProduced.value nextName
                SqlExpr.Coalesce(value,[fallback])
            else value) produced

    let presence nextName produced =
        QueryProduced.consume nextName (fun raw input ->
            let stored=SqlExpr.Unary(UnaryOperator.IsNull,nullCarrier input.Carrier raw)
            match input.OriginalComparisonOperand with
            | ValueNone -> stored
            | ValueSome original -> QueryStructuralOperand.consume nextName original (fun node -> SqlExpr.FunctionCall("jsonb_extract",[node;SqlExpr.Literal(SqlLiteral.String "$[2]")])) stored) produced

    let scalar variables nextName target produced =
        QueryProduced.consume nextName (fun raw input ->
            let stored=storedScalar variables nextName target (DirectValue(raw,input))
            match input.OriginalComparisonOperand with
            | ValueNone -> stored
            | ValueSome original ->
                QueryStructuralOperand.consume nextName original (fun node ->
                    if typeof<string>.Equals target then
                        SqlExpr.FunctionCall("base64",[SqlExpr.FunctionCall("jsonb_extract",[node;SqlExpr.Literal(SqlLiteral.String "$[3]")])])
                    else
                        let payload=SqlExpr.FunctionCall("jsonb",[SqlExpr.Binary(node,BinaryOperator.JsonText,SqlExpr.Literal(SqlLiteral.String "$[3]"))])
                        storedScalar variables nextName target (DirectValue(payload,{QuerySerializationInput.ForType target with Source=ValueSome payload;Carrier=CanonicalToken}))) stored) produced
