namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.JsonSerializator

/// Scalar callable equality consumes original stored values, not default comparison keys.
module internal QueryComparers =
    let scalar (target: Type) = JsonValue.HasScalarCarrier target && (target.IsValueType || typeof<string>.Equals target)

    let prepareWithProbe (context: QueryContext) (variables: Dictionary<string,obj>) target (expression: Expression) (probe: Expression option) =
        if not(scalar target) then
            raise(NotSupportedException("This comparer needs original model key construction. Normalize its key in a selector or call AsEnumerable() before the operation."))
        let equal = ModelMetadata.EqualityOperation target
        let acquire (values: obj[]) =
            let comparer = values[values.Length-1]
            NativeQueryEquality(Func<string,string,bool>(fun left right ->
                let right = if probe.IsSome then values[0] else JsonValue.ParseRuntime(target,right)
                equal.Invoke(comparer,JsonValue.ParseRuntime(target,left),right)))
        let inputs = match probe with Some probe -> [|probe;expression|] | None -> [|expression|]
        let operation = BoundValueCallExpression(inputs,typeof<NativeQueryEquality>,Func<obj array,obj>(fun values -> box(acquire values)))
        match context.BindQueryValue with
        | ValueSome binder when binder.IsValue operation -> binder.Scalar operation
        | _ ->
            let value = ExpressionEvaluation.evaluateExpr<obj> operation
            let name = "dp" + variables.Count.ToString(Globalization.CultureInfo.InvariantCulture)
            variables.Add(name,value)
            SqlExpr.Parameter name

    let prepare context variables target expression = prepareWithProbe context variables target expression None

    let key nextName produced =
        produced |> QueryProduced.canonical nextName
        |> QueryProduced.consume nextName (fun value _ -> SqlExpr.FunctionCall("json",[value]))

    let equal operation left right = SqlExpr.FunctionCall(NativeQueryEquality.FunctionName,[operation;left;right])

    let join (target: Type) operation left right =
        let relation = equal operation left right
        if target.IsValueType && isNull(TypeMetadata.nullableUnderlying target) then relation else
        let present value = SqlExpr.Binary(SqlExpr.FunctionCall("json_type",[value]),BinaryOperator.Ne,SqlExpr.Literal(SqlLiteral.String "null"))
        SqlExpr.CaseExpr((SqlExpr.Binary(present left,BinaryOperator.And,present right),relation),[],Some(SqlExpr.Literal(SqlLiteral.Boolean false)))
