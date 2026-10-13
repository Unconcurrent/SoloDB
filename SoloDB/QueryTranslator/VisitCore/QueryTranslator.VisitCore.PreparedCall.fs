namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers

/// Closed operations prepare layout constants and operands at their original sampling positions.
module internal QueryTranslatorVisitCorePreparedCall =
    let private tryLayout visitProduced (m: MethodCallExpression) (qb: QueryBuilder) render =
        let layout =
            if typeof<string>.Equals m.Method.DeclaringType then
                m.Arguments |> Seq.tryFindIndex (fun argument -> typeof<StringComparison>.Equals(argument.Type))
            elif m.Method.Name = "ToString" && not(isNull m.Object) && m.Arguments.Count > 0 &&
                 DateTimeFunctions.supportsTemporalToStringTranslationType m.Object.Type then Some 0
            else None
        match layout, qb.SourceContext.BindQueryValue with
        | Some index, ValueSome binder when isFullyConstant m.Arguments[index] && not(m.Arguments[index] :? ConstantExpression) ->
            let prepared = binder.PrepareConstant m.Arguments[index] m.Type
            let arguments = m.Arguments |> Seq.mapi (fun i argument -> i, argument) |> Seq.filter (fun (i, _) -> i <> index) |> Seq.toArray
            let expressions = Array.append (if isNull m.Object then Array.empty else [|m.Object|]) (arguments |> Array.map snd)
            let components = expressions |> Array.map (fun expression -> visitProduced expression qb)
            let produce sampled =
                let args = m.Arguments |> Seq.toArray
                args[index] <- Expression.Constant(sampled, m.Arguments[index].Type)
                let call = m.Update(m.Object, args)
                QueryProduced.construct qb.SourceContext.NextAlias (fun components ->
                    let operand expression _ =
                        match expressions |> Array.tryFindIndex (fun original -> obj.ReferenceEquals(original, expression)) with
                        | Some i -> fst components[i]
                        | None -> invalidOp "The operation requested an unprepared operand."
                    let stringOperand _ ignoreCase expression =
                        let value = operand expression qb
                        if ignoreCase then SqlExpr.FunctionCall("TO_LOWER", [value]) else value
                    QueryProduced.typed m.Type (render operand stringOperand call)) components
            Some(match prepared with PreparedConstant value -> produce value | DeferredConstant complete -> complete produce)
        | _ -> None

    let internal tryProduced visitProduced visitComparisonProduced (m: MethodCallExpression) (qb: QueryBuilder) render =
        match QueryTranslatorCharacters.trimmingOperation m with
        | ValueSome(kind,input) ->
            let source = visitProduced m.Object qb
            let argument =
                match input with
                | QueryTranslatorCharacters.Character -> visitComparisonProduced m.Arguments[0] qb
                | QueryTranslatorCharacters.Characters -> visitProduced m.Arguments[0] qb
                | _ -> QueryProduced.typed typeof<char[]> (SqlExpr.Literal SqlLiteral.Null)
            Some(QueryProduced.construct qb.SourceContext.NextAlias (fun components ->
                QueryTranslatorCharacters.trimming kind input (fst components[0]) (fst components[1])
                |> QueryProduced.typed m.Type) [|source;argument|])
        | ValueNone -> tryLayout visitProduced m qb render
