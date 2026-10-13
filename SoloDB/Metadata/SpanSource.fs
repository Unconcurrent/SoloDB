namespace SoloDatabase

open System
open System.Linq.Expressions

/// Closed span construction retains its original source and authored bounds.
module internal QuerySpanSource =
    type Source = { Value: Expression; Element: Type; String: bool; Start: Expression option; Length: Expression option }
    let private readOnlySpan (target: Type) =
        target.IsGenericType && target.Assembly = typeof<string>.Assembly &&
        target.GetGenericTypeDefinition().FullName = "System.ReadOnlySpan`1"
    let private parameters (types: Type array) (actual: Reflection.ParameterInfo array) =
        types.Length = actual.Length && Array.forall2 (fun (expected: Type) (parameter: Reflection.ParameterInfo) -> expected.Equals parameter.ParameterType) types actual
    let rec trySource (expression: Expression) =
        match expression with
        | :? UnaryExpression as conversion when conversion.NodeType = ExpressionType.Convert && isNull conversion.Method -> trySource conversion.Operand
        | _ when not(readOnlySpan expression.Type) -> None
        | _ ->
            let element = expression.Type.GetGenericArguments()[0]
            let array = element.MakeArrayType()
            let make source isString arguments =
                Some { Value = source; Element = element; String = isString
                       Start = arguments |> List.tryHead; Length = arguments |> List.tryItem 1 }
            match expression with
            | :? NewExpression as created when not(isNull created.Constructor) &&
                 (parameters [|array|] (created.Constructor.GetParameters()) || parameters [|array;typeof<int>;typeof<int>|] (created.Constructor.GetParameters())) ->
                make created.Arguments[0] false (created.Arguments |> Seq.skip 1 |> Seq.toList)
            | :? MethodCallExpression as call when call.Method.IsStatic && typeof<MemoryExtensions>.Equals call.Method.DeclaringType && call.Method.Name = "AsSpan" &&
                 typeof<char>.Equals element &&
                 (parameters [|typeof<string>|] (call.Method.GetParameters()) || parameters [|typeof<string>;typeof<int>|] (call.Method.GetParameters()) ||
                  parameters [|typeof<string>;typeof<int>;typeof<int>|] (call.Method.GetParameters())) ->
                make call.Arguments[0] true (call.Arguments |> Seq.skip 1 |> Seq.toList)
            | :? MethodCallExpression as call when call.Method.IsStatic && expression.Type.Equals call.Method.DeclaringType && call.Method.Name = "op_Implicit" &&
                 (parameters [|array|] (call.Method.GetParameters()) || typeof<char>.Equals element && parameters [|typeof<string>|] (call.Method.GetParameters())) ->
                make call.Arguments[0] (typeof<string>.Equals call.Arguments[0].Type) []
            | :? UnaryExpression as conversion when not(isNull conversion.Method) && conversion.Method.IsStatic && expression.Type.Equals conversion.Method.DeclaringType &&
                 conversion.Method.Name = "op_Implicit" &&
                 (parameters [|array|] (conversion.Method.GetParameters()) || typeof<char>.Equals element && parameters [|typeof<string>|] (conversion.Method.GetParameters())) ->
                make conversion.Operand (typeof<string>.Equals conversion.Operand.Type) []
            | _ -> None

    let tryWholeArray expression =
        trySource expression |> Option.bind (fun source -> if not source.String && source.Start.IsNone then Some source.Value else None)

    /// Subtraction avoids overflow even when the supplied length is Int32.MaxValue.
    let tryBounds available start length =
        let length = length |> Option.defaultValue (available - start)
        if start < 0L || start > available || length < 0L || length > available - start then ValueNone
        else ValueSome(struct(int start, int length))

    let bounds stringSource available start length =
        match tryBounds available start length with
        | ValueSome range -> range
        | ValueNone ->
            if stringSource then raise(ArgumentOutOfRangeException("start"))
            else raise(ArgumentOutOfRangeException())
