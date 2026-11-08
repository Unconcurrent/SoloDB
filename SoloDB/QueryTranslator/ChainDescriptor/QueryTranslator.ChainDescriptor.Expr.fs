namespace SoloDatabase

open System
open System.Linq
open System.Linq.Expressions
open System.Reflection
open System.Collections.ObjectModel
open SoloDatabase.SqlModel
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorVisitPost

/// Navigation over a LINQ method chain: reaching a call's source and argument, recognising a
/// rooted chain, and the identity selector used where a sequence carries no explicit selector.
/// Expression shape and shared overload validation, with no accumulated state; it sits below the descriptor layers
/// that both need it.
module internal ChainExpr =
    /// The framework default is immutable structural comparer material.
    let rec isDefaultComparer (expression: Expression) =
        match expression with
        | :? UnaryExpression as conversion when isReferenceUpcast conversion -> isDefaultComparer conversion.Operand
        | :? MemberExpression as access when isNull access.Expression ->
            match access.Member with
            | :? PropertyInfo as property ->
                let owner = property.DeclaringType
                not(isNull owner) && owner.IsGenericType
                && (typedefof<System.Collections.Generic.EqualityComparer<_>>.Equals(owner.GetGenericTypeDefinition()) ||
                    typedefof<System.Collections.Generic.Comparer<_>>.Equals(owner.GetGenericTypeDefinition()))
                && property.Name = "Default" && property.PropertyType.Equals owner
                && not(isNull property.GetMethod) && property.GetMethod.IsStatic
            | _ -> false
        | _ -> false

    let rec private ordinalComparer (expression: Expression) =
        match expression with
        | :? UnaryExpression as conversion when isReferenceUpcast conversion -> ordinalComparer conversion.Operand
        | :? ConstantExpression as constant -> Object.ReferenceEquals(constant.Value,StringComparer.Ordinal)
        | :? MemberExpression as access when isNull access.Expression ->
            typeof<StringComparer>.Equals(access.Member.DeclaringType) && access.Member.Name = "Ordinal"
        | _ -> false

    let isImmutableComparer expression = isDefaultComparer expression || ordinalComparer expression

    let rec isDeclaredComparer (expression: Expression) =
        match expression with
        | :? UnaryExpression as conversion when isReferenceUpcast conversion -> isDeclaredComparer conversion.Operand
        | :? MemberExpression as access when isNull access.Expression && typeof<StringComparer>.Equals(access.Member.DeclaringType) -> true
        | _ -> isImmutableComparer expression

    /// Grouping must use the same supported comparer contract at every chain depth.
    let validateGroupingComparer (operation: string) (keyType: Type) (expression: Expression) =
        let unsupported () =
            NotSupportedException(
                "Error: " + operation + " comparer overload is not supported.\n" +
                "Reason: SoloDB can translate " + operation + " only with the default equality comparer in SQL.\n" +
                "Fix: Remove the comparer or normalize the key inside the selector.")
        let value =
            try ExpressionEvaluation.evaluateExpr<obj> expression
            with _ -> raise (unsupported ())
        if not (isNull value) then
            let defaultComparer = ModelMetadata.DefaultComparer keyType
            if isNull defaultComparer || not (obj.ReferenceEquals(value, defaultComparer) || value.GetType().Equals(defaultComparer.GetType())) then
                raise (unsupported ())

    /// Resolve the real default operation before selecting its comparer-free SQL shape.
    [<System.Diagnostics.CodeAnalysis.DynamicDependency("MinBy``2", typeof<Queryable>);
      System.Diagnostics.CodeAnalysis.DynamicDependency("MaxBy``2", typeof<Queryable>);
      System.Diagnostics.CodeAnalysis.DynamicDependency("OrderBy``2", typeof<Queryable>);
      System.Diagnostics.CodeAnalysis.DynamicDependency("OrderByDescending``2", typeof<Queryable>);
      System.Diagnostics.CodeAnalysis.DynamicDependency("MinBy``2", typeof<Enumerable>);
      System.Diagnostics.CodeAnalysis.DynamicDependency("MaxBy``2", typeof<Enumerable>);
      System.Diagnostics.CodeAnalysis.DynamicDependency("OrderBy``2", typeof<Enumerable>);
      System.Diagnostics.CodeAnalysis.DynamicDependency("OrderByDescending``2", typeof<Enumerable>)>]
    let defaultComparerOverload (call: MethodCallExpression) =
        let method = call.Method
        let parameters = method.GetParameters()
        let owner = method.DeclaringType
        if not(typeof<Queryable>.Equals owner || typeof<Enumerable>.Equals owner) || parameters.Length = 0 then call,None else
        let comparerType = parameters[parameters.Length-1].ParameterType
        let equality = comparerType.IsGenericType && typedefof<System.Collections.Generic.IEqualityComparer<_>>.Equals(comparerType.GetGenericTypeDefinition())
        let ordering = comparerType.IsGenericType && typedefof<System.Collections.Generic.IComparer<_>>.Equals(comparerType.GetGenericTypeDefinition())
        if not(equality || ordering) then call,None else
        let keyType = comparerType.GetGenericArguments()[0]
        let comparer = call.Arguments[call.Arguments.Count-1]
        let ordinal = typeof<string>.Equals keyType && ordinalComparer comparer
        let nullComparer = match comparer with :? ConstantExpression as value -> isNull value.Value | _ -> false
        let stringOrdering = ordering && typeof<string>.Equals keyType
        let validate value =
            let fixedOrdinal = typeof<string>.Equals keyType && Object.ReferenceEquals(value,StringComparer.Ordinal)
            if not(isNull value) &&
               not(Object.ReferenceEquals(value,(if equality then ModelMetadata.DefaultComparer keyType else ModelMetadata.DefaultOrderingComparer keyType))) &&
               not(fixedOrdinal && (equality || stringOrdering)) then
                raise(NotSupportedException("This comparer requires its own query comparison operation."))
            box (if stringOrdering && fixedOrdinal then 1 else 0)
        let rec fixedValue (expression: Expression) =
            match expression with
            | :? UnaryExpression as conversion when isReferenceUpcast conversion -> fixedValue conversion.Operand
            | :? ConstantExpression as constant -> Some(validate constant.Value)
            | :? MemberExpression as access when isNull access.Expression && typeof<StringComparer>.Equals(access.Member.DeclaringType) ->
                raise(NotSupportedException("This comparer requires its own query comparison operation."))
            | _ -> None
        let operation =
            if isDefaultComparer comparer then Expression.Constant(0) :> Expression
            elif ordinal then Expression.Constant(1) :> Expression
            else
                match fixedValue comparer with
                | Some value -> Expression.Constant(unbox<int> value) :> Expression
                | None -> BoundValueCallExpression([|comparer|],typeof<int>,Func<obj array,obj>(fun values -> validate values[0])) :> Expression
        let check =
            if stringOrdering || isImmutableComparer comparer || nullComparer then None else Some operation
        let arguments = call.Arguments |> Seq.take (call.Arguments.Count-1) |> Seq.toArray
        let generic = method.GetGenericArguments()
        let replacement =
            owner.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
            |> Array.tryPick(fun candidate ->
                if candidate.Name <> method.Name || candidate.GetParameters().Length <> arguments.Length ||
                   candidate.GetGenericArguments().Length <> generic.Length then None else
                let closed = if candidate.IsGenericMethodDefinition then candidate.MakeGenericMethod generic else candidate
                let expected = closed.GetParameters()
                if closed.ReturnType.Equals(method.ReturnType) &&
                   Array.forall2 (fun (parameter: ParameterInfo) (original: ParameterInfo) -> parameter.ParameterType.Equals original.ParameterType)
                       expected parameters[0..parameters.Length-2] then Some closed else None)
        let normalized =
            match replacement with
            | Some replacement -> Expression.Call(replacement,arguments)
            | None -> call.Update(call.Object,Array.append arguments [|Expression.Constant(null,comparerType) :> Expression|])
        let normalized =
            if not stringOrdering then normalized else
            let key (lambda: LambdaExpression) =
                Expression.Lambda(lambda.Type,FixedStringOrderKeyExpression(lambda.Body,operation),lambda.Name,lambda.TailCall,lambda.Parameters)
            let argument lambda = if typeof<Queryable>.Equals owner then Expression.Quote(lambda) :> Expression else lambda :> Expression
            if arguments.Length = 1 then
                let parameter = Expression.Parameter(typeof<string>,"value")
                let selector = Expression.Lambda<Func<string,string>>(parameter,parameter) |> key
                let name =
                    match method.Name with
                    | "Min" -> "MinBy"
                    | "Max" -> "MaxBy"
                    | "OrderDescending" -> "OrderByDescending"
                    | _ -> "OrderBy"
                let ordered = owner.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                              |> Array.find(fun candidate -> candidate.Name = name && candidate.GetGenericArguments().Length = 2 && candidate.GetParameters().Length = 2)
                Expression.Call(ordered.MakeGenericMethod(typeof<string>,typeof<string>),arguments[0],argument selector)
            else
                let selector = tryExtractLambdaExpression arguments[1]
                               |> ValueOption.defaultWith(fun () -> raise(NotSupportedException("An ordinal order key requires its original selector expression.")))
                normalized.Update(normalized.Object,[|arguments[0];argument(key selector)|])
        normalized,check

    let normalizeComparer (context: QueryContext) call =
        let call,check = defaultComparerOverload call
        match check with
        | Some check ->
            match context.BindQueryValue with
            | ValueSome binder when binder.IsValue check -> binder.Scalar check |> ignore
            | _ -> ExpressionEvaluation.evaluateExpr<obj> check |> ignore
        | None -> ()
        call

    let sequenceElementType (sequenceType: Type) = ModelMetadata.SequenceElement sequenceType

    type Selector =
        { Body: Expression
          Parameters: ReadOnlyCollection<ParameterExpression> }
        member this.ReturnType = this.Body.Type

    let selector (lambda: LambdaExpression) =
        { Body = lambda.Body; Parameters = lambda.Parameters }

    let identitySelector (elementType: Type) =
        let parameter = Expression.Parameter(elementType)
        { Body = parameter; Parameters = Array.AsReadOnly [| parameter |] }

    let identitySelectorForSequence (expr: Expression) =
        // IGrouping<TKey,TElement> carries the element in its sequence contract.
        identitySelector (sequenceElementType expr.Type)

    let getSource (mce: MethodCallExpression) =
        if not (isNull mce.Object) then mce.Object
        elif mce.Arguments.Count > 0 then mce.Arguments.[0]
        else null

    let getArg (mce: MethodCallExpression) =
        if not (isNull mce.Object) then
            if mce.Arguments.Count >= 1 then Some mce.Arguments.[0] else None
        elif mce.Arguments.Count >= 2 then Some mce.Arguments.[1]
        else None

    /// Closed overload roles are shared by root and nested grouping adapters.
    type GroupBySelectors =
        { Key: Expression
          Element: Expression option
          Result: LambdaExpression option
          GroupType: Type }

    let groupBySelectors (method: MethodInfo) (arguments: Expression array) =
        let parameters = method.GetParameters()
        let generic = method.GetGenericArguments()
        let mutable element = None
        let mutable result = None
        let mutable elementType = generic[0]
        for index = 2 to parameters.Length - 1 do
            let declared = parameters[index].ParameterType
            if declared.IsGenericType && typedefof<System.Collections.Generic.IEqualityComparer<_>>.Equals(declared.GetGenericTypeDefinition()) then
                validateGroupingComparer "GroupBy" generic[1] arguments[index - 1]
            else
                let delegateType =
                    if declared.IsGenericType && typedefof<Expression<_>>.Equals(declared.GetGenericTypeDefinition()) then declared.GetGenericArguments()[0]
                    else declared
                let arity = delegateType.GetGenericArguments().Length - 1
                match QueryTranslatorVisitPost.tryExtractLambdaExpression arguments[index - 1] with
                | ValueSome lambda when lambda.Parameters.Count = arity && arity = 1 ->
                    element <- Some arguments[index - 1]
                    elementType <- lambda.ReturnType
                | ValueSome lambda when lambda.Parameters.Count = arity && arity = 2 -> result <- Some lambda
                | _ -> raise (NotSupportedException("GroupBy requires inline key, element and result selector expressions."))
        { Key = arguments[0]; Element = element; Result = result
          GroupType = if result.IsNone then sequenceElementType method.ReturnType else typedefof<IGrouping<_,_>>.MakeGenericType(generic[1], elementType) }

    let groupingResult (groupType: Type) (result: LambdaExpression) =
        let group = Expression.Parameter(groupType, "group")
        let items = Expression.Convert(group, result.Parameters[1].Type) :> Expression
        let body = inlineLambdaInvocation result [|Expression.Property(group, "Key") :> Expression; items|]
        { Body = body; Parameters = Array.AsReadOnly [|group|] }
