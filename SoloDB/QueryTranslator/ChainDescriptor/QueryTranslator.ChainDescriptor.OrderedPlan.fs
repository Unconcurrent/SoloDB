namespace SoloDatabase

open System
open System.Linq
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.ChainExpr

/// An ordered chain owns operator position; adapters provide only the correlated source
/// and the existing expression translator. No stage is reconstructed from accumulated flags.
module internal OrderedChainPlan =
    type Stage =
        | Filter of Selector
        | Project of Selector
        | Expand of Selector
        | OfType of Type * Type
        | Cast of Type * Type
        | Order of Selector * SortDirection * bool
        | Skip of Expression
        | Take of Expression
        | Distinct of Type
        | DistinctBy of Selector
        | Group of Selector * Selector option * bool * Type
        | Set of string * Expression * Selector option
        | Default of Expression option
        | While of Selector * bool
    type Plan = { Root: Expression; Stages: Stage list }
    type SourceColumns = IdentityOnly | FullValue
    type Adapter = {
        IsRoot: Expression -> bool
        IsValue: Expression -> bool
        MembershipContext: QueryMembershipContext
        LambdaContext: string
        Validate: Plan -> unit
        Source: SourceColumns -> Expression -> struct(SqlSelect * QuerySerializationInput)
        Translate: Expression -> QuerySerializationInput -> string -> Selector -> (Expression -> QueryProducedValue voption) voption -> QueryProducedValue * JoinShape list
        TranslateOrderKey: Expression -> QuerySerializationInput -> string -> Selector -> (Expression -> QueryProducedValue voption) voption -> QueryStructuralOrdering.Key * JoinShape list
        Value: Expression -> QueryProducedValue
        Variables: System.Collections.Generic.Dictionary<string,obj>
        DefaultValue: Type -> QueryProducedValue
        Comparison: Expression -> QueryProducedValue
        Alias: unit -> string
    }
    let lambda e =
        match tryExtractLambdaExpression e with ValueSome l -> Some(selector l) | ValueNone -> None
    let validateDefaultComparerArity operation argumentCount =
        let arity =
            match operation with
            | "OrderBy" | "OrderByDescending" | "ThenBy" | "ThenByDescending"
            | "DistinctBy" | "MinBy" | "MaxBy" | "Union" | "Intersect" | "Except" | "Contains" -> 2
            | "UnionBy" | "IntersectBy" | "ExceptBy" -> 3
            | "Distinct" | "Order" | "OrderDescending" -> 1
            | _ -> argumentCount
        if argumentCount > arity then
            raise (NotSupportedException("Custom query comparers cannot be translated to SQL. Use the default comparer overload or call AsEnumerable() first."))
    let validateOverload (call: MethodCallExpression) =
        if call.Method.Name = "CountBy" && call.Arguments.Count = 3 then
            validateGroupingComparer "CountBy" (call.Method.GetGenericArguments().[1]) call.Arguments.[2]
        validateDefaultComparerArity call.Method.Name call.Arguments.Count
    let parse isRoot (expr: Expression) =
        let rec loop (expr: Expression) stages =
            if isRoot expr then Some { Root = expr; Stages = stages } else
            match expr with
            | :? UnaryExpression as conversion when QueryTranslatorBaseHelpers.isReferenceUpcast conversion ->
                loop conversion.Operand stages
            | :? MethodCallExpression as c when c.Arguments.Count = 1
                                                    && ((typeof<Queryable>.Equals(c.Method.DeclaringType) && c.Method.Name = "AsQueryable")
                                                        || (typeof<Enumerable>.Equals(c.Method.DeclaringType) && c.Method.Name = "AsEnumerable")) ->
                loop (getSource c) stages
            | :? MethodCallExpression as c when (c.Method.Name = "ToArray" || c.Method.Name = "ToList")
                                                    && typeof<Enumerable>.Equals(c.Method.DeclaringType)
                                                    && c.Arguments.Count = 1 ->
                // Intermediate materialization preserves the sequence. Keep its source
                // stages intact rather than restarting extraction at the materializer.
                loop (getSource c) stages
            | :? MethodCallExpression as c when c.Method.Name = "Cast"
                                                    && (typeof<Enumerable>.Equals(c.Method.DeclaringType) || typeof<Queryable>.Equals(c.Method.DeclaringType))
                                                    && (identitySelectorForSequence (getSource c)).ReturnType.Equals(c.Method.GetGenericArguments().[0]) ->
                loop (getSource c) stages
            | :? MethodCallExpression as c when typeof<Enumerable>.Equals(c.Method.DeclaringType) || typeof<Queryable>.Equals(c.Method.DeclaringType) ->
                validateOverload c
                let arg = getArg c
                let mutable following = []
                let stage =
                    match c.Method.Name, arg with
                    | "Where", Some a -> lambda a |> Option.map Filter
                    | ("TakeWhile" | "SkipWhile"), Some a -> lambda a |> Option.map (fun l -> While(l,c.Method.Name="TakeWhile"))
                    | "Select", Some a -> lambda a |> Option.map Project
                    | "OfType", None -> Some(OfType(sequenceElementType (getSource c).Type, c.Method.GetGenericArguments().[0]))
                    | "Cast", None -> Some(Cast(sequenceElementType (getSource c).Type, c.Method.GetGenericArguments().[0]))
                    | "SelectMany", Some a when c.Arguments.Count = 2 -> lambda a |> Option.map Expand
                    | ("OrderBy" | "OrderByDescending" | "ThenBy" | "ThenByDescending"), Some a ->
                        lambda a |> Option.map (fun l -> Order(l, (if c.Method.Name.EndsWith("Descending", StringComparison.Ordinal) then SortDirection.Desc else SortDirection.Asc), c.Method.Name.StartsWith("Then", StringComparison.Ordinal)))
                    | "Skip", Some a -> Some(Skip a)
                    | "Take", Some a -> Some(Take a)
                    | "Distinct", None -> Some(Distinct(sequenceElementType (getSource c).Type))
                    | "DistinctBy", Some a -> lambda a |> Option.map DistinctBy
                    | "GroupBy", Some _ ->
                        let group = groupBySelectors c.Method (c.Arguments |> Seq.skip 1 |> Seq.toArray)
                        following <- group.Result |> Option.map (fun result -> [Project(groupingResult group.GroupType result)]) |> Option.defaultValue []
                        lambda group.Key |> Option.map (fun key -> Group(key, group.Element |> Option.bind lambda, false, group.GroupType))
                    | "CountBy", Some a when c.Arguments.Count = 2 || c.Arguments.Count = 3 ->
                        lambda a |> Option.map (fun key -> Group(key, None, true, sequenceElementType c.Type))
                    | ("Order" | "OrderDescending"), None ->
                        Some(Order(identitySelectorForSequence (getSource c), (if c.Method.Name = "OrderDescending" then SortDirection.Desc else SortDirection.Asc), false))
                    | ("Concat" | "Union" | "Intersect" | "Except"), Some a -> Some(Set(c.Method.Name, a, None))
                    | ("UnionBy" | "IntersectBy" | "ExceptBy"), Some a when c.Arguments.Count = 3 ->
                        lambda c.Arguments.[2] |> Option.map (fun key -> Set(c.Method.Name, a, Some key))
                    | "DefaultIfEmpty", a ->
                        let elementType = c.Method.GetGenericArguments().[0]
                        Some(Default(Some(a |> Option.defaultWith (fun () -> Expression.Constant((if elementType.IsValueType then RelationsAccessorCache.activateDefault elementType else null),elementType) :> Expression))))
                    | _ -> None
                match stage with
                | Some s -> loop (getSource c) (s :: (following @ stages))
                | None -> None
            | _ -> None
        loop expr []
