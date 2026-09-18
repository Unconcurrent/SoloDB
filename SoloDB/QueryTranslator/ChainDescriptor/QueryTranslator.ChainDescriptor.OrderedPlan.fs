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
        | Filter of LambdaExpression
        | Project of LambdaExpression
        | Expand of LambdaExpression
        | OfType of Type * Type
        | Cast of Type * Type
        | Order of LambdaExpression * SortDirection * bool
        | Skip of Expression
        | Take of Expression
        | Distinct of Type
        | DistinctBy of LambdaExpression
        | Group of LambdaExpression * bool
        | Set of string * Expression * LambdaExpression option
        | Default of Expression option
        | While of LambdaExpression * bool
    type Plan = { Root: Expression; Stages: Stage list }
    type SourceColumns = IdentityOnly | FullValue
    type Adapter = {
        IsRoot: Expression -> bool
        IsValue: Expression -> bool
        EntityMembershipById: bool
        LambdaContext: string
        Validate: Plan -> unit
        Source: SourceColumns -> Expression -> SqlSelect
        Translate: Expression -> string -> LambdaExpression -> SqlExpr * JoinShape list
        Value: Expression -> SqlExpr
        Alias: unit -> string
    }
    let lambda e =
        match tryExtractLambdaExpression e with ValueSome l -> Some l | ValueNone -> None
    let validateOverload (call: MethodCallExpression) =
        if call.Method.Name = "CountBy" && call.Arguments.Count = 3 then
            validateGroupingComparer "CountBy" (call.Method.GetGenericArguments().[1]) call.Arguments.[2]
        if call.Method.Name = "GroupBy" then
            let last = call.Method.GetParameters() |> Array.last
            if last.ParameterType.IsGenericType
               && last.ParameterType.GetGenericTypeDefinition() = typedefof<System.Collections.Generic.IEqualityComparer<_>> then
                validateGroupingComparer "GroupBy" (call.Method.GetGenericArguments().[1]) call.Arguments.[call.Arguments.Count - 1]
        let arity =
            match call.Method.Name with
            | "OrderBy" | "OrderByDescending" | "ThenBy" | "ThenByDescending"
            | "DistinctBy" | "MinBy" | "MaxBy" | "Union" | "Intersect" | "Except" | "Contains" -> 2
            | "UnionBy" | "IntersectBy" | "ExceptBy" -> 3
            | "Distinct" | "Order" | "OrderDescending" -> 1
            | _ -> call.Arguments.Count
        if call.Arguments.Count > arity then
            raise (NotSupportedException("Custom query comparers cannot be translated to SQL. Use the default comparer overload or call AsEnumerable() first."))
    let parse isRoot (expr: Expression) =
        let rec loop (expr: Expression) stages =
            if isRoot expr then Some { Root = expr; Stages = stages } else
            match expr with
            | :? MethodCallExpression as c when c.Arguments.Count = 1
                                                    && ((c.Method.DeclaringType = typeof<Queryable> && c.Method.Name = "AsQueryable")
                                                        || (c.Method.DeclaringType = typeof<Enumerable> && c.Method.Name = "AsEnumerable")) ->
                loop (getSource c) stages
            | :? MethodCallExpression as c when (c.Method.Name = "ToArray" || c.Method.Name = "ToList")
                                                    && c.Method.DeclaringType = typeof<Enumerable>
                                                    && c.Arguments.Count = 1 ->
                // Intermediate materialization preserves the sequence. Keep its source
                // stages intact rather than restarting extraction at the materializer.
                loop (getSource c) stages
            | :? MethodCallExpression as c when c.Method.Name = "Cast"
                                                    && (c.Method.DeclaringType = typeof<Enumerable> || c.Method.DeclaringType = typeof<Queryable>)
                                                    && (mkIdentityLambdaForSequence (getSource c)).ReturnType = c.Method.GetGenericArguments().[0] ->
                loop (getSource c) stages
            | :? MethodCallExpression as c when c.Method.DeclaringType = typeof<Enumerable> || c.Method.DeclaringType = typeof<Queryable> ->
                validateOverload c
                let arg = getArg c
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
                    | "GroupBy", Some a when c.Arguments.Count = 2
                                                || (c.Arguments.Count = 3 && c.Method.GetParameters().[2].ParameterType.IsGenericType
                                                    && c.Method.GetParameters().[2].ParameterType.GetGenericTypeDefinition() = typedefof<System.Collections.Generic.IEqualityComparer<_>>) -> lambda a |> Option.map (fun key -> Group(key, false))
                    | "CountBy", Some a when c.Arguments.Count = 2 || c.Arguments.Count = 3 ->
                        lambda a |> Option.map (fun key -> Group(key, true))
                    | ("Order" | "OrderDescending"), None ->
                        Some(Order(mkIdentityLambdaForSequence (getSource c), (if c.Method.Name = "OrderDescending" then SortDirection.Desc else SortDirection.Asc), false))
                    | ("Concat" | "Union" | "Intersect" | "Except"), Some a -> Some(Set(c.Method.Name, a, None))
                    | ("UnionBy" | "IntersectBy" | "ExceptBy"), Some a when c.Arguments.Count = 3 ->
                        lambda c.Arguments.[2] |> Option.map (fun key -> Set(c.Method.Name, a, Some key))
                    | "DefaultIfEmpty", a ->
                        let elementType = c.Method.GetGenericArguments().[0]
                        Some(Default(Some(a |> Option.defaultWith (fun () -> Expression.Constant((if elementType.IsValueType then Activator.CreateInstance(elementType) else null),elementType) :> Expression))))
                    | _ -> None
                match stage with
                | Some s -> loop (getSource c) (s :: stages)
                | None -> None
            | _ -> None
        loop expr []
