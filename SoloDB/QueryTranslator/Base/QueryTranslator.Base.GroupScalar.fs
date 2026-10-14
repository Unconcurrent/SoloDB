namespace SoloDatabase

open System
open System.Linq
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorVisitPost

/// Scalar demand consumes grouped rows before sequence payload publication.
module internal QueryGroupScalar =
    type private Demand = {
        Kind: AggregateKind
        Predicate: LambdaExpression option
        Selector: LambdaExpression option
        DiscardedProjection: LambdaExpression option
        Distinct: bool
        Quantifier: bool option
    }

    let internal withFallback selected fallback =
        match selected with
        | ValueNone -> fallback
        | ValueSome selected ->
            ValueSome(fun expression -> selected expression |> ValueOption.orElseWith (fun () -> fallback |> ValueOption.bind (fun fallback -> fallback expression)))

    let rec private isSource parameter (expression: Expression) =
        match expression with
        | :? ParameterExpression as source -> obj.ReferenceEquals(source, parameter)
        | :? UnaryExpression as converted when isReferenceUpcast converted -> isSource parameter converted.Operand
        | _ -> false

    let private lambda expression =
        match tryExtractLambdaExpression expression with ValueSome value -> Some value | _ -> None

    let private describe parameter (call: MethodCallExpression) =
        if call.Arguments.Count = 0 ||
           not(typeof<Enumerable>.Equals(call.Method.DeclaringType) || typeof<Queryable>.Equals(call.Method.DeclaringType)) then None
        else
        let kind =
            match call.Method.Name with
            | "Count" | "LongCount" | "Any" | "All" -> Some AggregateKind.Count
            | "Sum" -> Some AggregateKind.Sum
            | "Min" -> Some AggregateKind.Min
            | "Max" -> Some AggregateKind.Max
            | "Average" -> Some AggregateKind.Avg
            | _ -> None
        match kind with
        | None -> None
        | Some kind ->
            let quantifier = match call.Method.Name with "Any" -> Some false | "All" -> Some true | _ -> None
            let mutable source = call.Arguments[0]
            let mutable distinct = false
            let mutable selector = None
            let mutable predicate = None
            let mutable valid = true
            if call.Arguments.Count = 2 then
                match lambda call.Arguments[1] with
                | Some argument when kind = AggregateKind.Count -> predicate <- Some argument
                | Some argument -> selector <- Some argument
                | None -> valid <- false
            elif call.Arguments.Count <> 1 || call.Method.Name = "All" then valid <- false
            match source with
            | :? MethodCallExpression as stage when stage.Method.Name = "Distinct" && stage.Arguments.Count = 1
                                                    && typeof<Enumerable>.Equals(stage.Method.DeclaringType) ->
                distinct <- true
                source <- stage.Arguments[0]
            | _ -> ()
            if selector.IsNone then
                match source with
                | :? MethodCallExpression as stage when stage.Method.Name = "Select" && stage.Arguments.Count = 2
                                                        && typeof<Enumerable>.Equals(stage.Method.DeclaringType) ->
                    selector <- lambda stage.Arguments[1]
                    valid <- valid && selector.IsSome
                    source <- stage.Arguments[0]
                | _ -> ()
            if predicate.IsNone then
                match source with
                | :? MethodCallExpression as stage when stage.Method.Name = "Where" && stage.Arguments.Count = 2
                                                        && typeof<Enumerable>.Equals(stage.Method.DeclaringType) ->
                    predicate <- lambda stage.Arguments[1]
                    valid <- valid && predicate.IsSome
                    source <- stage.Arguments[0]
                | _ -> ()
            if not valid || not(isSource parameter source) || (kind = AggregateKind.Count && distinct)
               || (quantifier.IsSome && selector.IsSome) || (kind <> AggregateKind.Count && selector.IsNone) then None
            else
                Some { Kind = kind; Predicate = predicate
                       Selector = if kind = AggregateKind.Count then None else selector
                       DiscardedProjection = if kind = AggregateKind.Count then selector else None
                       Distinct = distinct; Quantifier = quantifier }

    let internal isKey parameter (expression: Expression) =
        match expression with
        | :? MemberExpression as memberAccess -> memberAccess.Member.Name = "Key" && isSource parameter memberAccess.Expression
        | _ -> false

    /// Admission inspects syntax only; it does not lower or acquire arguments.
    let internal supportsPredicate parameter (expression: Expression) guarded =
        let mutable supported = true
        let mutable lazyBranch = guarded
        let visitor =
            { new ExpressionVisitor() with
                override this.Visit(node: Expression) =
                    if isNull node then null
                    elif isKey parameter node then node
                    else
                        match node with
                        | :? ConditionalExpression as conditional ->
                            let previous = lazyBranch
                            this.Visit conditional.Test |> ignore
                            lazyBranch <- true
                            this.Visit conditional.IfTrue |> ignore
                            this.Visit conditional.IfFalse |> ignore
                            lazyBranch <- previous
                            node
                        | :? BinaryExpression as binary when binary.NodeType = ExpressionType.AndAlso || binary.NodeType = ExpressionType.OrElse ->
                            let previous = lazyBranch
                            this.Visit binary.Left |> ignore
                            lazyBranch <- true
                            this.Visit binary.Right |> ignore
                            lazyBranch <- previous
                            node
                        | :? MethodCallExpression as call ->
                            match describe parameter call with
                            | Some demand ->
                                if lazyBranch && (demand.Kind <> AggregateKind.Count || demand.Predicate.IsSome || demand.DiscardedProjection.IsSome) then supported <- false
                                for argument in [demand.Predicate; demand.Selector; demand.DiscardedProjection] do
                                    argument |> Option.iter (fun argument -> this.Visit argument.Body |> ignore)
                                node
                            | None -> base.Visit node
                        | :? ParameterExpression as value when obj.ReferenceEquals(value, parameter) -> supported <- false; node
                        | _ -> base.Visit node }
        visitor.Visit expression |> ignore
        supported

    /// LEFT padding is excluded through the actual matched-row presence fact.
    let internal tryTranslate parameter present (translate: LambdaExpression -> SqlExpr) (expression: Expression) =
        match expression with
        | :? MethodCallExpression as call ->
            match describe parameter call with
            | None -> None
            | Some demand ->
                let zero = SqlExpr.Literal(SqlLiteral.Integer 0L)
                let one = SqlExpr.Literal(SqlLiteral.Integer 1L)
                demand.DiscardedProjection |> Option.iter (fun projection -> translate projection |> ignore)
                let predicate = demand.Predicate |> Option.map translate
                let predicate =
                    match demand.Quantifier, predicate with
                    | Some true, Some predicate -> Some(SqlExpr.Unary(UnaryOperator.Not, predicate))
                    | _ -> predicate
                let predicate =
                    match present, predicate with
                    | Some presence, Some predicate -> Some(SqlExpr.Binary(presence, BinaryOperator.And, predicate))
                    | Some presence, None -> Some presence
                    | None, predicate -> predicate
                let aggregate =
                    if demand.Kind = AggregateKind.Count then
                        let argument =
                            match demand.Predicate, present with
                            | None, Some(SqlExpr.Unary(UnaryOperator.IsNotNull, value)) -> Some value
                            | _ -> predicate |> Option.map (fun predicate -> SqlExpr.CaseExpr((predicate, one), [], None))
                        SqlExpr.AggregateCall(AggregateKind.Count, argument, false, None)
                    else
                        let value =
                            match demand.Selector with
                            | Some selector -> translate selector
                            | None -> raise(NotSupportedException("Projected aggregate without Select is not supported."))
                        let value = predicate |> Option.map (fun predicate -> SqlExpr.CaseExpr((predicate, value), [], None)) |> Option.defaultValue value
                        let inputType = demand.Selector.Value.ReturnType
                        let aggregate = QueryTranslatorNumeric.aggregate call.Method inputType demand.Kind value demand.Distinct
                        if demand.Kind = AggregateKind.Sum then SqlExpr.Coalesce(aggregate, [QueryTranslatorNumeric.zero call.Method.ReturnType]) else aggregate
                let result =
                    match demand.Quantifier with
                    | Some all -> SqlExpr.Binary(aggregate, (if all then BinaryOperator.Eq else BinaryOperator.Gt), zero)
                    | None -> aggregate
                Some result
        | _ -> None
