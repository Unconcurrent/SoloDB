namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq.Expressions
open SoloDatabase.Metadata.Generation

/// Bounded compiler construction shapes retain their original component order.
module internal QueryConstructionExpressions =
    let referencesParameter (isLocal: ParameterExpression -> bool) (expression: Expression) =
        let mutable found = false
        let visitor =
            { new ExpressionVisitor() with
                override _.VisitParameter parameter =
                    if isLocal parameter then found <- true
                    parameter :> Expression }
        visitor.Visit expression |> ignore
        found

    let memberNames (construction: NewExpression) =
        if not(isNull construction.Members) && construction.Members.Count = construction.Arguments.Count then
            ValueSome(construction.Members |> Seq.map _.Name |> Seq.toArray)
        elif isNull construction.Members then
            match TypeMetadata.recordFields construction.Type with
            | ValueSome fields when fields.Length = construction.Arguments.Count &&
                                    (fields |> Array.mapi (fun index name ->
                                        match ModelMetadata.MemberType(construction.Type, name, false) with
                                        | Some target -> not(isNull target) && target.Equals(construction.Arguments[index].Type)
                                        | None -> false) |> Array.forall id) -> ValueSome fields
            | _ -> ValueNone
        else ValueNone

    let private namedConstruction (expression: Expression) =
        if Utils.isTuple expression.Type || Utils.isDateTimeLikeType expression.Type then ValueNone
        else
        match expression with
        | :? NewExpression as construction ->
            memberNames construction |> ValueOption.map (fun names -> struct(names, construction.Arguments :> IReadOnlyList<Expression>))
        | :? BoundValueCallExpression as construction when not(isNull construction.ProjectionNames) &&
                                                         construction.ProjectionNames.Length = construction.Inputs.Length ->
            ValueSome(struct(construction.ProjectionNames, construction.Inputs :> IReadOnlyList<Expression>))
        | _ -> ValueNone

    let private assignedLocal (block: BlockExpression) =
        if block.Variables.Count = 1 && block.Expressions.Count = 2 then
            match block.Expressions[0] with
            | :? BinaryExpression as assignment when assignment.NodeType = ExpressionType.Assign &&
                                                    obj.ReferenceEquals(assignment.Left, block.Variables[0]) &&
                                                    block.Variables[0].Type.Equals(assignment.Right.Type) ->
                ValueSome(struct(block.Variables[0], assignment.Right))
            | _ -> ValueNone
        else ValueNone

    // A cheap tail check prevents stored-local substitution from discarding
    // construction order before the complete bounded plan is acquired.
    let rec hasConstructionTail (expression: Expression) =
        match expression with
        | :? BlockExpression as block ->
            match assignedLocal block with
            | ValueSome _ -> hasConstructionTail block.Expressions[1]
            | ValueNone -> false
        | :? NewExpression -> true
        | :? BoundValueCallExpression as construction -> not(isNull construction.ProjectionNames)
        | _ -> false

    let orderedMembers (block: BlockExpression) =
        if not(hasConstructionTail block) then ValueNone
        else
            let bindings = ResizeArray<struct(ParameterExpression * Expression)>()
            let rec terminal (expression: Expression) =
                match expression with
                | :? BlockExpression as nested ->
                    match assignedLocal nested with
                    | ValueSome binding ->
                        bindings.Add binding
                        terminal nested.Expressions[1]
                    | ValueNone -> ValueNone
                | :? NewExpression | :? BoundValueCallExpression -> ValueSome expression
                | _ -> ValueNone
            match terminal block with
            | ValueSome construction when bindings.Count > 0 ->
                match namedConstruction construction with
                | ValueNone -> ValueNone
                | ValueSome(struct(names, arguments)) ->
                    let locals = bindings |> Seq.map (fun struct(local, _) -> local) |> Seq.toArray
                    let expressions = ResizeArray<Expression>()
                    let slots = Array.create arguments.Count -1
                    let isLocal parameter = locals |> Array.exists (fun local -> obj.ReferenceEquals(local, parameter))
                    let mutable valid = true
                    for index = 0 to bindings.Count - 1 do
                        let struct(local, rhs) = bindings[index]
                        let mutable slot = -1
                        for position = 0 to arguments.Count - 1 do
                            if obj.ReferenceEquals(arguments[position], local) then
                                if slot >= 0 then valid <- false
                                slot <- position
                        let mutable duplicate = false
                        for previous = 0 to index - 1 do
                            if obj.ReferenceEquals(locals[previous], local) then duplicate <- true
                        if slot < 0 || referencesParameter isLocal rhs || duplicate then
                            valid <- false
                        elif slots[slot] >= 0 then valid <- false
                        else
                            slots[slot] <- expressions.Count
                            expressions.Add rhs
                    for slot = 0 to slots.Length - 1 do
                        if slots[slot] < 0 then
                            let argument = arguments[slot]
                            if referencesParameter isLocal argument then valid <- false
                            slots[slot] <- expressions.Count
                            expressions.Add argument
                    if valid then ValueSome(struct(construction.Type, names, expressions.ToArray(), slots))
                    else ValueNone
            | _ -> ValueNone

    let memberInitializer (block: BlockExpression) =
        match assignedLocal block with
        | ValueSome(struct(local, (:? NewExpression as construction)))
            when construction.Arguments.Count = 0 && local.Type.Equals(construction.Type) ->
            let bindings = ResizeArray<MemberBinding>()
            let mutable returned = false
            let rec collect (expression: Expression) =
                if returned then false
                else
                    match expression with
                    | :? BlockExpression as nested when nested.Variables.Count = 0 ->
                        nested.Expressions |> Seq.forall collect
                    | :? ConstantExpression as constant when isNull constant.Value -> true
                    | :? BinaryExpression as assignment when assignment.NodeType = ExpressionType.Assign ->
                        match assignment.Left with
                        | :? MemberExpression as memberAccess when obj.ReferenceEquals(memberAccess.Expression, local) &&
                                                                   not(referencesParameter (fun parameter -> obj.ReferenceEquals(parameter, local)) assignment.Right) &&
                                                                   not(bindings |> Seq.exists (fun binding -> binding.Member.Equals(memberAccess.Member))) ->
                            bindings.Add(Expression.Bind(memberAccess.Member, assignment.Right))
                            true
                        | _ -> false
                    | _ when obj.ReferenceEquals(expression, local) -> returned <- true; true
                    | _ -> false
            if collect block.Expressions[1] && returned && bindings.Count > 0 then
                ValueSome(Expression.MemberInit(construction, bindings) :> Expression)
            else ValueNone
        | _ -> ValueNone
