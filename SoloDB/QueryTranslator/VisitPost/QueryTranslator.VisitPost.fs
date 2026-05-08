namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Collections.ObjectModel
open System.Linq.Expressions
open System.Reflection
open System.Runtime.InteropServices
open System.Text
open JsonFunctions
open Utils
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorBase

module internal QueryTranslatorVisitPost =
    [<Literal>]
    let internal updateManyRelationUnsupportedMessage =
        "Error: UpdateMany relation transform is not supported.\nReason: Only specific relation mutations are allowed in UpdateMany.\nFix: Use Ref.Set(DBRef.To/None) or RefMany.Add/Append/Remove/Clear, or apply changes outside UpdateMany."

    [<Literal>]
    let private updateManyDbRefManyPersistedIdMessage =
        "Error: UpdateMany DBRefMany Add/Remove requires a persisted target Id.\nReason: The target Id must be > 0 to reference an existing row. Cascade-insert via DBRef.From(unsaved) is not supported in UpdateMany.\nFix: Pass either an int64 row id or an entity whose Id property is > 0. Insert the target first via the target collection if the entity is unsaved."

    [<Literal>]
    let private updateManyDbRefValueMutationMessage =
        "Error: UpdateMany supports only single-hop DBRef.Value property mutation.\nReason: Any nested Ref.Value chain deeper than one hop (two-hop p.Ref.Value.Other.Value.X, three-hop p.A.Value.B.Value.C.Value.X, and deeper) is not lowerable through a single relation link table.\nFix: Run the inner-collection UpdateMany separately for each hop, or assign through the immediate p.Ref.Value.<Field> only."

    [<Literal>]
    let private updateManyDbRefFromUnsupportedMessage =
        "Error: UpdateMany cannot cascade-insert from DBRef.From in a transform.\nReason: DBRef.From is an insertion shape; cascade composition through UpdateMany is not supported.\nFix: Insert the target row via targetCollection.Insert(item) first, then reference its rowid in the UpdateMany transform via DBRef.To(item.Id)."

    [<Literal>]
    let private updateManyDbRefManyPositionalMessage =
        "Error: UpdateMany cannot mutate a DBRefMany by position (Insert, SetAt, RemoveAt, indexer assignment).\nReason: DBRefMany has set semantics; positional / Ordered ops require a Position column on the relation link table that the current schema does not declare.\nFix: Use Add, Append, Remove, or Clear by row id or saved entity. Positional ordering on DBRefMany requires schema-level Ordered support that this version does not provide."

    /// ε hop-trail diagnostics. Each message names the hop position and identifies the
    /// specific shape that is unsupported, so test cells can anchor on a precise vocabulary
    /// rather than the generic 'unsupported' fallback.
    let private chainPlainPropertyMidChainMessage (hopIndex: int) (ownerTypeName: string) (propertyName: string) =
        sprintf "Error: UpdateMany Ref-chain mutation encountered a non-relation (plain CLR) property '%s.%s' mid-chain at hop %d.\nReason: Every non-leaf hop in a Ref.Value chain must be DBRef-shape; '%s' is not DBRef-shape so the chain cannot be lowered to a finite SQL plan.\nFix: Replace the mid-chain plain CLR property with a DBRef property, or split the transform so the plain property is only addressed at the leaf."
            ownerTypeName propertyName hopIndex propertyName

    let private chainMethodCallMidChainMessage (hopIndex: int) =
        sprintf "Error: UpdateMany Ref-chain mutation encountered an unsupported method call mid-chain at hop %d.\nReason: Mid-chain method invocations are not lowerable — the chain walker only accepts DBRef property access and `.Value` accessors between hops.\nFix: Hoist the method call's result into a closure-captured local before the UpdateMany lambda and reference the hoisted value, or restructure the chain so no method call appears in the non-leaf hops."
            hopIndex

    let private chainPositionalMidChainMessage (hopIndex: int) =
        sprintf "Error: UpdateMany Ref-chain mutation encountered a non-leaf positional / mid-chain indexer at hop %d.\nReason: Positional access (indexer / IndexExpression) is not supported as a mid-chain positional hop — DBRefMany is a set, not a list, and positional addressing of the chain has no SQL lowering.\nFix: Use Add, Append, Remove, or Clear at the leaf hop, and avoid positional indexing in non-leaf hops."
            hopIndex

    /// Deterministic unsupported-shape messages.
    let internal multiSourceCrossRootProjectionMessage =
        "Error: Cross-root projection requires an explicit join key.\nReason: The projection accesses columns from multiple query roots without a defined key relationship.\nFix: Add a Join or GroupJoin with explicit key selectors before projecting across roots."

    let internal multiSourceClientEvalForbiddenMessage =
        "Error: Client-side evaluation is not supported for this query shape.\nReason: The expression cannot be translated to SQL and cannot be evaluated on the client in this context.\nFix: Restructure the query or call AsEnumerable() before using client-evaluated expressions."

    let private unwrapQuote (expr: Expression) =
        match expr with
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Quote -> ue.Operand
        | _ -> expr

    let rec private unwrapConvertAll (expr: Expression) =
        match expr with
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert -> unwrapConvertAll ue.Operand
        | _ -> expr

    let rec private normalizeUpdateManyBody (expr: Expression) =
        let expr = unwrapConvertAll expr
        match expr with
        | :? MethodCallExpression as mc when mc.Method.Name = "op_PipeRight" && mc.Arguments.Count >= 1 ->
            // F# "(x |> ignore)" wraps side-effecting call in op_PipeRight; keep the source call.
            normalizeUpdateManyBody mc.Arguments.[0]
        | :? MethodCallExpression as mc when (mc.Method.Name = "Ignore" || mc.Method.Name = "ignore") && mc.Arguments.Count = 1 ->
            normalizeUpdateManyBody mc.Arguments.[0]
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Quote || ue.NodeType = ExpressionType.Convert ->
            normalizeUpdateManyBody ue.Operand
        | _ ->
            expr

    let rec internal tryExtractLambdaExpression (expr: Expression) : LambdaExpression voption =
        let tryEvaluateAsLambda (candidate: Expression) =
            try
                match evaluateExpr<obj> candidate with
                | :? LambdaExpression as le -> ValueSome le
                | :? Expression as e ->
                    match e with
                    | :? LambdaExpression as le -> ValueSome le
                    | _ -> ValueNone
                | _ -> ValueNone
            with _ ->
                ValueNone

        match unwrapConvertAll expr with
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Quote || ue.NodeType = ExpressionType.Convert ->
            tryExtractLambdaExpression ue.Operand
        | :? LambdaExpression as le ->
            ValueSome le
        | :? MethodCallExpression as mc ->
            let fromObject =
                if isNull mc.Object then ValueNone
                else tryExtractLambdaExpression mc.Object
            match fromObject with
            | ValueSome _ as hit -> hit
            | ValueNone ->
                mc.Arguments
                |> Seq.tryPick (fun a ->
                    match tryExtractLambdaExpression a with
                    | ValueSome le -> Some le
                    | ValueNone -> None)
                |> function
                   | Some le -> ValueSome le
                   | None -> tryEvaluateAsLambda (mc :> Expression)
        | :? InvocationExpression as ie ->
            match tryExtractLambdaExpression ie.Expression with
            | ValueSome _ as hit -> hit
            | ValueNone -> tryEvaluateAsLambda (ie :> Expression)
        | _ ->
            ValueNone

    let rec private computePathKeyForUpdate (expr: Expression) : string =
        match expr with
        | :? MemberExpression as me when not (isNull me.Expression) ->
            let parent = computePathKeyForUpdate me.Expression
            if parent = "" then me.Member.Name else parent + "." + me.Member.Name
        | :? ParameterExpression -> ""
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert -> computePathKeyForUpdate ue.Operand
        | _ ->
            raise (NotSupportedException(
                sprintf "Unsupported relation update path expression node: %A. Rewrite update transform to direct member access from query root." expr.NodeType))

    let private tryLambdaBody (expression: Expression) =
        let expr = unwrapQuote expression
        match expr with
        | :? LambdaExpression as le -> le.Body
        | _ -> null

    let rec private containsDBRefValueMutationPath (expr: Expression) =
        match expr with
        | null -> false
        | :? MemberExpression as me ->
            let isBoundary =
                me.Member.Name = "Value" &&
                not (isNull me.Expression) &&
                DBRefTypeHelpers.isDBRefType (unwrapConvertAll me.Expression).Type
            isBoundary || containsDBRefValueMutationPath me.Expression
        | :? MethodCallExpression as mc ->
            let isBoundaryCall =
                mc.Method.Name = "get_Value" &&
                ((not (isNull mc.Object) && DBRefTypeHelpers.isDBRefType (unwrapConvertAll mc.Object).Type)
                 || (mc.Arguments.Count > 0 && DBRefTypeHelpers.isDBRefType (unwrapConvertAll mc.Arguments.[0]).Type))
            isBoundaryCall ||
            containsDBRefValueMutationPath mc.Object ||
            (mc.Arguments |> Seq.exists containsDBRefValueMutationPath)
        | :? UnaryExpression as ue ->
            containsDBRefValueMutationPath ue.Operand
        | :? BinaryExpression as be ->
            containsDBRefValueMutationPath be.Left || containsDBRefValueMutationPath be.Right
        | :? IndexExpression as ie ->
            containsDBRefValueMutationPath ie.Object ||
            (ie.Arguments |> Seq.exists containsDBRefValueMutationPath)
        | _ -> false

    let private tryAsInt64 (value: obj) =
        match value with
        | null -> ValueNone
        | :? int64 as x -> ValueSome x
        | :? int32 as x -> ValueSome (int64 x)
        | :? int16 as x -> ValueSome (int64 x)
        | :? int8 as x -> ValueSome (int64 x)
        | :? uint64 as x ->
            if x <= uint64 Int64.MaxValue then ValueSome (int64 x) else ValueNone
        | :? uint32 as x -> ValueSome (int64 x)
        | :? uint16 as x -> ValueSome (int64 x)
        | :? uint8 as x -> ValueSome (int64 x)
        | :? nativeint as x -> ValueSome (int64 x)
        | :? unativeint as x -> ValueSome (int64 x)
        | :? Nullable<int64> as x when x.HasValue -> ValueSome x.Value
        | _ -> ValueNone

    let private tryGetRootRelationMember (expr: Expression) =
        let expr = unwrapConvertAll expr
        match expr with
        | :? MemberExpression as me when not (isNull me.Expression) && isRootParameter me.Expression -> ValueSome me
        | _ -> ValueNone

    let private tryGetCallSourceAndArgStart (m: MethodCallExpression) =
        if not (isNull m.Object) then
            ValueSome struct (m.Object, 0)
        elif m.Arguments.Count > 0 then
            ValueSome struct (m.Arguments.[0], 1)
        else
            ValueNone

    /// Walks a Value-accessor expression: either MemberExpression(Value) on a DBRef-typed
    /// expression, or the equivalent get_Value method call. Returns the underlying DBRef
    /// expression on success, ValueNone otherwise.
    let private tryAsDbRefValueAccessor (expr: Expression) =
        let expr = unwrapConvertAll expr
        match expr with
        | :? MemberExpression as me when me.Member.Name = "Value" && not (isNull me.Expression)
                                          && DBRefTypeHelpers.isDBRefType (unwrapConvertAll me.Expression).Type ->
            ValueSome (unwrapConvertAll me.Expression)
        | :? MethodCallExpression as mc when mc.Method.Name = "get_Value" ->
            if not (isNull mc.Object) && DBRefTypeHelpers.isDBRefType (unwrapConvertAll mc.Object).Type then
                ValueSome (unwrapConvertAll mc.Object)
            elif mc.Arguments.Count > 0 && DBRefTypeHelpers.isDBRefType (unwrapConvertAll mc.Arguments.[0]).Type then
                ValueSome (unwrapConvertAll mc.Arguments.[0])
            else ValueNone
        | _ -> ValueNone

    /// Asserts that a chain's type sequence does not form a closed-loop cycle through
    /// distinct types. Two contracts apply:
    ///   - A chain that visits A, then a different type B, then returns to A rejects
    ///     (T -> S -> T). This includes longer cycles A -> B -> C -> A.
    ///   - A chain that walks a self-recursive type repeatedly (T -> T -> T -> ... -> T)
    ///     is allowed at any depth — it is depth, not a cycle.
    /// Implementation: build the type sequence (entry-owner :: per-hop targets), collapse
    /// consecutive runs of the same type, then detect duplicates in the collapsed sequence.
    /// A duplicate after collapsing means the chain LEFT a type and RETURNED to it, which
    /// is a closed-loop cycle. Same-type runs (linked-list-style self-recursion) collapse
    /// to a single entry and pass.
    let private assertChainHasNoTypeCycle (hops: ChainHopSpec list) : unit =
        match hops with
        | [] -> ()
        | firstHop :: _ ->
            let typeSeq = firstHop.OwnerType :: (hops |> List.map (fun h -> h.TargetType))
            let collapsed =
                typeSeq
                |> List.fold (fun acc t ->
                    match acc with
                    | head :: _ when head = t -> acc
                    | _ -> t :: acc) []
                |> List.rev
            let visited = HashSet<Type>()
            for t in collapsed do
                if not (visited.Add(t)) then
                    raise (InvalidOperationException(
                        sprintf "Error: UpdateMany Ref-chain mutation revisits a previously-visited type '%s' (cycle).\nReason: The Ref.Value chain forms a closed-loop cycle through distinct types — leaving a type and returning to it (e.g. T -> S -> T or A -> B -> C -> A). Such a chain cannot be lowered to a finite SQL plan.\nFix: Restructure the chain so distinct types do not form a closed loop. Pure self-recursion (T -> T -> T at any depth) is allowed; only loops through different types are rejected."
                            t.FullName))

    /// Walks a Ref.Value chain backwards from the innermost DBRef-typed expression to the root
    /// query parameter, producing a hop list ordered root-first. Validates each hop is a
    /// DBRef-shape property on the previous target type. Raises NotSupportedException for
    /// any non-chain shape encountered during the walk. Cycle detection is NOT performed
    /// here; callers that require it (B4 property-mutation paths) call
    /// assertChainHasNoTypeCycle on the result.
    let private walkRefChainBackwards (innerDbRefExpr: Expression) : ChainHopSpec list =
        // hopIndex grows as we walk inward (toward the leaf); when we walk backwards the
        // counter starts at the leaf-side hop and increments as we step toward the root.
        // The numeric is for diagnostics only — the resulting hop list is reversed at the
        // end so it ends up root-first.
        let rec walk (relMe: MemberExpression) (acc: ChainHopSpec list) (hopIndex: int) : ChainHopSpec list =
            if isNull relMe.Expression then
                raise (NotSupportedException updateManyRelationUnsupportedMessage)
            let ownerExpr = unwrapConvertAll relMe.Expression
            let ownerType = ownerExpr.Type
            let propName = relMe.Member.Name
            let propInfo = ownerType.GetProperty(propName, BindingFlags.Public ||| BindingFlags.Instance)
            if isNull propInfo then
                raise (NotSupportedException(chainPlainPropertyMidChainMessage hopIndex ownerType.Name propName))
            let propType = propInfo.PropertyType
            if not (DBRefTypeHelpers.isDBRefType propType) then
                raise (NotSupportedException(chainPlainPropertyMidChainMessage hopIndex ownerType.Name propName))
            let targetType = propType.GetGenericArguments().[0]
            let hop = { OwnerType = ownerType; RelationPropertyName = propName; TargetType = targetType }
            let acc = hop :: acc
            match ownerExpr with
            | :? ParameterExpression -> acc
            | :? MemberExpression
            | :? MethodCallExpression ->
                match tryAsDbRefValueAccessor ownerExpr with
                | ValueSome priorRelExpr ->
                    match unwrapConvertAll priorRelExpr with
                    | :? MemberExpression as priorRelMe -> walk priorRelMe acc (hopIndex + 1)
                    | :? MethodCallExpression -> raise (NotSupportedException(chainMethodCallMidChainMessage (hopIndex + 1)))
                    | _ -> raise (NotSupportedException updateManyRelationUnsupportedMessage)
                | ValueNone -> raise (NotSupportedException(chainMethodCallMidChainMessage hopIndex))
            | :? IndexExpression -> raise (NotSupportedException(chainPositionalMidChainMessage hopIndex))
            | _ -> raise (NotSupportedException updateManyRelationUnsupportedMessage)
        match unwrapConvertAll innerDbRefExpr with
        | :? MemberExpression as relMe ->
            let hops = walk relMe [] 0
            if hops.IsEmpty then raise (NotSupportedException updateManyRelationUnsupportedMessage)
            hops
        | :? MethodCallExpression -> raise (NotSupportedException(chainMethodCallMidChainMessage 0))
        | :? IndexExpression -> raise (NotSupportedException(chainPositionalMidChainMessage 0))
        | _ -> raise (NotSupportedException updateManyRelationUnsupportedMessage)

    /// Inspects an LHS that has a `.Value` somewhere in its parent chain but did not match
    /// any of the chain or single-hop shapes. Classifies the specific mid-chain violation
    /// (plain CLR property, method call, positional indexer) and raises with the matching
    /// hop-trail vocabulary. Returns unit on no match (caller continues the existing
    /// fall-through).
    let private tryRaiseMidChainClassified (lhs: Expression) : unit =
        // Walk the LHS expression chain looking for the first non-DBRef-shape hop AFTER
        // a `.Value` accessor. The presence of a `.Value` means the user expressed a
        // chain intent; an unsupported shape after that is a mid-chain violation.
        let rec scan (expr: Expression) (sawValue: bool) : unit =
            let expr = unwrapConvertAll expr
            match expr with
            | null -> ()
            | :? MemberExpression as me ->
                if me.Member.Name = "Value" && not (isNull me.Expression) &&
                   DBRefTypeHelpers.isDBRefType (unwrapConvertAll me.Expression).Type then
                    scan me.Expression true
                elif sawValue then
                    // Mid-chain plain CLR property (or non-relation property continuation).
                    let ownerName =
                        if isNull me.Expression then "<unknown>"
                        else (unwrapConvertAll me.Expression).Type.Name
                    raise (NotSupportedException(chainPlainPropertyMidChainMessage 0 ownerName me.Member.Name))
                else
                    scan me.Expression sawValue
            | :? MethodCallExpression as mc when sawValue ->
                raise (NotSupportedException(chainMethodCallMidChainMessage 0))
            | :? IndexExpression when sawValue ->
                raise (NotSupportedException(chainPositionalMidChainMessage 0))
            | :? MethodCallExpression as mc ->
                if not (isNull mc.Object) then scan mc.Object sawValue
            | _ -> ()
        scan lhs false

    /// Recognises any Ref.Value chain LHS of the shape `p.A.Value [.B.Value ...] .<TargetProp>`
    /// at depth >= 1 (single-hop and N-hop). On success returns (hops, leafTargetPropertyName).
    /// Single-hop is just a chain with one hop; routing it through this parser unifies the
    /// emit/execute path through the SqlDu pipeline.
    let private tryParseRefChainAssignmentLhs (lhs: Expression) : voption<ChainHopSpec list * string> =
        let lhs = unwrapConvertAll lhs
        match lhs with
        | :? MemberExpression as me when not (isNull me.Expression) ->
            match tryAsDbRefValueAccessor me.Expression with
            | ValueSome innerDbRefExpr ->
                let hops = walkRefChainBackwards innerDbRefExpr
                assertChainHasNoTypeCycle hops
                ValueSome (hops, me.Member.Name)
            | ValueNone -> ValueNone
        | _ -> ValueNone

    /// Chain variant of the setter-call shape `p.A.Value [.B.Value ...] .set_<TargetProp>(rhs)`
    /// at depth >= 1.
    let private tryParseRefChainSetterMethodCall (mc: MethodCallExpression) : voption<ChainHopSpec list * string * Expression> =
        if isNull mc.Method.Name || not (mc.Method.Name.StartsWith "set_") then ValueNone
        elif isNull mc.Object then ValueNone
        elif mc.Arguments.Count <> 1 then ValueNone
        else
            match tryAsDbRefValueAccessor mc.Object with
            | ValueSome innerDbRefExpr ->
                let propName = mc.Method.Name.Substring(4)
                let hops = walkRefChainBackwards innerDbRefExpr
                assertChainHasNoTypeCycle hops
                ValueSome (hops, propName, mc.Arguments.[0])
            | ValueNone -> ValueNone

    /// Chain-leaf DBRefMany source recognition. Source expression for Add/Remove/Clear is
    /// `p.A.Value.B.Value. … .<RelMany>` where `<RelMany>` is DBRefMany-shape and depth >= 1.
    /// On success returns (hops, leafManyPropertyName, leafTargetType).
    let private tryParseRefChainManySource (sourceExpr: Expression) : voption<ChainHopSpec list * string * Type> =
        let sourceExpr = unwrapConvertAll sourceExpr
        match sourceExpr with
        | :? MemberExpression as leafMe when
                not (isNull leafMe.Expression) &&
                DBRefTypeHelpers.isDBRefManyType leafMe.Type ->
            let leafManyName = leafMe.Member.Name
            let leafTargetType = leafMe.Type.GetGenericArguments().[0]
            let parent = unwrapConvertAll leafMe.Expression
            // If parent is the root parameter, this is the single-hop DBRefMany shape — defer.
            if parent :? ParameterExpression then ValueNone
            else
                match tryAsDbRefValueAccessor parent with
                | ValueSome innerDbRefExpr ->
                    let hops = walkRefChainBackwards innerDbRefExpr
                    ValueSome (hops, leafManyName, leafTargetType)
                | ValueNone -> ValueNone
        | _ -> ValueNone

    /// Build a MutateRefChainProperty op from a parsed chain assignment / setter LHS, applying
    /// leaf-side validations: target leaf property is not [<SoloId>]-marked, and RHS reduces to
    /// a closure-captured constant.
    let private buildChainPropertyMutation
            (hops: ChainHopSpec list)
            (leafTargetPropertyName: string)
            (rhsExpr: Expression) =
        let leafHop = List.last hops
        let leafTargetType = leafHop.TargetType
        let targetSoloIdProp =
            leafTargetType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
            |> Array.tryFind (fun p -> not (isNull (p.GetCustomAttribute<SoloDatabase.Attributes.SoloId>(true))))
        match targetSoloIdProp with
        | Some prop when prop.Name = leafTargetPropertyName ->
            raise (InvalidOperationException(
                sprintf "Error: Cannot use UpdateMany to write the [<SoloId>] field '%s.%s' through a Ref-chain mutation.\nReason: The [<SoloId>] field is the type's identity and may not be mutated through UpdateMany. To re-identify a row, delete and re-insert the entity."
                    leafTargetType.FullName leafTargetPropertyName))
        | _ -> ()
        let rhsValue =
            try evaluateExpr<obj> rhsExpr
            with _ ->
                raise (InvalidOperationException(
                    sprintf "Error: UpdateMany Ref-chain mutation of '%s' requires a closure-captured constant on the right-hand side.\nReason: The expression references the lambda parameter or a target-state value that cannot be reduced at translate time.\nFix: Hoist the value into a local variable above the UpdateMany call and reference that local in the lambda body."
                        leafTargetPropertyName))
        let jsonLiteral = JsonSerializator.JsonValue.Serialize(rhsValue).ToJsonString()
        let leafJsonPath = "$." + leafTargetPropertyName
        MutateRefChainProperty(hops, leafTargetType, leafJsonPath, jsonLiteral)

    let private parseDbRefSetValue (dbRefType: Type) (propertyPath: string) (valueExpr: Expression) =
        let dbRefDef = dbRefType.GetGenericTypeDefinition()
        let args = dbRefType.GetGenericArguments()
        let targetType = args.[0]
        let valueExpr = unwrapConvertAll valueExpr

        match valueExpr with
        // DBRef.From(...) — explicit cascade-insert shape; reject with a tailored Fix line.
        | :? MethodCallExpression as mc
            when mc.Method.Name = "From"
              && not (isNull mc.Method.DeclaringType)
              && mc.Method.DeclaringType.IsGenericType
              && DBRefTypeHelpers.isDBRefDefinition (mc.Method.DeclaringType.GetGenericTypeDefinition()) ->
            raise (NotSupportedException updateManyDbRefFromUnsupportedMessage)

        | :? MethodCallExpression as mc
            when mc.Method.Name = "To"
              && not (isNull mc.Method.DeclaringType)
              && mc.Method.DeclaringType.IsGenericType
              && DBRefTypeHelpers.isDBRefDefinition (mc.Method.DeclaringType.GetGenericTypeDefinition())
              && mc.Arguments.Count = 1 ->
            if DBRefTypeHelpers.isDBRefSingleDefinition dbRefDef then
                let rawId = evaluateExpr<obj> mc.Arguments.[0]
                match tryAsInt64 rawId with
                | ValueSome id when id > 0L -> SetDBRefToId(propertyPath, targetType, id)
                | _ -> raise (NotSupportedException updateManyRelationUnsupportedMessage)
            else
                let typedId = evaluateExpr<obj> mc.Arguments.[0]
                SetDBRefToTypedId(propertyPath, targetType, args.[1], typedId)

        | :? MethodCallExpression as mc
            when mc.Method.Name = "get_None"
              && not (isNull mc.Method.DeclaringType)
              && mc.Method.DeclaringType.IsGenericType
              && DBRefTypeHelpers.isDBRefDefinition (mc.Method.DeclaringType.GetGenericTypeDefinition()) ->
            SetDBRefToNone(propertyPath, targetType)

        | :? MemberExpression as me
            when me.Member.Name = "None"
              && isNull me.Expression
              && not (isNull me.Member.DeclaringType)
              && me.Member.DeclaringType.IsGenericType
              && DBRefTypeHelpers.isDBRefDefinition (me.Member.DeclaringType.GetGenericTypeDefinition()) ->
            SetDBRefToNone(propertyPath, targetType)

        | _ ->
            // Closure-captured DBRef value: a hoisted `DBRef.None`, `DBRef.To(id)`,
            // `DBRef.To(typedId)`, or `DBRef.From(unsaved)`. The AST shape here is the
            // closure access (e.g. MemberExpression onto a captured local), so we
            // evaluate to the runtime DBRef instance and classify by inspecting public
            // surface (Id, HasTypedId, TypedId) plus the internal PendingEntity probe.
            let captured = evaluateExpr<obj> valueExpr
            if isNull captured then
                SetDBRefToNone(propertyPath, targetType)
            else
                let t = captured.GetType()
                // Reject DBRef.From(unsaved) — pending entity present means cascade-insert.
                let pendingProp = t.GetProperty("PendingEntity", BindingFlags.NonPublic ||| BindingFlags.Public ||| BindingFlags.Instance)
                let hasPendingEntity =
                    if isNull pendingProp then false
                    else
                        let raw = pendingProp.GetValue(captured, null)
                        if isNull raw then false
                        else
                            let voptionType = raw.GetType()
                            let isSomeProp = voptionType.GetProperty("IsSome", BindingFlags.NonPublic ||| BindingFlags.Public ||| BindingFlags.Instance ||| BindingFlags.Static)
                            if isNull isSomeProp then false
                            elif isSomeProp.GetGetMethod(true).IsStatic then isSomeProp.GetValue(null, [| raw |]) :?> bool
                            else isSomeProp.GetValue(raw, null) :?> bool
                if hasPendingEntity then
                    raise (NotSupportedException updateManyDbRefFromUnsupportedMessage)
                // Legal capture: classify into SetDBRefToId / SetDBRefToTypedId / SetDBRefToNone.
                let readId () =
                    let idProp = t.GetProperty("Id", BindingFlags.Public ||| BindingFlags.Instance)
                    if isNull idProp then ValueNone
                    else tryAsInt64 (idProp.GetValue(captured))
                if DBRefTypeHelpers.isDBRefSingleDefinition dbRefDef then
                    match readId () with
                    | ValueSome id when id > 0L -> SetDBRefToId(propertyPath, targetType, id)
                    | _ -> SetDBRefToNone(propertyPath, targetType)
                else
                    let hasTypedIdProp = t.GetProperty("HasTypedId", BindingFlags.NonPublic ||| BindingFlags.Public ||| BindingFlags.Instance)
                    let hasTypedId =
                        if isNull hasTypedIdProp then false
                        else
                            match hasTypedIdProp.GetValue(captured) with
                            | :? bool as b -> b
                            | _ -> false
                    if hasTypedId then
                        let typedIdProp = t.GetProperty("TypedId", BindingFlags.Public ||| BindingFlags.Instance)
                        let typedIdVal = if isNull typedIdProp then null else typedIdProp.GetValue(captured)
                        SetDBRefToTypedId(propertyPath, targetType, args.[1], typedIdVal)
                    else
                        match readId () with
                        | ValueSome id when id > 0L -> SetDBRefToId(propertyPath, targetType, id)
                        | _ -> SetDBRefToNone(propertyPath, targetType)

    let private extractTargetIdForDbRefManyOrThrow (expr: Expression) =
        let valueObj = evaluateExpr<obj> expr
        if isNull valueObj then
            raise (NotSupportedException updateManyDbRefManyPersistedIdMessage)

        let idProp = valueObj.GetType().GetProperty("Id", BindingFlags.Public ||| BindingFlags.Instance)
        if isNull idProp then
            raise (NotSupportedException updateManyDbRefManyPersistedIdMessage)

        let rawId = idProp.GetValue valueObj
        match tryAsInt64 rawId with
        | ValueSome id when id > 0L -> id
        | _ -> raise (NotSupportedException updateManyDbRefManyPersistedIdMessage)

    let internal tryTranslateUpdateManyRelationTransform (expression: Expression) : UpdateManyRelationTransform voption =
        let body =
            let raw = tryLambdaBody expression
            if isNull raw then null else normalizeUpdateManyBody raw
        if isNull body then ValueNone else

        // B4 — DBRef.Value.<TargetProp> mutation. Try the supported single-hop shape first.
        // Out-of-scope variants (multi-hop, DBRefMany.Value, target-state-dependent RHS,
        // method-call mutations) fall through to the existing rejection paths.
        match body with
        | :? BinaryExpression as be when be.NodeType = ExpressionType.Assign ->
            match tryParseRefChainAssignmentLhs be.Left with
            | ValueSome (hops, leafPropName) ->
                buildChainPropertyMutation hops leafPropName be.Right |> ValueSome
            | ValueNone ->
                // Classify mid-chain violations (plain CLR property, method call, positional
                // indexer past a `.Value` accessor) so each unsupported shape gets a specific
                // hop-trail vocabulary instead of falling to the generic rejection.
                tryRaiseMidChainClassified be.Left
                if containsDBRefValueMutationPath body then
                    raise (InvalidOperationException updateManyDbRefValueMutationMessage)
                match tryGetRootRelationMember be.Left with
                | ValueSome me when DBRefTypeHelpers.isDBRefType me.Type || DBRefTypeHelpers.isDBRefManyType me.Type ->
                    raise (NotSupportedException updateManyRelationUnsupportedMessage)
                | _ -> ValueNone

        | :? MethodCallExpression as mc when mc.Method.Name.StartsWith "set_" ->
            match tryParseRefChainSetterMethodCall mc with
            | ValueSome (hops, leafPropName, rhsExpr) ->
                buildChainPropertyMutation hops leafPropName rhsExpr |> ValueSome
            | ValueNone ->
                if mc.Method.Name = "set_Item" && not (isNull mc.Object) then
                    let obj = unwrapConvertAll mc.Object
                    let rec hasValueInChain (e: Expression) =
                        match e with
                        | null -> false
                        | :? MemberExpression as me when me.Member.Name = "Value" &&
                                                         not (isNull me.Expression) &&
                                                         DBRefTypeHelpers.isDBRefType (unwrapConvertAll me.Expression).Type -> true
                        | :? MemberExpression as me -> hasValueInChain me.Expression
                        | :? MethodCallExpression as inner -> hasValueInChain inner.Object
                        | _ -> false
                    if hasValueInChain obj then
                        raise (NotSupportedException(chainPositionalMidChainMessage 0))
                if not (isNull mc.Object) then
                    tryRaiseMidChainClassified mc.Object
                if containsDBRefValueMutationPath body then
                    raise (InvalidOperationException updateManyDbRefValueMutationMessage)
                ValueNone

        | :? MethodCallExpression as mc when mc.Method.Name = "Set" ->
            let oldValue, newValue =
                if not (isNull mc.Object) && mc.Arguments.Count = 1 then
                    mc.Object, mc.Arguments.[0]
                elif mc.Arguments.Count >= 2 then
                    mc.Arguments.[0], mc.Arguments.[1]
                else
                    null, null

            if isNull oldValue || isNull newValue then ValueNone else
            match tryGetRootRelationMember oldValue with
            | ValueSome me when DBRefTypeHelpers.isDBRefType me.Type ->
                let propertyPath = computePathKeyForUpdate me
                parseDbRefSetValue me.Type propertyPath newValue |> ValueSome
            | ValueSome me when DBRefTypeHelpers.isDBRefManyType me.Type ->
                raise (NotSupportedException updateManyRelationUnsupportedMessage)
            | _ -> ValueNone

        | :? MethodCallExpression as mc when mc.Method.Name = "Add" || mc.Method.Name = "Append" || mc.Method.Name = "Remove" || mc.Method.Name = "Clear" || mc.Method.Name = "SetAt" || mc.Method.Name = "RemoveAt" || mc.Method.Name = "Insert" || mc.Method.Name = "set_Item" ->
            match tryGetCallSourceAndArgStart mc with
            | ValueSome struct (sourceExpr, argStart) ->
                // Try the multi-hop chain-leaf-Many shape first; single-hop falls through.
                match tryParseRefChainManySource sourceExpr with
                | ValueSome (hops, leafManyName, leafTargetType) ->
                    match mc.Method.Name with
                    | "Add"
                    | "Append" ->
                        if mc.Arguments.Count <= argStart then
                            raise (NotSupportedException updateManyRelationUnsupportedMessage)
                        let targetId = extractTargetIdForDbRefManyOrThrow mc.Arguments.[argStart]
                        RefChainManyAdd(hops, leafManyName, leafTargetType, targetId) |> ValueSome
                    | "Remove" ->
                        if mc.Arguments.Count <= argStart then
                            raise (NotSupportedException updateManyRelationUnsupportedMessage)
                        let targetId = extractTargetIdForDbRefManyOrThrow mc.Arguments.[argStart]
                        RefChainManyRemove(hops, leafManyName, leafTargetType, targetId) |> ValueSome
                    | "Clear" ->
                        RefChainManyClear(hops, leafManyName, leafTargetType) |> ValueSome
                    | "Insert"
                    | "SetAt"
                    | "RemoveAt"
                    | "set_Item" ->
                        raise (NotSupportedException updateManyDbRefManyPositionalMessage)
                    | _ ->
                        raise (NotSupportedException updateManyRelationUnsupportedMessage)
                | ValueNone ->
                match tryGetRootRelationMember sourceExpr with
                | ValueSome me when DBRefTypeHelpers.isDBRefManyType me.Type ->
                    let propertyPath = computePathKeyForUpdate me
                    let targetType = me.Type.GetGenericArguments().[0]
                    match mc.Method.Name with
                    | "Add"
                    | "Append" ->
                        if mc.Arguments.Count <= argStart then
                            raise (NotSupportedException updateManyRelationUnsupportedMessage)
                        let targetId = extractTargetIdForDbRefManyOrThrow mc.Arguments.[argStart]
                        AddDBRefMany(propertyPath, targetType, targetId) |> ValueSome
                    | "Remove" ->
                        if mc.Arguments.Count <= argStart then
                            raise (NotSupportedException updateManyRelationUnsupportedMessage)
                        let targetId = extractTargetIdForDbRefManyOrThrow mc.Arguments.[argStart]
                        RemoveDBRefMany(propertyPath, targetType, targetId) |> ValueSome
                    | "Clear" ->
                        ClearDBRefMany(propertyPath, targetType) |> ValueSome
                    | "Insert"
                    | "SetAt"
                    | "RemoveAt"
                    | "set_Item" ->
                        raise (NotSupportedException updateManyDbRefManyPositionalMessage)
                    | _ ->
                        raise (NotSupportedException updateManyRelationUnsupportedMessage)
                | ValueSome me when DBRefTypeHelpers.isDBRefType me.Type ->
                    raise (NotSupportedException updateManyRelationUnsupportedMessage)
                | _ -> ValueNone
            | ValueNone -> ValueNone

        | _ -> ValueNone
