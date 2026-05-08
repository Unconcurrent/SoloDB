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

    /// Tries to recognise an LHS of the shape `p.<RelationProp>.Value.<TargetProp>`.
    /// On success returns the root DBRef-typed MemberExpression and the target property name.
    /// Multi-hop paths (`p.X.Value.Y.Z`) return ValueNone here — caller treats as out of scope.
    let private tryParseDbRefValueAssignmentLhs (lhs: Expression) =
        let lhs = unwrapConvertAll lhs
        match lhs with
        | :? MemberExpression as me when not (isNull me.Expression) ->
            match tryAsDbRefValueAccessor me.Expression with
            | ValueSome dbRefExpr ->
                match tryGetRootRelationMember dbRefExpr with
                | ValueSome rootMe when DBRefTypeHelpers.isDBRefType rootMe.Type -> ValueSome (rootMe, me.Member.Name)
                | _ -> ValueNone
            | ValueNone -> ValueNone
        | _ -> ValueNone

    /// Tries to recognise an LHS of the shape `p.<RelationProp>.Value.set_<TargetProp>(rhs)`.
    /// Returns the root DBRef MemberExpression, target property name, and RHS expression.
    let private tryParseDbRefValueSetterMethodCall (mc: MethodCallExpression) =
        if isNull mc.Method.Name || not (mc.Method.Name.StartsWith "set_") then ValueNone
        elif isNull mc.Object then ValueNone
        elif mc.Arguments.Count <> 1 then ValueNone
        else
            match tryAsDbRefValueAccessor mc.Object with
            | ValueSome dbRefExpr ->
                match tryGetRootRelationMember dbRefExpr with
                | ValueSome rootMe when DBRefTypeHelpers.isDBRefType rootMe.Type ->
                    let propName = mc.Method.Name.Substring(4)
                    ValueSome (rootMe, propName, mc.Arguments.[0])
                | _ -> ValueNone
            | ValueNone -> ValueNone

    /// B4 — property mutation of the target row through a single-arg DBRef.Value.<X>.
    /// Validates: target X is not [<SoloId>]; RHS is a closure-captured constant.
    let private parseDbRefValueMutation
            (rootMe: MemberExpression)
            (targetPropertyName: string)
            (rhsExpr: Expression) =
        let dbRefType = unwrapConvertAll rootMe.Expression |> ignore; rootMe.Type
        let args = dbRefType.GetGenericArguments()
        let targetType = args.[0]
        // Reject [<SoloId>] target — closes the bypass around Item I.
        let targetSoloIdProp =
            targetType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
            |> Array.tryFind (fun p -> not (isNull (p.GetCustomAttribute<SoloDatabase.Attributes.SoloId>(true))))
        match targetSoloIdProp with
        | Some prop when prop.Name = targetPropertyName ->
            raise (InvalidOperationException(
                sprintf "Error: Cannot use UpdateMany to write the [<SoloId>] field '%s.%s' through a DBRef.Value mutation.\nReason: The [<SoloId>] field is the type's identity and may not be mutated through UpdateMany. To re-identify a row, delete and re-insert the entity."
                    targetType.FullName targetPropertyName))
        | _ -> ()
        // RHS must evaluate to a closure-captured constant — no reference to the lambda parameter.
        let rhsValue =
            try evaluateExpr<obj> rhsExpr
            with _ ->
                raise (InvalidOperationException(
                    sprintf "Error: UpdateMany DBRef.Value.%s mutation requires a closure-captured constant on the right-hand side.\nReason: The expression references the lambda parameter or a target-state value that cannot be reduced at translate time.\nFix: Hoist the value into a local variable above the UpdateMany call and reference that local in the lambda body."
                        targetPropertyName))
        let jsonLiteral = JsonSerializator.JsonValue.Serialize(rhsValue).ToJsonString()
        let ownerProperty = (tryGetRootRelationMember (rootMe :> Expression) |> ValueOption.get).Member.Name
        let targetPropertyJsonPath = "$." + targetPropertyName
        MutateDBRefTargetProperty(ownerProperty, targetType, targetPropertyJsonPath, jsonLiteral)

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
            // Closure-captured DBRef value (e.g. `let r = DBRef.From(unsaved)` hoisted out of
            // the lambda; body uses `p.Ref.Set r`). Inspect at runtime via reflection on the
            // PendingEntity voption property — if a pending entity is present, the captured
            // DBRef came from DBRef.From(...), and the From-cascade rejection applies.
            let captured = evaluateExpr<obj> valueExpr
            let hasPendingEntity =
                if isNull captured then false
                else
                    let t = captured.GetType()
                    let pendingProp = t.GetProperty("PendingEntity", BindingFlags.NonPublic ||| BindingFlags.Public ||| BindingFlags.Instance)
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
            raise (NotSupportedException updateManyRelationUnsupportedMessage)

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
            match tryParseDbRefValueAssignmentLhs be.Left with
            | ValueSome (rootMe, targetPropName) ->
                parseDbRefValueMutation rootMe targetPropName be.Right |> ValueSome
            | ValueNone ->
                // Fall through to the existing rejection / generic-assign handling.
                if containsDBRefValueMutationPath body then
                    raise (InvalidOperationException updateManyDbRefValueMutationMessage)
                match tryGetRootRelationMember be.Left with
                | ValueSome me when DBRefTypeHelpers.isDBRefType me.Type || DBRefTypeHelpers.isDBRefManyType me.Type ->
                    raise (NotSupportedException updateManyRelationUnsupportedMessage)
                | _ -> ValueNone

        | :? MethodCallExpression as mc when mc.Method.Name.StartsWith "set_" ->
            match tryParseDbRefValueSetterMethodCall mc with
            | ValueSome (rootMe, targetPropName, rhsExpr) ->
                parseDbRefValueMutation rootMe targetPropName rhsExpr |> ValueSome
            | ValueNone ->
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
