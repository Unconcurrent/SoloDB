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
        "Error: UpdateMany DBRefMany Add/Remove requires a persisted target Id.\nReason: The target Id must be > 0 to reference an existing row.\nFix: Save the target first or use a valid persisted Id."

    [<Literal>]
    let private updateManyDbRefValueMutationMessage =
        "Error: UpdateMany cannot mutate DBRef.Value members.\nReason: DBRef.Value is not mutable through UpdateMany.\nFix: Update the target collection explicitly, then update the reference."

    /// Deterministic unsupported-shape messages.
    let internal multiSourceCrossRootProjectionMessage =
        "MSQ002: Cross-root projection requires explicit join key."

    let internal multiSourceClientEvalForbiddenMessage =
        "MSQ004: Client-side evaluation is forbidden for this query shape."

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

    let private parseDbRefSetValue (dbRefType: Type) (propertyPath: string) (valueExpr: Expression) =
        let dbRefDef = dbRefType.GetGenericTypeDefinition()
        let args = dbRefType.GetGenericArguments()
        let targetType = args.[0]
        let valueExpr = unwrapConvertAll valueExpr

        match valueExpr with
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

        if containsDBRefValueMutationPath body then
            raise (InvalidOperationException updateManyDbRefValueMutationMessage)

        match body with
        | :? BinaryExpression as be when be.NodeType = ExpressionType.Assign ->
            match tryGetRootRelationMember be.Left with
            | ValueSome me when DBRefTypeHelpers.isDBRefType me.Type || DBRefTypeHelpers.isDBRefManyType me.Type ->
                raise (NotSupportedException updateManyRelationUnsupportedMessage)
            | _ -> ValueNone

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

        | :? MethodCallExpression as mc when mc.Method.Name = "Add" || mc.Method.Name = "Append" || mc.Method.Name = "Remove" || mc.Method.Name = "Clear" || mc.Method.Name = "SetAt" || mc.Method.Name = "RemoveAt" ->
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
                    | _ ->
                        raise (NotSupportedException updateManyRelationUnsupportedMessage)
                | ValueSome me when DBRefTypeHelpers.isDBRefType me.Type ->
                    raise (NotSupportedException updateManyRelationUnsupportedMessage)
                | _ -> ValueNone
            | ValueNone -> ValueNone

        | _ -> ValueNone
