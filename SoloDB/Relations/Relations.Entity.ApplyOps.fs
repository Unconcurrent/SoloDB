module internal SoloDatabase.RelationsEntityApplyOps

open System
open System.Reflection
open System.Runtime.Serialization
open System.Collections.Generic
open SoloDatabase
open SoloDatabase.Attributes
open SoloDatabase.Utils
open NativeSQLiteTools
open RelationsTypes
open RelationsSchemaBuilder
open RelationsSchemaValidator
open RelationsSchemaLinkTableDDL
open RelationsEntitySync

let internal applyOps (tx: RelationTxContext) (ownerId: int64) (plan: RelationWritePlan) (updateOwnerJson: bool) =
    let descriptorMap = relationDescriptorByPath tx tx.OwnerType

    let resolveDescriptor propertyPath =
        match descriptorMap.TryGetValue(propertyPath) with
        | true, descriptor -> descriptor
        | _ ->
            let firstSegment =
                let idx = propertyPath.IndexOf('.')
                if idx <= 0 then propertyPath else propertyPath.Substring(0, idx)
            match descriptorMap.TryGetValue(firstSegment) with
            | true, descriptor -> descriptor
            | _ ->
                raise (InvalidOperationException(
                    $"Error: Unknown relation property path '{propertyPath}' for owner type '{tx.OwnerType.FullName}'.\nReason: The path does not match any relation on the type.\nFix: Use a valid relation property path."))

    let deleteSingleLink (descriptor: RelationDescriptor) =
        RelationMutationStatements.linkDeleteByOwner tx.Connection descriptor.LinkTable "SourceId" ownerId |> ignore

    let insertSingleLink (descriptor: RelationDescriptor) (targetId: int64) =
        RelationMutationStatements.linkInsertPlain tx.Connection descriptor.LinkTable ownerId targetId |> ignore

    let applySingleSetAndSync (descriptor: RelationDescriptor) (targetId: int64) =
        deleteSingleLink descriptor
        insertSingleLink descriptor targetId
        if updateOwnerJson && shouldSyncDbRefJson descriptor then
            updateDbRefJson tx tx.OwnerTable ownerId descriptor.PropertyPath targetId

    let manyColumns (descriptor: RelationDescriptor) =
        if descriptor.OwnerUsesSourceColumn then "SourceId", "TargetId"
        else "TargetId", "SourceId"

    for op in plan.Ops do
        match op with
        | SetDBRefToId(propertyPath, targetType, targetId) ->
            let descriptor = resolveDescriptor propertyPath
            if descriptor.Kind <> Single then
                raise (InvalidOperationException(
                    $"Error: Relation '{propertyPath}' is not DBRef<T>.\nReason: The relation type does not match the expected DBRef.\nFix: Use a DBRef<T> relation or correct the property path."))
            if not (let expected = descriptor.TargetType in if isNull expected then isNull targetType else expected.Equals(targetType)) then
                raise (InvalidOperationException(
                    $"Error: Relation target type mismatch on '{propertyPath}'.\nReason: Expected {descriptor.TargetType.FullName}, got {targetType.FullName}.\nFix: Use the correct target type for this relation."))
            ensureTargetExists tx descriptor.TargetTable targetId
            applySingleSetAndSync descriptor targetId

        | SetDBRefToTypedId(propertyPath, targetType, targetIdType, targetTypedId) ->
            let descriptor = resolveDescriptor propertyPath
            if descriptor.Kind <> Single then
                raise (InvalidOperationException(
                    $"Error: Relation '{propertyPath}' is not DBRef<T>.\nReason: The relation type does not match the expected DBRef.\nFix: Use a DBRef<T> relation or correct the property path."))
            if not (let expected = descriptor.TargetType in if isNull expected then isNull targetType else expected.Equals(targetType)) then
                raise (InvalidOperationException(
                    $"Error: Relation target type mismatch on '{propertyPath}'.\nReason: Expected {descriptor.TargetType.FullName}, got {targetType.FullName}.\nFix: Use the correct target type for this relation."))
            match descriptor.TypedIdType with
            | ValueSome configured when (if isNull configured then isNull targetIdType else configured.Equals(targetIdType)) -> ()
            | ValueSome configured ->
                raise (InvalidOperationException(
                    $"Error: Typed id type mismatch on '{propertyPath}'.\nReason: Expected {configured.FullName}, got {targetIdType.FullName}.\nFix: Use the correct typed id type for this relation."))
            | ValueNone ->
                raise (InvalidOperationException(
                    $"Error: Relation '{propertyPath}' does not support typed-id UpdateMany Set.\nReason: This relation is DBRef<T> row-id based.\nFix: Use DBRef.To(int64) or change relation type to DBRef<TTarget,'TId>."))
            let targetId = resolveTypedIdToTargetId tx descriptor targetTypedId
            applySingleSetAndSync descriptor targetId

        | SetDBRefToNone(propertyPath, targetType) ->
            let descriptor = resolveDescriptor propertyPath
            if descriptor.Kind <> Single then
                raise (InvalidOperationException(
                    $"Error: Relation '{propertyPath}' is not DBRef<T>.\nReason: The relation type does not match the expected DBRef.\nFix: Use a DBRef<T> relation or correct the property path."))
            if not (let expected = descriptor.TargetType in if isNull expected then isNull targetType else expected.Equals(targetType)) then
                raise (InvalidOperationException(
                    $"Error: Relation target type mismatch on '{propertyPath}'.\nReason: Expected {descriptor.TargetType.FullName}, got {targetType.FullName}.\nFix: Use the correct target type for this relation."))
            deleteSingleLink descriptor
            if updateOwnerJson && shouldSyncDbRefJson descriptor then
                updateDbRefJson tx tx.OwnerTable ownerId descriptor.PropertyPath 0L

        | AddDBRefMany(propertyPath, targetType, targetId) ->
            let descriptor = resolveDescriptor propertyPath
            if descriptor.Kind <> Many then
                raise (InvalidOperationException(
                    $"Error: Relation '{propertyPath}' is not DBRefMany<T>.\nReason: The relation type does not match the expected DBRefMany.\nFix: Use a DBRefMany<T> relation or correct the property path."))
            if not (let expected = descriptor.TargetType in if isNull expected then isNull targetType else expected.Equals(targetType)) then
                raise (InvalidOperationException(
                    $"Error: Relation target type mismatch on '{propertyPath}'.\nReason: Expected {descriptor.TargetType.FullName}, got {targetType.FullName}.\nFix: Use the correct target type for this relation."))
            ensureTargetExists tx descriptor.TargetTable targetId
            let sourceId, targetIdValue =
                if descriptor.OwnerUsesSourceColumn then ownerId, targetId
                else targetId, ownerId
            let rowsAffected =
                RelationMutationStatements.linkInsertIgnoringConflict tx.Connection descriptor.LinkTable sourceId targetIdValue
            ensureLinkWriteApplied tx descriptor.LinkTable sourceId targetIdValue "INSERT OR IGNORE" rowsAffected

        | RemoveDBRefMany(propertyPath, targetType, targetId) ->
            let descriptor = resolveDescriptor propertyPath
            if descriptor.Kind <> Many then
                raise (InvalidOperationException(
                    $"Error: Relation '{propertyPath}' is not DBRefMany<T>.\nReason: The relation type does not match the expected DBRefMany.\nFix: Use a DBRefMany<T> relation or correct the property path."))
            if not (let expected = descriptor.TargetType in if isNull expected then isNull targetType else expected.Equals(targetType)) then
                raise (InvalidOperationException(
                    $"Error: Relation target type mismatch on '{propertyPath}'.\nReason: Expected {descriptor.TargetType.FullName}, got {targetType.FullName}.\nFix: Use the correct target type for this relation."))
            let ownerColumn, targetColumn = manyColumns descriptor
            RelationMutationStatements.linkDeleteByOwnerAndTarget tx.Connection descriptor.LinkTable ownerColumn ownerId targetColumn targetId |> ignore

        | ClearDBRefMany(propertyPath, targetType) ->
            let descriptor = resolveDescriptor propertyPath
            if descriptor.Kind <> Many then
                raise (InvalidOperationException(
                    $"Error: Relation '{propertyPath}' is not DBRefMany<T>.\nReason: The relation type does not match the expected DBRefMany.\nFix: Use a DBRefMany<T> relation or correct the property path."))
            if not (let expected = descriptor.TargetType in if isNull expected then isNull targetType else expected.Equals(targetType)) then
                raise (InvalidOperationException(
                    $"Error: Relation target type mismatch on '{propertyPath}'.\nReason: Expected {descriptor.TargetType.FullName}, got {targetType.FullName}.\nFix: Use the correct target type for this relation."))
            let ownerColumn, _ = manyColumns descriptor
            RelationMutationStatements.linkDeleteByOwner tx.Connection descriptor.LinkTable ownerColumn ownerId |> ignore

        | RefreshFileSystemRef _ | SetFileSystemRef _ | AddFileSystemRef _ | RemoveFileSystemRef _ | ClearFileSystemRefs _ | AcceptFileSystemRefChanges _ ->
            applyFileSystemReferenceOp tx ownerId updateOwnerJson op
