module internal SoloDatabase.RelationsEntityCascade

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

type private ReferenceComparer() =
    interface IEqualityComparer<obj> with
        member _.Equals(x, y) = Object.ReferenceEquals(x, y)
        member _.GetHashCode(x) = System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(x)

let internal refComparer = ReferenceComparer() :> IEqualityComparer<obj>

// ─── Recursive cascade-insert with full-graph circular guard ─────────────────

let rec internal cascadeInsertDeep
    (tx: RelationTxContext)
    (targetTable: string)
    (targetType: Type)
    (entity: obj)
    (visited: HashSet<obj>)
    (depth: int)
    =
    if depth > maxRelationDepth then
        raise (InvalidOperationException($"Reference mutation creates a path exceeding {maxRelationDepth} reference hops."))
    // Circular guard: detect if this exact object instance was already visited.
    if not (visited.Add(entity)) then
        raise (InvalidOperationException(
            $"Circular cascade-insert detected for type '{targetType.FullName}'. " +
            "Circular DBRef.From chains are not supported. Remove the circular reference."))

    // 1. Insert the entity itself (shallow).
    let insertedId = insertTargetEntity tx targetTable targetType entity

    // 2. Build a tx context for the target's collection.
    let childTx: RelationTxContext = {
        Connection = tx.Connection
        OwnerTable = targetTable
        OwnerType = targetType
        InTransaction = tx.InTransaction
        Preparation = tx.Preparation
        CollectionFactory = tx.CollectionFactory
    }

    RelationsGraphAdmission.inserted childTx insertedId

    // 3. Process the target entity's own relation properties.
    let descriptors = buildRelationDescriptors childTx targetType

    for descriptor in descriptors do
        match descriptor.Kind with
        | Single ->
            let value = descriptor.Property.Read.Invoke(entity)
            let refId = readDbRefId value
            if refId > 0L then
                // Already-resolved reference — create link row, validate target exists.
                ensureRelationSchema childTx descriptor
                ensureTargetExists childTx descriptor.TargetTable refId
                RelationMutationStatements.linkInsertPlain childTx.Connection descriptor.LinkTable insertedId refId |> ignore
                if shouldSyncDbRefJson descriptor then
                    updateDbRefJson childTx targetTable insertedId descriptor.PropertyPath refId
            else
                match tryGetPendingEntity value with
                | ValueSome pending ->
                    let entityId = readEntityIdOrZero descriptor.TargetType pending
                    if entityId > 0L then
                        // Existing entity, link-only.
                        let dbRef = createDbRefTo descriptor.Property.ReferenceType entityId
                        descriptor.Property.Write.Invoke(entity, dbRef)
                        ensureRelationSchema childTx descriptor
                        ensureTargetExists childTx descriptor.TargetTable entityId
                        RelationMutationStatements.linkInsertPlain childTx.Connection descriptor.LinkTable insertedId entityId |> ignore
                        if shouldSyncDbRefJson descriptor then
                            updateDbRefJson childTx targetTable insertedId descriptor.PropertyPath entityId
                    else
                        ensureRelationSchema childTx descriptor
                        let childId =
                            cascadeInsertDeep childTx descriptor.TargetTable descriptor.TargetType pending visited (depth + 1)
                        let dbRef = createDbRefTo descriptor.Property.ReferenceType childId
                        descriptor.Property.Write.Invoke(entity, dbRef)
                        let rowsAffected =
                            RelationMutationStatements.linkInsertReplacingConflict childTx.Connection descriptor.LinkTable insertedId childId
                        ensureLinkWriteApplied childTx descriptor.LinkTable insertedId childId "INSERT OR REPLACE" rowsAffected
                        if shouldSyncDbRefJson descriptor then
                            updateDbRefJson childTx targetTable insertedId descriptor.PropertyPath childId
                | ValueNone -> ()

        | Many ->
            let trackerObj = descriptor.Property.Read.Invoke(entity)
            match trackerObj with
            | :? IDBRefManyInternal as tracker ->
                // Always process Many items during cascade-insert, loaded or not.
                ensureRelationSchema childTx descriptor
                for item in tracker.GetCurrentItemsBoxed() do
                    if not (isNull item) then
                        let mutable itemId = readEntityIdOrZero descriptor.TargetType item
                        if itemId <= 0L then
                            // RECURSIVE cascade-insert for Many items.
                            itemId <-
                                cascadeInsertDeep childTx descriptor.TargetTable descriptor.TargetType item visited (depth + 1)
                        if itemId > 0L then
                            // Defense-in-depth symmetric with the Single path: when the
                            // descriptor declares a typed-id, verify the just-cascade-inserted
                            // child carries a non-default [<SoloId>] before linking. After the
                            // cascade write-path fix this branch is unreachable in normal flow;
                            // the assertion catches any future regression that reintroduces a
                            // default-shape child SoloId.
                            match descriptor.TypedIdType with
                            | ValueSome _ -> extractSoloIdOrFail descriptor.TargetType item |> ignore
                            | ValueNone -> ()
                            let sourceId, targetId =
                                if descriptor.OwnerUsesSourceColumn then insertedId, itemId
                                else itemId, insertedId
                            let rowsAffected =
                                RelationMutationStatements.linkInsertIgnoringConflict childTx.Connection descriptor.LinkTable sourceId targetId
                            ensureLinkWriteApplied childTx descriptor.LinkTable sourceId targetId "INSERT OR IGNORE" rowsAffected
            | _ -> ()

    let filesystem = getFileSystemReferenceSpecs targetType
    if filesystem.Length <> 0 then
        ensureFileSystemReferenceTable childTx.Connection
        ensureFileSystemOwnerCleanup childTx.Connection (formatName childTx.OwnerTable)
        for spec in filesystem do ensureFileSystemReferenceSchema childTx spec
        let ops = ResizeArray<RelationUpdateManyOp>()
        prepareFileSystemReferences childTx entity ValueNone false true true ops
        for op in ops do applyFileSystemReferenceOp childTx insertedId false op

    visited.Remove(entity) |> ignore
    insertedId

let internal resolveSingleTargetIdAndCascade (tx: RelationTxContext) (descriptor: RelationDescriptor) (owner: obj) (visited: HashSet<obj>) (depth: int) =
    let value = descriptor.Property.Read.Invoke(owner)
    let id = readDbRefId value
    if id > 0L then
        // Preflight target-exists for DBRef.To(id) before owner mutation.
        ensureTargetExists tx descriptor.TargetTable id
        id
    else
        // PendingEntity is tested first because the eager .From(entityWithSoloId) constructor
        // sets _hasTypedId=true && _isLoaded=true on the DBRef instance, while
        // HasPendingTypedId additionally requires not _isLoaded — so tryGetPendingTypedId
        // returns None and this branch correctly falls through to the entity-cascade path
        // below. The ordering also guards against a future relaxation of HasPendingTypedId
        // reintroducing the alias bug — a .From(unsaved) MUST cascade-insert, not look up
        // an existing target row.
        let dbRefType = descriptor.Property.ReferenceType
        let isTyped = isTypedDbRef dbRefType
        match tryGetPendingEntity value with
        | ValueSome pending ->
            let entityId = readEntityIdOrZero descriptor.TargetType pending
            if entityId > 0L then
                // Entity already persisted (Id > 0) — link-only, no re-insert.
                ensureTargetExists tx descriptor.TargetTable entityId
                let dbRef =
                    if isTyped then
                        let typedId = extractSoloIdOrFail descriptor.TargetType pending
                        createDbRefLoadedTyped dbRefType entityId typedId pending
                    else
                        createDbRefTo dbRefType entityId
                descriptor.Property.Write.Invoke(owner, dbRef)
                entityId
            else
                let insertedId =
                    cascadeInsertDeep tx descriptor.TargetTable descriptor.TargetType pending visited (depth + 1)
                let dbRef =
                    if isTyped then
                        let typedId = extractSoloIdOrFail descriptor.TargetType pending
                        createDbRefLoadedTyped dbRefType insertedId typedId pending
                    else
                        createDbRefTo dbRefType insertedId
                descriptor.Property.Write.Invoke(owner, dbRef)
                insertedId
        | ValueNone ->
            match tryGetPendingTypedId value with
            | ValueSome typedId ->
                ensureRelationSchema tx descriptor
                let resolvedId = resolveTypedIdToTargetId tx descriptor typedId
                let dbRef =
                    if isTyped then
                        // Pending typed-id from .To(typedId): the typed id is already known and survived
                        // the resolver; stamp it onto the result so .TypedId on the post-cascade DBRef
                        // returns the original typed id without a hydration step.
                        createDbRefResolvedTyped dbRefType resolvedId typedId
                    else
                        createDbRefTo dbRefType resolvedId
                descriptor.Property.Write.Invoke(owner, dbRef)
                resolvedId
            | ValueNone ->
                0L

let internal collectManyTargetIdsAndCascade (tx: RelationTxContext) (descriptor: RelationDescriptor) (owner: obj) (includeWhenUnloaded: bool) (visited: HashSet<obj>) (depth: int) =
    let trackerObj = descriptor.Property.Read.Invoke(owner)
    if isNull trackerObj then
        if includeWhenUnloaded then ValueSome [||] else ValueNone
    else
        match trackerObj with
        | :? IDBRefManyInternal as tracker ->
            if not includeWhenUnloaded && not tracker.IsLoaded then
                ValueNone
            else
                let ids = System.Collections.Generic.HashSet<int64>()
                let canBatch = EntityIdentity.hasTrivialInt64IdReader descriptor.TargetType
                let pending = ResizeArray<int64>()
                let flush() =
                    ensureTargetsExist tx descriptor.TargetTable pending
                    for id in pending do ids.Add id |> ignore
                    pending.Clear()
                for item in tracker.GetCurrentItemsBoxed() do
                    if isNull item then
                        ()
                    else
                        let mutable id = readEntityIdOrZero descriptor.TargetType item
                        if id <= 0L then
                            flush()
                            id <-
                                cascadeInsertDeep tx descriptor.TargetTable descriptor.TargetType item visited (depth + 1)
                            // Defense-in-depth symmetric with the Single path and the recursive
                            // Many branch in cascadeInsertDeep: when the relation declares a
                            // typed-id contract, the just-cascade-inserted item must carry a
                            // non-default [<SoloId>] before its rowid is added to the link plan.
                            match descriptor.TypedIdType with
                            | ValueSome _ -> extractSoloIdOrFail descriptor.TargetType item |> ignore
                            | ValueNone -> ()
                        elif id > 0L then
                            // Preflight target-exists for pre-existing many items before owner mutation.
                            if canBatch then
                                pending.Add id
                                if pending.Count = 500 then flush()
                            else
                                ensureTargetExists tx descriptor.TargetTable id
                        if id > 0L && (not canBatch || pending.Count = 0) then
                            ids.Add(id) |> ignore
                flush()
                ValueSome (ids |> Seq.toArray)
        | _ ->
            raise (InvalidOperationException(
                $"Error: Property '{descriptor.OwnerType.FullName}.{descriptor.Property.Name}' does not implement IDBRefManyInternal.\nReason: The relation property type is incorrect.\nFix: Use DBRefMany<T> or DBRefMany<TTarget,'TId> for multi-relations."))

let internal readSingleIdNoCascade (descriptor: RelationDescriptor) (owner: obj) =
    descriptor.Property.Read.Invoke(owner) |> readDbRefId
