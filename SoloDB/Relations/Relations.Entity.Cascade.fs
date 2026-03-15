module internal SoloDatabase.RelationsEntityCascade

open System
open System.Reflection
open System.Runtime.Serialization
open System.Collections.Generic
open Microsoft.Data.Sqlite
open SoloDatabase
open SoloDatabase.Attributes
open SoloDatabase.Utils
open SQLiteTools
open RelationsTypes
open RelationsSchema
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
    (typeStack: HashSet<Type>)
    =
    // Circular guard: detect if this exact object instance was already visited.
    if not (visited.Add(entity)) then
        raise (InvalidOperationException(
            $"Circular cascade-insert detected for type '{targetType.FullName}'. " +
            "Circular DBRef.From chains are not supported. Use DBRef.To(id) to break the cycle."))

    // 1. Insert the entity itself (shallow).
    let insertedId = insertTargetEntity tx targetTable targetType entity

    // 2. Build a tx context for the target's collection.
    let childTx: RelationTxContext = {
        Connection = tx.Connection
        OwnerTable = targetTable
        OwnerType = targetType
        InTransaction = tx.InTransaction
    }

    // 3. Process the target entity's own relation properties.
    let descriptors = buildRelationDescriptors childTx targetType

    for descriptor in descriptors do
        match descriptor.Kind with
        | Single ->
            let value = (RelationsAccessorCache.compiledPropGetter descriptor.Property).Invoke(entity)
            let refId = readDbRefId value
            if refId > 0L then
                // Already-resolved reference — create link row, validate target exists.
                ensureRelationSchema childTx descriptor
                ensureTargetExists childTx descriptor.TargetTable refId
                childTx.Connection.Execute(
                    $"INSERT INTO {quoteIdentifier descriptor.LinkTable}(SourceId, TargetId) VALUES(@sourceId, @targetId);",
                    {| sourceId = insertedId; targetId = refId |}) |> ignore
                if shouldSyncDbRefJson descriptor then
                    updateDbRefJson childTx targetTable insertedId descriptor.PropertyPath refId
            else
                match tryGetPendingEntity value with
                | ValueSome pending ->
                    let entityId = readEntityIdOrZero descriptor.TargetType pending
                    if entityId > 0L then
                        // Existing entity, link-only.
                        let dbRef = createDbRefTo descriptor.Property.PropertyType entityId
                        (RelationsAccessorCache.compiledPropSetter descriptor.Property).Invoke(entity, dbRef)
                        ensureRelationSchema childTx descriptor
                        ensureTargetExists childTx descriptor.TargetTable entityId
                        childTx.Connection.Execute(
                            $"INSERT INTO {quoteIdentifier descriptor.LinkTable}(SourceId, TargetId) VALUES(@sourceId, @targetId);",
                            {| sourceId = insertedId; targetId = entityId |}) |> ignore
                        if shouldSyncDbRefJson descriptor then
                            updateDbRefJson childTx targetTable insertedId descriptor.PropertyPath entityId
                    else
                        // RECURSIVE cascade-insert.
                        // Type-stack guard: detect semantic cycles through cloned instances.
                        if not (typeStack.Add(descriptor.TargetType)) then
                            raise (InvalidOperationException(
                                $"Circular cascade-insert detected for type '{descriptor.TargetType.FullName}'. " +
                                "Circular DBRef.From chains are not supported. Use DBRef.To(id) to break the cycle."))
                        ensureRelationSchema childTx descriptor
                        let childId =
                            try cascadeInsertDeep childTx descriptor.TargetTable descriptor.TargetType pending visited typeStack
                            finally typeStack.Remove(descriptor.TargetType) |> ignore
                        let dbRef = createDbRefTo descriptor.Property.PropertyType childId
                        (RelationsAccessorCache.compiledPropSetter descriptor.Property).Invoke(entity, dbRef)
                        let rowsAffected =
                            childTx.Connection.Execute(
                                $"INSERT OR REPLACE INTO {quoteIdentifier descriptor.LinkTable}(SourceId, TargetId) VALUES(@sourceId, @targetId);",
                                {| sourceId = insertedId; targetId = childId |})
                        ensureLinkWriteApplied childTx descriptor.LinkTable insertedId childId "INSERT OR REPLACE" rowsAffected
                        if shouldSyncDbRefJson descriptor then
                            updateDbRefJson childTx targetTable insertedId descriptor.PropertyPath childId
                | ValueNone -> ()

        | Many ->
            let trackerObj = (RelationsAccessorCache.compiledPropGetter descriptor.Property).Invoke(entity)
            match trackerObj with
            | :? IDBRefManyInternal as tracker ->
                // Always process Many items during cascade-insert, loaded or not.
                ensureRelationSchema childTx descriptor
                for item in tracker.GetCurrentItemsBoxed() do
                    if not (isNull item) then
                        let mutable itemId = readEntityIdOrZero descriptor.TargetType item
                        if itemId <= 0L then
                            // RECURSIVE cascade-insert for Many items.
                            // Type-stack guard: detect semantic cycles through cloned instances.
                            if not (typeStack.Add(descriptor.TargetType)) then
                                raise (InvalidOperationException(
                                    $"Circular cascade-insert detected for type '{descriptor.TargetType.FullName}'. " +
                                    "Circular DBRef.From chains are not supported. Use DBRef.To(id) to break the cycle."))
                            itemId <-
                                try cascadeInsertDeep childTx descriptor.TargetTable descriptor.TargetType item visited typeStack
                                finally typeStack.Remove(descriptor.TargetType) |> ignore
                        if itemId > 0L then
                            let sourceId, targetId =
                                if descriptor.OwnerUsesSourceColumn then insertedId, itemId
                                else itemId, insertedId
                            let rowsAffected =
                                childTx.Connection.Execute(
                                    $"INSERT OR IGNORE INTO {quoteIdentifier descriptor.LinkTable}(SourceId, TargetId) VALUES(@sourceId, @targetId);",
                                    {| sourceId = sourceId; targetId = targetId |})
                            ensureLinkWriteApplied childTx descriptor.LinkTable sourceId targetId "INSERT OR IGNORE" rowsAffected
            | _ -> ()

    insertedId

let internal resolveSingleTargetIdAndCascade (tx: RelationTxContext) (descriptor: RelationDescriptor) (owner: obj) (visited: HashSet<obj>) (typeStack: HashSet<Type>) =
    let value = (RelationsAccessorCache.compiledPropGetter descriptor.Property).Invoke(owner)
    let id = readDbRefId value
    if id > 0L then
        // Preflight target-exists for DBRef.To(id) before owner mutation.
        ensureTargetExists tx descriptor.TargetTable id
        id
    else
        match tryGetPendingTypedId value with
        | ValueSome typedId ->
            ensureRelationSchema tx descriptor
            let resolvedId = resolveTypedIdToTargetId tx descriptor typedId
            let dbRef = createDbRefTo descriptor.Property.PropertyType resolvedId
            (RelationsAccessorCache.compiledPropSetter descriptor.Property).Invoke(owner, dbRef)
            resolvedId
        | ValueNone ->
            match tryGetPendingEntity value with
            | ValueSome pending ->
                let entityId = readEntityIdOrZero descriptor.TargetType pending
                if entityId > 0L then
                    // Entity already persisted (Id > 0) — link-only, no re-insert.
                    // Preflight target-exists for From(existing) before owner mutation.
                    ensureTargetExists tx descriptor.TargetTable entityId
                    let dbRef = createDbRefTo descriptor.Property.PropertyType entityId
                    (RelationsAccessorCache.compiledPropSetter descriptor.Property).Invoke(owner, dbRef)
                    entityId
                else
                    // Type-stack guard: detect semantic cycles through cloned instances.
                    if not (typeStack.Add(descriptor.TargetType)) then
                        raise (InvalidOperationException(
                            $"Circular cascade-insert detected for type '{descriptor.TargetType.FullName}'. " +
                            "Circular DBRef.From chains are not supported. Use DBRef.To(id) to break the cycle."))
                    let insertedId =
                        try cascadeInsertDeep tx descriptor.TargetTable descriptor.TargetType pending visited typeStack
                        finally typeStack.Remove(descriptor.TargetType) |> ignore
                    let dbRef = createDbRefTo descriptor.Property.PropertyType insertedId
                    (RelationsAccessorCache.compiledPropSetter descriptor.Property).Invoke(owner, dbRef)
                    insertedId
            | ValueNone ->
                0L

let internal collectManyTargetIdsAndCascade (tx: RelationTxContext) (descriptor: RelationDescriptor) (owner: obj) (includeWhenUnloaded: bool) (visited: HashSet<obj>) (typeStack: HashSet<Type>) =
    let trackerObj = (RelationsAccessorCache.compiledPropGetter descriptor.Property).Invoke(owner)
    if isNull trackerObj then
        if includeWhenUnloaded then ValueSome [||] else ValueNone
    else
        match trackerObj with
        | :? IDBRefManyInternal as tracker ->
            if not includeWhenUnloaded && not tracker.IsLoaded then
                ValueNone
            else
                let ids = System.Collections.Generic.HashSet<int64>()
                for item in tracker.GetCurrentItemsBoxed() do
                    if isNull item then
                        ()
                    else
                        let mutable id = readEntityIdOrZero descriptor.TargetType item
                        if id <= 0L then
                            // Type-stack guard: detect semantic cycles through cloned instances.
                            if not (typeStack.Add(descriptor.TargetType)) then
                                raise (InvalidOperationException(
                                    $"Circular cascade-insert detected for type '{descriptor.TargetType.FullName}'. " +
                                    "Circular DBRef.From chains are not supported. Use DBRef.To(id) to break the cycle."))
                            id <-
                                try cascadeInsertDeep tx descriptor.TargetTable descriptor.TargetType item visited typeStack
                                finally typeStack.Remove(descriptor.TargetType) |> ignore
                        elif id > 0L then
                            // Preflight target-exists for pre-existing many items before owner mutation.
                            ensureTargetExists tx descriptor.TargetTable id
                        if id > 0L then
                            ids.Add(id) |> ignore
                ValueSome (ids |> Seq.toArray)
        | _ ->
            raise (InvalidOperationException(
                $"Error: Property '{descriptor.OwnerType.FullName}.{descriptor.Property.Name}' does not implement IDBRefManyInternal.\nReason: The relation property type is incorrect.\nFix: Use DBRefMany<T> or DBRefMany<TTarget,'TId> for multi-relations."))

let internal readSingleIdNoCascade (descriptor: RelationDescriptor) (owner: obj) =
    (RelationsAccessorCache.compiledPropGetter descriptor.Property).Invoke(owner) |> readDbRefId
