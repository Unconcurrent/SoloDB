
module internal SoloDatabase.RelationsBatchLoad

open System
open System.Reflection
open System.Collections.Generic
open SoloDatabase
open SoloDatabase.Attributes
open SoloDatabase.Utils
open SoloDatabase.JsonSerializator
open NativeSQLiteTools
open JsonFunctions
open RelationsTypes
open RelationsSchemaBuilder
open RelationsSchemaValidator
open RelationsSchemaLinkTableDDL
open RelationsEntitySync
open RelationsEntityCascade
open RelationsEntityApplyOps

let private maxRecursiveDepth = Utils.maxRelationDepth

let private loadSingleProperties
    (tx: RelationTxContext)
    (excludedPaths: HashSet<string>) (includedPaths: HashSet<string>) (whitelistMode: bool)
    (ownerEntities: (int64 * obj) array) (depth: int) (currentPrefix: string)
    =
    let connection, ownerTable, ownerType = tx.Connection, tx.OwnerTable, tx.OwnerType
    RelationsFileSystemBatchLoad.batchLoadFileSystemProperties connection ownerTable ownerType excludedPaths includedPaths whitelistMode ownerEntities Single currentPrefix
    if ownerEntities.Length = 0 then ()
    else

    let singleDescriptors = buildRelationDescriptors tx ownerType |> Array.filter (fun d -> d.Kind = Single)
    if singleDescriptors.Length = 0 then ()
    else

    let ownerTable = formatName ownerTable
    let fullPath (propName: string) =
        if currentPrefix = "" then propName else currentPrefix + "." + propName

    let shouldLoadPath (propName: string) =
        let fp = fullPath propName
        // Check exact or prefix-based exclusion
        if excludedPaths |> Seq.exists (fun e -> e = fp || fp.StartsWith(e + ".")) then false
        // Whitelist mode: only admitted paths load
        elif whitelistMode then
            includedPaths |> Seq.exists (fun i -> i = fp || i.StartsWith(fp + ".") || fp.StartsWith(i + "."))
        // Default load-all: everything loads
        else true

    for descriptor in singleDescriptors do
        let prop, targetType = descriptor.Property, descriptor.TargetType
        if not (shouldLoadPath prop.Name) then ()
        else

        let targetTable = descriptor.TargetTable
        let qTarget = quoteIdentifier targetTable
        requireWritableInt64Id targetType

        let ownerTargets = ResizeArray<int64 * obj * int64>()
        let distinctTargetIds = HashSet<int64>()

        let propGetter = prop.Read
        for (ownerId, ownerObj) in ownerEntities do
            let dbRefObj = propGetter.Invoke(ownerObj)
            let targetId = readDbRefId dbRefObj
            if targetId > 0L then
                ownerTargets.Add(ownerId, ownerObj, targetId)
                distinctTargetIds.Add(targetId) |> ignore

        if ownerTargets.Count > 0 then
            let payloads = Dictionary<int64, string>()
            let loadedTargets = Dictionary<struct(int64 * int64), obj>()
            let targetIds = distinctTargetIds |> Seq.toArray
            let batchSize = 500
            let mutable offset = 0
            while offset < targetIds.Length do
                let length = min batchSize (targetIds.Length - offset)
                let paramNames = Array.init length (fun i -> $"@_tid{i}")
                let inClause = String.Join(", ", paramNames)
                let sql = $"SELECT Id, json_quote(Value) as ValueJSON FROM {qTarget} WHERE Id IN ({inClause});"
                let mutable statement = connection.OpenStatement(sql)
                try
                    for i = 0 to length - 1 do statement.BindInt64(i + 1, targetIds.[offset + i])
                    while statement.Step() do
                        let row = statement.Row
                        if not (row.IsDBNull(1)) then payloads.[row.GetInt64(0)] <- row.GetString(1)
                    statement.Dispose()
                with error -> statement.Abort(error); reraise()
                offset <- offset + length

            // Typed DBRef<'TTarget,'TId>: stamp _typedId from the materialized target's [<SoloId>] so
            // .TypedId on the hydrated reference returns the typed id without a separate query.
            // Single-arg DBRef<'T>: keep the legacy 2-arg Loaded path.
            let propSetter = prop.Write
            let isTyped = isTypedDbRef prop.ReferenceType
            let idWriter = EntityIdentity.compiledInt64IdWriter targetType
            for (ownerId, ownerObj, targetId) in ownerTargets do
                match payloads.TryGetValue(targetId) with
                | true, json ->
                    // Root Singles share targets; recursive distinct physical owners keep
                    // independent targets while repeated views of one owner share them.
                    let key = struct((if depth = 0 then 0L else ownerId), targetId)
                    let targetObj =
                        match loadedTargets.TryGetValue key with
                        | true, target -> target
                        | _ ->
                            let target = JsonValue.ParseRuntime(targetType,json)
                            idWriter.Invoke(target, targetId)
                            loadedTargets.Add(key, target)
                            target
                    let loadedRef =
                        if isTyped then
                            match SoloIdAccessor.TryGetBoxedValue(targetType, targetObj) with
                            | ValueSome typedId ->
                                createDbRefLoadedTyped prop.ReferenceType targetId typedId targetObj
                            | ValueNone ->
                                // Target row exists but has no [<SoloId>] populated. Fall back to the
                                // legacy unstamped form — .TypedId access on this DBRef will throw with
                                // the "not hydrated" diagnostic, which is correct: the typed id is
                                // genuinely unknown at this row.
                                createDbRefLoaded prop.ReferenceType targetId targetObj
                        else
                            createDbRefLoaded prop.ReferenceType targetId targetObj
                    propSetter.Invoke(ownerObj, loadedRef)
                | _ -> ()

let private loadManyProperties
    (tx: RelationTxContext)
    (excludedPaths: HashSet<string>) (includedPaths: HashSet<string>) (whitelistMode: bool)
    (ownerEntities: (int64 * obj) array) (currentPrefix: string)
    =
    let connection, ownerTable, ownerType = tx.Connection, tx.OwnerTable, tx.OwnerType
    RelationsFileSystemBatchLoad.batchLoadFileSystemProperties connection ownerTable ownerType excludedPaths includedPaths whitelistMode ownerEntities Many currentPrefix
    if ownerEntities.Length = 0 then ()
    else

    let ownerTable = formatName ownerTable
    let manyDescriptors = buildRelationDescriptors tx ownerType |> Array.filter (fun d -> d.Kind = Many)
    if manyDescriptors.Length = 0 then ()
    else

    let fullPath (propName: string) =
        if currentPrefix = "" then propName else currentPrefix + "." + propName

    let shouldLoadPath (propName: string) =
        let fp = fullPath propName
        if excludedPaths |> Seq.exists (fun e -> e = fp || fp.StartsWith(e + ".")) then false
        elif whitelistMode then
            includedPaths |> Seq.exists (fun i -> i = fp || i.StartsWith(fp + ".") || fp.StartsWith(i + "."))
        else true

    let mutable ownerIds: int64 array = null
    for descriptor in manyDescriptors do
        if not (shouldLoadPath descriptor.Property.Name) then ()
        else

        let qLink = quoteIdentifier descriptor.LinkTable
        let qTarget = quoteIdentifier descriptor.TargetTable
        let batchSize = 500
        if isNull ownerIds then ownerIds <- ownerEntities |> Array.map fst |> Array.distinct
        let grouped = Dictionary<int64, ResizeArray<int64 * obj>>()
        let idWriter = EntityIdentity.compiledInt64IdWriter descriptor.TargetType
        let ownerColumn = if descriptor.OwnerUsesSourceColumn then "SourceId" else "TargetId"
        let targetColumn = if descriptor.OwnerUsesSourceColumn then "TargetId" else "SourceId"

        let mutable offset = 0
        while offset < ownerIds.Length do
            let length = min batchSize (ownerIds.Length - offset)
            let paramNames = Array.init length (fun i -> $"@_bid{i}")
            let inClause = String.Join(", ", paramNames)
            let orderClause =
                match descriptor.OrderBy with
                | DBRefOrder.TargetId -> $" ORDER BY {qTarget}.Id"
                | DBRefOrder.Undefined -> ""
                | _ -> ""
            let sql =
                $"SELECT lnk.{ownerColumn} AS OwnerId, {qTarget}.Id AS TargetId, json_quote({qTarget}.Value) as ValueJSON " +
                $"FROM {qLink} lnk JOIN {qTarget} ON {qTarget}.Id = lnk.{targetColumn} " +
                $"WHERE lnk.{ownerColumn} IN ({inClause})" + orderClause + ";"
            let mutable statement = connection.OpenStatement(sql)
            try
                for i = 0 to length - 1 do statement.BindInt64(i + 1, ownerIds.[offset + i])
                while statement.Step() do
                    let row = statement.Row
                    if not (row.IsDBNull(2)) then
                        let ownerId, targetId = row.GetInt64(0), row.GetInt64(1)
                        let targetObj =
                            if row.CanBorrowUtf16 then JsonValue.ParseRuntime(descriptor.TargetType,row.TextUtf16(2))
                            else JsonValue.ParseRuntime(descriptor.TargetType,row.GetString(2))
                        idWriter.Invoke(targetObj, targetId)
                        match grouped.TryGetValue(ownerId) with
                        | true, list -> list.Add(targetId, targetObj)
                        | _ ->
                            let list = ResizeArray()
                            list.Add(targetId, targetObj)
                            grouped.[ownerId] <- list
                statement.Dispose()
            with error -> statement.Abort(error); reraise()
            offset <- offset + length

        let trackerGetter = descriptor.Property.Read
        let trackerSetter = descriptor.Property.Write
        let trackerCtor = RelationsAccessorCache.compiledDefaultCtor descriptor.Property.ReferenceType
        for (ownerId, ownerObj) in ownerEntities do
            let tracker = trackerGetter.Invoke(ownerObj)
            match tracker with
            | :? IDBRefManyInternal as internal' ->
                match grouped.TryGetValue(ownerId) with
                | true, items -> internal'.SetLoadedBoxed (items |> Seq.map snd) (items |> Seq.map fst)
                | _ -> internal'.SetLoadedBoxed Seq.empty Seq.empty
            | null ->
                let instance = trackerCtor.Invoke()
                trackerSetter.Invoke(ownerObj, instance)
                let internal' = instance :?> IDBRefManyInternal
                match grouped.TryGetValue(ownerId) with
                | true, items -> internal'.SetLoadedBoxed (items |> Seq.map snd) (items |> Seq.map fst)
                | _ -> internal'.SetLoadedBoxed Seq.empty Seq.empty
            | _ -> ()


/// A frontier groups I/O; each object keeps its own bounded ancestor path.
type private HydrationNode = {
    Id: int64
    Entity: obj
    Ancestors: struct(string * Type * int64) list
}

let private pathAdmitted (excluded: HashSet<string>) (included: HashSet<string>) whitelist path =
    not (excluded |> Seq.exists (fun e -> e = path || path.StartsWith(e + ".", StringComparison.Ordinal)))
    && (not whitelist || included |> Seq.exists (fun i -> i = path || i.StartsWith(path + ".", StringComparison.Ordinal) || path.StartsWith(i + ".", StringComparison.Ordinal)))

let private hasReferenceProperties ownerType =
    (getRelationSpecs ownerType).Length > 0 || (getFileSystemReferenceSpecs ownerType).Length > 0

let private hydrateDescendants tx excluded included whitelist entities initialKinds =
    let rec loop (tx: RelationTxContext) (nodes: HydrationNode array) depth prefix kinds =
        if nodes.Length > 0 && depth < maxRecursiveDepth then
            for descriptor in buildRelationDescriptors tx tx.OwnerType do
                let path = if prefix = "" then descriptor.Property.Name else prefix + "." + descriptor.Property.Name
                if List.contains descriptor.Kind kinds && hasReferenceProperties descriptor.TargetType && pathAdmitted excluded included whitelist path then
                    let getter = descriptor.Property.Read
                    let children = ResizeArray<HydrationNode>()
                    let add (parent: HydrationNode) id entity =
                        let key = struct(descriptor.TargetTable, descriptor.TargetType, id)
                        if id > 0L && not (isNull entity) && not (List.contains key parent.Ancestors) then
                            children.Add { Id = id; Entity = entity; Ancestors = key :: parent.Ancestors }
                    for node in nodes do
                        let reference = getter.Invoke(node.Entity)
                        match descriptor.Kind with
                        | Single ->
                            if not (isNull reference) then
                                let loaded = RelationsAccessorCache.compiledGetter descriptor.Property.ReferenceType "IsLoaded"
                                if loaded.Invoke(reference) :?> bool then
                                    let value = RelationsAccessorCache.compiledGetter descriptor.Property.ReferenceType "Value"
                                    add node (readDbRefId reference) (value.Invoke reference)
                        | Many ->
                            match reference with
                            | :? IDBRefManyInternal as tracker when tracker.IsLoaded ->
                                let readId = EntityIdentity.compiledInt64IdReader descriptor.TargetType
                                for entity in tracker.GetCurrentItemsBoxed() do
                                    if not (isNull entity) then add node (readId.Invoke entity) entity
                            | _ -> ()
                    if children.Count > 0 then
                        let childTx = { tx with OwnerTable = descriptor.TargetTable; OwnerType = descriptor.TargetType }
                        let frontier = children.ToArray()
                        let entities = frontier |> Array.map (fun n -> n.Id, n.Entity)
                        loadSingleProperties childTx excluded included whitelist entities (depth + 1) path
                        loadManyProperties childTx excluded included whitelist entities path
                        loop childTx frontier (depth + 1) path [Single; Many]
    if buildRelationDescriptors tx tx.OwnerType |> Array.exists (fun d -> List.contains d.Kind initialKinds && hasReferenceProperties d.TargetType) then
        let nodes = entities |> Array.map (fun (id, entity) -> { Id = id; Entity = entity; Ancestors = [] })
        loop tx nodes 0 "" initialKinds

let private readContext connection ownerTable ownerType inTransaction = {
    Connection = connection; OwnerTable = formatName ownerTable; OwnerType = ownerType
    InTransaction = inTransaction; CollectionFactory = None; Preparation = RelationPreparation()
}

let internal batchLoadDBRefProperties connection ownerTable ownerType excluded included whitelist entities inTransaction =
    if (getRelationSpecs ownerType).Length = 0 then
        RelationsFileSystemBatchLoad.batchLoadFileSystemProperties connection ownerTable ownerType excluded included whitelist entities Single ""
    else
        let tx = readContext connection ownerTable ownerType inTransaction
        loadSingleProperties tx excluded included whitelist entities 0 ""
        hydrateDescendants tx excluded included whitelist entities [Single]

let internal batchLoadDBRefManyProperties connection ownerTable ownerType excluded included whitelist entities inTransaction =
    if (getRelationSpecs ownerType).Length = 0 then
        RelationsFileSystemBatchLoad.batchLoadFileSystemProperties connection ownerTable ownerType excluded included whitelist entities Many ""
    else
        let tx = readContext connection ownerTable ownerType inTransaction
        loadManyProperties tx excluded included whitelist entities ""
        hydrateDescendants tx excluded included whitelist entities [Many]

let internal recurseLoadedRelationTargets connection ownerTable ownerType excluded included whitelist entities inTransaction =
    if (getRelationSpecs ownerType).Length > 0 then
        let tx = readContext connection ownerTable ownerType inTransaction
        hydrateDescendants tx excluded included whitelist entities [Single; Many]
