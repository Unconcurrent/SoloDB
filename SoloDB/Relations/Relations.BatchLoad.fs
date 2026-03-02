
module internal SoloDatabase.RelationsBatchLoad

open System
open System.Reflection
open System.Collections.Generic
open Microsoft.Data.Sqlite
open SoloDatabase
open SoloDatabase.Attributes
open SoloDatabase.Utils
open SoloDatabase.JsonSerializator
open SQLiteTools
open JsonFunctions
open RelationsTypes
open RelationsSchema
open RelationsEntity

let batchLoadDBRefProperties
    (connection: SqliteConnection)
    (ownerTable: string)
    (ownerType: Type)
    (excludedPaths: HashSet<string>)
    (includedPaths: HashSet<string>)
    (ownerEntities: (int64 * obj) array)
    =
    if ownerEntities.Length = 0 then ()
    else

    let specs = getRelationSpecs ownerType
    let singleSpecs = specs |> Array.filter (fun (_, kind, _, _, _, _, _, _) -> kind = Single)
    if singleSpecs.Length = 0 then ()
    else

    let ownerTable = formatName ownerTable
    let shouldLoadPath (path: string) =
        if excludedPaths.Contains(path) then false
        elif includedPaths.Count > 0 then includedPaths.Contains(path)
        else true

    for (prop, _kind, targetType, _typedIdType, _onDelete, _onOwnerDelete, _isUnique, _orderBy) in singleSpecs do
        if not (shouldLoadPath prop.Name) then ()
        else

        let targetTable = resolveTargetCollectionName connection ownerTable prop.Name targetType
        let qTarget = quoteIdentifier targetTable
        let idProp = getWritableInt64IdPropertyOrThrow targetType

        let ownerTargets = ResizeArray<obj * int64>()
        let distinctTargetIds = HashSet<int64>()

        let propGetter = RelationsAccessorCache.compiledPropGetter prop
        for (_ownerId, ownerObj) in ownerEntities do
            let dbRefObj = propGetter.Invoke(ownerObj)
            let targetId = readDbRefId dbRefObj
            if targetId > 0L then
                ownerTargets.Add(ownerObj, targetId)
                distinctTargetIds.Add(targetId) |> ignore

        if ownerTargets.Count > 0 then
            let loadedTargets = Dictionary<int64, obj>()
            let targetIds = distinctTargetIds |> Seq.toArray
            let batchSize = 500
            let chunks =
                let mutable i = 0
                [| while i < targetIds.Length do
                       let len = min batchSize (targetIds.Length - i)
                       yield targetIds.[i .. i + len - 1]
                       i <- i + len |]

            for chunk in chunks do
                let paramNames = chunk |> Array.mapi (fun i _ -> $"@_tid{i}")
                let inClause = String.Join(", ", paramNames)
                let sql = $"SELECT Id, json_quote(Value) as ValueJSON FROM {qTarget} WHERE Id IN ({inClause});"
                let parameters = Dictionary<string, obj>(chunk.Length)
                for i = 0 to chunk.Length - 1 do
                    parameters.[paramNames.[i]] <- box chunk.[i]

                let idWriter = RelationsAccessorCache.compiledInt64IdWriter targetType
                for row in connection.Query<{| Id: int64; ValueJSON: string |}>(sql, parameters) do
                    if not (isNull row.ValueJSON) then
                        let json = JsonValue.Parse row.ValueJSON
                        let targetObj = json.ToObject(targetType)
                        idWriter.Invoke(targetObj, row.Id)
                        loadedTargets.[row.Id] <- targetObj

            let propSetter = RelationsAccessorCache.compiledPropSetter prop
            for (ownerObj, targetId) in ownerTargets do
                match loadedTargets.TryGetValue(targetId) with
                | true, targetObj ->
                    let loadedRef = createDbRefLoaded prop.PropertyType targetId targetObj
                    propSetter.Invoke(ownerObj, loadedRef)
                | _ -> ()

let batchLoadDBRefManyProperties
    (connection: SqliteConnection)
    (ownerTable: string)
    (ownerType: Type)
    (excludedPaths: HashSet<string>)
    (includedPaths: HashSet<string>)
    (ownerEntities: (int64 * obj) array)
    (inTransaction: bool)
    =
    if ownerEntities.Length = 0 then ()
    else

    let ownerTable = formatName ownerTable
    let tx: RelationTxContext = { Connection = connection; OwnerTable = ownerTable; OwnerType = ownerType; InTransaction = inTransaction }
    let manyDescriptors = buildRelationDescriptors tx ownerType |> Array.filter (fun d -> d.Kind = Many)
    if manyDescriptors.Length = 0 then ()
    else

    let shouldLoadPath (path: string) =
        if excludedPaths.Contains(path) then false
        elif includedPaths.Count > 0 then includedPaths.Contains(path)
        else true

    for descriptor in manyDescriptors do
        if not (shouldLoadPath descriptor.Property.Name) then ()
        else

        let qLink = quoteIdentifier descriptor.LinkTable
        let qTarget = quoteIdentifier descriptor.TargetTable
        let batchSize = 500
        let ownerIds = ownerEntities |> Array.map fst
        let grouped = Dictionary<int64, ResizeArray<int64 * obj>>()
        let idWriter = RelationsAccessorCache.compiledInt64IdWriter descriptor.TargetType
        let ownerColumn = if descriptor.OwnerUsesSourceColumn then "SourceId" else "TargetId"
        let targetColumn = if descriptor.OwnerUsesSourceColumn then "TargetId" else "SourceId"

        let chunks =
            let mutable i = 0
            [| while i < ownerIds.Length do
                   let len = min batchSize (ownerIds.Length - i)
                   yield ownerIds.[i .. i + len - 1]
                   i <- i + len |]

        for chunk in chunks do
            let paramNames = chunk |> Array.mapi (fun i _ -> $"@_bid{i}")
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
            let parameters = Dictionary<string, obj>(chunk.Length)
            for i = 0 to chunk.Length - 1 do
                parameters.[paramNames.[i]] <- box chunk.[i]

            for row in connection.Query<{| OwnerId: int64; TargetId: int64; ValueJSON: string |}>(sql, parameters) do
                if not (isNull row.ValueJSON) then
                    let json = JsonValue.Parse row.ValueJSON
                    let targetObj = json.ToObject(descriptor.TargetType)
                    idWriter.Invoke(targetObj, row.TargetId)
                    match grouped.TryGetValue(row.OwnerId) with
                    | true, list -> list.Add(row.TargetId, targetObj)
                    | _ ->
                        let list = ResizeArray()
                        list.Add(row.TargetId, targetObj)
                        grouped.[row.OwnerId] <- list

        let trackerGetter = RelationsAccessorCache.compiledPropGetter descriptor.Property
        let trackerSetter = RelationsAccessorCache.compiledPropSetter descriptor.Property
        let trackerCtor = RelationsAccessorCache.compiledDefaultCtor descriptor.Property.PropertyType
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
