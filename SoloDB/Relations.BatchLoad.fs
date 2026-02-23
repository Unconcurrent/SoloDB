
module internal SoloDatabase.RelationsBatchLoad

open System
open System.Reflection
open System.Collections.Generic
open Microsoft.Data.Sqlite
open SoloDatabase
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
    (ownerEntities: (int64 * obj) array)
    =
    if ownerEntities.Length = 0 then ()
    else

    let specs = getRelationSpecs ownerType
    let singleSpecs = specs |> Array.filter (fun (_, kind, _, _, _, _) -> kind = Single)
    if singleSpecs.Length = 0 then ()
    else

    let ownerTable = formatName ownerTable
    for (prop, _kind, targetType, _onDelete, _onOwnerDelete, _isUnique) in singleSpecs do
        if excludedPaths.Contains(prop.Name) then ()
        else

        let targetTable = resolveTargetCollectionName connection ownerTable prop.Name targetType
        let qTarget = quoteIdentifier targetTable
        let idProp = getWritableInt64IdPropertyOrThrow targetType

        let ownerTargets = ResizeArray<obj * int64>()
        let distinctTargetIds = HashSet<int64>()

        for (_ownerId, ownerObj) in ownerEntities do
            let dbRefObj = prop.GetValue(ownerObj)
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

                for row in connection.Query<{| Id: int64; ValueJSON: string |}>(sql, parameters) do
                    if not (isNull row.ValueJSON) then
                        let json = JsonValue.Parse row.ValueJSON
                        let targetObj = json.ToObject(targetType)
                        idProp.SetValue(targetObj, box row.Id)
                        loadedTargets.[row.Id] <- targetObj

            for (ownerObj, targetId) in ownerTargets do
                match loadedTargets.TryGetValue(targetId) with
                | true, targetObj ->
                    let loadedRef = createDbRefLoaded prop.PropertyType targetId targetObj
                    prop.SetValue(ownerObj, loadedRef)
                | _ -> ()

let batchLoadDBRefManyProperties
    (connection: SqliteConnection)
    (ownerTable: string)
    (ownerType: Type)
    (excludedPaths: HashSet<string>)
    (ownerEntities: (int64 * obj) array)
    =
    if ownerEntities.Length = 0 then ()
    else

    let ownerTable = formatName ownerTable
    let tx: RelationTxContext = { Connection = connection; OwnerTable = ownerTable; OwnerType = ownerType; InTransaction = false }
    let manyDescriptors = buildRelationDescriptors tx ownerType |> Array.filter (fun d -> d.Kind = Many)
    if manyDescriptors.Length = 0 then ()
    else

    for descriptor in manyDescriptors do
        if excludedPaths.Contains(descriptor.Property.Name) then ()
        else

        let qLink = quoteIdentifier descriptor.LinkTable
        let qTarget = quoteIdentifier descriptor.TargetTable
        let batchSize = 500
        let ownerIds = ownerEntities |> Array.map fst
        let grouped = Dictionary<int64, ResizeArray<int64 * obj>>()
        let idProp = getWritableInt64IdPropertyOrThrow descriptor.TargetType
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
            let sql =
                $"SELECT lnk.{ownerColumn} AS OwnerId, {qTarget}.Id AS TargetId, json_quote({qTarget}.Value) as ValueJSON " +
                $"FROM {qLink} lnk JOIN {qTarget} ON {qTarget}.Id = lnk.{targetColumn} " +
                $"WHERE lnk.{ownerColumn} IN ({inClause});"
            let parameters = Dictionary<string, obj>(chunk.Length)
            for i = 0 to chunk.Length - 1 do
                parameters.[paramNames.[i]] <- box chunk.[i]

            for row in connection.Query<{| OwnerId: int64; TargetId: int64; ValueJSON: string |}>(sql, parameters) do
                if not (isNull row.ValueJSON) then
                    let json = JsonValue.Parse row.ValueJSON
                    let targetObj = json.ToObject(descriptor.TargetType)
                    idProp.SetValue(targetObj, box row.TargetId)
                    match grouped.TryGetValue(row.OwnerId) with
                    | true, list -> list.Add(row.TargetId, targetObj)
                    | _ ->
                        let list = ResizeArray()
                        list.Add(row.TargetId, targetObj)
                        grouped.[row.OwnerId] <- list

        for (ownerId, ownerObj) in ownerEntities do
            let tracker = descriptor.Property.GetValue(ownerObj)
            match tracker with
            | :? IDBRefManyInternal as internal' ->
                match grouped.TryGetValue(ownerId) with
                | true, items -> internal'.SetLoadedBoxed (items |> Seq.map snd) (items |> Seq.map fst)
                | _ -> internal'.SetLoadedBoxed Seq.empty Seq.empty
            | null ->
                let dbRefManyType = typedefof<DBRefMany<_>>.MakeGenericType(descriptor.TargetType)
                let instance = Activator.CreateInstance(dbRefManyType)
                descriptor.Property.SetValue(ownerObj, instance)
                let internal' = instance :?> IDBRefManyInternal
                match grouped.TryGetValue(ownerId) with
                | true, items -> internal'.SetLoadedBoxed (items |> Seq.map snd) (items |> Seq.map fst)
                | _ -> internal'.SetLoadedBoxed Seq.empty Seq.empty
            | _ -> ()
