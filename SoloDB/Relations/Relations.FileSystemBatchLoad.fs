
module internal SoloDatabase.RelationsFileSystemBatchLoad

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

/// Hydrate only returned owners and their referenced headers; filesystem content is never read.
let internal batchLoadFileSystemProperties
    (connection: NativeSqliteConnection) (ownerTable: string) (ownerType: Type)
    (excludedPaths: HashSet<string>) (includedPaths: HashSet<string>) (whitelistMode: bool)
    (ownerEntities: (int64 * obj) array) (kind: RelationKind) (prefix: string) =
    if ownerEntities.Length <> 0 then
        let ownerTable = formatName ownerTable
        let mutable ownerIds: int64 array = null
        for spec in getFileSystemReferenceSpecs ownerType do
            let path = if prefix = "" then spec.Property.Name else prefix + "." + spec.Property.Name
            let excluded = excludedPaths |> Seq.exists (fun e -> e = path || path.StartsWith(e + ".", StringComparison.Ordinal))
            let included = not whitelistMode || (includedPaths |> Seq.exists (fun i -> i = path || i.StartsWith(path + ".", StringComparison.Ordinal) || path.StartsWith(i + ".", StringComparison.Ordinal)))
            if spec.Kind = kind && not excluded && included then
                let getter = spec.Property.Read
                let setter = spec.Property.Write
                let memberships = Dictionary<int64, ResizeArray<int64>>()
                let targets = HashSet<int64>()
                let add ownerId targetId =
                    targets.Add targetId |> ignore
                    match memberships.TryGetValue ownerId with
                    | true, ids -> ids.Add targetId
                    | _ -> let ids = ResizeArray<int64>() in ids.Add targetId; memberships.Add(ownerId, ids)
                match kind with
                | Single ->
                    for ownerId, owner in ownerEntities do
                        let id = readDbRefId (getter.Invoke owner)
                        if id > 0L then add ownerId id
                | Many ->
                    if isNull ownerIds then ownerIds <- ownerEntities |> Array.map fst |> Array.distinct
                    let ids = ownerIds
                    let column = if spec.TargetTable = "SoloDBFileHeader" then "FileId" else "DirectoryId"
                    for offset in 0 .. 500 .. ids.Length - 1 do
                        let count = min 500 (ids.Length - offset)
                        let names = Array.init count (fun i -> "@id" + string i)
                        let inClause = String.Join(",", names)
                        let sql = $"SELECT OwnerId,{column} AS TargetId FROM SoloDBFileSystemReference WHERE OwnerCollection=@owner AND Property=@property AND {column} IS NOT NULL AND OwnerId IN ({inClause})"
                        let mutable statement = connection.OpenStatement(sql)
                        try
                            statement.BindText(1, ownerTable)
                            statement.BindText(2, spec.Property.Name)
                            for i = 0 to count - 1 do statement.BindInt64(i + 3, ids.[offset + i])
                            while statement.Step() do add (statement.Row.GetInt64(0)) (statement.Row.GetInt64(1))
                            statement.Dispose()
                        with error -> statement.Abort(error); reraise()
                let loaded = Dictionary<int64, obj>(targets.Count)
                let targetIds = targets |> Seq.toArray
                let isFile = spec.TargetTable = "SoloDBFileHeader"
                let refType = if isFile then typeof<FileRef> else typeof<DirectoryRef>
                let factory = (RelationsAccessorCache.compiledDbRefFactories refType).Loaded
                for offset in 0 .. 500 .. targetIds.Length - 1 do
                    let count = min 500 (targetIds.Length - offset)
                    let parameters = Dictionary<string, obj>(count)
                    let names = Array.init count (fun i -> let name = "@id" + string i in parameters.Add(name, targetIds.[offset+i]); name)
                    let predicate = (if isFile then "fh.Id" else "dh.Id") + " IN (" + String.Join(",", names) + ") ORDER BY " + (if isFile then "fh.Id" else "dh.Id")
                    if isFile then
                        for header in FileStorageHelpers.getFilesWhere connection predicate (fun statement -> statement.BindValues parameters) do
                            loaded.Add(header.Id, factory.Invoke(header.Id, header))
                    else
                        for header in FileStorageCore.tryGetDirectoriesWhere connection predicate (fun statement -> statement.BindValues parameters) do
                            loaded.Add(header.Id, factory.Invoke(header.Id, header))
                for ownerId, owner in ownerEntities do
                    match kind with
                    | Single ->
                        let id = readDbRefId (getter.Invoke owner)
                        match loaded.TryGetValue id with
                        | true, value -> setter.Invoke(owner, value)
                        | _ -> ()
                    | Many ->
                        let tracker =
                            match getter.Invoke owner with
                            | :? IFileSystemRefMany as tracker -> tracker
                            | _ ->
                                let value = (RelationsAccessorCache.compiledDefaultCtor spec.Property.ReferenceType).Invoke()
                                setter.Invoke(owner, value)
                                value :?> IFileSystemRefMany
                        let items = ResizeArray<obj>()
                        let ids = ResizeArray<int64>()
                        match memberships.TryGetValue ownerId with
                        | true, members ->
                            for id in members do
                                match loaded.TryGetValue id with
                                | true, value -> items.Add value; ids.Add id
                                | _ -> ()
                        | _ -> ()
                        tracker.SetLoadedBoxed items ids
