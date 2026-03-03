
module internal SoloDatabase.RelationsDelete

open System
open Microsoft.Data.Sqlite
open SoloDatabase.Attributes
open SoloDatabase.Utils
open SQLiteTools
open RelationsTypes
open RelationsSchema
open RelationsEntity

let internal withDeleteGuard (deleteCtx: DeleteTraversalContext) (tableName: string) (id: int64) (fn: unit -> unit) =
    let key = $"{tableName}|{id}"
    let set = deleteCtx.GuardSet
    if set.Contains(key) then
        ()
    else
        set.Add(key) |> ignore
        fn()
        set.Remove(key) |> ignore

let internal metadataLinkLayout (connection: SqliteConnection) (row: RelationMetadataRow) (relationKind: RelationKind) =
    match relationKind with
    | Single ->
        linkTableFromRelationName row.Name, "SourceId", "TargetId"
    | Many ->
        let canonicalTable = linkTableFromRelationName (canonicalManyRelationName row.OwnerCollection row.TargetCollection)
        let useShared = sqliteTableExistsByName connection canonicalTable
        let ownerUsesSourceColumn =
            if useShared then
                StringComparer.Ordinal.Compare(row.OwnerCollection, row.TargetCollection) <= 0
            else
                true
        let ownerColumn, targetColumn =
            if ownerUsesSourceColumn then "SourceId", "TargetId"
            else "TargetId", "SourceId"
        let linkTable = if useShared then canonicalTable else linkTableFromRelationName row.Name
        linkTable, ownerColumn, targetColumn

let internal hasOutgoingRestrictLinkToGuardedEntity (deleteCtx: DeleteTraversalContext) (tx: RelationTxContext) (tableName: string) (entityId: int64) =
    ensureTxContext tx
    ensureTransaction tx
    if String.IsNullOrWhiteSpace tableName then
        raise (ArgumentException("tableName is required.", "tableName"))
    if entityId <= 0L then
        raise (ArgumentOutOfRangeException("entityId", entityId, "entityId must be > 0."))

    let ownerTable = formatName tableName
    let rows = readMetadataByOwner tx.Connection ownerTable
    let guard = deleteCtx.GuardSet
    rows
    |> Array.exists (fun row ->
        let onDelete = parseOnDeletePolicy row.OnDelete
        if onDelete <> DeletePolicy.Restrict then
            false
        else
            let relationKind = stringToRelationKind row.RefKind
            let linkTable, ownerColumn, targetColumn = metadataLinkLayout tx.Connection row relationKind
            let qLink = quoteIdentifier linkTable
            tx.Connection.Query<int64>($"SELECT {targetColumn} FROM {qLink} WHERE {ownerColumn} = @ownerId;", {| ownerId = entityId |})
            |> Seq.exists (fun targetId ->
                let key = $"{formatName row.TargetCollection}|{targetId}"
                guard.Contains(key)))

let rec internal applyOwnerDeletePoliciesCore (deleteCtx: DeleteTraversalContext) (tx: RelationTxContext) (ownerTable: string) (ownerId: int64) (deleteTargets: bool) =
    ensureTxContext tx
    ensureTransaction tx
    if String.IsNullOrWhiteSpace ownerTable then
        raise (ArgumentException("ownerTable is required.", "ownerTable"))
    if ownerId <= 0L then
        raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))

    ensureRelationCatalogTable tx.Connection
    let ownerTable = formatName ownerTable
    let rows = readMetadataByOwner tx.Connection ownerTable

    withDeleteGuard deleteCtx ownerTable ownerId (fun () ->
        for row in rows do
            let relationKind = stringToRelationKind row.RefKind
            let linkTable, ownerColumn, targetColumn = metadataLinkLayout tx.Connection row relationKind
            let qLink = quoteIdentifier linkTable
            let targetIds =
                tx.Connection.Query<int64>($"SELECT {targetColumn} FROM {qLink} WHERE {ownerColumn} = @ownerId;", {| ownerId = ownerId |})
                |> Seq.toArray
            let hasLinks = targetIds.Length > 0
            match relationKind with
            | Single ->
                match parseOnOwnerDeletePolicy row.OnOwnerDelete with
                | DeletePolicy.Restrict ->
                    if hasLinks then
                        raise (InvalidOperationException(
                            $"Error: Cannot delete owner '{ownerTable}' Id={ownerId}.\nReason: Relation '{row.PropertyName}' uses OnOwnerDelete=Restrict and still has links.\nFix: Remove related links or change the delete policy before deleting."))
                | DeletePolicy.Unlink ->
                    if hasLinks then
                        tx.Connection.Execute($"DELETE FROM {qLink} WHERE {ownerColumn} = @ownerId;", {| ownerId = ownerId |}) |> ignore
                | DeletePolicy.Deletion ->
                    if hasLinks then
                        tx.Connection.Execute($"DELETE FROM {qLink} WHERE {ownerColumn} = @ownerId;", {| ownerId = ownerId |}) |> ignore
                        if deleteTargets then
                            let distinctTargetIds = targetIds |> Seq.distinct |> Seq.toArray
                            for targetId in distinctTargetIds do
                                if globalRefCountCore tx row.TargetCollection targetId = 0L then
                                    let targetTable = formatName row.TargetCollection
                                    let targetKey = $"{targetTable}|{targetId}"
                                    if not (deleteCtx.GuardSet.Contains(targetKey)) &&
                                       not (hasOutgoingRestrictLinkToGuardedEntity deleteCtx tx targetTable targetId) then
                                        applyTargetDeletePoliciesCore deleteCtx tx targetTable targetId
                                        applyOwnerDeletePoliciesCore deleteCtx tx targetTable targetId true
                                        tx.Connection.Execute($"DELETE FROM {quoteIdentifier targetTable} WHERE Id = @id;", {| id = targetId |}) |> ignore
                | DeletePolicy.Cascade ->
                    raise (InvalidOperationException(
                        $"Error: Invalid relation metadata on '{ownerTable}.{row.PropertyName}'.\nReason: OnOwnerDelete=Cascade is not supported.\nFix: Use OnOwnerDelete=Deletion or update the metadata to a supported policy."))
                | _ ->
                    raise (InvalidOperationException(
                        $"Error: Invalid OnOwnerDelete policy on relation '{ownerTable}.{row.PropertyName}'.\nReason: The policy value is unsupported.\nFix: Use Restrict, Unlink, or Deletion."))

            | Many ->
                match parseOnOwnerDeletePolicy row.OnOwnerDelete with
                | DeletePolicy.Restrict ->
                    if hasLinks then
                        raise (InvalidOperationException(
                            $"Error: Cannot delete owner '{ownerTable}' Id={ownerId}.\nReason: Relation '{row.PropertyName}' uses OnOwnerDelete=Restrict and still has links.\nFix: Remove related links or change the delete policy before deleting."))
                | DeletePolicy.Unlink ->
                    if hasLinks then
                        tx.Connection.Execute($"DELETE FROM {qLink} WHERE {ownerColumn} = @ownerId;", {| ownerId = ownerId |}) |> ignore
                | DeletePolicy.Deletion ->
                    if hasLinks then
                        tx.Connection.Execute($"DELETE FROM {qLink} WHERE {ownerColumn} = @ownerId;", {| ownerId = ownerId |}) |> ignore
                        if deleteTargets then
                            let distinctTargetIds = targetIds |> Seq.distinct |> Seq.toArray
                            for targetId in distinctTargetIds do
                                if globalRefCountCore tx row.TargetCollection targetId = 0L then
                                    let targetTable = formatName row.TargetCollection
                                    let targetKey = $"{targetTable}|{targetId}"
                                    if not (deleteCtx.GuardSet.Contains(targetKey)) &&
                                       not (hasOutgoingRestrictLinkToGuardedEntity deleteCtx tx targetTable targetId) then
                                        applyTargetDeletePoliciesCore deleteCtx tx targetTable targetId
                                        applyOwnerDeletePoliciesCore deleteCtx tx targetTable targetId true
                                        tx.Connection.Execute($"DELETE FROM {quoteIdentifier targetTable} WHERE Id = @id;", {| id = targetId |}) |> ignore
                | DeletePolicy.Cascade ->
                    raise (InvalidOperationException(
                        $"Error: Invalid relation metadata on '{ownerTable}.{row.PropertyName}'.\nReason: OnOwnerDelete=Cascade is not supported.\nFix: Use OnOwnerDelete=Deletion or update the metadata to a supported policy."))
                | _ ->
                    raise (InvalidOperationException(
                        $"Error: Invalid OnOwnerDelete policy on relation '{ownerTable}.{row.PropertyName}'.\nReason: The policy value is unsupported.\nFix: Use Restrict, Unlink, or Deletion."))
)

and internal applyTargetDeletePoliciesCore (deleteCtx: DeleteTraversalContext) (tx: RelationTxContext) (targetTable: string) (targetId: int64) =
    ensureTxContext tx
    ensureTransaction tx
    if String.IsNullOrWhiteSpace targetTable then
        raise (ArgumentException("targetTable is required.", "targetTable"))
    if targetId <= 0L then
        raise (ArgumentOutOfRangeException("targetId", targetId, "targetId must be > 0."))

    ensureRelationCatalogTable tx.Connection
    let targetTable = formatName targetTable
    let rows = readMetadataByTarget tx.Connection targetTable

    withDeleteGuard deleteCtx targetTable targetId (fun () ->
        for row in rows do
            let onDelete = parseOnDeletePolicy row.OnDelete
            let relationKind = stringToRelationKind row.RefKind
            let linkTable, ownerColumn, targetColumn = metadataLinkLayout tx.Connection row relationKind
            let qLink = quoteIdentifier linkTable
            let ownerIds =
                tx.Connection.Query<int64>($"SELECT {ownerColumn} FROM {qLink} WHERE {targetColumn} = @targetId;", {| targetId = targetId |})
                |> Seq.distinct
                |> Seq.toArray

            if ownerIds.Length > 0 then
                match onDelete with
                | DeletePolicy.Restrict ->
                    raise (InvalidOperationException(
                        $"Error: Cannot delete '{targetTable}' Id={targetId}.\nReason: Relation '{row.OwnerCollection}.{row.PropertyName}' uses OnDelete=Restrict.\nFix: Remove related links or change the delete policy before deleting."))

                | DeletePolicy.Unlink ->
                    tx.Connection.Execute($"DELETE FROM {qLink} WHERE {targetColumn} = @targetId;", {| targetId = targetId |}) |> ignore
                    if relationKind = Single then
                        for ownerId in ownerIds do
                            updateDbRefJson tx row.OwnerCollection ownerId row.PropertyName 0L

                | DeletePolicy.Cascade ->
                    let ownerTable = formatName row.OwnerCollection
                    let qOwner = quoteIdentifier ownerTable
                    for ownerId in ownerIds do
                        let ownerKey = $"{ownerTable}|{ownerId}"
                        if not (deleteCtx.GuardSet.Contains(ownerKey)) then
                            applyTargetDeletePoliciesCore deleteCtx tx ownerTable ownerId
                            applyOwnerDeletePoliciesCore deleteCtx tx ownerTable ownerId true
                            if globalRefCountCore tx ownerTable ownerId = 0L then
                                tx.Connection.Execute($"DELETE FROM {qOwner} WHERE Id = @id;", {| id = ownerId |}) |> ignore

                | DeletePolicy.Deletion ->
                    raise (InvalidOperationException(
                        "Error: Invalid delete policy.\nReason: OnDelete cannot be Deletion.\nFix: Use Restrict, Cascade, or Unlink for OnDelete."))
                | _ ->
                    raise (InvalidOperationException(
                        $"Error: Invalid OnDelete policy on relation '{row.OwnerCollection}.{row.PropertyName}'.\nReason: The policy value is unsupported.\nFix: Use Restrict, Cascade, or Unlink."))
)

and internal globalRefCountCore (tx: RelationTxContext) (targetTable: string) (targetId: int64) =
    ensureTxContext tx
    ensureTransaction tx
    if String.IsNullOrWhiteSpace targetTable then
        raise (ArgumentException("targetTable is required.", "targetTable"))
    if targetId <= 0L then
        raise (ArgumentOutOfRangeException("targetId", targetId, "targetId must be > 0."))

    ensureRelationCatalogTable tx.Connection
    let targetTable = formatName targetTable
    let rows = readMetadataByTarget tx.Connection targetTable
    let mutable count = 0L
    for row in rows do
        let relationKind = stringToRelationKind row.RefKind
        let linkTable, _, targetColumn = metadataLinkLayout tx.Connection row relationKind
        count <- count + tx.Connection.QueryFirst<int64>($"SELECT COUNT(*) FROM {quoteIdentifier linkTable} WHERE {targetColumn} = @targetId;", {| targetId = targetId |})
    count
