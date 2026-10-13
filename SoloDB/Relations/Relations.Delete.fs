
module internal SoloDatabase.RelationsDelete

open System
open System.Collections.Generic
open SoloDatabase.Attributes
open SoloDatabase.Utils
open NativeSQLiteTools
open RelationsTypes
open RelationsSchemaBuilder
open RelationsSchemaValidator
open RelationsSchemaLinkTableDDL
open RelationsEntitySync
open RelationsEntityCascade
open RelationsEntityApplyOps

let internal withDeleteGuard (deleteCtx: DeleteTraversalContext) (tableName: string) (id: int64) (fn: unit -> unit) =
    let key = $"{tableName}|{id}"
    let set = deleteCtx.GuardSet
    if set.Contains(key) then
        ()
    else
        set.Add(key) |> ignore
        fn()
        set.Remove(key) |> ignore

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
            let mutable statement = tx.Connection.OpenStatement($"SELECT {targetColumn} FROM {qLink} WHERE {ownerColumn} = @ownerId;")
            try
                statement.BindInt64(1, entityId)
                let mutable found = false
                while not found && statement.Step() do
                    let targetId = statement.Row.GetInt64(0)
                    found <- guard.Contains($"{row.TargetCollection}|{targetId}")
                statement.Dispose()
                found
            with error -> statement.Abort(error); reraise())

let private hasLink (connection: NativeSqliteConnection) linkTable column id =
    let mutable statement = connection.OpenStatement($"SELECT EXISTS(SELECT 1 FROM {quoteIdentifier linkTable} WHERE {column} = @id);")
    try
        statement.BindInt64(1, id)
        statement.Step() |> ignore
        let exists = statement.Row.GetBoolean(0)
        statement.Dispose()
        exists
    with error -> statement.Abort(error); reraise()

let private hasRelationLink (tx: RelationTxContext) (row: RelationMetadataRow) (ownerSide: bool) (id: int64) =
    let linkTable, ownerColumn, targetColumn = metadataLinkLayout tx.Connection row (stringToRelationKind row.RefKind)
    let column = if ownerSide then ownerColumn else targetColumn
    hasLink tx.Connection linkTable column id

let private rejectOwnerDelete ownerTable ownerId (row: RelationMetadataRow) =
    raise (InvalidOperationException(
        $"Error: Cannot delete owner '{ownerTable}' Id={ownerId}.\nReason: Relation '{row.PropertyName}' uses OnOwnerDelete=Restrict and still has links.\nFix: Remove related links or change the delete policy before deleting."))

let private rejectTargetDelete targetTable targetId (row: RelationMetadataRow) =
    raise (InvalidOperationException(
        $"Error: Cannot delete '{targetTable}' Id={targetId}.\nReason: Relation '{row.OwnerCollection}.{row.PropertyName}' uses OnDelete=Restrict.\nFix: Remove related links or change the delete policy before deleting."))

let private checkOwnerRestrictions tx ownerTable ownerId (rows: RelationMetadataRow array) skipRestrict =
    if not skipRestrict then
        for row in rows do
            if parseOnOwnerDeletePolicy row.OnOwnerDelete = DeletePolicy.Restrict && hasRelationLink tx row true ownerId then
                rejectOwnerDelete ownerTable ownerId row

let private checkTargetRestrictions tx targetTable targetId (ownerUnlinks: RelationMetadataRow array) (rows: RelationMetadataRow array) =
    for row in rows do
        if parseOnDeletePolicy row.OnDelete = DeletePolicy.Restrict && hasRelationLink tx row false targetId then
            let removedByOwner =
                if stringToRelationKind row.RefKind <> Many then false
                else
                    let incomingTable, _, incomingColumn = metadataLinkLayout tx.Connection row Many
                    ownerUnlinks
                    |> Array.exists (fun outgoing ->
                        if stringToRelationKind outgoing.RefKind <> Many then false
                        else
                            let outgoingTable, outgoingColumn, _ = metadataLinkLayout tx.Connection outgoing Many
                            outgoingTable = incomingTable && outgoingColumn = incomingColumn)
            // Inverse views of an owner's shared Many links disappear with that owner's links.
            if not removedByOwner then rejectTargetDelete targetTable targetId row

let private readDeleteFacts (tx: RelationTxContext) tableName id ownerFirst =
    let ownerRows = readMetadataByOwner tx.Connection tableName
    let targetRows = readMetadataByTarget tx.Connection tableName
    checkOwnerRestrictions tx tableName id ownerRows false
    checkTargetRestrictions tx tableName id (if ownerFirst then ownerRows else Array.empty) targetRows
    struct (ownerRows, targetRows)

let private checkCascadeOwners (deleteCtx: DeleteTraversalContext) (tx: RelationTxContext) targetId (rows: RelationMetadataRow array) =
    // Only directly linked cascade owners are visited. A later unlink must not erase their restrictions.
    let mutable checkedOwners: HashSet<struct (string * int64)> = null
    for row in rows do
        if parseOnDeletePolicy row.OnDelete = DeletePolicy.Cascade then
            let linkTable, ownerColumn, targetColumn = metadataLinkLayout tx.Connection row (stringToRelationKind row.RefKind)
            let mutable statement = tx.Connection.OpenStatement($"SELECT {ownerColumn} FROM {quoteIdentifier linkTable} WHERE {targetColumn} = @targetId;")
            try
                statement.BindInt64(1, targetId)
                while statement.Step() do
                    let ownerId = statement.Row.GetInt64(0)
                    if not (deleteCtx.GuardSet.Contains($"{row.OwnerCollection}|{ownerId}")) then
                        if isNull checkedOwners then checkedOwners <- HashSet<struct (string * int64)>()
                        if checkedOwners.Add(struct (row.OwnerCollection, ownerId)) then
                            readDeleteFacts tx row.OwnerCollection ownerId false |> ignore
                statement.Dispose()
            with error -> statement.Abort(error); reraise()

let rec private applyOwnerDeletePoliciesFromRows (deleteCtx: DeleteTraversalContext) (tx: RelationTxContext) (ownerTable: string) (ownerId: int64) (deleteTargets: bool) (skipRestrict: bool) (rows: RelationMetadataRow array) =
    ensureTxContext tx
    ensureTransaction tx
    if String.IsNullOrWhiteSpace ownerTable then
        raise (ArgumentException("ownerTable is required.", "ownerTable"))
    if ownerId <= 0L then
        raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))

    ensureRelationCatalogTable tx.Connection
    let ownerTable = formatName ownerTable
    withDeleteGuard deleteCtx ownerTable ownerId (fun () ->
        let mutable deletionTargets: HashSet<struct (string * int64)> = null
        for row in rows do
            let relationKind = stringToRelationKind row.RefKind
            let linkTable, ownerColumn, targetColumn = metadataLinkLayout tx.Connection row relationKind
            let qLink = quoteIdentifier linkTable
            let policy = parseOnOwnerDeletePolicy row.OnOwnerDelete
            let hasLinks =
                if policy = DeletePolicy.Deletion && deleteTargets then
                    let mutable statement = tx.Connection.OpenStatement($"SELECT {targetColumn} FROM {qLink} WHERE {ownerColumn} = @ownerId;")
                    try
                        statement.BindInt64(1, ownerId)
                        let mutable found = false
                        while statement.Step() do
                            let targetId = statement.Row.GetInt64(0)
                            found <- true
                            if isNull deletionTargets then deletionTargets <- HashSet<struct (string * int64)>()
                            deletionTargets.Add(struct (row.TargetCollection, targetId)) |> ignore
                        statement.Dispose()
                        found
                    with error -> statement.Abort(error); reraise()
                else
                    hasLink tx.Connection linkTable ownerColumn ownerId
            // Policy branches are identical for Single and Many after metadataLinkLayout resolution.
            match policy with
            | DeletePolicy.Restrict ->
                if hasLinks then
                    if skipRestrict then
                        RelationMutationStatements.linkDeleteByOwner tx.Connection linkTable ownerColumn ownerId |> ignore
                    else
                        rejectOwnerDelete ownerTable ownerId row
            | DeletePolicy.Unlink
            | DeletePolicy.Deletion ->
                if hasLinks then
                    RelationMutationStatements.linkDeleteByOwner tx.Connection linkTable ownerColumn ownerId |> ignore
            | DeletePolicy.Cascade ->
                raise (InvalidOperationException(
                    $"Error: Invalid relation metadata on '{ownerTable}.{row.PropertyName}'.\nReason: OnOwnerDelete=Cascade is not supported.\nFix: Use OnOwnerDelete=Deletion or update the metadata to a supported policy."))
            | _ ->
                raise (InvalidOperationException(
                    $"Error: Invalid OnOwnerDelete policy on relation '{ownerTable}.{row.PropertyName}'.\nReason: The policy value is unsupported.\nFix: Use Restrict, Unlink, or Deletion."))
        if not (isNull deletionTargets) then
            let removeOrphan = Predicate<struct (string * int64)>(fun struct (targetTable, targetId) ->
                if globalRefCountCore tx targetTable targetId <> 0L then false
                else
                    let targetKey = $"{targetTable}|{targetId}"
                    if deleteCtx.GuardSet.Contains(targetKey) ||
                       hasOutgoingRestrictLinkToGuardedEntity deleteCtx tx targetTable targetId then false
                    else
                        applyEntityDeletePoliciesCore deleteCtx tx targetTable targetId false
                        let mutable statement = tx.Connection.OpenStatement($"DELETE FROM {quoteIdentifier targetTable} WHERE Id = @id;")
                        try
                            statement.BindInt64(1, targetId)
                            statement.Step() |> ignore
                            statement.Dispose()
                        with error -> statement.Abort(error); reraise()
                        true)
            // Deleting one candidate may unlink another. Only revisit the original remaining
            // candidates, and only after removing at least one, so this traversal terminates.
            while deletionTargets.Count > 0 && deletionTargets.RemoveWhere(removeOrphan) > 0 do ()
    )

and private applyTargetDeletePoliciesFromRows (deleteCtx: DeleteTraversalContext) (tx: RelationTxContext) (targetTable: string) (targetId: int64) (rows: RelationMetadataRow array) =
    ensureTxContext tx
    ensureTransaction tx
    if String.IsNullOrWhiteSpace targetTable then
        raise (ArgumentException("targetTable is required.", "targetTable"))
    if targetId <= 0L then
        raise (ArgumentOutOfRangeException("targetId", targetId, "targetId must be > 0."))

    ensureRelationCatalogTable tx.Connection
    let targetTable = formatName targetTable
    withDeleteGuard deleteCtx targetTable targetId (fun () ->
        for row in rows do
            let onDelete = parseOnDeletePolicy row.OnDelete
            let relationKind = stringToRelationKind row.RefKind
            let linkTable, ownerColumn, targetColumn = metadataLinkLayout tx.Connection row relationKind
            let qLink = quoteIdentifier linkTable
            let ownerIds =
                let mutable statement = tx.Connection.OpenStatement($"SELECT {ownerColumn} FROM {qLink} WHERE {targetColumn} = @targetId;")
                try
                    statement.BindInt64(1, targetId)
                    let seen = HashSet<int64>()
                    let ids = ResizeArray<int64>()
                    while statement.Step() do
                        let id = statement.Row.GetInt64(0)
                        if seen.Add(id) then ids.Add(id)
                    statement.Dispose()
                    ids.ToArray()
                with error -> statement.Abort(error); reraise()

            if ownerIds.Length > 0 then
                match onDelete with
                | DeletePolicy.Restrict ->
                    rejectTargetDelete targetTable targetId row

                | DeletePolicy.Unlink ->
                    RelationMutationStatements.linkDeleteByTarget tx.Connection linkTable targetColumn targetId |> ignore
                    if relationKind = RelationKind.Single then
                        for ownerId in ownerIds do
                            updateDbRefJson tx row.OwnerCollection ownerId row.PropertyName 0L

                | DeletePolicy.Cascade ->
                    let ownerTable = row.OwnerCollection
                    let qOwner = quoteIdentifier ownerTable
                    for ownerId in ownerIds do
                        let ownerKey = $"{ownerTable}|{ownerId}"
                        if not (deleteCtx.GuardSet.Contains(ownerKey)) then
                            applyEntityDeletePoliciesCore deleteCtx tx ownerTable ownerId false
                            if globalRefCountCore tx ownerTable ownerId = 0L then
                                let mutable statement = tx.Connection.OpenStatement($"DELETE FROM {qOwner} WHERE Id = @id;")
                                try
                                    statement.BindInt64(1, ownerId)
                                    statement.Step() |> ignore
                                    statement.Dispose()
                                with error -> statement.Abort(error); reraise()

                | DeletePolicy.Deletion ->
                    raise (InvalidOperationException(
                        "Error: Invalid delete policy.\nReason: OnDelete cannot be Deletion.\nFix: Use Restrict, Cascade, or Unlink for OnDelete."))
                | _ ->
                    raise (InvalidOperationException(
                        $"Error: Invalid OnDelete policy on relation '{row.OwnerCollection}.{row.PropertyName}'.\nReason: The policy value is unsupported.\nFix: Use Restrict, Cascade, or Unlink."))
)

and internal applyEntityDeletePoliciesCore (deleteCtx: DeleteTraversalContext) (tx: RelationTxContext) (tableName: string) (id: int64) (ownerFirst: bool) =
    ensureTxContext tx
    ensureTransaction tx
    ensureRelationCatalogTable tx.Connection
    let tableName = formatName tableName
    let struct (ownerRows, targetRows) = readDeleteFacts tx tableName id ownerFirst
    checkCascadeOwners deleteCtx tx id targetRows
    if ownerFirst then
        applyOwnerDeletePoliciesFromRows deleteCtx tx tableName id true false ownerRows
        applyTargetDeletePoliciesFromRows deleteCtx tx tableName id targetRows
    else
        applyTargetDeletePoliciesFromRows deleteCtx tx tableName id targetRows
        applyOwnerDeletePoliciesFromRows deleteCtx tx tableName id true false ownerRows

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
        let mutable statement = tx.Connection.OpenStatement($"SELECT COUNT(*) FROM {quoteIdentifier linkTable} WHERE {targetColumn} = @targetId;")
        try
            statement.BindInt64(1, targetId)
            statement.Step() |> ignore
            count <- count + statement.Row.GetInt64(0)
            statement.Dispose()
        with error -> statement.Abort(error); reraise()
    count

let internal applyOwnerDeletePoliciesCore deleteCtx tx ownerTable ownerId deleteTargets skipRestrict =
    ensureTxContext tx
    ensureTransaction tx
    ensureRelationCatalogTable tx.Connection
    let ownerTable = formatName ownerTable
    let rows = readMetadataByOwner tx.Connection ownerTable
    checkOwnerRestrictions tx ownerTable ownerId rows skipRestrict
    applyOwnerDeletePoliciesFromRows deleteCtx tx ownerTable ownerId deleteTargets skipRestrict rows

let internal applyTargetDeletePoliciesCore deleteCtx tx targetTable targetId =
    ensureTxContext tx
    ensureTransaction tx
    ensureRelationCatalogTable tx.Connection
    let targetTable = formatName targetTable
    let rows = readMetadataByTarget tx.Connection targetTable
    checkTargetRestrictions tx targetTable targetId Array.empty rows
    checkCascadeOwners deleteCtx tx targetId rows
    applyTargetDeletePoliciesFromRows deleteCtx tx targetTable targetId rows
