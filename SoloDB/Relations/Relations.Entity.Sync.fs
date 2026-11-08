module internal SoloDatabase.RelationsEntitySync

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
open SoloDatabase.SqlModel

/// Normalize catalog identities at their owning read boundary.
let private readMetadata (connection: NativeSqliteConnection) sql collection =
    let mutable statement = connection.OpenStatement(sql)
    try
        statement.BindText(1, collection)
        let rows = ResizeArray<RelationMetadataRow>()
        while statement.Step() do
            let row = statement.Row
            rows.Add {
                Name = row.GetString(0)
                SourceCollection = formatName (row.GetString(1))
                TargetCollection = formatName (row.GetString(2))
                PropertyName = row.GetString(3)
                OwnerCollection = formatName (row.GetString(4))
                RefKind = row.GetString(5)
                OnDelete = row.GetString(6)
                OnOwnerDelete = row.GetString(7)
                IsUnique = row.GetInt64(8)
            }
        statement.Dispose()
        rows.ToArray()
    with error -> statement.Abort(error); reraise()

let internal readMetadataByOwner (connection: NativeSqliteConnection) (ownerTable: string) =
    readMetadata connection
        "SELECT Name, SourceCollection, TargetCollection, PropertyName, OwnerCollection, RefKind, OnDelete, OnOwnerDelete, IsUnique FROM SoloDBRelation WHERE OwnerCollection = @ownerCollection AND TargetCollection NOT IN ('SoloDBFileHeader','SoloDBDirectoryHeader');"
        ownerTable

let internal readAllMetadataByOwner (connection: NativeSqliteConnection) (ownerTable: string) =
    readMetadata connection
        "SELECT Name, SourceCollection, TargetCollection, PropertyName, OwnerCollection, RefKind, OnDelete, OnOwnerDelete, IsUnique FROM SoloDBRelation WHERE OwnerCollection = @ownerCollection;"
        ownerTable

let internal readMetadataByTarget (connection: NativeSqliteConnection) (targetTable: string) =
    readMetadata connection
        "SELECT Name, SourceCollection, TargetCollection, PropertyName, OwnerCollection, RefKind, OnDelete, OnOwnerDelete, IsUnique FROM SoloDBRelation WHERE TargetCollection = @targetCollection;"
        targetTable

let internal relationDescriptorByPath (tx: RelationTxContext) (ownerType: Type) =
    let entry = tx.Preparation.Entry(tx.Connection, formatName tx.OwnerTable, ownerType)
    match entry.DescriptorMap with
    | ValueSome map when tx.Preparation.AllowReuse -> map
    | _ ->
        let map = Dictionary<string, RelationDescriptor>(StringComparer.Ordinal)
        for descriptor in buildRelationDescriptors tx ownerType do
            map.[descriptor.PropertyPath] <- descriptor
        entry.DescriptorMap <- ValueSome map
        map

let internal metadataLinkLayout (connection: NativeSqliteConnection) (row: RelationMetadataRow) (relationKind: RelationKind) =
    match relationKind with
    | Single ->
        linkTableFromRelationName row.Name, "SourceId", "TargetId"
    | Many ->
        let canonicalTable = linkTableFromRelationName (canonicalManyRelationName row.OwnerCollection row.TargetCollection)
        let useShared = sqliteTableExistsByName connection canonicalTable
        let ownerUsesSourceColumn =
            if useShared then
                sharedManyOwnerUsesSourceColumn row.OwnerCollection row.TargetCollection
            else
                true
        let ownerColumn, targetColumn =
            if ownerUsesSourceColumn then "SourceId", "TargetId"
            else "TargetId", "SourceId"
        let linkTable = if useShared then canonicalTable else linkTableFromRelationName row.Name
        linkTable, ownerColumn, targetColumn

let internal insertTargetEntity (tx: RelationTxContext) (targetTable: string) (targetType: Type) (entity: obj) =
    if isNull entity then
        raise (ArgumentNullException("entity", "Cascade insert target entity cannot be null."))

    // Reject negative id before any schema side effects.
    let id = readEntityIdOrZero targetType entity
    if id < 0L then
        raise (InvalidOperationException(
            $"Error: Invalid Id {id} on pending relation target '{targetType.FullName}'.\nReason: Id must be >= 0.\nFix: Use 0 for new entities or a positive Id for existing entities."))

    ensureCollectionTableExists tx.Connection targetTable

    if id <= 0L then
        if (SoloIdAccessor.TryGetMember targetType).IsSome then tx.Preparation.DisableReuse()
        try
            CustomIdRunner.RunBoxedIfEmpty(targetType, entity, targetTable, tx)
        with
        | :? InvalidOperationException -> reraise()
        | ex ->
            raise (InvalidOperationException(
                sprintf "Error: Cascade-insert generator failed for target '%s'; the transaction is being rolled back.\nReason: %s\nFix: ensure the registered IIdGenerator can generate an ID from the target entity and its transactional collection." targetType.FullName ex.Message,
                ex))

    let json = serializeEntityForStorage targetType entity
    let qTarget = quoteIdentifier targetTable
    let sql =
        if id > 0L then $"INSERT INTO {qTarget}(Id, Value) VALUES (@id, jsonb(@jsonText)) RETURNING Id;"
        else $"INSERT INTO {qTarget}(Value) VALUES (jsonb(@jsonText)) RETURNING Id;"
    let insertedId =
        let mutable statement = tx.Connection.OpenStatement(sql)
        try
            if id > 0L then
                statement.BindInt64(1, id)
                statement.BindTextBuffer(2, json)
            else statement.BindTextBuffer(1, json)
            let insertedId = if statement.Step() then statement.Row.GetInt64(0) else Seq.head (Seq.empty<int64>)
            statement.Dispose()
            insertedId
        with error -> statement.Abort(error); reraise()

    writeEntityId targetType entity insertedId
    insertedId

let private ensureTargetTable (tx: RelationTxContext) targetTable =
    let tables = tx.Preparation.TargetTables(tx.Connection)
    if not tx.Preparation.AllowReuse || not (tables.Contains targetTable) then
        ensureCollectionTableExists tx.Connection targetTable
        tables.Add targetTable |> ignore

let private missingTarget targetTable targetId =
    raise (InvalidOperationException(
        $"Error: Relation target '{targetTable}' with Id={targetId} does not exist.\nReason: The target row is missing.\nFix: Insert the target first or correct the Id."))

let internal ensureTargetExists (tx: RelationTxContext) (targetTable: string) (targetId: int64) =
    ensureTargetTable tx targetTable
    let exists =
        let mutable statement = tx.Connection.OpenStatement($"SELECT EXISTS (SELECT 1 FROM {quoteIdentifier targetTable} WHERE Id = @id)")
        try
            statement.BindInt64(1, targetId)
            statement.Step() |> ignore
            let exists = statement.Row.GetBoolean(0)
            statement.Dispose()
            exists
        with error -> statement.Abort(error); reraise()
    if not exists then missingTarget targetTable targetId

/// The caller supplies one bounded span with no intervening user-code boundary.
let internal ensureTargetsExist (tx: RelationTxContext) (targetTable: string) (targetIds: ResizeArray<int64>) =
    if targetIds.Count > 0 then
        ensureTargetTable tx targetTable
        let names = Array.init targetIds.Count (fun i -> "@id" + string i)
        let inClause = String.Join(",", names)
        let existing = HashSet<int64>()
        let mutable statement = tx.Connection.OpenStatement($"SELECT Id FROM {quoteIdentifier targetTable} WHERE Id IN ({inClause})")
        try
            for i = 0 to targetIds.Count - 1 do statement.BindInt64(i + 1, targetIds.[i])
            while statement.Step() do existing.Add(statement.Row.GetInt64(0)) |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()
        for id in targetIds do
            if not (existing.Contains id) then missingTarget targetTable id

let internal readDbRefId (dbRefObj: obj) =
    if isNull dbRefObj then
        0L
    else
        match dbRefObj with
        | :? IDocumentReference as reference -> reference.Id
        | _ -> RelationsAccessorCache.compiledDbRefIdReader(dbRefObj.GetType()).Invoke(dbRefObj)

let internal tryGetValueOptionValue (valueOpt: obj) =
    if isNull valueOpt then
        ValueNone
    else
        match RelationsAccessorCache.compiledValueOptionAccessors (valueOpt.GetType()) with
        | ValueNone -> ValueNone
        | ValueSome acc ->
            if acc.IsSome.Invoke(valueOpt) then
                let value = acc.GetValue.Invoke(valueOpt)
                if isNull value then ValueNone else ValueSome value
            else
                ValueNone

let internal tryGetVOptionProperty (dbRefObj: obj) (propertyName: string) =
    if isNull dbRefObj then
        ValueNone
    else
        match RelationsAccessorCache.tryCompiledGetter (dbRefObj.GetType()) propertyName with
        | ValueNone -> ValueNone
        | ValueSome getter -> getter.Invoke(dbRefObj) |> tryGetValueOptionValue

let internal tryGetPendingEntity (dbRefObj: obj) =
    match dbRefObj with
    | :? IDocumentReference as reference -> reference.PendingEntity
    | _ -> tryGetVOptionProperty dbRefObj "PendingEntity"

/// Returns the pending typed id only when the DBRef is in the transient $dbrefTypedId-marker state
/// — _id=0 && _hasTypedId && not _isLoaded. The shared accessor owner resolves
/// its predicate and typed value once, without reflection during entity traversal.
let internal tryGetPendingTypedId (dbRefObj: obj) : obj voption =
    if isNull dbRefObj then ValueNone
    else
        match dbRefObj with
        | :? IDocumentReference as reference -> reference.PendingTypedId
        | _ ->
        match RelationsAccessorCache.compiledPendingTypedIdAccessors (dbRefObj.GetType()) with
        | ValueSome struct(predicate, value) when predicate.Invoke(dbRefObj) -> ValueSome (value.Invoke(dbRefObj))
        | _ -> ValueNone

let internal createDbRefTo (dbRefType: Type) (id: int64) =
    let factories = RelationsAccessorCache.compiledDbRefFactories dbRefType
    factories.ToOrResolved.Invoke(id)

let internal createDbRefLoaded (dbRefType: Type) (id: int64) (entity: obj) =
    let factories = RelationsAccessorCache.compiledDbRefFactories dbRefType
    factories.Loaded.Invoke(id, entity)

/// Typed DBRef<'TTarget,'TId>: builds .Resolved(id, typedId). For single-arg DBRef<'T> the typed factory
/// is null and the caller must fall back to createDbRefTo.
let internal createDbRefResolvedTyped (dbRefType: Type) (id: int64) (typedId: obj) =
    let factories = RelationsAccessorCache.compiledDbRefFactories dbRefType
    factories.ResolvedTyped.Invoke(id, typedId)

/// Typed DBRef<'TTarget,'TId>: builds .Loaded(id, typedId, entity). For single-arg DBRef<'T> the typed
/// factory is null and the caller must fall back to createDbRefLoaded.
let internal createDbRefLoadedTyped (dbRefType: Type) (id: int64) (typedId: obj) (entity: obj) =
    let factories = RelationsAccessorCache.compiledDbRefFactories dbRefType
    factories.LoadedTyped.Invoke(id, typedId, entity)

/// True iff the DBRef type is the two-arg form DBRef<'TTarget,'TId>. Decides whether the cascade
/// path uses the typed factory (Resolved/Loaded with typed id) or the legacy single-arg factory.
let internal isTypedDbRef (dbRefType: Type) =
    DBRefTypeHelpers.isDBRefTypedType dbRefType

/// Reads the [<SoloId>] value from a target entity, boxed. Returns ValueNone when the entity has
/// no [<SoloId>] property or the value is null/default. After CustomIdRunner runs in the cascade
/// primitive (insertTargetEntity), the in-memory entity always carries a non-default SoloId for
/// typed-id targets — single source, no fallback to a post-insert DB read.
let internal tryExtractSoloId (targetType: Type) (entity: obj) : obj voption =
    SoloIdAccessor.TryGetBoxedValue(targetType, entity)

/// Cascade typed-id resolver: fails loud when the entity's [<SoloId>] is missing OR equal to its
/// CLR-default value. The default-equality check defends the read side against the value-type
/// DBRef collision pattern (N parents aliasing on the same default literal under JOIN). With
/// the cascade write site running the IIdGenerator and the runner refactor in place, this
/// branch is unreachable in normal flow; it is the tripwire for any future regression that
/// reintroduces a path to the broken shape.
let internal extractSoloIdOrFail (targetType: Type) (entity: obj) : obj =
    match tryExtractSoloId targetType entity with
    | ValueNone ->
        raise (InvalidOperationException(
            sprintf "Error: cannot stamp typed DBRef<%s,_>: the entity's [<SoloId>] is null/missing after cascade-insert.\nReason: the registered IIdGenerator either did not run or produced an empty value.\nFix: ensure the IIdGenerator's GenerateId returns a non-empty value." targetType.Name))
    | ValueSome v ->
        match SoloIdAccessor.TryGetMember targetType with
        | ValueSome prop when prop.IdType.IsValueType ->
            if prop.IsDefault(v) then
                raise (InvalidOperationException(
                    sprintf "Error: cannot stamp typed DBRef<%s,_>: the entity's [<SoloId>] equals the CLR default value for type '%s' after cascade-insert.\nReason: the registered IIdGenerator did not run or produced a default value, which would alias every default-keyed row under JOIN.\nFix: ensure the IIdGenerator's GenerateId returns a non-default value." targetType.Name prop.IdType.Name))
            v
        | _ -> v

let internal updateDbRefJson (tx: RelationTxContext) (ownerTable: string) (ownerId: int64) (propertyPath: string) (targetId: int64) =
    let jsonText = if targetId = 0L then "null" else string targetId
    RelationMutationStatements.setOwnerValueJson tx.Connection ownerTable propertyPath jsonText ownerId

let internal shouldSyncDbRefJson (descriptor: RelationDescriptor) =
    not descriptor.Property.Ignored

let internal ensureLinkWriteApplied (tx: RelationTxContext) (linkTable: string) (sourceId: int64) (targetId: int64) (opName: string) (rowsAffected: int) =
    if rowsAffected <= 0 then
        let exists =
            let mutable statement = tx.Connection.OpenStatement($"SELECT EXISTS (SELECT 1 FROM {quoteIdentifier linkTable} WHERE SourceId = @sourceId AND TargetId = @targetId)")
            try
                statement.BindInt64(1, sourceId)
                statement.BindInt64(2, targetId)
                statement.Step() |> ignore
                let exists = statement.Row.GetBoolean(0)
                statement.Dispose()
                exists
            with error -> statement.Abort(error); reraise()
        if not exists then
            raise (InvalidOperationException(
                $"Error: Relation link write failed in '{linkTable}'.\nReason: {opName} reported no affected rows and no link row exists for ({sourceId}, {targetId}).\nFix: Verify relation mutation inputs and constraints before retrying."))

let internal resolveTypedIdToTargetId (tx: RelationTxContext) (descriptor: RelationDescriptor) (typedId: obj) =
    match descriptor.TypedIdType, descriptor.TargetSoloIdProperty with
    | ValueSome idType, ValueSome soloIdProp ->
        ensureCollectionTableExists tx.Connection descriptor.TargetTable
        if isNull typedId then
            raise (InvalidOperationException(
                $"Error: Null typed id on relation '{descriptor.OwnerType.FullName}.{descriptor.Property.Name}'.\nReason: DBRef<'TTarget,'TId>.To requires a non-null typed id.\nFix: Use DBRef.None or provide a valid typed id."))
        if not (idType.IsAssignableFrom(typedId.GetType())) then
            raise (InvalidOperationException(
                $"Error: Typed id mismatch on relation '{descriptor.OwnerType.FullName}.{descriptor.Property.Name}'.\nReason: Expected id type '{idType.FullName}', got '{typedId.GetType().FullName}'.\nFix: Use the exact target [SoloId] type."))

        if not (StoredValueParameter.canSerializeWithoutEntityCode soloIdProp.IdType) then
            tx.Preparation.DisableReuse()
        let variables = Dictionary<string, obj>()
        let typedIdParameter =
            StoredValueParameter.allocateNamedForDeclaredType
                variables "typedId" soloIdProp.IdType typedId
        let predicate =
            SqlExpr.Binary(
                StoredValueParameter.declaredExpression soloIdProp.IdType
                    (SqlExpr.JsonExtractExpr(None, "Value", JsonPathOps.ofList [soloIdProp.Name])),
                BinaryOperator.Eq,
                typedIdParameter)
        let emitContext = EmitContext()
        emitContext.InlineLiterals <- true
        let predicateSql = (EmitSelect.emitExpr emitContext predicate).Sql
        let matches =
            let mutable statement = tx.Connection.OpenStatement($"SELECT Id FROM {quoteIdentifier descriptor.TargetTable} WHERE {predicateSql};")
            try
                statement.BindValues(variables)
                let matches =
                    if not (statement.Step()) then ValueNone
                    else
                        let id = statement.Row.GetInt64(0)
                        ValueSome(struct (id, statement.Step()))
                statement.Dispose()
                matches
            with error -> statement.Abort(error); reraise()

        match matches with
        | ValueSome(struct (id, false)) -> id
        | ValueNone ->
            raise (InvalidOperationException(
                $"Error: Typed relation target not found for '{descriptor.OwnerType.FullName}.{descriptor.Property.Name}'.\nReason: No row in '{descriptor.TargetTable}' matches [SoloId] value '{typedId}'.\nFix: Insert the target first or correct the typed id."))
        | _ ->
            raise (InvalidOperationException(
                $"Error: Ambiguous typed relation target for '{descriptor.OwnerType.FullName}.{descriptor.Property.Name}'.\nReason: Multiple rows in '{descriptor.TargetTable}' share [SoloId] value '{typedId}'.\nFix: Enforce unique index on the [SoloId] path and deduplicate data."))
    | _ ->
        raise (InvalidOperationException(
            $"Error: Relation '{descriptor.OwnerType.FullName}.{descriptor.Property.Name}' is not configured for typed-id resolution.\nReason: Missing DBRef<'TTarget,'TId> metadata.\nFix: Use DBRef<T> with row id or configure typed-id relation correctly."))


let internal prepareFileSystemReferences (tx: RelationTxContext) (owner: obj) (oldOwner: obj voption) (replace: bool) (insert: bool) (singleFromStoredJson: bool) (ops: ResizeArray<RelationUpdateManyOp>) =
    for spec in getFileSystemReferenceSpecs tx.OwnerType do
        let value = spec.Property.Read.Invoke(owner)
        let property = spec.Property.Name
        match spec.Kind with
        | Single ->
            let id = readDbRefId value
            let oldId =
                match oldOwner with
                | ValueSome old -> readDbRefId (spec.Property.Read.Invoke old)
                | _ -> 0L
            if (insert && id > 0L) || replace || (not insert && oldId <> id) then
                if singleFromStoredJson then ops.Add(RefreshFileSystemRef property)
                else ops.Add(SetFileSystemRef(property, id))
        | Many ->
            match value with
            | :? IFileSystemRefMany as tracker ->
                if replace || insert then
                    if replace then ops.Add(ClearFileSystemRefs property)
                    let ids = HashSet<int64>()
                    for item in tracker.GetCurrentItemsBoxed() do
                        let id = (item :?> IFileSystemReferenceValue).Id
                        if id <= 0L then invalidArg property "An empty filesystem reference cannot be a Many member."
                        if ids.Add id then ops.Add(AddFileSystemRef(property, id))
                    ops.Add(AcceptFileSystemRefChanges(tracker, true))
                elif tracker.HasPendingMutations then
                    if tracker.WasCleared then ops.Add(ClearFileSystemRefs property)
                    for id in tracker.RemovedIds do ops.Add(RemoveFileSystemRef(property, id))
                    for id in tracker.AddedIds do ops.Add(AddFileSystemRef(property, id))
                    ops.Add(AcceptFileSystemRefChanges(tracker, tracker.IsLoaded || tracker.WasCleared))
            | null when not replace && not insert -> ()
            | null when insert -> ()
            | null -> nullArg property
            | _ -> invalidArg property "Invalid filesystem Many reference."


let private applyFileSystemReferenceSelection (tx: RelationTxContext) selection updateOwnerJson (op: RelationUpdateManyOp) =
    match op with
    | SetFileSystemRef(propertyPath, _)
    | RefreshFileSystemRef propertyPath
    | AddFileSystemRef(propertyPath, _)
    | RemoveFileSystemRef(propertyPath, _)
    | ClearFileSystemRefs propertyPath ->
        let targetId =
            match op with
            | SetFileSystemRef(_, id) | AddFileSystemRef(_, id) | RemoveFileSystemRef(_, id) -> id
            | _ -> 0L
        let spec =
            getFileSystemReferenceSpecs tx.OwnerType
            |> Array.find (fun spec -> spec.Property.Name = propertyPath)
        let column = if spec.TargetTable = "SoloDBFileHeader" then "FileId" else "DirectoryId"
        let owner = formatName tx.OwnerTable
        let run (statement, variables) = StatementExecution.execute tx.Connection statement variables
        let clear() = RelationMutationStatements.filesystemLinkDelete owner propertyPath column ValueNone selection |> run |> ignore
        let add() =
            if targetId <= 0L then raise (ArgumentOutOfRangeException(nameof targetId, "Many members require a positive target Id."))
            match selection with
            | RelationMutationStatements.OwnerSelection.Batch(_, _, count) ->
                if RelationMutationStatements.ownerCount tx.Connection owner selection <> int64 count then
                    invalidOp "A reference owner no longer exists. Reload the owners before linking a filesystem target."
            | _ -> ()
            let written = RelationMutationStatements.filesystemLinkInsert owner propertyPath (relationKindToString spec.Kind) column targetId selection |> run
            match selection with
            | RelationMutationStatements.OwnerSelection.One ownerId when written = 0 ->
                let exists =
                    let mutable statement = tx.Connection.OpenStatement($"SELECT EXISTS(SELECT 1 FROM SoloDBFileSystemReference WHERE OwnerCollection=@owner AND OwnerId=@ownerId AND Property=@property AND {column}=@targetId)")
                    try
                        statement.BindText(1, owner)
                        statement.BindInt64(2, ownerId)
                        statement.BindText(3, propertyPath)
                        statement.BindInt64(4, targetId)
                        statement.Step() |> ignore
                        let exists = statement.Row.GetBoolean(0)
                        statement.Dispose()
                        exists
                    with error -> statement.Abort(error); reraise()
                if not exists then
                    invalidOp "The reference owner no longer exists. Reload the owner before linking a filesystem target."
            | _ -> ()
        match op with
        | SetFileSystemRef _ ->
            if spec.Kind <> Single then invalidOp "Single assignment requires a Single filesystem reference."
            clear()
            if targetId > 0L then add()
            if updateOwnerJson then
                let json = if targetId = 0L then "null" else string targetId
                RelationMutationStatements.ownerValueJsonSetForSelection owner propertyPath json selection |> run |> ignore
        | RefreshFileSystemRef _ ->
            if spec.Kind <> Single then invalidOp "Refreshing stored identity requires a Single filesystem reference."
            match selection with
            | RelationMutationStatements.OwnerSelection.Batch(_, _, count) ->
                if RelationMutationStatements.ownerCount tx.Connection owner selection <> int64 count then
                    invalidOp "A reference owner no longer exists. Reload the owners before linking a filesystem target."
            | _ -> ()
            clear()
            let written = RelationMutationStatements.filesystemSingleLinkFromJson owner propertyPath column selection |> run
            if written = 0 then
                if RelationMutationStatements.ownerCount tx.Connection owner selection = 0L then
                    invalidOp "The reference owner no longer exists. Reload the owner before linking a filesystem target."
        | AddFileSystemRef _ ->
            if spec.Kind <> Many then invalidOp "Add requires a Many filesystem reference."
            add()
        | RemoveFileSystemRef _ ->
            if spec.Kind <> Many then invalidOp "Remove requires a Many filesystem reference."
            RelationMutationStatements.filesystemLinkDelete owner propertyPath column (ValueSome targetId) selection |> run |> ignore
        | ClearFileSystemRefs _ ->
            if spec.Kind <> Many then invalidOp "Clear requires a Many filesystem reference."
            clear()
        | _ -> invalidOp "Unexpected filesystem reference operation."
    | AcceptFileSystemRefChanges(tracker, loaded) -> tracker.AcceptChanges(loaded)
    | _ -> invalidArg (nameof op) "Expected a filesystem reference operation."


let internal applyFileSystemReferenceOp tx ownerId updateOwnerJson op =
    applyFileSystemReferenceSelection tx (RelationMutationStatements.OwnerSelection.One ownerId) updateOwnerJson op

/// The selection is fixed before callbacks and chunks retain the provider parameter bound.
let internal applyFileSystemReferenceToOwners (tx: RelationTxContext) (ownerIds: int64 array) op =
    for offset in 0 .. 500 .. ownerIds.Length - 1 do
        let count = min 500 (ownerIds.Length - offset)
        let selection = RelationMutationStatements.OwnerSelection.Batch(ownerIds, offset, count)
        applyFileSystemReferenceSelection tx selection true op


/// Replacements apply reference effects before publishing owner JSON. Reconcile Singles
/// afterward so callback reassignment and deletion cannot detach protection from stored facts.
let internal refreshFileSystemSingles (tx: RelationTxContext) (ownerIds: int64 array) =
    for spec in getFileSystemReferenceSpecs tx.OwnerType do
        if spec.Kind = Single then
            for offset in 0 .. 500 .. ownerIds.Length - 1 do
                let count = min 500 (ownerIds.Length - offset)
                let selection = RelationMutationStatements.OwnerSelection.Batch(ownerIds, offset, count)
                applyFileSystemReferenceSelection tx selection false (RefreshFileSystemRef spec.Property.Name)
