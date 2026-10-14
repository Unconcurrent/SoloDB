namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq.Expressions
open System.Text
open SoloDatabase.Types
open JsonFunctions
open Utils
open NativeSQLiteTools
open SoloDatabase
open SoloDatabase.SqlModel

type private SoloIdWriteScanner(soloIdProp: CustomIdMember) =
    inherit ExpressionVisitor()
    let matches (memberValue: System.Reflection.MemberInfo) =
        memberValue.Name = soloIdProp.Name &&
        (let owner = memberValue.DeclaringType
         let expected = soloIdProp.DeclaringType
         if isNull owner then isNull expected else owner.Equals(expected))
    let mutable found = false
    member _.Found = found
    override this.VisitBinary(node: BinaryExpression) =
        if node.NodeType = ExpressionType.Assign then
            match node.Left with
            | :? MemberExpression as me when matches me.Member ->
                found <- true
            | _ -> ()
        base.VisitBinary(node)
    override this.VisitMethodCall(node: MethodCallExpression) =
        if node.Method.Name = "set_" + soloIdProp.Name &&
           (let owner = node.Method.DeclaringType
            let expected = soloIdProp.DeclaringType
            if isNull owner then isNull expected else owner.Equals(expected)) &&
           node.Arguments.Count = 1 && node.Arguments.[0].Type.Equals(soloIdProp.IdType) then
            found <- true
        elif node.Method.Name = "Set" && node.Method.IsStatic && node.Arguments.Count >= 1 then
            // Extensions.Set(<member>, value) — the UpdateMany DSL setter form. Treat as a
            // write to the first-argument member if it resolves to the [<SoloId>] property.
            match node.Arguments.[0] with
            | :? MemberExpression as me when matches me.Member ->
                found <- true
            | _ -> ()
        base.VisitMethodCall(node)

type internal CollectionMutationOps<'T>() =

    /// UpdateMany [<SoloId>]-write rejection at translate time. Walks each json transform
    /// for assignments to the [<SoloId>] property; raises before any SQL runs. Narrow: rejects
    /// exactly the assignment shape; non-SoloId UpdateMany transforms are unaffected.
    static member private RejectSoloIdWriteInTransforms (transforms: Expression<System.Action<'T>> array) =
        match CustomTypeId<'T>.Get() with
        | Some custom ->
            for expression in transforms do
                let scanner = SoloIdWriteScanner(custom.Member)
                scanner.Visit(expression) |> ignore
                if scanner.Found then
                    raise (InvalidOperationException(
                        sprintf "Error: Cannot use UpdateMany to write the [<SoloId>] field '%s.%s'.\nReason: The [<SoloId>] field is the type's identity and may not be mutated through UpdateMany. To re-identify a row, delete and re-insert the entity."
                            typeof<'T>.FullName custom.Member.Name))
        | None -> ()

    /// SELECT-and-check variant for non-relation write paths that don't already load
    /// the existing row. Reads the matched row's SoloId via jsonb_extract on the same
    /// connection + variables and applies the same accept/reject rule as the in-memory
    /// helper below.
    static member private RejectEmptyMutationOfNonEmptySoloIdAgainstStored
            (opName: string)
            (conn: NativeSqliteConnection)
            (collectionName: string)
            (filterSql: string)
            (variables: Dictionary<string, obj>)
            (item: 'T) =
        match CustomTypeId<'T>.Get() with
        | Some custom ->
            let newId = custom.GetId(box item)
            let isNewEmpty =
                match custom.Generator with
                | :? SoloDatabase.Attributes.IIdGenerator as g -> g.IsEmpty newId
                | :? SoloDatabase.Attributes.IIdGenerator<'T> as g -> g.IsEmpty newId
                | _ -> false
            if isNewEmpty then
                let soloPath = "$." + custom.Member.Name
                let storedIdSql =
                    sprintf "SELECT jsonb_extract(Value, '%s') FROM \"%s\" WHERE (%s) LIMIT 1"
                        soloPath collectionName filterSql
                let storedId =
                    let mutable statement = conn.OpenStatement(storedIdSql)
                    try
                        statement.BindValues(variables)
                        let value = if statement.Step() && not (statement.Row.IsDBNull(0)) then statement.Row.GetValue(0) else null
                        statement.Dispose()
                        value
                    with error -> statement.Abort(error); reraise()
                if not (isNull storedId) then
                    // The SQLite reader returns numeric values as int64; normalize into
                    // the property's declared CLR type before equality / IsEmpty checks.
                    let normalizedStored =
                        try System.Convert.ChangeType(storedId, custom.Member.IdType)
                        with _ -> storedId
                    let equal =
                        if isNull normalizedStored && isNull newId then true
                        elif isNull normalizedStored || isNull newId then false
                        else normalizedStored.Equals(newId)
                    if not equal then
                        raise (InvalidOperationException(
                            sprintf "Error: %s requires a populated [<SoloId>] field on the supplied entity.\nReason: the registered IIdGenerator '%s' considers the supplied SoloId value empty while the stored row carries a different SoloId. Updating with an empty SoloId would silently overwrite the stored identity.\nFix: hydrate the SoloId from the existing row before mutating, or use Insert/InsertOrReplace which generates the id automatically."
                                opName (custom.Generator.GetType().FullName)))
        | None -> ()

    /// Reject when the supplied entity carries an empty `[<SoloId>]` value AND the stored
    /// row's SoloId is non-empty — that's the silent-identity-overwrite case where a
    /// caller forgot to hydrate the SoloId before mutating. When stored and supplied
    /// SoloIds are equal (including both being IsEmpty for primitive-id types whose
    /// "empty" value is also a stored value), the operation is accepted.
    static member private RejectEmptyMutationOfNonEmptySoloId (opName: string) (oldEntity: 'T) (newEntity: 'T) =
        match CustomTypeId<'T>.Get() with
        | Some custom ->
            let oldId = custom.GetId(box oldEntity)
            let newId = custom.GetId(box newEntity)
            let isNewEmpty =
                match custom.Generator with
                | :? SoloDatabase.Attributes.IIdGenerator as g -> g.IsEmpty newId
                | :? SoloDatabase.Attributes.IIdGenerator<'T> as g -> g.IsEmpty newId
                | _ -> false
            let equal =
                if isNull oldId && isNull newId then true
                elif isNull oldId || isNull newId then false
                else oldId.Equals(newId)
            if isNewEmpty && not equal then
                raise (InvalidOperationException(
                    sprintf "Error: %s requires a populated [<SoloId>] field on the supplied entity.\nReason: the registered IIdGenerator '%s' considers the supplied SoloId value empty while the stored row carries a non-empty SoloId. Updating with an empty SoloId would silently overwrite the stored identity.\nFix: hydrate the SoloId from the existing row before mutating, or use Insert/InsertOrReplace which generates the id automatically."
                        opName (custom.Generator.GetType().FullName)))
        | None -> ()

    /// Whole-row write paths (Update, ReplaceOne, ReplaceMany) accept a user-supplied entity.
    /// If the registered IIdGenerator considers the SoloId empty, fail loud — generation is
    /// for Insert and cascade-create only.
    static member private ValidateNonEmptySoloId (opName: string) (item: 'T) =
        match CustomTypeId<'T>.Get() with
        | Some custom ->
            let extractedId = custom.GetId(box item)
            let isEmpty =
                match custom.Generator with
                | :? SoloDatabase.Attributes.IIdGenerator as g -> g.IsEmpty extractedId
                | :? SoloDatabase.Attributes.IIdGenerator<'T> as g -> g.IsEmpty extractedId
                | _ -> false
            if isEmpty then
                raise (InvalidOperationException(
                    sprintf "Error: %s requires a populated [<SoloId>] field on the supplied entity.\nReason: the registered IIdGenerator '%s' considers the current SoloId value empty.\nFix: hydrate the SoloId from the existing row before mutating, or use Insert/InsertOrReplace which generates the id automatically."
                        opName (custom.Generator.GetType().FullName)))
        | None -> ()

    static member Update
        (item: 'T)
        (name: string)
        (hasRelations: bool)
        (withTransaction: (NativeSqliteConnection -> unit) -> unit)
        (ensureRelationTx: NativeSqliteConnection -> RelationsTypes.RelationTxContext)
        (mkRelationPathSets: unit -> 'a * 'b)
        (setSerializedItem: IDictionary<string, obj> -> 'T -> unit) =

        if isNull (box item) then raise (ArgumentNullException(nameof(item)))

        let filterExpr, variables =
            if EntityDocumentId<'T>.Value then
                let id = EntityDocumentId<'T>.Read item
                let variables = Dictionary<string,obj>()
                variables.["_uid0"] <- box id
                SqlExpr.Binary(SqlExpr.Column(None,"Id"), BinaryOperator.Eq, SqlExpr.Parameter "_uid0"), variables
            else
                match CustomTypeId<'T>.Get() with
                | Some customId ->
                    let id = customId.GetId (item |> box)
                    let idProp = customId.Member
                    let customVariables = Dictionary<string, obj>()
                    let idParameter =
                        QueryableHelperBase.allocateNamedStoredValueParam
                            customVariables "_cid0" idProp.IdType id
                    let predicate =
                        SqlExpr.Binary(
                            StoredValueParameter.declaredExpression idProp.IdType
                                (SqlExpr.JsonExtractExpr(None, "Value", SqlModel.JsonPathOps.ofList [idProp.Name])),
                            BinaryOperator.Eq,
                            idParameter)
                    predicate, customVariables
                | None ->
                    let typeName = typeof<'T>.Name
                    let message =
                        sprintf "Error: Item type %s has no int64 Id or custom Id.\nReason: Updates require a stable identifier.\nFix: Add an int64 Id property or configure a custom Id strategy."
                            typeName
                    raise (InvalidOperationException(message))
        let whereSql = HydrationSqlBuilder.emitExprToSql filterExpr

        if hasRelations then
            withTransaction (fun conn ->
                let tx = ensureRelationTx conn

                // mutation-prep old-state read with DBRefMany-only hydration.
                // Build DU-based WHERE from entity Id (no raw SQL string surgery).
                let manyVars = Dictionary<string, obj>()
                let whereExpr =
                    if EntityDocumentId<'T>.Value then
                        let id = EntityDocumentId<'T>.Read item
                        manyVars.["_mid0"] <- box id
                        SqlExpr.Binary(SqlExpr.Column(Some "o", "Id"), BinaryOperator.Eq, SqlExpr.Parameter "_mid0")
                    else
                        match CustomTypeId<'T>.Get() with
                        | Some customId ->
                            let id = customId.GetId(item |> box)
                            let idParameter =
                                QueryableHelperBase.allocateNamedStoredValueParam
                                    manyVars "_mid0" customId.Member.IdType id
                            SqlExpr.Binary(
                                StoredValueParameter.declaredExpression customId.Member.IdType
                                    (SqlExpr.JsonExtractExpr(Some "o", "Value", SqlModel.JsonPathOps.ofList [customId.Member.Name])),
                                BinaryOperator.Eq,
                                idParameter)
                        | None -> raise (InvalidOperationException("Update requires int64 Id or custom Id."))
                let sql, manyHydrated =
                    HydrationSqlBuilder.buildManyOnlyHydratedSql conn name typeof<'T> (QueryTranslator.mutationWhere conn name whereExpr) manyVars true
                let oldRow = readHydratedDocument conn sql manyVars
                if isNull oldRow then
                    raise (KeyNotFoundException "Could not Update any entities with specified Id.")

                let oldOwner = fromSQLite<'T> oldRow
                if manyHydrated && not (isNull oldRow.HydrationJSON) then
                    let hydMap = Dictionary<int64, string>()
                    hydMap.[oldRow.Id.Value] <- oldRow.HydrationJSON
                    HydrationManyPopulator.populateFromHydrationJson typeof<'T> [| (oldRow.Id.Value, box oldOwner) |] hydMap
                CollectionMutationOps<'T>.RejectEmptyMutationOfNonEmptySoloId "Update" oldOwner item
                let writePlan = RelationsPlan.prepareUpdate tx oldRow.Id.Value (box oldOwner) (box item)

                setSerializedItem variables item
                let count =
                    let mutable statement = conn.OpenStatement($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE {whereSql}")
                    try
                        statement.BindValues(variables)
                        statement.Step() |> ignore
                        let affected = statement.RecordsAffected
                        statement.Dispose()
                        affected
                    with error -> statement.Abort(error); reraise()
                if count <= 0 then
                    raise (KeyNotFoundException "Could not Update any entities with specified Id.")

                RelationsSync.syncUpdate tx oldRow.Id.Value writePlan
                RelationsGraphAdmission.validate tx
            )
        else
            withTransaction (fun conn ->
                CollectionMutationOps<'T>.RejectEmptyMutationOfNonEmptySoloIdAgainstStored "Update" conn name whereSql variables item
                setSerializedItem variables item
                let count =
                    let mutable statement = conn.OpenStatement($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE {whereSql}")
                    try
                        statement.BindValues(variables)
                        statement.Step() |> ignore
                        let affected = statement.RecordsAffected
                        statement.Dispose()
                        affected
                    with error -> statement.Abort(error); reraise()
                if count <= 0 then
                    raise (KeyNotFoundException "Could not Update any entities with specified Id."))

    static member DeleteMany
        (filter: Expression<Func<'T, bool>>)
        (name: string)
        (withTransaction: (NativeSqliteConnection -> int) -> int)
        (requiresRelationDeleteHandling: NativeSqliteConnection -> bool)
        (ensureRelationTx: NativeSqliteConnection -> RelationsTypes.RelationTxContext)
        (selectMutationRows: NativeSqliteConnection -> Expression<Func<'T, bool>> -> bool -> DbObjectRow array) =

        if isNull filter then raise (ArgumentNullException(nameof(filter)))

        withTransaction (fun conn ->
            let requiresRelationHandling = requiresRelationDeleteHandling conn

            if requiresRelationHandling then
                let tx = ensureRelationTx conn

                let rows = selectMutationRows conn filter false
                if rows.Length = 0 then
                    0
                else
                    for row in rows do
                        let owner = fromSQLite<'T> row
                        let ownerId = row.Id.Value
                        let deletePlan = RelationsPlan.prepareDeleteOwner tx ownerId (box owner)
                        RelationsSync.syncDeleteOwner tx deletePlan

                    let ids = ResizeArray<string>(rows.Length)
                    for i in 0 .. rows.Length - 1 do
                        let key = $"id{i}"
                        ids.Add("@" + key)

                    let idList = String.Join(",", ids)
                    let sql = $"DELETE FROM \"{name}\" WHERE Id IN ({idList})"
                    let mutable statement = conn.OpenStatement(sql)
                    try
                        for i = 0 to rows.Length - 1 do statement.BindInt64(i + 1, rows.[i].Id.Value)
                        statement.Step() |> ignore
                        let affected = statement.RecordsAffected
                        statement.Dispose()
                        affected
                    with error -> statement.Abort(error); reraise()
            else
                // translateWhereExpr + emitted WHERE for DeleteMany.
                let filterExpr, variables = QueryTranslator.translateWhereExpr name filter
                let filterSql = QueryTranslator.mutationWhere conn name filterExpr |> HydrationSqlBuilder.emitExprToSql
                let mutable statement = conn.OpenStatement($"DELETE FROM \"{name}\" WHERE {filterSql}")
                try
                    statement.BindValues(variables)
                    statement.Step() |> ignore
                    let affected = statement.RecordsAffected
                    statement.Dispose()
                    affected
                with error -> statement.Abort(error); reraise()
        )

    static member DeleteOne
        (filter: Expression<Func<'T, bool>>)
        (name: string)
        (withTransaction: (NativeSqliteConnection -> int) -> int)
        (requiresRelationDeleteHandling: NativeSqliteConnection -> bool)
        (ensureRelationTx: NativeSqliteConnection -> RelationsTypes.RelationTxContext)
        (selectMutationRows: NativeSqliteConnection -> Expression<Func<'T, bool>> -> bool -> DbObjectRow array) =

        if isNull filter then raise (ArgumentNullException(nameof(filter)))

        withTransaction (fun conn ->
            let requiresRelationHandling = requiresRelationDeleteHandling conn

            if requiresRelationHandling then
                let tx = ensureRelationTx conn

                let rows = selectMutationRows conn filter true
                if rows.Length = 0 then
                    0
                else
                    let oldRow = rows.[0]
                    let owner = fromSQLite<'T> oldRow
                    let deletePlan = RelationsPlan.prepareDeleteOwner tx oldRow.Id.Value (box owner)
                    RelationsSync.syncDeleteOwner tx deletePlan
                    deleteDocumentById conn name oldRow.Id.Value
            else
                // translateWhereExpr + emitted WHERE for DeleteOne no-rel.
                let filterExpr, variables = QueryTranslator.translateWhereExpr name filter
                let filterSql = QueryTranslator.mutationWhere conn name filterExpr |> HydrationSqlBuilder.emitExprToSql
                let mutable statement = conn.OpenStatement($"DELETE FROM \"{name}\" WHERE Id in (SELECT Id FROM \"{name}\" WHERE ({filterSql}) LIMIT 1)")
                try
                    statement.BindValues(variables)
                    statement.Step() |> ignore
                    let affected = statement.RecordsAffected
                    statement.Dispose()
                    affected
                with error -> statement.Abort(error); reraise()
        )

    static member ReplaceMany
        (item: 'T)
        (filter: Expression<Func<'T, bool>>)
        (name: string)
        (hasRelations: bool)
        (withTransaction: (NativeSqliteConnection -> int) -> int)
        (ensureRelationTx: NativeSqliteConnection -> RelationsTypes.RelationTxContext)
        (mkRelationPathSets: unit -> 'a * 'b)
        (setSerializedItem: IDictionary<string, obj> -> 'T -> unit) =

        if isNull (box item) then raise (ArgumentNullException(nameof(item)))
        if isNull filter then raise (ArgumentNullException(nameof(filter)))

        // translateWhereExpr canonical predicate path for ReplaceMany.
        let filterExpr, variables = QueryTranslator.translateWhereExpr name filter
        let filterSql conn = QueryTranslator.mutationWhere conn name filterExpr |> HydrationSqlBuilder.emitExprToSql

        if hasRelations then
            withTransaction (fun conn ->
                let filterSql = filterSql conn
                let tx = ensureRelationTx conn

                // mutation-prep old-state read with DBRefMany-only hydration.
                let sql, manyHydrated =
                    HydrationSqlBuilder.buildManyOnlyHydratedSqlWithRawWhere conn name typeof<'T> filterSql false
                let oldRows =
                    let mutable statement = conn.OpenStatement(sql)
                    try
                        statement.BindValues(variables)
                        let rows = ResizeArray<DbObjectRow>()
                        while statement.Step() do rows.Add(SqlRowValue.Document(statement.Row))
                        statement.Dispose()
                        rows.ToArray()
                    with error -> statement.Abort(error); reraise()
                if oldRows.Length = 0 then
                    0
                else
                    let oldOwners = oldRows |> Array.map (fun row -> fromSQLite<'T> row |> box)
                    let ownerIds = oldRows |> Array.map (fun row -> row.Id.Value)
                    let ownerPairs = Array.zip ownerIds oldOwners
                    if manyHydrated then
                        let hydMap = Dictionary<int64, string>()
                        for row in oldRows do
                            if not (isNull row.HydrationJSON) then
                                hydMap.[row.Id.Value] <- row.HydrationJSON
                        HydrationManyPopulator.populateFromHydrationJson typeof<'T> ownerPairs hydMap
                    // Reject empty-SoloId mutation against any matched row whose stored
                    // SoloId is non-empty; the helper compares on the first matched row,
                    // which is sufficient for the rejection contract because all rows
                    // would receive the same supplied entity content.
                    match oldRows |> Array.tryHead with
                    | Some row ->
                        let oldOwner = fromSQLite<'T> row
                        CollectionMutationOps<'T>.RejectEmptyMutationOfNonEmptySoloId "ReplaceMany" oldOwner item
                    | None -> ()
                    RelationsSync.syncReplaceMany tx (ownerIds :> seq<_>) (oldOwners :> seq<_>) (box item)
                    setSerializedItem variables item
                    let affected =
                        let mutable statement = conn.OpenStatement($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE {filterSql}")
                        try
                            statement.BindValues(variables)
                            statement.Step() |> ignore
                            let affected = statement.RecordsAffected
                            statement.Dispose()
                            affected
                        with error -> statement.Abort(error); reraise()
                    RelationsEntitySync.refreshFileSystemSingles tx ownerIds
                    RelationsGraphAdmission.validate tx
                    affected
            )
        else
            withTransaction (fun conn ->
                let filterSql = filterSql conn
                CollectionMutationOps<'T>.RejectEmptyMutationOfNonEmptySoloIdAgainstStored "ReplaceMany" conn name filterSql variables item
                setSerializedItem variables item
                let mutable statement = conn.OpenStatement($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE {filterSql}")
                try
                    statement.BindValues(variables)
                    statement.Step() |> ignore
                    let affected = statement.RecordsAffected
                    statement.Dispose()
                    affected
                with error -> statement.Abort(error); reraise())

    static member ReplaceOne
        (item: 'T)
        (filter: Expression<Func<'T, bool>>)
        (name: string)
        (hasRelations: bool)
        (withTransaction: (NativeSqliteConnection -> int) -> int)
        (ensureRelationTx: NativeSqliteConnection -> RelationsTypes.RelationTxContext)
        (mkRelationPathSets: unit -> 'a * 'b)
        (setSerializedItem: IDictionary<string, obj> -> 'T -> unit) =

        if isNull (box item) then raise (ArgumentNullException(nameof(item)))
        if isNull filter then raise (ArgumentNullException(nameof(filter)))

        // translateWhereExpr canonical predicate path for ReplaceOne.
        let filterExpr, variables = QueryTranslator.translateWhereExpr name filter
        let filterSql conn = QueryTranslator.mutationWhere conn name filterExpr |> HydrationSqlBuilder.emitExprToSql

        if hasRelations then
            withTransaction (fun conn ->
                let filterSql = filterSql conn
                let tx = ensureRelationTx conn

                let sql, manyHydrated =
                    HydrationSqlBuilder.buildManyOnlyHydratedSqlWithRawWhere conn name typeof<'T> $"({filterSql})" true
                let oldRow = readHydratedDocument conn sql variables
                if isNull oldRow then
                    0
                else
                    let oldOwner = fromSQLite<'T> oldRow
                    if manyHydrated && not (isNull oldRow.HydrationJSON) then
                        let hydMap = Dictionary<int64, string>()
                        hydMap.[oldRow.Id.Value] <- oldRow.HydrationJSON
                        HydrationManyPopulator.populateFromHydrationJson typeof<'T> [| (oldRow.Id.Value, box oldOwner) |] hydMap
                    CollectionMutationOps<'T>.RejectEmptyMutationOfNonEmptySoloId "ReplaceOne" oldOwner item
                    RelationsSync.syncReplaceOne tx oldRow.Id.Value (box oldOwner) (box item)
                    setSerializedItem variables item
                    let affected =
                        let mutable statement = conn.OpenStatement($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE Id = @id")
                        try
                            statement.BindValue(1, variables.["item"])
                            statement.BindInt64(2, oldRow.Id.Value)
                            statement.Step() |> ignore
                            let affected = statement.RecordsAffected
                            statement.Dispose()
                            affected
                        with error -> statement.Abort(error); reraise()
                    RelationsEntitySync.refreshFileSystemSingles tx [| oldRow.Id.Value |]
                    RelationsGraphAdmission.validate tx
                    affected
            )
        else
            withTransaction (fun conn ->
                let filterSql = filterSql conn
                CollectionMutationOps<'T>.RejectEmptyMutationOfNonEmptySoloIdAgainstStored "ReplaceOne" conn name filterSql variables item
                setSerializedItem variables item
                let mutable statement = conn.OpenStatement($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE Id in (SELECT Id FROM \"{name}\" WHERE ({filterSql}) LIMIT 1)")
                try
                    statement.BindValues(variables)
                    statement.Step() |> ignore
                    let affected = statement.RecordsAffected
                    statement.Dispose()
                    affected
                with error -> statement.Abort(error); reraise())

    static member private ExpandUpdateTransforms (transforms: Expression<System.Action<'T>> array) =
        let expanded = ResizeArray<Expression<System.Action<'T>>>(transforms.Length)
        for expression in transforms do
            match expression.Body with
            | :? BlockExpression as block when block.Variables.Count = 0 ->
                for child in block.Expressions do
                    expanded.Add(Expression.Lambda<System.Action<'T>>(child, expression.Parameters))
            | _ -> expanded.Add expression
        expanded.ToArray()

    static member UpdateMany
        (transform: Expression<System.Action<'T>> array)
        (filter: Expression<Func<'T, bool>>)
        (name: string)
        (hasRelations: bool)
        (getConnection: unit -> NativeSqliteConnection)
        (withTransaction: (NativeSqliteConnection -> int) -> int)
        (ensureRelationTx: NativeSqliteConnection -> RelationsTypes.RelationTxContext)
        (selectMutationRows: NativeSqliteConnection -> Expression<Func<'T, bool>> -> bool -> DbObjectRow array)
        (executeJsonUpdateManyByRows: NativeSqliteConnection -> DbObjectRow array -> ResizeArray<Expression<System.Action<'T>>> -> int) =

        let transform = nullArgCheck (nameof transform) transform
        let filter = nullArgCheck (nameof filter) filter
        if transform |> Array.exists isNull then
            raise (ArgumentException("transform cannot contain null elements.", nameof(transform)))

        match transform.Length with
        | 0 -> 0
        | _ ->

        CollectionMutationOps<'T>.RejectSoloIdWriteInTransforms transform

        // Decompose `Action<'T>` lambdas whose body is a BlockExpression into one lambda
        // per child statement. F# `fun o -> a; b; c` lowers to a Block whose children are
        // executed in order; treating each child as its own transform preserves the
        // last-writer-wins semantics expected by callers and lets each statement be
        // classified individually as relation-op vs. json-set.
        let expandedTransforms =
            let needsExpansion =
                transform |> Array.exists (fun expression ->
                    match expression.Body with
                    | :? BlockExpression as block -> block.Variables.Count = 0
                    | _ -> false)
            if needsExpansion then
                CollectionMutationOps<'T>.ExpandUpdateTransforms transform
            else
                transform

        let relationTransforms = ResizeArray<QueryTranslatorBaseTypes.UpdateManyRelationTransform>()
        let jsonTransforms = ResizeArray<Expression<System.Action<'T>>>()

        for expression in expandedTransforms do
            match QueryTranslatorVisitPost.tryTranslateUpdateManyRelationTransform expression with
            | ValueSome op -> relationTransforms.Add op
            | ValueNone -> jsonTransforms.Add expression

        if hasRelations then
            withTransaction (fun conn ->
                let tx = ensureRelationTx conn

                let selectedRows =
                    if relationTransforms.Count = 0 && jsonTransforms.Count = 0 then
                        Array.empty
                    else
                        selectMutationRows conn filter false

                RelationsGraphAdmission.captureMany tx (selectedRows |> Seq.map (fun row -> row.Id.Value))

                // Owner JSON callbacks precede target-side chain and link writes.
                // Capture physical membership before either pass, and admit their
                // combined final graph only after both passes and callbacks finish.
                let affected = executeJsonUpdateManyByRows conn selectedRows jsonTransforms

                let documentReferenceOp property targetType mutation =
                    match mutation with
                    | QueryTranslatorBaseTypes.DocumentReferenceMutation.SetId id -> RelationsTypes.SetDBRefToId(property, targetType, id)
                    | QueryTranslatorBaseTypes.DocumentReferenceMutation.SetTypedId(idType, id) -> RelationsTypes.SetDBRefToTypedId(property, targetType, idType, id)
                    | QueryTranslatorBaseTypes.DocumentReferenceMutation.Clear -> RelationsTypes.SetDBRefToNone(property, targetType)

                // Partition: B4 chain ops (depth >= 1) vs link-touching apply ops.
                let chainOps = ResizeArray<QueryTranslatorBaseTypes.UpdateManyRelationTransform>()
                let applyOpsRaw = ResizeArray<QueryTranslatorBaseTypes.UpdateManyRelationTransform>()
                for op in relationTransforms do
                    match op with
                    | QueryTranslatorBaseTypes.MutateDocumentRef(hops, _, _, _) when not hops.IsEmpty ->
                        chainOps.Add op
                    | QueryTranslatorBaseTypes.MutateRefChainProperty _
                    | QueryTranslatorBaseTypes.RefChainManyAdd _
                    | QueryTranslatorBaseTypes.RefChainManyRemove _
                    | QueryTranslatorBaseTypes.RefChainManyClear _
                    | QueryTranslatorBaseTypes.MutateFileSystemRef _ ->
                        chainOps.Add op
                    | other -> applyOpsRaw.Add other

                // Chain executor. Resolves per-hop relation descriptors and emits a chain
                // SELECT (joins through link tables hop-by-hop) anchored on selectedRows ids,
                // then wraps as UPDATE / DELETE / INSERT … SELECT depending on the op shape.
                if chainOps.Count > 0 && selectedRows.Length > 0 then
                    let resolveDescriptor (ownerTx: RelationsTypes.RelationTxContext) propertyName =
                        let descriptors = RelationsEntitySync.relationDescriptorByPath ownerTx ownerTx.OwnerType
                        match descriptors.TryGetValue(propertyName) with
                        | true, descriptor -> descriptor
                        | _ ->
                            raise (InvalidOperationException(
                                sprintf "Error: UpdateMany Ref-chain mutation cannot resolve a relation descriptor for hop property '%s' on type '%s'.\nReason: The chain walker emitted a hop whose relation property is not registered as a DBRef/DBRefMany on its declared owner type. This indicates a schema/translator drift.\nFix: Confirm the property is a DBRef on the owner type and that the relation schema for that owner has been initialized."
                                    propertyName ownerTx.OwnerType.FullName))

                    let resolveChain (hops: QueryTranslatorBaseTypes.ChainHopSpec list) =
                        let descriptors = ResizeArray<RelationsTypes.RelationDescriptor>(hops.Length)
                        let mutable ownerTx = tx
                        for hop in hops do
                            let descriptor = resolveDescriptor { ownerTx with OwnerType = hop.OwnerType } hop.RelationPropertyName
                            descriptors.Add(descriptor)
                            ownerTx <- { tx with OwnerTable = descriptor.TargetTable; OwnerType = descriptor.TargetType }
                        descriptors.ToArray(), ownerTx

                    // Build the chain SELECT as a SqlDu SqlSelect tree. Joins through link tables
                    // hop-by-hop; entry side anchored on a parameterized IN-list against
                    // selectedRows ids. Returns (chainSelect, leafProjection) — leafProjection is
                    // the inner Projection record so callers can append a sibling column for
                    // INSERT … SELECT shapes that need (leafTargetId, savedTargetId).
                    let buildChainSqlSelectForOwners (descArr: RelationsTypes.RelationDescriptor array) (vars: Dictionary<string, obj>) ownerOffset ownerCount : SqlSelect * Projection =
                        let sourceColOf (d: RelationsTypes.RelationDescriptor) = if d.OwnerUsesSourceColumn then "SourceId" else "TargetId"
                        let targetColOf (d: RelationsTypes.RelationDescriptor) = if d.OwnerUsesSourceColumn then "TargetId" else "SourceId"
                        let alias (i: int) = "l" + i.ToString(System.Globalization.CultureInfo.InvariantCulture)
                        let leafIdx = descArr.Length - 1

                        let baseSource = BaseTable(descArr.[0].LinkTable, Some (alias 0))

                        let joins =
                            [ for i in 1 .. leafIdx ->
                                let prev = descArr.[i - 1]
                                let cur = descArr.[i]
                                let onExpr =
                                    Binary(
                                        Column(Some (alias i), sourceColOf cur),
                                        Eq,
                                        Column(Some (alias (i - 1)), targetColOf prev))
                                ConditionedJoin(Inner, BaseTable(cur.LinkTable, Some (alias i)), onExpr) ]

                        let entryColumn = Column(Some (alias 0), sourceColOf descArr.[0])
                        let idParams =
                            [ for j in 0 .. ownerCount - 1 ->
                                let key = "muid" + j.ToString(System.Globalization.CultureInfo.InvariantCulture)
                                if not (vars.ContainsKey(key)) then
                                    vars.[key] <- selectedRows.[ownerOffset + j].Id.Value :> obj
                                Parameter key ]
                        let whereExpr =
                            match idParams with
                            | [ single ] -> Binary(entryColumn, Eq, single)
                            | head :: tail -> InList(entryColumn, head, tail)
                            | [] -> Literal(Boolean false)

                        let leafProj =
                            { Alias = None
                              Expr = Column(Some (alias leafIdx), targetColOf descArr.[leafIdx]) }

                        let core =
                            { Source = Some baseSource
                              Joins = joins
                              Projections = Explicit(leafProj, [])
                              Where = Some whereExpr
                              GroupBy = []
                              Having = None
                              OrderBy = []
                              Limit = None
                              Offset = None
                              Distinct = false }

                        { Ctes = []; Body = SingleSelect core }, leafProj

                    let buildChainSqlSelect descriptors vars =
                        buildChainSqlSelectForOwners descriptors vars 0 selectedRows.Length

                    // Emit and execute a SqlDu statement through the canonical executor.
                    //
                    // This corridor used to run the optimizer pipeline here, loading an index model
                    // for every base table the statement referenced and reading it from the relation
                    // transaction's connection rather than the executing one. That is all gone:
                    // measured across the whole Release suite, the pipeline changed no statement
                    // that reached this path, so it was machinery and a second connection with no
                    // observable effect. See StatementExecution for the measurement and for what
                    // reintroducing it would require.
                    let executeSqlDu (stmt: SqlStatement) (vars: Dictionary<string, obj>) =
                        StatementExecution.execute conn stmt vars
                        |> ignore


                    let jsonbSetExpr (jsonPath: string) (paramName: string) =
                        FunctionCall("jsonb_set", [
                            Column(None, "Value")
                            Literal(String jsonPath)
                            FunctionCall("jsonb", [Parameter paramName])
                        ])

                    // Each Single hop yields at most one leaf per selected entry. Keep the
                    // deduplicated IDs in source order before callbacks can mutate membership.
                    let selectChainOwners descriptors =
                        if Array.isEmpty descriptors then selectedRows |> Array.map (fun row -> row.Id.Value)
                        else
                            let ids = ResizeArray<int64>()
                            let seen = HashSet<int64>()
                            for offset in 0 .. 500 .. selectedRows.Length - 1 do
                                let count = min 500 (selectedRows.Length - offset)
                                let variables = Dictionary<string, obj>()
                                let select, _ = buildChainSqlSelectForOwners descriptors variables offset count
                                let mutable statement = conn.OpenStatement(StatementExecution.emit(SelectStmt select))
                                try
                                    statement.BindValues(variables)
                                    while statement.Step() do
                                        let id = statement.Row.GetInt64(0)
                                        if seen.Add(id) then ids.Add(id)
                                    statement.Dispose()
                                with error -> statement.Abort(error); reraise()
                            ids.ToArray()

                    let captureChainOwners descriptors leafTx =
                        let ids = selectChainOwners descriptors
                        RelationsGraphAdmission.captureMany leafTx ids
                        ids

                    for op in chainOps do
                        let hops =
                            match op with
                            | QueryTranslatorBaseTypes.MutateDocumentRef(hops, _, _, _)
                            | QueryTranslatorBaseTypes.MutateFileSystemRef(hops, _, _)
                            | QueryTranslatorBaseTypes.MutateRefChainProperty(hops, _, _, _)
                            | QueryTranslatorBaseTypes.RefChainManyAdd(hops, _, _, _)
                            | QueryTranslatorBaseTypes.RefChainManyRemove(hops, _, _, _)
                            | QueryTranslatorBaseTypes.RefChainManyClear(hops, _, _) -> hops
                            | _ -> failwith "Non-chain op leaked into chainOps partition; partitioning logic bug."
                        let descriptors, leafTx = resolveChain hops
                        match op with
                        | QueryTranslatorBaseTypes.MutateDocumentRef(_, property, targetType, mutation) ->
                            let ownerIds = captureChainOwners descriptors leafTx
                            let writePlan: RelationsTypes.RelationWritePlan = {
                                Kind = RelationsTypes.RelationPlanKind.UpdateMany
                                OwnerType = leafTx.OwnerType
                                Ops = [ documentReferenceOp property targetType mutation ]
                            }
                            for ownerId in ownerIds do RelationsSync.syncUpdate leafTx ownerId writePlan

                        | QueryTranslatorBaseTypes.MutateFileSystemRef(_, property, mutation) ->
                            let ownerIds = selectChainOwners descriptors
                            RelationsCore.ensureSchemaForOwnerType leafTx leafTx.OwnerType
                            RelationsGraphAdmission.captureMany leafTx ownerIds
                            let referenceOp =
                                match mutation with
                                | FileSystemRefMutation.Set id -> RelationsTypes.SetFileSystemRef(property, id)
                                | FileSystemRefMutation.Add id -> RelationsTypes.AddFileSystemRef(property, id)
                                | FileSystemRefMutation.Remove id -> RelationsTypes.RemoveFileSystemRef(property, id)
                                | FileSystemRefMutation.Clear -> RelationsTypes.ClearFileSystemRefs property
                            RelationsCore.withRelationSqliteWrap "sync" "UpdateMany.filesystem" (fun () ->
                                RelationsEntitySync.applyFileSystemReferenceToOwners leafTx ownerIds referenceOp)

                        | QueryTranslatorBaseTypes.MutateRefChainProperty(_, _, leafJsonPath, jsonLiteral) ->
                            let vars = Dictionary<string, obj>()
                            vars.["v"] <- jsonLiteral
                            let chainSelect, _ = buildChainSqlSelect descriptors vars
                            let updateStmt: UpdateStatement = {
                                TableName = leafTx.OwnerTable
                                SetClauses = [ "Value", jsonbSetExpr leafJsonPath "v" ]
                                Where = Some (InSubquery(Column(None, "Id"), chainSelect))
                            }
                            executeSqlDu (UpdateStmt updateStmt) vars

                        | QueryTranslatorBaseTypes.RefChainManyAdd(_, leafManyName, _leafTargetType, savedTargetId) ->
                            let leafDesc = resolveDescriptor leafTx leafManyName
                            captureChainOwners descriptors leafTx |> ignore
                            let leafSourceCol = if leafDesc.OwnerUsesSourceColumn then "SourceId" else "TargetId"
                            let leafTargetCol = if leafDesc.OwnerUsesSourceColumn then "TargetId" else "SourceId"
                            let vars = Dictionary<string, obj>()
                            vars.["sav"] <- savedTargetId :> obj
                            let chainSelect, leafProj = buildChainSqlSelect descriptors vars
                            // Append the @sav projection so chain SELECT yields (leafId, savedId).
                            let augmented =
                                let savProj = { Alias = None; Expr = Parameter "sav" }
                                match chainSelect.Body with
                                | SingleSelect core ->
                                    { chainSelect with
                                        Body = SingleSelect { core with Projections = Explicit(leafProj, [ savProj ]) } }
                                | _ -> chainSelect
                            let insertStmt : InsertStatement = {
                                TableName = leafDesc.LinkTable
                                Columns = [ leafSourceCol; leafTargetCol ]
                                Source = InsertSelect augmented
                                ConflictResolution = OrIgnore
                                Returning = None
                            }
                            executeSqlDu (InsertStmt insertStmt) vars

                        | QueryTranslatorBaseTypes.RefChainManyRemove(_, leafManyName, _leafTargetType, savedTargetId) ->
                            let leafDesc = resolveDescriptor leafTx leafManyName
                            captureChainOwners descriptors leafTx |> ignore
                            let leafSourceCol = if leafDesc.OwnerUsesSourceColumn then "SourceId" else "TargetId"
                            let leafTargetCol = if leafDesc.OwnerUsesSourceColumn then "TargetId" else "SourceId"
                            let vars = Dictionary<string, obj>()
                            vars.["sav"] <- savedTargetId :> obj
                            let chainSelect, _ = buildChainSqlSelect descriptors vars
                            let deleteStmt : DeleteStatement = {
                                TableName = leafDesc.LinkTable
                                Where = Some (
                                    Binary(
                                        InSubquery(Column(None, leafSourceCol), chainSelect),
                                        And,
                                        Binary(Column(None, leafTargetCol), Eq, Parameter "sav")))
                            }
                            executeSqlDu (DeleteStmt deleteStmt) vars

                        | QueryTranslatorBaseTypes.RefChainManyClear(_, leafManyName, _leafTargetType) ->
                            let leafDesc = resolveDescriptor leafTx leafManyName
                            captureChainOwners descriptors leafTx |> ignore
                            let leafSourceCol = if leafDesc.OwnerUsesSourceColumn then "SourceId" else "TargetId"
                            let vars = Dictionary<string, obj>()
                            let chainSelect, _ = buildChainSqlSelect descriptors vars
                            let deleteStmt : DeleteStatement = {
                                TableName = leafDesc.LinkTable
                                Where = Some (InSubquery(Column(None, leafSourceCol), chainSelect))
                            }
                            executeSqlDu (DeleteStmt deleteStmt) vars

                        | _ ->
                            failwith "Non-chain op leaked into chainOps partition; partitioning logic bug."

                if applyOpsRaw.Count > 0 then
                    let mappedOps =
                        applyOpsRaw
                        |> Seq.map (function
                            | QueryTranslatorBaseTypes.MutateDocumentRef([], path, targetType, mutation) -> documentReferenceOp path targetType mutation
                            | QueryTranslatorBaseTypes.AddDBRefMany(path, targetType, targetId) -> RelationsTypes.AddDBRefMany(path, targetType, targetId)
                            | QueryTranslatorBaseTypes.RemoveDBRefMany(path, targetType, targetId) -> RelationsTypes.RemoveDBRefMany(path, targetType, targetId)
                            | QueryTranslatorBaseTypes.ClearDBRefMany(path, targetType) -> RelationsTypes.ClearDBRefMany(path, targetType)
                            | QueryTranslatorBaseTypes.MutateDocumentRef _
                            | QueryTranslatorBaseTypes.MutateRefChainProperty _
                            | QueryTranslatorBaseTypes.RefChainManyAdd _
                            | QueryTranslatorBaseTypes.RefChainManyRemove _
                            | QueryTranslatorBaseTypes.RefChainManyClear _
                            | QueryTranslatorBaseTypes.MutateFileSystemRef _ ->
                                failwith "Chain op leaked into applyOpsRaw partition; partitioning logic bug.")
                        |> Seq.toList

                    for row in selectedRows do
                        let writePlan: RelationsTypes.RelationWritePlan = {
                            Kind = RelationsTypes.RelationPlanKind.UpdateMany
                            OwnerType = typeof<'T>
                            Ops = mappedOps
                        }
                        RelationsSync.syncUpdate tx row.Id.Value writePlan

                RelationsGraphAdmission.validate tx
                if jsonTransforms.Count = 0 then selectedRows.Length else affected
            )
        else
            use conn = getConnection()

            // One dictionary for the statement, filled in the order the SQL reads it: every
            // assignment in source order, then the predicate. The names are observable, so this
            // order is part of the contract rather than an artefact of how the text was built.
            let variables = Dictionary<string, obj>()

            let assignments =
                [ for expression in jsonTransforms do
                    yield! QueryTranslator.translateUpdateAssignments name expression variables ]

            let setValue =
                SqlExpr.FunctionCall("jsonb_set",
                    SqlExpr.Column(None, "Value")
                    :: [ for (path, value) in assignments do yield path; yield value ])

            let where = QueryTranslator.translateWhereExprInto name filter variables |> QueryTranslator.mutationWhere conn name

            let statement =
                UpdateStmt
                    { TableName = name
                      SetClauses = [ ("Value", setValue) ]
                      Where = Some where }

            StatementExecution.execute conn statement variables
