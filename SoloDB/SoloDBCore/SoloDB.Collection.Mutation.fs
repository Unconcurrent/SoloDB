namespace SoloDatabase

open Microsoft.Data.Sqlite
open System
open System.Collections.Generic
open System.Linq.Expressions
open System.Text
open SoloDatabase.Types
open JsonFunctions
open Utils
open SQLiteTools
open SoloDatabase
open SqlDu.Engine.C1.Spec

type private SoloIdWriteScanner(soloIdProp: System.Reflection.PropertyInfo) =
    inherit ExpressionVisitor()
    let setterMethod = soloIdProp.GetSetMethod(true)
    let mutable found = false
    member _.Found = found
    override this.VisitBinary(node: BinaryExpression) =
        if node.NodeType = ExpressionType.Assign then
            match node.Left with
            | :? MemberExpression as me when obj.Equals(me.Member, soloIdProp :> System.Reflection.MemberInfo) ->
                found <- true
            | _ -> ()
        base.VisitBinary(node)
    override this.VisitMethodCall(node: MethodCallExpression) =
        if not (isNull setterMethod) && obj.Equals(node.Method, setterMethod) then
            found <- true
        elif node.Method.Name = "Set" && node.Method.IsStatic && node.Arguments.Count >= 1 then
            // Extensions.Set(<member>, value) — the UpdateMany DSL setter form. Treat as a
            // write to the first-argument member if it resolves to the [<SoloId>] property.
            match node.Arguments.[0] with
            | :? MemberExpression as me when obj.Equals(me.Member, soloIdProp :> System.Reflection.MemberInfo) ->
                found <- true
            | _ -> ()
        base.VisitMethodCall(node)

type internal CollectionMutationOps<'T>() =

    /// UpdateMany [<SoloId>]-write rejection at translate time. Walks each json transform
    /// for assignments to the [<SoloId>] property; raises before any SQL runs. Narrow: rejects
    /// exactly the assignment shape; non-SoloId UpdateMany transforms are unaffected.
    static member private RejectSoloIdWriteInTransforms (transforms: Expression<System.Action<'T>> array) =
        match CustomTypeId<'T>.Value with
        | Some custom ->
            for expression in transforms do
                let scanner = SoloIdWriteScanner(custom.Property)
                scanner.Visit(expression) |> ignore
                if scanner.Found then
                    raise (InvalidOperationException(
                        sprintf "Error: Cannot use UpdateMany to write the [<SoloId>] field '%s.%s'.\nReason: The [<SoloId>] field is the type's identity and may not be mutated through UpdateMany. To re-identify a row, delete and re-insert the entity."
                            typeof<'T>.FullName custom.Property.Name))
        | None -> ()

    /// SELECT-and-check variant for non-relation write paths that don't already load
    /// the existing row. Reads the matched row's SoloId via jsonb_extract on the same
    /// connection + variables and applies the same accept/reject rule as the in-memory
    /// helper below.
    static member private RejectEmptyMutationOfNonEmptySoloIdAgainstStored
            (opName: string)
            (conn: SqliteConnection)
            (collectionName: string)
            (filterSql: string)
            (variables: IDictionary<string, obj>)
            (item: 'T) =
        match CustomTypeId<'T>.Value with
        | Some custom ->
            let newId = custom.GetId(box item)
            let isNewEmpty =
                match custom.Generator with
                | :? SoloDatabase.Attributes.IIdGenerator as g -> g.IsEmpty newId
                | :? SoloDatabase.Attributes.IIdGenerator<'T> as g -> g.IsEmpty newId
                | _ -> false
            if isNewEmpty then
                let soloPath = "$." + custom.Property.Name
                let storedIdSql =
                    sprintf "SELECT jsonb_extract(Value, '%s') FROM \"%s\" WHERE (%s) LIMIT 1"
                        soloPath collectionName filterSql
                let storedId = conn.QueryFirstOrDefault<obj>(storedIdSql, variables)
                if not (isNull storedId) then
                    // The SQLite reader returns numeric values as int64; normalize into
                    // the property's declared CLR type before equality / IsEmpty checks.
                    let normalizedStored =
                        try System.Convert.ChangeType(storedId, custom.Property.PropertyType)
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
        match CustomTypeId<'T>.Value with
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
        match CustomTypeId<'T>.Value with
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
        (withTransaction: (SqliteConnection -> unit) -> unit)
        (ensureRelationTx: SqliteConnection -> Relations.RelationTxContext)
        (mkRelationPathSets: unit -> 'a * 'b)
        (setSerializedItem: IDictionary<string, obj> -> 'T -> unit) =

        if isNull (box item) then raise (ArgumentNullException(nameof(item)))

        // translateWhereExpr returns SqlExpr DU + variables (canonical predicate path).
        let filterExpr, variables =
            if HasTypeId<'T>.Value then
                let id = HasTypeId<'T>.Read item
                QueryTranslator.translateWhereExpr name (ExpressionHelper.get(fun (x: 'T) -> x.Dyn<int64>("Id") = id))
            else
                match CustomTypeId<'T>.Value with
                | Some customId ->
                    let id = customId.GetId (item |> box)
                    let idProp = CustomTypeId<'T>.Value.Value.Property
                    QueryTranslator.translateWhereExpr name (ExpressionHelper.get(fun (x: 'T) -> x.Dyn<obj>(idProp) = id))
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
                    if HasTypeId<'T>.Value then
                        let id = HasTypeId<'T>.Read item
                        manyVars.["_mid0"] <- box id
                        SqlExpr.Binary(SqlExpr.Column(Some "o", "Id"), BinaryOperator.Eq, SqlExpr.Parameter "_mid0")
                    else
                        match CustomTypeId<'T>.Value with
                        | Some customId ->
                            let id = customId.GetId(item |> box)
                            manyVars.["_mid0"] <- id
                            SqlExpr.Binary(
                                SqlExpr.FunctionCall("jsonb_extract",
                                    [SqlExpr.Column(Some "o", "Value"); SqlExpr.Literal(SqlLiteral.String ("$." + customId.Property.Name))]),
                                BinaryOperator.Eq,
                                SqlExpr.Parameter "_mid0")
                        | None -> raise (InvalidOperationException("Update requires int64 Id or custom Id."))
                let sql, manyHydrated =
                    HydrationSqlBuilder.buildManyOnlyHydratedSql conn name typeof<'T> whereExpr manyVars true
                QueryCommandInstrumentation.Increment()
                let oldRow = conn.QueryFirstOrDefault<DbObjectRow>(sql, manyVars)
                if isNull oldRow then
                    raise (KeyNotFoundException "Could not Update any entities with specified Id.")

                let oldOwner = fromSQLite<'T> oldRow
                if manyHydrated && not (isNull oldRow.HydrationJSON) then
                    let hydMap = Dictionary<int64, string>()
                    hydMap.[oldRow.Id.Value] <- oldRow.HydrationJSON
                    HydrationManyPopulator.populateFromHydrationJson typeof<'T> [| (oldRow.Id.Value, box oldOwner) |] hydMap
                CollectionMutationOps<'T>.RejectEmptyMutationOfNonEmptySoloId "Update" oldOwner item
                let writePlan = Relations.prepareUpdate tx oldRow.Id.Value (box oldOwner) (box item)

                setSerializedItem variables item
                let count = conn.Execute ($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE {whereSql}", variables)
                if count <= 0 then
                    raise (KeyNotFoundException "Could not Update any entities with specified Id.")

                Relations.syncUpdate tx oldRow.Id.Value writePlan
            )
        else
            withTransaction (fun conn ->
                CollectionMutationOps<'T>.RejectEmptyMutationOfNonEmptySoloIdAgainstStored "Update" conn name whereSql variables item
                setSerializedItem variables item
                let count = conn.Execute ($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE {whereSql}", variables)
                if count <= 0 then
                    raise (KeyNotFoundException "Could not Update any entities with specified Id."))

    static member DeleteMany
        (filter: Expression<Func<'T, bool>>)
        (name: string)
        (withTransaction: (SqliteConnection -> int) -> int)
        (requiresRelationDeleteHandling: SqliteConnection -> bool)
        (ensureRelationTx: SqliteConnection -> Relations.RelationTxContext)
        (selectMutationRows: SqliteConnection -> Expression<Func<'T, bool>> -> bool -> DbObjectRow array) =

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
                        let deletePlan = Relations.prepareDeleteOwner tx ownerId (box owner)
                        Relations.syncDeleteOwner tx deletePlan

                    let deleteVars = Dictionary<string, obj>(rows.Length)
                    let ids = ResizeArray<string>(rows.Length)
                    for i in 0 .. rows.Length - 1 do
                        let key = $"id{i}"
                        ids.Add("@" + key)
                        deleteVars.[key] <- rows.[i].Id.Value :> obj

                    let idList = String.Join(",", ids)
                    let sql = $"DELETE FROM \"{name}\" WHERE Id IN ({idList})"
                    conn.Execute(sql, deleteVars)
            else
                // translateWhereExpr + emitted WHERE for DeleteMany.
                let filterExpr, variables = QueryTranslator.translateWhereExpr name filter
                let filterSql = HydrationSqlBuilder.emitExprToSql filterExpr
                conn.Execute ($"DELETE FROM \"{name}\" WHERE {filterSql}", variables)
        )

    static member DeleteOne
        (filter: Expression<Func<'T, bool>>)
        (name: string)
        (withTransaction: (SqliteConnection -> int) -> int)
        (requiresRelationDeleteHandling: SqliteConnection -> bool)
        (ensureRelationTx: SqliteConnection -> Relations.RelationTxContext)
        (selectMutationRows: SqliteConnection -> Expression<Func<'T, bool>> -> bool -> DbObjectRow array) =

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
                    let deletePlan = Relations.prepareDeleteOwner tx oldRow.Id.Value (box owner)
                    Relations.syncDeleteOwner tx deletePlan
                    conn.Execute ($"DELETE FROM \"{name}\" WHERE Id = @id", {| id = oldRow.Id.Value |})
            else
                // translateWhereExpr + emitted WHERE for DeleteOne no-rel.
                let filterExpr, variables = QueryTranslator.translateWhereExpr name filter
                let filterSql = HydrationSqlBuilder.emitExprToSql filterExpr
                conn.Execute ($"DELETE FROM \"{name}\" WHERE Id in (SELECT Id FROM \"{name}\" WHERE ({filterSql}) LIMIT 1)", variables)
        )

    static member ReplaceMany
        (item: 'T)
        (filter: Expression<Func<'T, bool>>)
        (name: string)
        (hasRelations: bool)
        (withTransaction: (SqliteConnection -> int) -> int)
        (ensureRelationTx: SqliteConnection -> Relations.RelationTxContext)
        (mkRelationPathSets: unit -> 'a * 'b)
        (setSerializedItem: IDictionary<string, obj> -> 'T -> unit) =

        if isNull (box item) then raise (ArgumentNullException(nameof(item)))
        if isNull filter then raise (ArgumentNullException(nameof(filter)))

        // translateWhereExpr canonical predicate path for ReplaceMany.
        let filterExpr, variables = QueryTranslator.translateWhereExpr name filter
        let filterSql = HydrationSqlBuilder.emitExprToSql filterExpr

        if hasRelations then
            withTransaction (fun conn ->
                let tx = ensureRelationTx conn

                // mutation-prep old-state read with DBRefMany-only hydration.
                let sql, manyHydrated =
                    HydrationSqlBuilder.buildManyOnlyHydratedSqlWithRawWhere conn name typeof<'T> filterSql false
                QueryCommandInstrumentation.Increment()
                let oldRows = conn.Query<DbObjectRow>(sql, variables) |> Seq.toArray
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
                    Relations.syncReplaceMany tx (ownerIds :> seq<_>) (oldOwners :> seq<_>) (box item)
                    setSerializedItem variables item
                    conn.Execute ($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE {filterSql}", variables)
            )
        else
            withTransaction (fun conn ->
                CollectionMutationOps<'T>.RejectEmptyMutationOfNonEmptySoloIdAgainstStored "ReplaceMany" conn name filterSql variables item
                setSerializedItem variables item
                conn.Execute ($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE {filterSql}", variables))

    static member ReplaceOne
        (item: 'T)
        (filter: Expression<Func<'T, bool>>)
        (name: string)
        (hasRelations: bool)
        (withTransaction: (SqliteConnection -> int) -> int)
        (ensureRelationTx: SqliteConnection -> Relations.RelationTxContext)
        (mkRelationPathSets: unit -> 'a * 'b)
        (setSerializedItem: IDictionary<string, obj> -> 'T -> unit) =

        if isNull (box item) then raise (ArgumentNullException(nameof(item)))
        if isNull filter then raise (ArgumentNullException(nameof(filter)))

        // translateWhereExpr canonical predicate path for ReplaceOne.
        let filterExpr, variables = QueryTranslator.translateWhereExpr name filter
        let filterSql = HydrationSqlBuilder.emitExprToSql filterExpr

        if hasRelations then
            withTransaction (fun conn ->
                let tx = ensureRelationTx conn

                let sql, manyHydrated =
                    HydrationSqlBuilder.buildManyOnlyHydratedSqlWithRawWhere conn name typeof<'T> $"({filterSql})" true
                QueryCommandInstrumentation.Increment()
                let oldRow = conn.QueryFirstOrDefault<DbObjectRow>(sql, variables)
                if isNull oldRow then
                    0
                else
                    let oldOwner = fromSQLite<'T> oldRow
                    if manyHydrated && not (isNull oldRow.HydrationJSON) then
                        let hydMap = Dictionary<int64, string>()
                        hydMap.[oldRow.Id.Value] <- oldRow.HydrationJSON
                        HydrationManyPopulator.populateFromHydrationJson typeof<'T> [| (oldRow.Id.Value, box oldOwner) |] hydMap
                    CollectionMutationOps<'T>.RejectEmptyMutationOfNonEmptySoloId "ReplaceOne" oldOwner item
                    Relations.syncReplaceOne tx oldRow.Id.Value (box oldOwner) (box item)
                    setSerializedItem variables item
                    conn.Execute ($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE Id = @id", {| item = variables.["item"]; id = oldRow.Id.Value |})
            )
        else
            withTransaction (fun conn ->
                CollectionMutationOps<'T>.RejectEmptyMutationOfNonEmptySoloIdAgainstStored "ReplaceOne" conn name filterSql variables item
                setSerializedItem variables item
                conn.Execute ($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE Id in (SELECT Id FROM \"{name}\" WHERE ({filterSql}) LIMIT 1)", variables))

    static member UpdateMany
        (transform: Expression<System.Action<'T>> array)
        (filter: Expression<Func<'T, bool>>)
        (name: string)
        (hasRelations: bool)
        (getConnection: unit -> SqliteConnection)
        (withTransaction: (SqliteConnection -> int) -> int)
        (ensureRelationTx: SqliteConnection -> Relations.RelationTxContext)
        (selectMutationRows: SqliteConnection -> Expression<Func<'T, bool>> -> bool -> DbObjectRow array)
        (executeJsonUpdateManyByRows: SqliteConnection -> DbObjectRow array -> ResizeArray<Expression<System.Action<'T>>> -> int) =

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
            [|
                for expr in transform do
                    let lambda = expr :> LambdaExpression
                    match lambda.Body with
                    | :? BlockExpression as block when block.Variables.Count = 0 ->
                        for child in block.Expressions do
                            yield Expression.Lambda<System.Action<'T>>(child, lambda.Parameters)
                    | _ -> yield expr
            |]

        let relationTransforms = ResizeArray<QueryTranslatorBase.UpdateManyRelationTransform>()
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

                // Composition order: target-side chain mutations (and link writes) fire
                // BEFORE owner-side JSON transforms. The chain SELECT anchors on
                // selectedRows ids snapshotted before any mutation, so target-side rewrites
                // see the pre-mutation owner set; the owner JSON pass then writes the
                // owner side last.
                let mutable affected = 0

                // Partition: B4 chain ops (depth >= 1) vs link-touching apply ops.
                let chainOps = ResizeArray<QueryTranslatorBase.UpdateManyRelationTransform>()
                let applyOpsRaw = ResizeArray<QueryTranslatorBase.UpdateManyRelationTransform>()
                for op in relationTransforms do
                    match op with
                    | QueryTranslatorBaseTypes.MutateRefChainProperty _
                    | QueryTranslatorBaseTypes.RefChainManyAdd _
                    | QueryTranslatorBaseTypes.RefChainManyRemove _
                    | QueryTranslatorBaseTypes.RefChainManyClear _ ->
                        chainOps.Add op
                    | other -> applyOpsRaw.Add other

                // Chain executor. Resolves per-hop relation descriptors and emits a chain
                // SELECT (joins through link tables hop-by-hop) anchored on selectedRows ids,
                // then wraps as UPDATE / DELETE / INSERT … SELECT depending on the op shape.
                if chainOps.Count > 0 && selectedRows.Length > 0 then
                    let descriptorCache = Dictionary<Type, RelationsTypes.RelationDescriptor array>()
                    let getDescriptors (ownerType: Type) =
                        match descriptorCache.TryGetValue(ownerType) with
                        | true, d -> d
                        | false, _ ->
                            // buildRelationDescriptors uses tx.OwnerTable for link-table naming,
                            // so a tx bound to the entry-point collection mis-names every
                            // descriptor for non-entry hops. Build a hop-local tx context that
                            // names ownerTable from the hop's owner type instead. This keeps the
                            // SQLite connection, transaction state, and metadata source intact.
                            let hopTx : RelationsTypes.RelationTxContext = {
                                Connection = tx.Connection
                                OwnerTable = Utils.formatName ownerType.Name
                                OwnerType = ownerType
                                InTransaction = tx.InTransaction
                            }
                            let d = RelationsSchemaValidator.buildRelationDescriptors hopTx ownerType
                            descriptorCache.[ownerType] <- d
                            d
                    let resolveHopDescriptor (ownerType: Type) (relationPropertyName: string) : RelationsTypes.RelationDescriptor =
                        let descs = getDescriptors ownerType
                        match descs |> Array.tryFind (fun d -> d.PropertyPath = relationPropertyName) with
                        | Some d -> d
                        | None ->
                            raise (InvalidOperationException(
                                sprintf "Error: UpdateMany Ref-chain mutation cannot resolve a relation descriptor for hop property '%s' on type '%s'.\nReason: The chain walker emitted a hop whose relation property is not registered as a DBRef/DBRefMany on its declared owner type. This indicates a schema/translator drift.\nFix: Confirm the property is a DBRef on the owner type and that the relation schema for that owner has been initialized."
                                    relationPropertyName ownerType.FullName))

                    // Build the chain SELECT as a SqlDu SqlSelect tree. Joins through link tables
                    // hop-by-hop; entry side anchored on a parameterized IN-list against
                    // selectedRows ids. Returns (chainSelect, leafProjection) — leafProjection is
                    // the inner Projection record so callers can append a sibling column for
                    // INSERT … SELECT shapes that need (leafTargetId, savedTargetId).
                    let buildChainSqlSelect (hops: QueryTranslatorBaseTypes.ChainHopSpec list) (vars: Dictionary<string, obj>) : SqlSelect * Projection =
                        let descArr =
                            hops
                            |> List.map (fun h -> resolveHopDescriptor h.OwnerType h.RelationPropertyName)
                            |> List.toArray
                        let sourceColOf (d: RelationsTypes.RelationDescriptor) = if d.OwnerUsesSourceColumn then "SourceId" else "TargetId"
                        let targetColOf (d: RelationsTypes.RelationDescriptor) = if d.OwnerUsesSourceColumn then "TargetId" else "SourceId"
                        let alias i = sprintf "l%d" i
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
                            [ for j in 0 .. selectedRows.Length - 1 ->
                                let key = sprintf "muid%d" j
                                if not (vars.ContainsKey(key)) then
                                    vars.[key] <- selectedRows.[j].Id.Value :> obj
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

                    // Run a SqlDu statement through the same pass pipeline that
                    // Queryable.HelperBase.fs:95-113 uses for SELECTs, then emit and execute.
                    // The pass list mirrors the SELECT-path visitor coverage:
                    //   ConstantFoldPass, FlattenPass, PushdownPass, ProjectionPass,
                    //   CompositeGroupByCanonicalizationPass, IndexPlanShapingPass (with
                    //   IndexModel), JsonbRewritePolicyPass (with IndexModel).
                    // The IndexModel is loaded for every base table referenced by the
                    // statement (chain link tables + leaf target/link table) so chain
                    // SELECT joins through link tables receive index-plan shaping.
                    let executeSqlDu (stmt: SqlStatement) (vars: Dictionary<string, obj>) =
                        let tables = HashSet<string>()
                        let rec collectStmt (s: SqlStatement) =
                            match s with
                            | SelectStmt sel -> collectSelect sel
                            | InsertStmt ins ->
                                tables.Add(ins.TableName) |> ignore
                                match ins.Source with
                                | InsertSelect sel -> collectSelect sel
                                | InsertValues _ -> ()
                            | UpdateStmt upd ->
                                tables.Add(upd.TableName) |> ignore
                                upd.Where |> Option.iter collectExpr
                                for (_, e) in upd.SetClauses do collectExpr e
                            | DeleteStmt del ->
                                tables.Add(del.TableName) |> ignore
                                del.Where |> Option.iter collectExpr
                            | DdlStmt _ -> ()
                        and collectSelect (sel: SqlSelect) =
                            for cte in sel.Ctes do collectSelect cte.Query
                            match sel.Body with
                            | SingleSelect core -> collectCore core
                            | UnionAllSelect(h, t) ->
                                collectCore h
                                for c in t do collectCore c
                        and collectCore (core: SelectCore) =
                            match core.Source with
                            | Some (BaseTable(t, _)) -> tables.Add(t) |> ignore
                            | Some (DerivedTable(inner, _)) -> collectSelect inner
                            | Some (FromJsonEach(e, _)) -> collectExpr e
                            | None -> ()
                            for j in core.Joins do
                                match j with
                                | CrossJoin (BaseTable(t, _)) -> tables.Add(t) |> ignore
                                | CrossJoin (DerivedTable(inner, _)) -> collectSelect inner
                                | CrossJoin (FromJsonEach(e, _)) -> collectExpr e
                                | ConditionedJoin(_, BaseTable(t, _), onExpr) ->
                                    tables.Add(t) |> ignore
                                    collectExpr onExpr
                                | ConditionedJoin(_, DerivedTable(inner, _), onExpr) ->
                                    collectSelect inner
                                    collectExpr onExpr
                                | ConditionedJoin(_, FromJsonEach(e, _), onExpr) ->
                                    collectExpr e
                                    collectExpr onExpr
                            core.Where |> Option.iter collectExpr
                            core.Having |> Option.iter collectExpr
                            for p in (ProjectionSetOps.toList core.Projections) do collectExpr p.Expr
                            for ob in core.OrderBy do collectExpr ob.Expr
                        and collectExpr (expr: SqlExpr) =
                            SqlExpr.fold (fun () node ->
                                match node with
                                | InSubquery(_, sel) -> collectSelect sel
                                | ScalarSubquery sel -> collectSelect sel
                                | Exists sel -> collectSelect sel
                                | _ -> ()) () expr
                        collectStmt stmt

                        let indexModel = SoloDatabase.IndexModel.loadModelForTables tx.Connection (tables :> seq<string>)
                        let passes = [
                            ConstantFoldPass.constantFold
                            FlattenPass.subqueryFlatten
                            PushdownPass.predicatePushdown
                            ProjectionPass.projectionPushdown
                            CompositeGroupByCanonicalizationPass.compositeGroupByCanonicalization
                            IndexPlanShapingPass.indexPlanShaping indexModel
                            JsonbRewritePolicyPass.jsonbRewritePolicy indexModel
                        ]
                        let firstRound = PassRunner.runPipeline passes stmt
                        let pipelineResult = PassRunner.runPipelineToFixedPoint passes firstRound
                        let emitted = EmitStatement.emitStatement (EmitContext(InlineLiterals = true)) pipelineResult.Output
                        SqlCapture.OnSqlEmitted |> Option.iter (fun cb -> cb emitted.Sql)
                        conn.Execute(emitted.Sql, vars) |> ignore

                    let jsonbSetExpr (jsonPath: string) (paramName: string) =
                        FunctionCall("jsonb_set", [
                            Column(None, "Value")
                            Literal(String jsonPath)
                            FunctionCall("jsonb", [Parameter paramName])
                        ])

                    for op in chainOps do
                        match op with
                        | QueryTranslatorBaseTypes.MutateRefChainProperty(hops, _leafTargetType, leafJsonPath, jsonLiteral) ->
                            let vars = Dictionary<string, obj>()
                            vars.["v"] <- jsonLiteral
                            let chainSelect, _ = buildChainSqlSelect hops vars
                            let leafDesc = resolveHopDescriptor (List.last hops).OwnerType (List.last hops).RelationPropertyName
                            let updateStmt : UpdateStatement = {
                                TableName = leafDesc.TargetTable
                                SetClauses = [ "Value", jsonbSetExpr leafJsonPath "v" ]
                                Where = Some (InSubquery(Column(None, "Id"), chainSelect))
                            }
                            executeSqlDu (UpdateStmt updateStmt) vars

                        | QueryTranslatorBaseTypes.RefChainManyAdd(hops, leafManyName, _leafTargetType, savedTargetId) ->
                            let innerOwnerType = (List.last hops).TargetType
                            let leafDesc = resolveHopDescriptor innerOwnerType leafManyName
                            let leafSourceCol = if leafDesc.OwnerUsesSourceColumn then "SourceId" else "TargetId"
                            let leafTargetCol = if leafDesc.OwnerUsesSourceColumn then "TargetId" else "SourceId"
                            let vars = Dictionary<string, obj>()
                            vars.["sav"] <- savedTargetId :> obj
                            let chainSelect, leafProj = buildChainSqlSelect hops vars
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

                        | QueryTranslatorBaseTypes.RefChainManyRemove(hops, leafManyName, _leafTargetType, savedTargetId) ->
                            let innerOwnerType = (List.last hops).TargetType
                            let leafDesc = resolveHopDescriptor innerOwnerType leafManyName
                            let leafSourceCol = if leafDesc.OwnerUsesSourceColumn then "SourceId" else "TargetId"
                            let leafTargetCol = if leafDesc.OwnerUsesSourceColumn then "TargetId" else "SourceId"
                            let vars = Dictionary<string, obj>()
                            vars.["sav"] <- savedTargetId :> obj
                            let chainSelect, _ = buildChainSqlSelect hops vars
                            let deleteStmt : DeleteStatement = {
                                TableName = leafDesc.LinkTable
                                Where = Some (
                                    Binary(
                                        InSubquery(Column(None, leafSourceCol), chainSelect),
                                        And,
                                        Binary(Column(None, leafTargetCol), Eq, Parameter "sav")))
                            }
                            executeSqlDu (DeleteStmt deleteStmt) vars

                        | QueryTranslatorBaseTypes.RefChainManyClear(hops, leafManyName, _leafTargetType) ->
                            let innerOwnerType = (List.last hops).TargetType
                            let leafDesc = resolveHopDescriptor innerOwnerType leafManyName
                            let leafSourceCol = if leafDesc.OwnerUsesSourceColumn then "SourceId" else "TargetId"
                            let vars = Dictionary<string, obj>()
                            let chainSelect, _ = buildChainSqlSelect hops vars
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
                            | QueryTranslatorBaseTypes.SetDBRefToId(path, targetType, targetId) -> RelationsTypes.SetDBRefToId(path, targetType, targetId)
                            | QueryTranslatorBaseTypes.SetDBRefToTypedId(path, targetType, targetIdType, targetTypedId) -> RelationsTypes.SetDBRefToTypedId(path, targetType, targetIdType, targetTypedId)
                            | QueryTranslatorBaseTypes.SetDBRefToNone(path, targetType) -> RelationsTypes.SetDBRefToNone(path, targetType)
                            | QueryTranslatorBaseTypes.AddDBRefMany(path, targetType, targetId) -> RelationsTypes.AddDBRefMany(path, targetType, targetId)
                            | QueryTranslatorBaseTypes.RemoveDBRefMany(path, targetType, targetId) -> RelationsTypes.RemoveDBRefMany(path, targetType, targetId)
                            | QueryTranslatorBaseTypes.ClearDBRefMany(path, targetType) -> RelationsTypes.ClearDBRefMany(path, targetType)
                            | QueryTranslatorBaseTypes.MutateRefChainProperty _
                            | QueryTranslatorBaseTypes.RefChainManyAdd _
                            | QueryTranslatorBaseTypes.RefChainManyRemove _
                            | QueryTranslatorBaseTypes.RefChainManyClear _ ->
                                failwith "Chain op leaked into applyOpsRaw partition; partitioning logic bug.")
                        |> Seq.toList

                    for row in selectedRows do
                        let writePlan: Relations.RelationWritePlan = {
                            Kind = RelationsTypes.RelationPlanKind.UpdateMany
                            OwnerType = typeof<'T>
                            Ops = mappedOps
                        }
                        Relations.syncUpdate tx row.Id.Value writePlan

                    if jsonTransforms.Count = 0 then
                        affected <- selectedRows.Length

                // Owner-side JSON transforms run AFTER the target-side chain mutations and
                // link writes, completing the target-first composition order.
                let jsonAffected = executeJsonUpdateManyByRows conn selectedRows jsonTransforms
                if jsonTransforms.Count > 0 then
                    affected <- jsonAffected

                affected
            )
        else
            use conn = getConnection()
            let variables = Dictionary<string, obj>()
            let fullSQL = StringBuilder()
            let inline append (txt: string) = ignore (fullSQL.Append txt)

            append "UPDATE \""
            append name
            append "\" SET Value = jsonb_set(Value, "

            for expression in jsonTransforms do
                QueryTranslator.translateUpdateMode name expression fullSQL variables

            fullSQL.Remove(fullSQL.Length - 1, 1) |> ignore

            append ")  WHERE "
            QueryTranslator.translateQueryable name filter fullSQL variables

            conn.Execute(fullSQL.ToString(), variables)
