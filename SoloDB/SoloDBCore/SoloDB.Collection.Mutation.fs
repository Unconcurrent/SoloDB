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
        CollectionMutationOps<'T>.ValidateNonEmptySoloId "Update" item

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
                let writePlan = Relations.prepareUpdate tx oldRow.Id.Value (box oldOwner) (box item)

                setSerializedItem variables item
                let count = conn.Execute ($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE {whereSql}", variables)
                if count <= 0 then
                    raise (KeyNotFoundException "Could not Update any entities with specified Id.")

                Relations.syncUpdate tx oldRow.Id.Value writePlan
            )
        else
            withTransaction (fun conn ->
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
        CollectionMutationOps<'T>.ValidateNonEmptySoloId "ReplaceMany" item

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
                    Relations.syncReplaceMany tx (ownerIds :> seq<_>) (oldOwners :> seq<_>) (box item)
                    setSerializedItem variables item
                    conn.Execute ($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE {filterSql}", variables)
            )
        else
            withTransaction (fun conn ->
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
        CollectionMutationOps<'T>.ValidateNonEmptySoloId "ReplaceOne" item

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
                    Relations.syncReplaceOne tx oldRow.Id.Value (box oldOwner) (box item)
                    setSerializedItem variables item
                    conn.Execute ($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE Id = @id", {| item = variables.["item"]; id = oldRow.Id.Value |})
            )
        else
            withTransaction (fun conn ->
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

        let relationTransforms = ResizeArray<QueryTranslatorBase.UpdateManyRelationTransform>()
        let jsonTransforms = ResizeArray<Expression<System.Action<'T>>>()

        for expression in transform do
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

                let mutable affected = executeJsonUpdateManyByRows conn selectedRows jsonTransforms

                // Partition: target-side B4 mutations vs link-touching apply ops.
                let mutateOps = ResizeArray<string * Type * string * string>()
                let applyOpsRaw = ResizeArray<QueryTranslatorBase.UpdateManyRelationTransform>()
                for op in relationTransforms do
                    match op with
                    | QueryTranslatorBaseTypes.MutateDBRefTargetProperty(ownerProp, targetType, jsonPath, jsonLiteral) ->
                        mutateOps.Add (ownerProp, targetType, jsonPath, jsonLiteral)
                    | other -> applyOpsRaw.Add other

                // Target-side first: drive UPDATE on the target collection through the link table,
                // anchored on selected owner ids snapshot. Empty selectedRows → no-op.
                if mutateOps.Count > 0 && selectedRows.Length > 0 then
                    let descriptors = RelationsSchemaValidator.buildRelationDescriptors tx typeof<'T>
                    for (ownerProp, _targetType, jsonPath, jsonLiteral) in mutateOps do
                        let descriptor =
                            match descriptors |> Array.tryFind (fun d -> d.PropertyPath = ownerProp) with
                            | Some d -> d
                            | None ->
                                raise (InvalidOperationException(
                                    sprintf "Error: UpdateMany cannot resolve a relation descriptor for owner property '%s' on type '%s'.\nReason: The translated transform refers to a DBRef property that has no registered relation descriptor for this owner type. This typically indicates a two-hop chain like p.Ref.Value.Other.Value.X where Other is not a relation property of the owner type, or a malformed transform expression.\nFix: Confirm the property is a DBRef on the owner type and use a single-hop p.Ref.Value.Field shape."
                                        ownerProp typeof<'T>.FullName))
                        let qLink = sprintf "\"%s\"" (descriptor.LinkTable.Replace("\"", "\"\""))
                        let qTarget = sprintf "\"%s\"" (descriptor.TargetTable.Replace("\"", "\"\""))
                        let sourceCol, targetCol =
                            if descriptor.OwnerUsesSourceColumn then "SourceId", "TargetId"
                            else "TargetId", "SourceId"
                        let idVars = Dictionary<string, obj>()
                        idVars.["v"] <- jsonLiteral
                        let inListBuilder = StringBuilder()
                        for i in 0 .. selectedRows.Length - 1 do
                            if i > 0 then inListBuilder.Append(",") |> ignore
                            let key = sprintf "muid%d" i
                            inListBuilder.Append("@").Append(key) |> ignore
                            idVars.[key] <- selectedRows.[i].Id.Value :> obj
                        let updateSql =
                            sprintf "UPDATE %s SET Value = jsonb_set(Value, '%s', jsonb(@v)) WHERE Id IN (SELECT %s FROM %s WHERE %s IN (%s));"
                                qTarget jsonPath targetCol qLink sourceCol (inListBuilder.ToString())
                        conn.Execute(updateSql, idVars) |> ignore

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
                            | QueryTranslatorBaseTypes.MutateDBRefTargetProperty _ ->
                                failwith "MutateDBRefTargetProperty already handled in target-side pass")
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
