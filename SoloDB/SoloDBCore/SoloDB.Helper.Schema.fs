namespace SoloDatabase

open SoloDatabase.JsonSerializator
open System.Linq.Expressions
open System
open System.Reflection
open Utils
open NativeSQLiteTools
open SoloDatabase
open SoloDatabase.SqlModel

module internal HelperSchema =
    /// Emit a SqlExpr to SQL string.
    /// For index DDL, we need the DU expression emitted without table aliases.
    /// Use the standalone emitter (same as HydrationSqlBuilder.emitExprToSql).
    let private emitExprToSqlLocal (expr: SqlExpr) : string =
        let ctx = EmitContext()
        ctx.InlineLiterals <- true
        let emitted = EmitExpr.emitExprWith EmitSelect.emitSelect ctx expr
        emitted.Sql

    /// Strip source aliases from index operands while retaining opaque subqueries.
    let rec private stripAlias (expr: SqlExpr) : SqlExpr =
        match expr with
        | SqlExpr.InSubquery _ | SqlExpr.Exists _ | SqlExpr.ScalarSubquery _ -> expr
        | _ ->
            match mapChildren stripAlias id expr with
            | SqlExpr.Column(Some _, col) -> SqlExpr.Column(None, col)
            | SqlExpr.JsonExtractExpr(Some _, col, path) -> SqlExpr.JsonExtractExpr(None, col, path)
            | SqlExpr.JsonRootExtract(Some _, col) -> SqlExpr.JsonRootExtract(None, col)
            | value -> value

    let private tryGetTupleExprArgs (expr: SqlExpr) : SqlExpr list option =
        match expr with
        | SqlExpr.FunctionCall("jsonb_array", args) -> Some args
        | SqlExpr.JsonArrayExpr args -> Some args
        | _ -> None
    /// Recursively checks whether an expression tree references a relation type
    /// (DBRefMany member access or DBRef.Value navigation) that cannot produce a
    /// valid SQLite index expression.
    let rec private containsRelationSubExpression (expr: Expression) =
        match expr with
        | :? MemberExpression as me ->
            // DBRefMany.Count / DBRefMany.IsLoaded etc — member on a DBRefMany-typed expression.
            if not (isNull me.Expression) && DBRefTypeHelpers.isDBRefManyType me.Expression.Type then
                true
            // DBRef.Value.Property — member on an expression whose type is DBRef, meaning .Value navigation.
            elif not (isNull me.Expression) && me.Member.Name = "Value" && DBRefTypeHelpers.isDBRefType me.Expression.Type then
                true
            // Allow DBRef.Id and DBRef.HasValue (direct members on DBRef, NOT .Value navigation).
            elif not (isNull me.Expression) && DBRefTypeHelpers.isDBRefType me.Expression.Type then
                false
            // Recurse into inner expression for nested member access chains.
            elif not (isNull me.Expression) then
                containsRelationSubExpression me.Expression
            else false
        | :? MethodCallExpression as mc ->
            // Any()/All() on a DBRefMany source — reject.
            let hasDBRefManyArg =
                (not (isNull mc.Object) && DBRefTypeHelpers.isDBRefManyType mc.Object.Type) ||
                (mc.Arguments.Count > 0 && DBRefTypeHelpers.isDBRefManyType mc.Arguments.[0].Type)
            hasDBRefManyArg
        | :? NewExpression as ne ->
            // Tuple: check all arguments.
            ne.Arguments |> Seq.exists containsRelationSubExpression
        | :? UnaryExpression as ue ->
            containsRelationSubExpression ue.Operand
        | _ -> false

    let internal getIndexWhereAndNameShared<'T, 'R> (name: string) (expression: Expression<System.Func<'T, 'R>>) =
        if isNull expression then raise (ArgumentNullException(nameof(expression)))

        let isDirectIdAccess =
            match expression.Body with
            | :? MemberExpression as me ->
                me.Member.Name = "Id" &&
                match me.Expression with
                | :? ParameterExpression -> true
                | _ -> false
            | _ -> false

        if isDirectIdAccess then raise (ArgumentException "The Id of a collection is always stored in an index.")

        // Expression-tree structural guard: reject relation-derived expressions BEFORE calling translate,
        // because the translator may silently fall through to jsonb_extract for unresolved relation metadata.
        if containsRelationSubExpression expression.Body then
            raise (ArgumentException "Cannot index a relation expression that resolves through link tables (e.g. DBRefMany.Count, DBRef.Value.Property). Only direct column expressions and DBRef.Id are supported.")

        // Use translateWhereExpr → DU → strip aliases → emit.
        // No .Replace/.Substring string surgery.
        let duExpr, variables = QueryTranslator.translateWhereExpr name expression
        if variables.Count > 0 then raise (ArgumentException "Cannot have variables in index.")
        // Strip table-qualification at DU level (not string Replace).
        let stripped = stripAlias duExpr
        if stripped |> SqlExpr.exists (function
            | SqlExpr.InSubquery _ | SqlExpr.Exists _ | SqlExpr.ScalarSubquery _ -> true
            | _ -> false) then
            raise (ArgumentException "Cannot index a relation expression that resolves through link tables (e.g. DBRefMany.Count, DBRef.Value.Property). Only direct column expressions and DBRef.Id are supported.")
        if stripped |> SqlExpr.exists (function
            | SqlExpr.FunctionCall(name, _) -> name = QueryTranslatorSpanSource.name
            | _ -> false) then raise (ArgumentException "Cannot index span membership.")
        let emitted = emitExprToSqlLocal stripped
        let expressionBody = expression.Body

        if QueryTranslatorBaseHelpers.isAnyConstant expressionBody then
            raise (InvalidOperationException
                "Error: Cannot index an outside or constant expression.\nReason: Index expressions must reference the entity parameter.\nFix: Use a member access on the entity parameter.")

        let whereSQL =
            match expressionBody with
            | :? NewExpression as ne when isTuple ne.Type ->
                // Tuple translation may arrive as jsonb_array(...) or JsonArrayExpr.
                // Unwrap to (arg1, arg2, ...) at DU level so CREATE INDEX sees plain expressions.
                match tryGetTupleExprArgs stripped with
                | Some args ->
                    let argsSql = args |> List.map (QueryTranslatorNumeric.nativeExtraction >> emitExprToSqlLocal) |> String.concat ", "
                    $"({argsSql})"
                | _ ->
                    $"({emitted})"
            | :? MethodCallExpression
            | :? MemberExpression ->
                let indexTerm = QueryTranslatorNumeric.nativeExtraction stripped
                $"({emitExprToSqlLocal indexTerm})"
            | other -> raise (ArgumentException (sprintf "Cannot index an expression with type: %s" (other.GetType().FullName)))

        let expressionStr = whereSQL.ToCharArray() |> Seq.filter (fun c -> Char.IsAsciiLetterOrDigit c || c = '_') |> Seq.map string |> String.concat ""
        let indexName = $"{name}_index_{expressionStr}"
        indexName, whereSQL

    let private ensureIndexLocal<'T, 'R> (collectionName: string) (conn: NativeSqliteConnection) (expression: Expression<System.Func<'T, 'R>>) =
        let indexName, whereSQL = getIndexWhereAndNameShared<'T, 'R> collectionName expression
        let indexSQL = $"CREATE INDEX IF NOT EXISTS {indexName} ON \"{collectionName}\"{whereSQL}"
        let mutable statement = conn.OpenStatement(indexSQL)
        try
            statement.Step() |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()

    let private ensureUniqueAndIndexLocal<'T, 'R> (collectionName: string) (conn: NativeSqliteConnection) (expression: Expression<System.Func<'T, 'R>>) =
        let indexName, whereSQL = getIndexWhereAndNameShared<'T, 'R> collectionName expression
        let indexSQL = $"CREATE UNIQUE INDEX IF NOT EXISTS {indexName} ON \"{collectionName}\"{whereSQL}"
        let mutable statement = conn.OpenStatement(indexSQL)
        try
            statement.Step() |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()

    let private indexExpressionName name expression =
        let whereSQL = "(" + emitExprToSqlLocal expression + ")"
        let expressionStr = whereSQL.ToCharArray() |> Seq.filter (fun c -> Char.IsAsciiLetterOrDigit c || c = '_') |> Seq.map string |> String.concat ""
        $"{name}_index_{expressionStr}",whereSQL

    let private matchesDeclaredIndex name expression unique (entry: IndexModel.IndexEntry) =
        entry.TableName = name && entry.IsUnique = unique &&
        match entry.Terms with
        | [term] when term.Direction = Asc && String.Equals(term.Collation,"BINARY",StringComparison.OrdinalIgnoreCase) ->
            ExpressionMatcher.expressionMatchesIndex name expression term.Expression
        | _ -> false

    let private legacyUnsignedIndex name conn valueType property unique =
        if not(QueryTranslatorNumeric.unsigned64 valueType) then None else
        let expression = SqlExpr.JsonExtractExpr(None,"Value",JsonPathOps.ofList [property])
        let indexName,_ = indexExpressionName name expression
        match IndexModel.readIndexForSchema conn name indexName with
        | None -> None
        | Some(Some entry) when matchesDeclaredIndex name expression unique entry -> Some indexName
        | _ -> raise(InvalidOperationException($"The declared numerical index '{indexName}' has an unsupported schema definition."))

    let private ensureStoredIndex name (conn: NativeSqliteConnection) valueType property unique =
        let expression =
            SqlExpr.JsonExtractExpr(None,"Value",JsonPathOps.ofList [property])
            |> StoredValueParameter.declaredExpression valueType
        let indexName,whereSQL = indexExpressionName name expression
        let create (connection: NativeSqliteConnection) =
            let before = IndexModel.readIndexForSchema connection name indexName
            let old = legacyUnsignedIndex name connection valueType property unique
            if SQLiteToolsHandlerFaultState.isInEventHandlerScope connection && (before.IsNone || old.IsSome) then
                raise(InvalidOperationException("The numerical index must be initialized before use in an event callback."))
            if before.IsNone then
                let uniqueSQL = if unique then "UNIQUE " else ""
                let mutable statement = connection.OpenStatement($"CREATE {uniqueSQL}INDEX IF NOT EXISTS {indexName} ON \"{name}\"{whereSQL}")
                try
                    statement.Step() |> ignore
                    statement.Dispose()
                with error -> statement.Abort(error); reraise()
            match IndexModel.readIndexForSchema connection name indexName with
            | Some(Some entry) when matchesDeclaredIndex name expression unique entry -> ()
            | _ -> raise(InvalidOperationException($"The declared numerical index '{indexName}' does not match its required expression."))
            match old with
            | Some oldName ->
                let quotedName = oldName.Replace("\"","\"\"")
                let mutable statement = connection.OpenStatement($"DROP INDEX \"{quotedName}\"")
                try
                    statement.Step() |> ignore
                    statement.Dispose()
                with error -> statement.Abort(error); reraise()
            | None -> ()
            if before.IsNone || old.IsSome then RuntimeIndexModelCache.invalidate conn.ConnectionString name
        if QueryTranslatorNumeric.unsigned64 valueType then
            Connections.NativeTransactional(conn).WithTransaction create
        else
            let uniqueSQL = if unique then "UNIQUE " else ""
            let mutable statement = conn.OpenStatement($"CREATE {uniqueSQL}INDEX IF NOT EXISTS {indexName} ON \"{name}\"{whereSQL}")
            try
                statement.Step() |> ignore
                statement.Dispose()
            with error -> statement.Abort(error); reraise()

    let internal ensureUniqueIndexForIdentity (collectionName: string) (conn: NativeSqliteConnection) (memberValue: CustomIdMember) =
        if memberValue.Name = "Id" then raise (ArgumentException "The Id of a collection is always stored in an index.")
        ensureStoredIndex collectionName conn memberValue.IdType memberValue.Name true

    let internal hasUniqueIndexForIdentity name conn (memberValue: CustomIdMember) =
        let expression =
            SqlExpr.JsonExtractExpr(None,"Value",JsonPathOps.ofList [memberValue.Name])
            |> StoredValueParameter.declaredExpression memberValue.IdType
        let indexName,_ = indexExpressionName name expression
        match IndexModel.readIndexForSchema conn name indexName with
        | Some(Some entry) -> matchesDeclaredIndex name expression true entry
        | _ -> false

    let internal ensureDeclaredIndex (name: string) (conn: NativeSqliteConnection) (allowPrimaryId: bool) (index: DeclaredIndex) =
        if not allowPrimaryId && index.CustomIdentity && index.Name = "Id" then
            raise (ArgumentException "The Id of a collection is always stored in an index.")
        if not (index.CustomIdentity && index.Name = "Id" && typeof<int64>.Equals(index.ValueType)) then
            ensureStoredIndex name conn index.ValueType index.Name index.Unique

    let private usesDeclaredIndexes<'T> () =
        not(typeof<JsonSerializator.JsonValue>.IsAssignableFrom typeof<'T>)

    let internal hasUnsignedIndexes<'T> () =
        usesDeclaredIndexes<'T>() &&
        (IndexMetadata.fields<'T>() |> Array.exists(fun index -> QueryTranslatorNumeric.unsigned64 index.ValueType))

    let internal ensureUnsignedIndexes<'T> name conn =
        if usesDeclaredIndexes<'T>() then
            for index in IndexMetadata.fields<'T>() do
                if QueryTranslatorNumeric.unsigned64 index.ValueType then
                    ensureDeclaredIndex name conn true index

    let private ensureDeclaredIndexesFieldsLocal<'T> (name: string) (conn: NativeSqliteConnection) =
        for index in IndexMetadata.fields<'T>() do ensureDeclaredIndex name conn true index

    let internal getSQLForTriggersForTable (name: string) =
        RelationsSharedSql.getSQLForTriggersForTable name

    /// <summary>
    /// Creates update triggers for a collection table.
    /// </summary>
    /// <param name="name">The name of the collection.</param>
    /// <param name="conn">The active SQLite connection.</param>
    let internal createTriggersForTable (name: string) (conn: NativeSqliteConnection) =
        let mutable triggers = conn.AcquireScript(NativeStatementSource.Text (getSQLForTriggersForTable name), null)
        try
            triggers.Consume() |> ignore
            triggers.Dispose()
        with error -> triggers.Abort(error); reraise()

    /// <summary>
    /// Creates a new table for a collection, including its metadata entry and declared indexes.
    /// </summary>
    let internal createTableInner<'T> (name: string) (conn: NativeSqliteConnection) =
        let mutable statement = conn.OpenStatement(RelationsSharedSql.createCollectionTableSql $"\"{name}\"")
        try
            statement.Step() |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()
        let mutable statement = conn.OpenStatement("INSERT INTO SoloDBCollections(Name) VALUES (@name) ON CONFLICT(Name) DO NOTHING;")
        try
            statement.BindText(1, name)
            statement.Step() |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()
        createTriggersForTable name conn |> ignore

        // Ignore the untyped collections.
        if usesDeclaredIndexes<'T>() then
            ensureDeclaredIndexesFieldsLocal<'T> name conn
