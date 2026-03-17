namespace SoloDatabase

open SoloDatabase.JsonSerializator
open Microsoft.Data.Sqlite
open System.Linq.Expressions
open System
open System.Reflection
open Utils
open SQLiteTools
open SoloDatabase
open SqlDu.Engine.C1.Spec

module internal HelperSchema =
    /// Emit a SqlExpr to SQL string.
    /// For index DDL, we need the DU expression emitted without table aliases.
    /// Use the standalone C2 emitter (same as HydrationSqlBuilder.emitExprToSql).
    let private emitExprToSqlLocal (expr: SqlExpr) : string =
        let ctx = EmitContext()
        ctx.InlineLiterals <- true
        let emitted = EmitExpr.emitExprWith EmitSelect.emitSelect ctx expr
        emitted.Sql

    /// Strip source aliases from SqlExpr (inline — Preprocess not yet compiled).
    let rec private stripAlias (expr: SqlExpr) : SqlExpr =
        match expr with
        | SqlExpr.Column(Some _, col) -> SqlExpr.Column(None, col)
        | SqlExpr.Column(None, _) -> expr
        | SqlExpr.Literal _ | SqlExpr.Parameter _ -> expr
        | SqlExpr.JsonExtractExpr(Some _, col, path) -> SqlExpr.JsonExtractExpr(None, col, path)
        | SqlExpr.JsonExtractExpr(None, _, _) -> expr
        | SqlExpr.JsonRootExtract(Some _, col) -> SqlExpr.JsonRootExtract(None, col)
        | SqlExpr.JsonRootExtract(None, _) -> expr
        | SqlExpr.FunctionCall(name, args) -> SqlExpr.FunctionCall(name, args |> List.map stripAlias)
        | SqlExpr.Unary(op, inner) -> SqlExpr.Unary(op, stripAlias inner)
        | SqlExpr.Binary(l, op, r) -> SqlExpr.Binary(stripAlias l, op, stripAlias r)
        | SqlExpr.Cast(inner, ty) -> SqlExpr.Cast(stripAlias inner, ty)
        | _ -> expr
    let private tryGetIndexedAttribute (p: PropertyInfo) =
        p.GetCustomAttributes(true)
        |> Array.tryPick (function
            | :? SoloDatabase.Attributes.IndexedAttribute as a -> Some a
            | _ -> None)

    let internal getIndexesFieldsShared<'a>() =
        typeof<'a>.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
        |> Array.choose(
            fun p ->
                match tryGetIndexedAttribute p with
                | None -> None
                | Some a -> Some(p, a))

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

        // R45-12 Slice B: use translateWhereExpr → DU → strip aliases → emit.
        // No .Replace/.Substring string surgery.
        let duExpr, variables = QueryTranslator.translateWhereExpr name expression
        if variables.Count > 0 then raise (ArgumentException "Cannot have variables in index.")
        // Strip table-qualification at DU level (not string Replace).
        let stripped = stripAlias duExpr
        // Safety net: reject subqueries.
        let emitted = emitExprToSqlLocal stripped
        if emitted.Contains "SELECT" then
            raise (ArgumentException "Cannot index a relation expression that resolves through link tables (e.g. DBRefMany.Count, DBRef.Value.Property). Only direct column expressions and DBRef.Id are supported.")
        let expressionBody = expression.Body

        if QueryTranslator.isAnyConstant expressionBody then
            raise (InvalidOperationException
                "Error: Cannot index an outside or constant expression.\nReason: Index expressions must reference the entity parameter.\nFix: Use a member access on the entity parameter.")

        let whereSQL =
            match expressionBody with
            | :? NewExpression as ne when isTuple ne.Type ->
                // Tuple: DU is FunctionCall("jsonb_array", [arg1; arg2; ...]).
                // Unwrap to (arg1, arg2, ...) at DU level.
                match stripped with
                | SqlDu.Engine.C1.Spec.SqlExpr.FunctionCall("jsonb_array", args) ->
                    let argsSql = args |> List.map emitExprToSqlLocal |> String.concat ", "
                    $"({argsSql})"
                | _ ->
                    // Fallback: emit as-is with parens (stripped already has no jsonb_array wrapper in some cases).
                    $"({emitted})"
            | :? MethodCallExpression
            | :? MemberExpression ->
                $"({emitted})"
            | other -> raise (ArgumentException (sprintf "Cannot index an expression with type: %s" (other.GetType().FullName)))

        let expressionStr = whereSQL.ToCharArray() |> Seq.filter (fun c -> Char.IsAsciiLetterOrDigit c || c = '_') |> Seq.map string |> String.concat ""
        let indexName = $"{name}_index_{expressionStr}"
        indexName, whereSQL

    let private ensureIndexLocal<'T, 'R> (collectionName: string) (conn: SqliteConnection) (expression: Expression<System.Func<'T, 'R>>) =
        let indexName, whereSQL = getIndexWhereAndNameShared<'T, 'R> collectionName expression
        let indexSQL = $"CREATE INDEX IF NOT EXISTS {indexName} ON \"{collectionName}\"{whereSQL}"
        conn.Execute(indexSQL) |> ignore

    let private ensureUniqueAndIndexLocal<'T, 'R> (collectionName: string) (conn: SqliteConnection) (expression: Expression<System.Func<'T, 'R>>) =
        let indexName, whereSQL = getIndexWhereAndNameShared<'T, 'R> collectionName expression
        let indexSQL = $"CREATE UNIQUE INDEX IF NOT EXISTS {indexName} ON \"{collectionName}\"{whereSQL}"
        conn.Execute(indexSQL) |> ignore

    let private buildPropertyLambda (entityType: Type) (pi: PropertyInfo) =
        let parameter = Expression.Parameter(entityType, "row")
        let body = Expression.Property(parameter, pi)
        Expression.Lambda(body, parameter)

    let internal getIndexWhereAndNameForPropertyShared (name: string) (entityType: Type) (pi: PropertyInfo) =
        let expression = buildPropertyLambda entityType pi

        let isDirectIdAccess =
            match expression.Body with
            | :? MemberExpression as me ->
                me.Member.Name = "Id" &&
                match me.Expression with
                | :? ParameterExpression -> true
                | _ -> false
            | _ -> false

        if isDirectIdAccess then raise (ArgumentException "The Id of a collection is always stored in an index.")

        if containsRelationSubExpression expression.Body then
            raise (ArgumentException "Cannot index a relation expression that resolves through link tables (e.g. DBRefMany.Count, DBRef.Value.Property). Only direct column expressions and DBRef.Id are supported.")

        // R45-12 Slice B: DU-level index expression emission (no string surgery).
        let duExpr, variables = QueryTranslator.translateWhereExpr name expression
        if variables.Count > 0 then raise (ArgumentException "Cannot have variables in index.")
        let stripped = stripAlias duExpr
        let emitted = emitExprToSqlLocal stripped
        if emitted.Contains "SELECT" then
            raise (ArgumentException "Cannot index a relation expression that resolves through link tables (e.g. DBRefMany.Count, DBRef.Value.Property). Only direct column expressions and DBRef.Id are supported.")

        let whereSQL =
            match expression.Body with
            | :? NewExpression as ne when isTuple ne.Type ->
                match stripped with
                | SqlDu.Engine.C1.Spec.SqlExpr.FunctionCall("jsonb_array", args) ->
                    let argsSql = args |> List.map emitExprToSqlLocal |> String.concat ", "
                    $"({argsSql})"
                | _ -> $"({emitted})"
            | :? MethodCallExpression
            | :? MemberExpression ->
                $"({emitted})"
            | other -> raise (ArgumentException (sprintf "Cannot index an expression with type: %s" (other.GetType().FullName)))

        let expressionStr = whereSQL.ToCharArray() |> Seq.filter (fun c -> Char.IsAsciiLetterOrDigit c || c = '_') |> Seq.map string |> String.concat ""
        let indexName = $"{name}_index_{expressionStr}"
        indexName, whereSQL

    let private ensureDeclaredIndexesFieldsLocal<'T> (name: string) (conn: SqliteConnection) =
        for (pi, indexed) in getIndexesFieldsShared<'T>() do
            let isSoloId = not (isNull (pi.GetCustomAttribute<SoloDatabase.Attributes.SoloId>(true)))
            let isGeneratedPrimaryId = isSoloId && pi.Name = "Id"
            let usesPhysicalIdColumn = isGeneratedPrimaryId && pi.PropertyType = typeof<int64>
            if usesPhysicalIdColumn then
                ()
            elif isGeneratedPrimaryId then
                let whereSQL = "(jsonb_extract(Value, '$.Id'))"
                let expressionStr = whereSQL.ToCharArray() |> Seq.filter (fun c -> Char.IsAsciiLetterOrDigit c || c = '_') |> Seq.map string |> String.concat ""
                let indexName = $"{name}_index_{expressionStr}"
                let createSql =
                    if indexed.Unique then
                        $"CREATE UNIQUE INDEX IF NOT EXISTS {indexName} ON \"{name}\"{whereSQL}"
                    else
                        $"CREATE INDEX IF NOT EXISTS {indexName} ON \"{name}\"{whereSQL}"
                conn.Execute(createSql) |> ignore
            elif isSoloId then
                let indexName, whereSQL = getIndexWhereAndNameForPropertyShared name typeof<'T> pi
                let createSql =
                    if indexed.Unique then
                        $"CREATE UNIQUE INDEX IF NOT EXISTS {indexName} ON \"{name}\"{whereSQL}"
                    else
                        $"CREATE INDEX IF NOT EXISTS {indexName} ON \"{name}\"{whereSQL}"
                conn.Execute(createSql) |> ignore
            else
                let ensureIndexesFn = if indexed.Unique then ensureUniqueAndIndexLocal else ensureIndexLocal
                let _code = ensureIndexesFn name conn (ExpressionHelper.get<obj, obj>(fun row -> row.Dyn<obj>(pi.Name)))
                ()

    /// <summary>
    /// Returns the SQL that creates the event triggers for a collection table.
    /// Delegates to the canonical shared SQL module.
    /// </summary>
    /// <param name="name">The name of the collection.</param>
    let internal getSQLForTriggersForTable (name: string) =
        RelationsSharedSql.getSQLForTriggersForTable name

    /// <summary>
    /// Creates update triggers for a collection table.
    /// </summary>
    /// <param name="name">The name of the collection.</param>
    /// <param name="conn">The active SQLite connection.</param>
    let internal createTriggersForTable (name: string) (conn: SqliteConnection) =
        conn.Execute(getSQLForTriggersForTable name) |> ignore

    /// <summary>
    /// Creates a new table for a collection, including its metadata entry and declared indexes.
    /// </summary>
    let internal createTableInner<'T> (name: string) (conn: SqliteConnection) =
        conn.Execute(RelationsSharedSql.createCollectionTableSql $"\"{name}\"") |> ignore
        conn.Execute("INSERT INTO SoloDBCollections(Name) VALUES (@name) ON CONFLICT(Name) DO NOTHING;", {|name = name|}) |> ignore
        createTriggersForTable name conn |> ignore

        // Ignore the untyped collections.
        if not (typeof<JsonSerializator.JsonValue>.IsAssignableFrom typeof<'T>) then
            ensureDeclaredIndexesFieldsLocal<'T> name conn
