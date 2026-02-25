namespace SoloDatabase

open SoloDatabase.JsonSerializator
open Microsoft.Data.Sqlite
open System.Linq.Expressions
open System
open System.Reflection
open Utils
open SQLiteTools

module internal HelperSchema =
    let private getIndexesFieldsLocal<'a>() =
        typeof<'a>.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
        |> Array.choose(
            fun p ->
                match p.GetCustomAttribute<SoloDatabase.Attributes.IndexedAttribute>(true) with
                | a when isNull a -> None
                | a -> Some(p, a))

    let private getIndexWhereAndNameLocal<'T, 'R> (name: string) (expression: Expression<System.Func<'T, 'R>>) =
        if isNull expression then raise (ArgumentNullException(nameof(expression)))

        let whereSQL, variables = QueryTranslator.translate name expression
        let whereSQL = whereSQL.Replace($"\"{name}\".Value", "Value")
        if whereSQL.Contains $"\"{name}\".Id" then raise (ArgumentException "The Id of a collection is always stored in an index.")
        if variables.Count > 0 then raise (ArgumentException "Cannot have variables in index.")
        let expressionBody = expression.Body

        if QueryTranslator.isAnyConstant expressionBody then
            raise (InvalidOperationException
                "Error: Cannot index an outside or constant expression.\nReason: Index expressions must reference the entity parameter.\nFix: Use a member access on the entity parameter.")

        let whereSQL =
            match expressionBody with
            | :? NewExpression as ne when isTuple ne.Type ->
                whereSQL.Substring("json_array".Length)
            | :? MethodCallExpression
            | :? MemberExpression ->
                $"({whereSQL})"
            | other -> raise (ArgumentException (sprintf "Cannot index an expression with type: %s" (other.GetType().FullName)))

        let expressionStr = whereSQL.ToCharArray() |> Seq.filter (fun c -> Char.IsAsciiLetterOrDigit c || c = '_') |> Seq.map string |> String.concat ""
        let indexName = $"{name}_index_{expressionStr}"
        indexName, whereSQL

    let private ensureIndexLocal<'T, 'R> (collectionName: string) (conn: SqliteConnection) (expression: Expression<System.Func<'T, 'R>>) =
        let indexName, whereSQL = getIndexWhereAndNameLocal<'T, 'R> collectionName expression
        let indexSQL = $"CREATE INDEX IF NOT EXISTS {indexName} ON \"{collectionName}\"{whereSQL}"
        conn.Execute(indexSQL) |> ignore

    let private ensureUniqueAndIndexLocal<'T, 'R> (collectionName: string) (conn: SqliteConnection) (expression: Expression<System.Func<'T, 'R>>) =
        let indexName, whereSQL = getIndexWhereAndNameLocal<'T, 'R> collectionName expression
        let indexSQL = $"CREATE UNIQUE INDEX IF NOT EXISTS {indexName} ON \"{collectionName}\"{whereSQL}"
        conn.Execute(indexSQL) |> ignore

    let private ensureDeclaredIndexesFieldsLocal<'T> (name: string) (conn: SqliteConnection) =
        for (pi, indexed) in getIndexesFieldsLocal<'T>() do
            let ensureIndexesFn = if indexed.Unique then ensureUniqueAndIndexLocal else ensureIndexLocal
            let _code = ensureIndexesFn name conn (ExpressionHelper.get<obj, obj>(fun row -> row.Dyn<obj>(pi.Name)))
            ()

    /// <summary>
    /// Returns the SQL that creates the event triggers for a collection table.
    /// </summary>
    /// <param name="name">The name of the collection.</param>
    let internal getSQLForTriggersForTable (name: string) =
        let triggerName = $"SoloDB_Update_{name}"
        let insertTriggerName = $"SoloDB_Insert_{name}"
        let deleteTriggerName = $"SoloDB_Delete_{name}"
        let updatedTriggerName = $"SoloDB_Updated_{name}"
        let insertedTriggerName = $"SoloDB_Inserted_{name}"
        let deletedTriggerName = $"SoloDB_Deleted_{name}"
        $"""
            CREATE TRIGGER IF NOT EXISTS "{insertTriggerName}"
            BEFORE INSERT ON "{name}"
            FOR EACH ROW
            WHEN SHOULD_HANDLE_INSERTING('{name}') = 1
            BEGIN
                SELECT CASE
                    WHEN message IS NULL THEN NULL
                    ELSE RAISE(ABORT, message)
                END
                FROM (
                    SELECT ON_INSERTING_HANDLER('{name}', json(NEW.Value)) AS message
                );
            END;

            CREATE TRIGGER IF NOT EXISTS "{triggerName}"
            BEFORE UPDATE ON "{name}"
            FOR EACH ROW
            WHEN SHOULD_HANDLE_UPDATING('{name}') = 1
            BEGIN
                SELECT CASE
                    WHEN message IS NULL THEN NULL
                    ELSE RAISE(ABORT, message)
                END
                FROM (
                    SELECT ON_UPDATING_HANDLER('{name}', json(OLD.Value), json(NEW.Value)) AS message
                );
            END;

            CREATE TRIGGER IF NOT EXISTS "{deleteTriggerName}"
            BEFORE DELETE ON "{name}"
            FOR EACH ROW
            WHEN SHOULD_HANDLE_DELETING('{name}') = 1
            BEGIN
                SELECT CASE
                    WHEN message IS NULL THEN NULL
                    ELSE RAISE(ABORT, message)
                END
                FROM (
                    SELECT ON_DELETING_HANDLER('{name}', json(OLD.Value)) AS message
                );
            END;

            CREATE TRIGGER IF NOT EXISTS "{insertedTriggerName}"
            AFTER INSERT ON "{name}"
            FOR EACH ROW
            WHEN SHOULD_HANDLE_INSERTED('{name}') = 1
            BEGIN
                SELECT CASE
                    WHEN message IS NULL THEN NULL
                    ELSE RAISE(ABORT, message)
                END
                FROM (
                    SELECT ON_INSERTED_HANDLER('{name}', json(NEW.Value)) AS message
                );
            END;

            CREATE TRIGGER IF NOT EXISTS "{updatedTriggerName}"
            AFTER UPDATE ON "{name}"
            FOR EACH ROW
            WHEN SHOULD_HANDLE_UPDATED('{name}') = 1
            BEGIN
                SELECT CASE
                    WHEN message IS NULL THEN NULL
                    ELSE RAISE(ABORT, message)
                END
                FROM (
                    SELECT ON_UPDATED_HANDLER('{name}', json(OLD.Value), json(NEW.Value)) AS message
                );
            END;

            CREATE TRIGGER IF NOT EXISTS "{deletedTriggerName}"
            AFTER DELETE ON "{name}"
            FOR EACH ROW
            WHEN SHOULD_HANDLE_DELETED('{name}') = 1
            BEGIN
                SELECT CASE
                    WHEN message IS NULL THEN NULL
                    ELSE RAISE(ABORT, message)
                END
                FROM (
                    SELECT ON_DELETED_HANDLER('{name}', json(OLD.Value)) AS message
                );
            END;
        """

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
        conn.Execute($"CREATE TABLE \"{name}\" (
                        Id INTEGER NOT NULL PRIMARY KEY UNIQUE,
                        Value JSONB NOT NULL
                    );") |> ignore
        conn.Execute("INSERT INTO SoloDBCollections(Name) VALUES (@name);", {|name = name|}) |> ignore
        createTriggersForTable name conn |> ignore

        // Ignore the untyped collections.
        if not (typeof<JsonSerializator.JsonValue>.IsAssignableFrom typeof<'T>) then
            ensureDeclaredIndexesFieldsLocal<'T> name conn
