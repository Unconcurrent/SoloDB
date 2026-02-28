
module internal SoloDatabase.RelationsSharedSql

/// Canonical DDL for the SoloDBTypeCollectionMap table.
/// Used by bootstrap migration (v3->v4) and runtime lazy-init.
let [<Literal>] createTypeCollectionMapTableSql = """
CREATE TABLE IF NOT EXISTS SoloDBTypeCollectionMap (
    TypeKey TEXT NOT NULL,
    CollectionName TEXT NOT NULL,
    UNIQUE(TypeKey, CollectionName)
) STRICT;
"""

/// Canonical column list for collection tables.
/// Used by createTableInner (HelperSchema) and ensureCollectionTableExists (RelationsSchema).
let [<Literal>] collectionTableColumnsSql =
    "Id INTEGER NOT NULL PRIMARY KEY UNIQUE, Value JSONB NOT NULL, Metadata JSONB NOT NULL DEFAULT '{}'"

/// Build a CREATE TABLE statement for a collection table using the canonical column list.
let createCollectionTableSql (qTable: string) =
    $"CREATE TABLE {qTable} ({collectionTableColumnsSql});"

/// Canonical trigger SQL for collection event hooks.
/// Used by HelperSchema.createTriggersForTable and RelationsSchema.ensureCollectionTableExists.
let getSQLForTriggersForTable (name: string) =
    let updateTriggerName = $"SoloDB_Update_{name}"
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

CREATE TRIGGER IF NOT EXISTS "{updateTriggerName}"
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
