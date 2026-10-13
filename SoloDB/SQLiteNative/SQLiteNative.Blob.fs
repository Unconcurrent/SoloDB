namespace SoloDatabase

/// Explicit identity opens the same native incremental stream used for selected BLOBs.
[<Sealed>]
type NativeSqliteBlob(connection: NativeSqliteConnection, databaseName: string, tableName: string, columnName: string, rowid: int64, readOnly: bool) =
    inherit NativeBlobStream(NativeBlobAccess.openBlob connection.DatabaseOwner databaseName tableName columnName rowid readOnly)
    new(connection, tableName, columnName, rowid, readOnly) = new NativeSqliteBlob(connection, "main", tableName, columnName, rowid, readOnly)
    new(connection, tableName, columnName, rowid) = new NativeSqliteBlob(connection, "main", tableName, columnName, rowid, false)
