namespace SoloDatabase

/// <summary>
/// Represents the storage location of a SoloDB database.
/// </summary>
[<Struct>]
type internal SoloDBLocation =
/// <summary>The database is stored in a physical file.</summary>
| File of filePath: string
/// <summary>The database is stored in-memory.</summary>
| Memory of name: string
/// <summary>The database uses an explicitly owned storage input.</summary>
| Storage of input: SqliteStorage
