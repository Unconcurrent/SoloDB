namespace SqlDu.Engine.C1.Spec

type InsertConflictResolution =
    | NoConflictResolution
    | OrIgnore
    | OrReplace

type InsertStatement = {
    TableName: string
    Columns: string list
    Values: SqlExpr list list
    ConflictResolution: InsertConflictResolution
    Returning: SqlExpr list option
}

type UpdateStatement = {
    TableName: string
    SetClauses: (string * SqlExpr) list
    Where: SqlExpr option
}

type DeleteStatement = {
    TableName: string
    Where: SqlExpr option
}

type DdlStatement = {
    Sql: string
}

type SqlStatement =
    | SelectStmt of SqlSelect
    | InsertStmt of InsertStatement
    | UpdateStmt of UpdateStatement
    | DeleteStmt of DeleteStatement
    | DdlStmt of DdlStatement
