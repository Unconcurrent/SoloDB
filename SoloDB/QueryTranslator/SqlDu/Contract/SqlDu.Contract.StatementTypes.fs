namespace SqlDu.Engine.C1.Spec

type internal InsertConflictResolution =
    | NoConflictResolution
    | OrIgnore
    | OrReplace

type internal InsertSource =
    | InsertValues of SqlExpr list list
    | InsertSelect of SqlSelect

type internal InsertStatement = {
    TableName: string
    Columns: string list
    Source: InsertSource
    ConflictResolution: InsertConflictResolution
    Returning: SqlExpr list option
}

type internal UpdateStatement = {
    TableName: string
    SetClauses: (string * SqlExpr) list
    Where: SqlExpr option
}

type internal DeleteStatement = {
    TableName: string
    Where: SqlExpr option
}

type internal DdlStatement = {
    Sql: string
}

type internal SqlStatement =
    | SelectStmt of SqlSelect
    | InsertStmt of InsertStatement
    | UpdateStmt of UpdateStatement
    | DeleteStmt of DeleteStatement
    | DdlStmt of DdlStatement
