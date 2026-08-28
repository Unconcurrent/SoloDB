/// Typed statement ownership for relation mutations.
///
/// Relation mutation SQL used to be assembled as interpolated text at each write site, which left
/// it outside the typed statement contract: invisible to the optimizer, invisible to the emission
/// boundary, and parameterised only by the convention each site happened to follow. This module is
/// the single owner of those statements. Every relation write builds a `SqlStatement` here and
/// executes it through the one statement executor.
///
/// Two things SQLite decides for us, and they are decided here rather than argued at each site:
///
/// 1. An identifier cannot be bound. Table and column names are therefore statement fields, not
///    parameters, and they reach SQL through the emitter's identifier quoting rather than through
///    any local quoting helper.
/// 2. A value's binding depends on the role SQLite gives it, not on where it came from. A row id is
///    an integer operand and is bound as the exact integer. A JSON path is a control string the json
///    functions consume, so it is carried as path structure and emitted as a path token. Raw JSON
///    spelling handed to the json constructor is bound as the exact string, because encoding it as a
///    stored value would change what that function receives.
module internal SoloDatabase.RelationMutationStatements

open System.Collections.Generic
open Microsoft.Data.Sqlite
open SqlDu.Engine.C1.Spec

/// Execute a relation mutation statement, returning the affected row count.
///
/// The transaction's connection is passed as both executor authorities. These statements carry no
/// subquery, so they select canonical emission and should read no index model at all; passing one
/// connection for execution and a different one for inspection would encode a second connection
/// assumption into a transactional relation write, which this path must not carry.
let internal execute (connection: SqliteConnection) (stmt: SqlStatement) (variables: Dictionary<string, obj>) : int =
    StatementExecution.execute connection connection (StatementExecution.policyFor stmt) stmt variables

/// A link row insert. The conflict resolution is the caller's, because plain, ignore and replace
/// are three different persistence semantics on this path and collapsing them would silently change
/// behaviour: a plain insert must still raise on a constraint violation, an ignore must be a no-op,
/// and a replace must displace the existing row.
let internal linkInsert (linkTable: string) (conflict: InsertConflictResolution) (sourceId: int64) (targetId: int64) =
    let variables = Dictionary<string, obj>()
    variables.["sourceId"] <- box sourceId
    variables.["targetId"] <- box targetId
    let stmt =
        InsertStmt {
            TableName = linkTable
            Columns = [ "SourceId"; "TargetId" ]
            Source = InsertValues [ [ SqlExpr.Parameter "sourceId"; SqlExpr.Parameter "targetId" ] ]
            ConflictResolution = conflict
            Returning = None
        }
    stmt, variables

/// A plain link insert: a constraint violation must surface, not be swallowed.
let internal linkInsertPlain (linkTable: string) (sourceId: int64) (targetId: int64) =
    linkInsert linkTable NoConflictResolution sourceId targetId

/// A link insert that is a no-op when the row already exists.
let internal linkInsertIgnoringConflict (linkTable: string) (sourceId: int64) (targetId: int64) =
    linkInsert linkTable OrIgnore sourceId targetId

/// A link insert that displaces the existing row.
let internal linkInsertReplacingConflict (linkTable: string) (sourceId: int64) (targetId: int64) =
    linkInsert linkTable OrReplace sourceId targetId

/// Delete every link row whose owner-side column holds this id. The column is descriptor-selected,
/// because a mutually-many relation stores one of its two sides through the target column.
let internal linkDeleteByOwner (linkTable: string) (ownerColumn: string) (ownerId: int64) =
    let variables = Dictionary<string, obj>()
    variables.["ownerId"] <- box ownerId
    let stmt =
        DeleteStmt {
            TableName = linkTable
            Where = Some (SqlExpr.Binary(SqlExpr.Column(None, ownerColumn), BinaryOperator.Eq, SqlExpr.Parameter "ownerId"))
        }
    stmt, variables

/// Delete every link row whose target-side column holds this id.
///
/// This is a separate builder rather than a reuse of the owner-side one: the value bound here is a
/// target id, and binding it under an owner name would make the emitted parameter say something the
/// value is not. The role a parameter plays is the name it carries.
let internal linkDeleteByTarget (linkTable: string) (targetColumn: string) (targetId: int64) =
    let variables = Dictionary<string, obj>()
    variables.["targetId"] <- box targetId
    let stmt =
        DeleteStmt {
            TableName = linkTable
            Where = Some (SqlExpr.Binary(SqlExpr.Column(None, targetColumn), BinaryOperator.Eq, SqlExpr.Parameter "targetId"))
        }
    stmt, variables

/// Delete the one link row addressed by both of its endpoints.
let internal linkDeleteByOwnerAndTarget (linkTable: string) (ownerColumn: string) (ownerId: int64) (targetColumn: string) (targetId: int64) =
    let variables = Dictionary<string, obj>()
    variables.["ownerId"] <- box ownerId
    variables.["targetId"] <- box targetId
    let predicate =
        SqlExpr.Binary(
            SqlExpr.Binary(SqlExpr.Column(None, ownerColumn), BinaryOperator.Eq, SqlExpr.Parameter "ownerId"),
            BinaryOperator.And,
            SqlExpr.Binary(SqlExpr.Column(None, targetColumn), BinaryOperator.Eq, SqlExpr.Parameter "targetId"))
    let stmt = DeleteStmt { TableName = linkTable; Where = Some predicate }
    stmt, variables

/// Set one DBRef property inside the owner's Value document.
///
/// The written value is JSON spelling — a row id or the null literal — consumed by the json
/// constructor, so it is bound as the exact string it must remain. Applying stored-value encoding
/// here would hand the constructor an encoded value instead of the JSON it expects.
let internal ownerValueJsonSet (ownerTable: string) (propertyPath: string) (jsonText: string) (ownerId: int64) =
    let variables = Dictionary<string, obj>()
    variables.["jsonText"] <- box jsonText
    variables.["ownerId"] <- box ownerId
    let assignment =
        SqlExpr.JsonSetExpr(
            SqlExpr.Column(None, "Value"),
            [ JsonPathOps.ofList [ propertyPath ], SqlExpr.FunctionCall("jsonb", [ SqlExpr.Parameter "jsonText" ]) ])
    let stmt =
        UpdateStmt {
            TableName = ownerTable
            SetClauses = [ "Value", assignment ]
            Where = Some (SqlExpr.Binary(SqlExpr.Column(None, "Id"), BinaryOperator.Eq, SqlExpr.Parameter "ownerId"))
        }
    stmt, variables

/// The relation-version path inside the owner's Metadata document.
let [<Literal>] internal relationVersionSegment = "RelationVersion"

/// Increment the owner's relation version, creating the Metadata document and the version member if
/// either is absent.
///
/// The increment is cast to text and reparsed by the json constructor so the member stays a JSON
/// number rather than becoming a stored double; that is the shape the previous release wrote and it
/// is preserved exactly.
let internal ownerRelationVersionIncrement (ownerTable: string) (ownerId: int64) =
    let variables = Dictionary<string, obj>()
    variables.["ownerId"] <- box ownerId
    let path = JsonPathOps.ofList [ relationVersionSegment ]
    let currentVersion =
        SqlExpr.Coalesce(
            SqlExpr.JsonExtractExpr(None, "Metadata", path),
            [ SqlExpr.Literal(SqlLiteral.Integer 0L) ])
    let incremented =
        SqlExpr.FunctionCall(
            "jsonb",
            [ SqlExpr.Cast(
                SqlExpr.Binary(currentVersion, BinaryOperator.Add, SqlExpr.Literal(SqlLiteral.Integer 1L)),
                "TEXT") ])
    let emptyDocument = SqlExpr.FunctionCall("jsonb", [ SqlExpr.Literal(SqlLiteral.String "{}") ])
    let assignment =
        SqlExpr.JsonSetExpr(
            SqlExpr.Coalesce(SqlExpr.Column(None, "Metadata"), [ emptyDocument ]),
            [ path, incremented ])
    let stmt =
        UpdateStmt {
            TableName = ownerTable
            SetClauses = [ "Metadata", assignment ]
            Where = Some (SqlExpr.Binary(SqlExpr.Column(None, "Id"), BinaryOperator.Eq, SqlExpr.Parameter "ownerId"))
        }
    stmt, variables
