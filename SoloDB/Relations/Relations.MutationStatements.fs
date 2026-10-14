/// Typed relation mutation statements with descriptor-selected identifiers and explicit value roles.
/// SQL uses the canonical emitter; fixed writes bind primitive values through checked native scopes.
module internal SoloDatabase.RelationMutationStatements

open System.Collections.Generic
open SoloDatabase.SqlModel

/// A link row insert. The conflict resolution is the caller's, because plain, ignore and replace
/// are three different persistence semantics on this path and collapsing them would silently change
/// behaviour: a plain insert must still raise on a constraint violation, an ignore must be a no-op,
/// and a replace must displace the existing row.
let internal linkInsert (connection: NativeSqliteConnection) (linkTable: string) (conflict: InsertConflictResolution) (sourceId: int64) (targetId: int64) =
    let stmt =
        InsertStmt {
            TableName = linkTable
            Columns = [ "SourceId"; "TargetId" ]
            Source = InsertValues [ [ SqlExpr.Parameter "sourceId"; SqlExpr.Parameter "targetId" ] ]
            ConflictResolution = conflict
            Returning = None
        }
    let mutable statement = connection.OpenStatement(StatementExecution.emit stmt)
    try
        statement.BindInt64(1, sourceId)
        statement.BindInt64(2, targetId)
        statement.Step() |> ignore
        let affected = statement.RecordsAffected
        statement.Dispose()
        affected
    with error -> statement.Abort(error); reraise()

/// A plain link insert: a constraint violation must surface, not be swallowed.
let internal linkInsertPlain (connection: NativeSqliteConnection) (linkTable: string) (sourceId: int64) (targetId: int64) =
    linkInsert connection linkTable NoConflictResolution sourceId targetId

/// A link insert that is a no-op when the row already exists.
let internal linkInsertIgnoringConflict (connection: NativeSqliteConnection) (linkTable: string) (sourceId: int64) (targetId: int64) =
    linkInsert connection linkTable OrIgnore sourceId targetId

/// A link insert that displaces the existing row.
let internal linkInsertReplacingConflict (connection: NativeSqliteConnection) (linkTable: string) (sourceId: int64) (targetId: int64) =
    linkInsert connection linkTable OrReplace sourceId targetId

let private deleteLinkByColumn (connection: NativeSqliteConnection) linkTable column parameterName id =
    let stmt =
        DeleteStmt {
            TableName = linkTable
            Where = Some (SqlExpr.Binary(SqlExpr.Column(None, column), BinaryOperator.Eq, SqlExpr.Parameter parameterName))
        }
    let mutable statement = connection.OpenStatement(StatementExecution.emit stmt)
    try
        statement.BindInt64(1, id)
        statement.Step() |> ignore
        let affected = statement.RecordsAffected
        statement.Dispose()
        affected
    with error -> statement.Abort(error); reraise()

/// Deletes owner membership using the descriptor-selected physical column.
let internal linkDeleteByOwner (connection: NativeSqliteConnection) (linkTable: string) (ownerColumn: string) (ownerId: int64) =
    deleteLinkByColumn connection linkTable ownerColumn "ownerId" ownerId

/// Deletes target membership while retaining its diagnostic parameter name.
let internal linkDeleteByTarget (connection: NativeSqliteConnection) (linkTable: string) (targetColumn: string) (targetId: int64) =
    deleteLinkByColumn connection linkTable targetColumn "targetId" targetId

/// Delete the one link row addressed by both of its endpoints.
let internal linkDeleteByOwnerAndTarget (connection: NativeSqliteConnection) (linkTable: string) (ownerColumn: string) (ownerId: int64) (targetColumn: string) (targetId: int64) =
    let predicate =
        SqlExpr.Binary(
            SqlExpr.Binary(SqlExpr.Column(None, ownerColumn), BinaryOperator.Eq, SqlExpr.Parameter "ownerId"),
            BinaryOperator.And,
            SqlExpr.Binary(SqlExpr.Column(None, targetColumn), BinaryOperator.Eq, SqlExpr.Parameter "targetId"))
    let stmt = DeleteStmt { TableName = linkTable; Where = Some predicate }
    let mutable statement = connection.OpenStatement(StatementExecution.emit stmt)
    try
        statement.BindInt64(1, ownerId)
        statement.BindInt64(2, targetId)
        statement.Step() |> ignore
        let affected = statement.RecordsAffected
        statement.Dispose()
        affected
    with error -> statement.Abort(error); reraise()

/// Set one DBRef property inside the owner's Value document.
///
/// The written value is JSON spelling — a row id or the null literal — consumed by the json
/// constructor, so it is bound as the exact string it must remain. Applying stored-value encoding
/// here would hand the constructor an encoded value instead of the JSON it expects.
let private ownerValueJsonStatement (ownerTable: string) (propertyPath: string) =
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
    stmt

let internal ownerValueJsonSet ownerTable propertyPath jsonText ownerId =
    let variables = Dictionary<string, obj>()
    variables.["jsonText"] <- box jsonText
    variables.["ownerId"] <- box ownerId
    ownerValueJsonStatement ownerTable propertyPath, variables

let internal setOwnerValueJson (connection: NativeSqliteConnection) ownerTable propertyPath jsonText ownerId =
    let mutable statement = connection.OpenStatement(StatementExecution.emit (ownerValueJsonStatement ownerTable propertyPath))
    try
        statement.BindText(1, jsonText)
        statement.BindInt64(2, ownerId)
        statement.Step() |> ignore
        statement.Dispose()
    with error -> statement.Abort(error); reraise()

/// Fixed matched identities share one input across all statements in a bulk mutation.
/// Compact transport is prepared only when distinct host parameters exceed the live budget.
[<Sealed>]
type internal SelectedOwners(count: int, idAt: int -> int64) =
    let mutable compact: string = null

    member _.Count = count

    member _.Predicate(connection: NativeSqliteConnection, variables: Dictionary<string, obj>, column: SqlExpr, prefix: string, singleEquality: bool) =
        if count = 0 then SqlExpr.Literal(SqlLiteral.Boolean false)
        elif count <= connection.VariableLimit - variables.Count then
            let parameters =
                [ for index in 0 .. count - 1 ->
                    let key = prefix + string index
                    variables.[key] <- box (idAt index)
                    SqlExpr.Parameter key ]
            match parameters with
            | [ single ] when singleEquality -> SqlExpr.Binary(column, BinaryOperator.Eq, single)
            | head :: tail -> SqlExpr.InList(column, head, tail)
            | [] -> SqlExpr.Literal(SqlLiteral.Boolean false)
        else
            if isNull compact then
                compact <- JsonSerializator.JsonValue.WritePhysicalIds(count, idAt)
            variables.[prefix] <- compact
            let core: SelectCore = {
                Source = Some (FromJsonEach(SqlExpr.Parameter prefix, None))
                Joins = []
                Projections = Explicit({ Alias = None; Expr = SqlExpr.Column(None, "value") }, [])
                Where = None; GroupBy = []; Having = None; OrderBy = []
                Limit = None; Offset = None; Distinct = false }
            SqlExpr.InSubquery(column, { Ctes = []; Body = SingleSelect core })

/// A physical owner or a bounded slice of the already matched owner identities.
[<RequireQualifiedAccess>]
type internal OwnerSelection =
    | One of int64
    | Batch of ids: int64 array * offset: int * count: int
    | All

let private bindOwnerSelection selection (variables: Dictionary<string, obj>) alias column =
    let column = SqlExpr.Column(alias, column)
    match selection with
    | OwnerSelection.All -> SqlExpr.Literal(SqlLiteral.Boolean true)
    | OwnerSelection.One id ->
        variables.["ownerId"] <- box id
        SqlExpr.Binary(column, BinaryOperator.Eq, SqlExpr.Parameter "ownerId")
    | OwnerSelection.Batch(ids, offset, count) ->
        let parameters = [for i in 0 .. count - 1 ->
                            let name = "ownerId" + string i
                            variables.[name] <- box ids.[offset + i]
                            SqlExpr.Parameter name]
        match parameters with
        | head :: tail -> SqlExpr.InList(column, head, tail)
        | [] -> SqlExpr.Literal(SqlLiteral.Boolean false)

let private selectCore source projections predicate : SelectCore = {
    Source = Some source; Joins = []; Projections = ProjectionSetOps.ofList projections
    Where = Some predicate; GroupBy = []; Having = None; OrderBy = []
    Limit = None; Offset = None; Distinct = false }

let private wrap core : SqlSelect = { Ctes = []; Body = SingleSelect core }
let private both a b = SqlExpr.Binary(a, BinaryOperator.And, b)
let private equalsParameter alias column parameter = SqlExpr.Binary(SqlExpr.Column(alias, column), BinaryOperator.Eq, SqlExpr.Parameter parameter)
let private filesystemParameters owner property =
    let variables = Dictionary<string, obj>()
    variables.["owner"] <- box owner
    variables.["property"] <- box property
    variables

let private filesystemScope variables selection alias =
    both (equalsParameter alias "OwnerCollection" "owner")
        (both (equalsParameter alias "Property" "property") (bindOwnerSelection selection variables alias "OwnerId"))

/// Idempotence is an indexed membership predicate, never broad conflict suppression.
/// The real target and declaration foreign keys still reject every invalid new link.
let private filesystemLinkInsertCore currentJson owner property kind column targetId selection =
    let variables = filesystemParameters owner property
    variables.["kind"] <- box kind
    if not currentJson then variables.["targetId"] <- box targetId
    let ownerAlias = Some "_fsowner"
    let existingAlias = Some "_fsexisting"
    let target =
        if currentJson then SqlExpr.JsonExtractExpr(ownerAlias, "Value", JsonPathOps.ofList [property])
        else SqlExpr.Parameter "targetId"
    let existingPredicate =
        both (equalsParameter existingAlias "OwnerCollection" "owner")
            (both (equalsParameter existingAlias "Property" "property")
                (both (SqlExpr.Binary(SqlExpr.Column(existingAlias, column), BinaryOperator.Eq, target))
                    (SqlExpr.Binary(SqlExpr.Column(existingAlias, "OwnerId"), BinaryOperator.Eq, SqlExpr.Column(ownerAlias, "Id")))))
    let existing = selectCore (BaseTable("SoloDBFileSystemReference", existingAlias))
                        [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] existingPredicate |> wrap
    let predicate = both (bindOwnerSelection selection variables ownerAlias "Id") (SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists existing))
    let predicate =
        if currentJson then both predicate (both (SqlExpr.Unary(UnaryOperator.IsNotNull, target)) (SqlExpr.Binary(target, BinaryOperator.Ne, SqlExpr.Literal(SqlLiteral.Integer 0L))))
        else predicate
    let source = selectCore (BaseTable(owner, ownerAlias))
                    [for value in [SqlExpr.Parameter "owner"; SqlExpr.Column(ownerAlias, "Id"); SqlExpr.Parameter "property"; SqlExpr.Parameter "kind"; target] -> { Alias = None; Expr = value }]
                    predicate |> wrap
    InsertStmt { TableName = "SoloDBFileSystemReference"; Columns = ["OwnerCollection"; "OwnerId"; "Property"; "RefKind"; column]
                 Source = InsertSelect source; ConflictResolution = NoConflictResolution; Returning = None }, variables

let internal filesystemLinkInsert owner property kind column targetId selection =
    filesystemLinkInsertCore false owner property kind column targetId selection

/// Whole-row writes have already published JSON and callbacks may have changed it.
/// Protect the identity actually stored, without rewriting it from the original object.
let internal filesystemSingleLinkFromJson owner property column selection =
    filesystemLinkInsertCore true owner property "Single" column 0L selection

let internal filesystemLinkDelete owner property column targetId selection =
    let variables = filesystemParameters owner property
    let predicate = filesystemScope variables selection None
    let predicate =
        match targetId with
        | ValueSome id -> variables.["targetId"] <- box id; both predicate (equalsParameter None column "targetId")
        | ValueNone -> predicate
    DeleteStmt { TableName = "SoloDBFileSystemReference"; Where = Some predicate }, variables

let internal ownerCount (connection: NativeSqliteConnection) owner selection =
    let variables = Dictionary<string, obj>()
    let predicate = bindOwnerSelection selection variables None "Id"
    let select = SelectStmt(selectCore (BaseTable(owner, None)) [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }] predicate |> wrap)
    let mutable statement = connection.OpenStatement(StatementExecution.emit select)
    try
        statement.BindValues(variables)
        statement.Step() |> ignore
        let count = statement.Row.GetInt64(0)
        statement.Dispose()
        count
    with error -> statement.Abort(error); reraise()

let private withOwnerSelection selection (statement, variables: Dictionary<string, obj>) =
    variables.Remove("ownerId") |> ignore
    match statement with
    | UpdateStmt update -> UpdateStmt { update with Where = Some(bindOwnerSelection selection variables None "Id") }, variables
    | _ -> invalidOp "An owner selection requires an update statement."

let internal ownerValueJsonSetForSelection owner property json selection =
    ownerValueJsonSet owner property json 0L |> withOwnerSelection selection
