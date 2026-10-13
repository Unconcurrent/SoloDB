module internal SoloDatabase.IndexModel

open SoloDatabase
open SoloDatabase.SqlModel
open SoloDatabase.NativeSQLiteTools
open System
open System.Collections.Generic
open SoloDatabase.IndexExpression
open SoloDatabase.Metadata.Generation

// ══════════════════════════════════════════════════════════════
// Index Knowledge Model
//
// Read-only model of available indexes for optimizer decisions.
// Index definitions and optional cardinality estimates come from SQLite.
// Estimates inform costs; they never establish ordering or correctness.
//
// Index expressions are stored as SqlExpr DU nodes to enable
// structural matching against DU tree nodes during optimization.
// ══════════════════════════════════════════════════════════════

/// One declared index key in SQLite's key order.
type IndexTerm = {
    Expression: SqlExpr
    Direction: SortDirection
    Collation: string
}

/// An index and its ordered key terms; the rowid trailer is implicit.
type IndexEntry = {
    TableName: string
    IndexName: string
    Terms: IndexTerm list
    IsUnique: bool
}

/// Read-only index knowledge model.
type IndexModel = {
    Indexes: IndexEntry list
}

/// Empty model (no indexes known).
let emptyModel : IndexModel = { Indexes = [] }

/// Find all indexes for a given table.
let indexesForTable (model: IndexModel) (tableName: string) : IndexEntry list =
    model.Indexes |> List.filter (fun e -> e.TableName = tableName)

/// Check if a table has any indexes in the model.
let hasIndexes (model: IndexModel) (tableName: string) : bool =
    model.Indexes |> List.exists (fun e -> e.TableName = tableName)

type private IndexListRow = {
    Name: string
    IsUnique: bool
    IsPartial: bool
}

type private IndexXInfoRow = {
    Cid: int
    Name: string option
    Desc: int
    Collation: string option
    Key: int
}

type private IndexSqlLookup =
    | MissingIndex
    | ExistingIndex of sql: string option

let private readStringOption (reader: SqlRow) (ordinal: int) : string option =
    if reader.IsDBNull ordinal then None
    else Some(reader.GetString ordinal)

let private isTransientMetadataRace (ex: NativeSqliteException) =
    ex.SqliteErrorCode = 1
    && (ex.Message.IndexOf("no such index", StringComparison.OrdinalIgnoreCase) >= 0
        || ex.Message.IndexOf("no such table", StringComparison.OrdinalIgnoreCase) >= 0
        || ex.Message.IndexOf("database schema has changed", StringComparison.OrdinalIgnoreCase) >= 0)

let private readIndexList strict (connection: NativeSqliteConnection) (tableName: string) : IndexListRow list =
    try
        let mutable statement = connection.OpenStatement("SELECT name, \"unique\", partial FROM pragma_index_list(@tableName);")
        try
            statement.BindText(1, tableName)
            let rows = ResizeArray<IndexListRow>()
            while statement.Step() do
                rows.Add({ Name = statement.Row.GetString 0; IsUnique = statement.Row.GetBoolean 1; IsPartial = statement.Row.GetBoolean 2 })
            statement.Dispose()
            rows |> Seq.toList
        with error -> statement.Abort(error); reraise()
    with
    | :? ArgumentOutOfRangeException when not strict -> []
    | :? NativeSqliteException as ex when not strict && isTransientMetadataRace ex -> []

let private readIndexXInfo strict (connection: NativeSqliteConnection) (indexName: string) : IndexXInfoRow list =
    try
        let mutable statement = connection.OpenStatement("SELECT cid, name, \"desc\", coll, key FROM pragma_index_xinfo(@indexName) ORDER BY seqno;")
        try
            statement.BindText(1, indexName)
            let rows = ResizeArray<IndexXInfoRow>()
            while statement.Step() do
                rows.Add({
                    Cid = statement.Row.GetInt32 0
                    Name = readStringOption statement.Row 1
                    Desc = statement.Row.GetInt32 2
                    Collation = readStringOption statement.Row 3
                    Key = statement.Row.GetInt32 4
                })
            statement.Dispose()
            rows |> Seq.toList
        with error -> statement.Abort(error); reraise()
    with
    | :? NativeSqliteException as ex when not strict && isTransientMetadataRace ex -> []

let private readIndexSql strict (connection: NativeSqliteConnection) (tableName: string) (indexName: string) : IndexSqlLookup =
    try
        let mutable statement = connection.OpenStatement("SELECT sql FROM sqlite_master WHERE type = 'index' AND tbl_name = @tableName AND name = @indexName LIMIT 1;")
        try
            statement.BindText(1, tableName)
            statement.BindText(2, indexName)
            let result =
                if not (statement.Step()) then MissingIndex
                elif statement.Row.IsDBNull 0 then ExistingIndex None
                else
                    let sql = statement.Row.GetString 0
                    ExistingIndex (if String.IsNullOrWhiteSpace sql then None else Some sql)
            statement.Dispose()
            result
        with error -> statement.Abort(error); reraise()
    with
    | :? ArgumentOutOfRangeException when not strict -> MissingIndex
    | :? NativeSqliteException as ex when not strict && isTransientMetadataRace ex -> MissingIndex

let private tryBuildIndexEntry (tableName: string) (row: IndexListRow) (xinfo: IndexXInfoRow list) (indexSql: string option) : IndexEntry option =
    if row.IsPartial then None
    else
        let keyRows = xinfo |> List.filter (fun x -> x.Key = 1)
        let hasAuxiliaryColumns =
            xinfo |> List.exists (fun x -> x.Key = 0 && x.Cid >= 0)
        if keyRows.IsEmpty || hasAuxiliaryColumns then None
        else
            let sqlTerms =
                indexSql |> Option.bind tryExtractIndexTerms |> Option.defaultValue [] |> List.toArray
            let terms =
                keyRows |> List.mapi (fun i keyRow ->
                    let expression =
                        if keyRow.Cid >= 0 then
                            keyRow.Name |> Option.bind tryNormalizeSingleKeyColumnExpr
                        elif keyRow.Cid = -2 && sqlTerms.Length = keyRows.Length then
                            tryNormalizeIndexExpression sqlTerms.[i]
                        else None
                    expression |> Option.map (fun expression -> {
                        Expression = expression
                        Direction = if keyRow.Desc = 0 then Asc else Desc
                        Collation = defaultArg keyRow.Collation "BINARY" }))
            if terms |> List.exists Option.isNone then None
            else Some {
                    TableName = tableName
                    IndexName = row.Name
                    Terms = terms |> List.choose id
                    IsUnique = row.IsUnique
                }

/// Schema mutation distinguishes absence, unsupported definitions and failed reads.
let readIndexForSchema (connection: NativeSqliteConnection) tableName indexName =
    readIndexList true connection tableName
    |> List.tryFind(fun row -> row.Name = indexName)
    |> Option.map(fun row ->
        let xinfo = readIndexXInfo true connection row.Name
        match readIndexSql true connection tableName row.Name with
        | MissingIndex -> raise(InvalidOperationException("The index disappeared while reading its schema definition."))
        | ExistingIndex sql -> tryBuildIndexEntry tableName row xinfo sql)

/// Load index entries for a single table from runtime SQLite metadata.
/// Fail-closed on concurrent schema changes: return empty list on any exception.
/// An omitted index model is always safe (conservative query plan).
let loadTableIndexes (connection: NativeSqliteConnection) (tableName: string) : IndexEntry list =
    try
        readIndexList false connection tableName
        |> List.choose (fun row ->
            let xinfo = readIndexXInfo false connection row.Name
            match readIndexSql false connection tableName row.Name with
            | MissingIndex -> None
            | ExistingIndex indexSql -> tryBuildIndexEntry tableName row xinfo indexSql)
    with _ -> []

/// Load an index model for multiple tables from runtime SQLite metadata.
let loadModelForTables (connection: NativeSqliteConnection) (tableNames: string seq) : IndexModel =
    let indexes =
        tableNames
        |> Seq.distinct
        |> Seq.collect (loadTableIndexes connection)
        |> Seq.toList
    { Indexes = indexes }

/// Cost facts are separate from schema facts. Payload sampling is lazy so
/// ordinary aggregate translation never reads document samples.
type TableEstimate = {
    Rows: int64
    PayloadBytes: Lazy<float option>
}

let private samplePayloadBytes (connection: NativeSqliteConnection) (table: string) =
    try
        let quoted = "\"" + table.Replace("\"", "\"\"") + "\""
        let mutable range = connection.OpenStatement("SELECT (SELECT MIN(Id) FROM " + quoted + "), (SELECT MAX(Id) FROM " + quoted + ")")
        let first, last =
            try
                if not (range.Step()) then Seq.head (Seq.empty<unit>)
                let row = range.Row
                let first = if row.IsDBNull(0) then Nullable() else Nullable(row.GetInt64(0))
                let last = if row.IsDBNull(1) then Nullable() else Nullable(row.GetInt64(1))
                range.Dispose()
                first, last
            with error -> range.Abort(error); reraise()

        if not first.HasValue || not last.HasValue then None else
        let low, high = decimal first.Value, decimal last.Value
        let sql = "SELECT length(Value) FROM " + quoted + " WHERE Id >= @start ORDER BY Id LIMIT 4;"
        let mutable bytes, count = 0.0, 0
        // Bounded primary-key seeks sample across the identifier range rather
        // than assuming the oldest or newest documents represent the table.
        for i in 0 .. 7 do
            let mutable statement = connection.OpenStatement(sql)
            try
                statement.BindInt64(1, int64 (low + (high - low) * decimal i / 7M))
                while statement.Step() do
                    if not (statement.Row.IsDBNull 0) then
                        bytes <- bytes + float (statement.Row.GetInt64 0)
                        count <- count + 1
                statement.Dispose()
            with error -> statement.Abort(error); reraise()
        if count = 0 then None else Some(max 1.0 (bytes / float count))
    with :? NativeSqliteException -> None

/// Read current ANALYZE estimates once at compilation, independently of the
/// cached index definitions. Missing or malformed estimates disable costing.
let loadTableEstimates (connection: NativeSqliteConnection) (tableNames: string seq) : Map<string, TableEstimate> =
    try
        let mutable exists = connection.OpenStatement("SELECT 1 FROM sqlite_master WHERE name = 'sqlite_stat1' AND type = 'table';")
        let hasStatistics =
            try
                let found = exists.Step()
                exists.Dispose()
                found
            with error -> exists.Abort(error); reraise()
        if not hasStatistics then Map.empty else
        let sql = "SELECT stat FROM sqlite_stat1 WHERE tbl = @tableName;"
        tableNames |> Seq.distinct |> Seq.choose (fun table ->
            let mutable statement = connection.OpenStatement(sql)
            try
                statement.BindText(1, table)
                let mutable estimate = 0L
                while statement.Step() do
                    if not (statement.Row.IsDBNull 0) then
                        let stat = statement.Row.GetString 0
                        let endOfCount = stat.IndexOf(' ')
                        let first = if endOfCount < 0 then stat else stat.Substring(0, endOfCount)
                        match Int64.TryParse(first, Globalization.NumberStyles.None, Globalization.CultureInfo.InvariantCulture) with
                        | true, count when count > estimate -> estimate <- count
                        | _ -> ()
                statement.Dispose()
                if estimate > 0L then Some(table, { Rows = estimate; PayloadBytes = lazy (samplePayloadBytes connection table) }) else None
            with error -> statement.Abort(error); reraise()) |> Map.ofSeq
    with :? NativeSqliteException -> Map.empty
