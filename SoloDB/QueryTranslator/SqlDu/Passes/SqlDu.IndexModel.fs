module internal SoloDatabase.IndexModel

open SoloDatabase.SqlModel
open Microsoft.Data.Sqlite
open System
open System.Collections.Generic
open System.Text
open System.Text.RegularExpressions

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

let private toInt (value: obj) : int =
    Convert.ToInt32(value, Globalization.CultureInfo.InvariantCulture)

let private toBoolFromInt (value: obj) : bool =
    toInt value <> 0

let private readStringOption (reader: SqliteDataReader) (ordinal: int) : string option =
    if reader.IsDBNull ordinal then None
    else Some(reader.GetString ordinal)

let private isTransientMetadataRace (ex: SqliteException) =
    ex.SqliteErrorCode = 1
    && (ex.Message.IndexOf("no such index", StringComparison.OrdinalIgnoreCase) >= 0
        || ex.Message.IndexOf("no such table", StringComparison.OrdinalIgnoreCase) >= 0
        || ex.Message.IndexOf("database schema has changed", StringComparison.OrdinalIgnoreCase) >= 0)

let private readIndexList (connection: SqliteConnection) (tableName: string) : IndexListRow list =
    try
        use cmd = connection.CreateCommand()
        cmd.CommandText <- "SELECT name, \"unique\", partial FROM pragma_index_list(@tableName);"
        cmd.Parameters.AddWithValue("@tableName", tableName) |> ignore
        use reader = cmd.ExecuteReader()
        let rows = ResizeArray<IndexListRow>()
        while reader.Read() do
            rows.Add({
                Name = reader.GetString 0
                IsUnique = toBoolFromInt (reader.GetValue 1)
                IsPartial = toBoolFromInt (reader.GetValue 2)
            })
        rows |> Seq.toList
    with
    | :? ArgumentOutOfRangeException -> []
    | :? SqliteException as ex when isTransientMetadataRace ex -> []

let private readIndexXInfo (connection: SqliteConnection) (indexName: string) : IndexXInfoRow list =
    try
        use cmd = connection.CreateCommand()
        cmd.CommandText <- "SELECT cid, name, \"desc\", coll, key FROM pragma_index_xinfo(@indexName) ORDER BY seqno;"
        cmd.Parameters.AddWithValue("@indexName", indexName) |> ignore
        use reader = cmd.ExecuteReader()
        let rows = ResizeArray<IndexXInfoRow>()
        while reader.Read() do
            rows.Add({
                Cid = toInt (reader.GetValue 0)
                Name = readStringOption reader 1
                Desc = toInt (reader.GetValue 2)
                Collation = readStringOption reader 3
                Key = toInt (reader.GetValue 4)
            })
        rows |> Seq.toList
    with
    | :? ArgumentOutOfRangeException ->
        // Microsoft.Data.Sqlite can surface this during concurrent schema changes while preparing pragma_index_xinfo.
        // Fail closed the same way as "no such index/table": omit unstable index metadata for this snapshot.
        []
    | :? SqliteException as ex when isTransientMetadataRace ex -> []

let private readIndexSql (connection: SqliteConnection) (tableName: string) (indexName: string) : string option =
    try
        use cmd = connection.CreateCommand()
        cmd.CommandText <- "SELECT sql FROM sqlite_master WHERE type = 'index' AND tbl_name = @tableName AND name = @indexName LIMIT 1;"
        cmd.Parameters.AddWithValue("@tableName", tableName) |> ignore
        cmd.Parameters.AddWithValue("@indexName", indexName) |> ignore
        let sql = cmd.ExecuteScalar()
        match sql with
        | :? string as text when not (String.IsNullOrWhiteSpace text) -> Some text
        | _ -> None
    with
    | :? ArgumentOutOfRangeException -> None
    | :? SqliteException as ex when isTransientMetadataRace ex -> None

let private recIsWrappedByOuterParens (s: string) : bool =
    if String.IsNullOrWhiteSpace s then false
    else
        let t = s.Trim()
        if t.Length < 2 || t.[0] <> '(' || t.[t.Length - 1] <> ')' then
            false
        else
            let mutable depth = 0
            let mutable inSingle = false
            let mutable inDouble = false
            let mutable wraps = true
            let mutable i = 0
            while i < t.Length && wraps do
                let c = t.[i]
                if inSingle then
                    if c = '\'' then inSingle <- false
                elif inDouble then
                    if c = '"' then inDouble <- false
                else
                    if c = '\'' then inSingle <- true
                    elif c = '"' then inDouble <- true
                    elif c = '(' then depth <- depth + 1
                    elif c = ')' then
                        depth <- depth - 1
                        if depth = 0 && i <> t.Length - 1 then
                            wraps <- false
                i <- i + 1
            wraps && depth = 0 && not inSingle && not inDouble

let private stripOuterParens (s: string) : string =
    let mutable current = s.Trim()
    let mutable changed = true
    while changed do
        if recIsWrappedByOuterParens current then
            current <- current.Substring(1, current.Length - 2).Trim()
        else
            changed <- false
    current

let private splitTopLevelCommaTerms (s: string) : string list =
    let result = ResizeArray<string>()
    let mutable depth = 0
    let mutable inSingle = false
    let mutable inDouble = false
    let current = StringBuilder()

    let flush () =
        let term = current.ToString().Trim()
        current.Clear() |> ignore
        if term <> "" then result.Add term

    for c in s do
        if inSingle then
            current.Append(c) |> ignore
            if c = '\'' then inSingle <- false
        elif inDouble then
            current.Append(c) |> ignore
            if c = '"' then inDouble <- false
        else
            match c with
            | '\'' ->
                inSingle <- true
                current.Append(c) |> ignore
            | '"' ->
                inDouble <- true
                current.Append(c) |> ignore
            | '(' ->
                depth <- depth + 1
                current.Append(c) |> ignore
            | ')' ->
                depth <- depth - 1
                current.Append(c) |> ignore
            | ',' when depth = 0 ->
                flush ()
            | _ ->
                current.Append(c) |> ignore

    flush ()
    result |> Seq.toList

let private tryExtractIndexTerms (indexSql: string) : string list option =
    let sql = indexSql.Trim()
    let openParen = sql.IndexOf '('
    if openParen < 0 then None
    else
        let mutable depth = 0
        let mutable inSingle = false
        let mutable inDouble = false
        let mutable closeParen = -1
        let mutable i = openParen
        while i < sql.Length && closeParen < 0 do
            let c = sql.[i]
            if inSingle then
                if c = '\'' then inSingle <- false
            elif inDouble then
                if c = '"' then inDouble <- false
            else
                if c = '\'' then inSingle <- true
                elif c = '"' then inDouble <- true
                elif c = '(' then depth <- depth + 1
                elif c = ')' then
                    depth <- depth - 1
                    if depth = 0 then closeParen <- i
            i <- i + 1

        if closeParen < 0 then None
        else
            let inner = sql.Substring(openParen + 1, closeParen - openParen - 1).Trim()
            match splitTopLevelCommaTerms inner with
            | [] -> None
            | terms -> Some terms

let private columnIdentifierRegex = Regex(@"^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled)
let private jsonExtractRegex =
    Regex(
        @"^\s*jsonb_extract\s*\(\s*(?:""Value""|Value)\s*,\s*'(?<path>\$\.[A-Za-z0-9_\.]+)'\s*\)\s*$",
        RegexOptions.Compiled ||| RegexOptions.IgnoreCase)
let private castRegex =
    Regex(
        @"^\s*cast\s*\(\s*(?<inner>.+)\s+as\s+(?<type>[A-Za-z0-9_]+)\s*\)\s*$",
        RegexOptions.Compiled ||| RegexOptions.IgnoreCase ||| RegexOptions.Singleline)

let private unquoteIdentifier (identifier: string) : string =
    let t = identifier.Trim()
    if t.Length >= 2 && ((t.[0] = '"' && t.[t.Length - 1] = '"') || (t.[0] = '`' && t.[t.Length - 1] = '`') || (t.[0] = '[' && t.[t.Length - 1] = ']')) then
        t.Substring(1, t.Length - 2)
    else t

let private tryNormalizeSingleKeyColumnExpr (columnName: string) : SqlExpr option =
    let normalized = unquoteIdentifier columnName
    if columnIdentifierRegex.IsMatch normalized then
        Some(Column(None, normalized))
    else
        None

let private tryNormalizeJsonExtractExpr (expr: string) : SqlExpr option =
    let m = jsonExtractRegex.Match(stripOuterParens expr)
    if not m.Success then None
    else
        let path = m.Groups.["path"].Value
        if not (path.StartsWith "$.") then None
        else
            let segments =
                path.Substring(2).Split('.')
                |> Array.toList
            if segments.IsEmpty || segments |> List.exists String.IsNullOrWhiteSpace then None
            else Some(JsonExtractExpr(None, "Value", JsonPathOps.ofList segments))

let private tryNormalizeCastJsonExtractExpr (expr: string) : SqlExpr option =
    let m = castRegex.Match(stripOuterParens expr)
    if not m.Success then None
    else
        match tryNormalizeJsonExtractExpr (m.Groups.["inner"].Value) with
        | Some inner ->
            let sqlType = m.Groups.["type"].Value.ToUpperInvariant()
            Some(Cast(inner, sqlType))
        | None -> None

let private termDirectionRegex = Regex(@"\s+(?:ASC|DESC)\s*$", RegexOptions.IgnoreCase)
let private termCollationRegex =
    Regex(@"\s+COLLATE\s+(?:""[^""]+""|[A-Za-z_][A-Za-z0-9_]*)\s*$", RegexOptions.IgnoreCase)

let private tryNormalizeIndexExpression (term: string) : SqlExpr option =
    let expression = termCollationRegex.Replace(termDirectionRegex.Replace(term, ""), "")
    match tryNormalizeJsonExtractExpr expression with
    | Some expr -> Some expr
    | None -> tryNormalizeCastJsonExtractExpr expression

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

/// Load index entries for a single table from runtime SQLite metadata.
/// Fail-closed on concurrent schema changes: return empty list on any exception.
/// An omitted index model is always safe (conservative query plan).
let loadTableIndexes (connection: SqliteConnection) (tableName: string) : IndexEntry list =
    try
        readIndexList connection tableName
        |> List.choose (fun row ->
            let xinfo = readIndexXInfo connection row.Name
            let indexSql = readIndexSql connection tableName row.Name
            tryBuildIndexEntry tableName row xinfo indexSql)
    with _ -> []

/// Load an index model for multiple tables from runtime SQLite metadata.
let loadModelForTables (connection: SqliteConnection) (tableNames: string seq) : IndexModel =
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

let private samplePayloadBytes (connection: SqliteConnection) (table: string) =
    try
        let quoted = "\"" + table.Replace("\"", "\"\"") + "\""
        use bounds = connection.CreateCommand()
        bounds.CommandText <- "SELECT MIN(Id) FROM " + quoted
        let first = bounds.ExecuteScalar()
        bounds.CommandText <- "SELECT MAX(Id) FROM " + quoted
        let last = bounds.ExecuteScalar()
        if isNull first || Convert.IsDBNull first || isNull last || Convert.IsDBNull last then None else
        let low, high = Convert.ToDecimal(first), Convert.ToDecimal(last)
        use command = connection.CreateCommand()
        command.CommandText <- "SELECT length(Value) FROM " + quoted + " WHERE Id >= @start ORDER BY Id LIMIT 4;"
        let start = command.Parameters.Add("@start", SqliteType.Integer)
        let mutable bytes, count = 0.0, 0
        // Bounded primary-key seeks sample across the identifier range rather
        // than assuming the oldest or newest documents represent the table.
        for i in 0 .. 7 do
            start.Value <- int64 (low + (high - low) * decimal i / 7M)
            use reader = command.ExecuteReader()
            while reader.Read() do
                if not (reader.IsDBNull 0) then
                    bytes <- bytes + float (reader.GetInt64 0)
                    count <- count + 1
        if count = 0 then None else Some(max 1.0 (bytes / float count))
    with :? SqliteException -> None

/// Read current ANALYZE estimates once at compilation, independently of the
/// cached index definitions. Missing or malformed estimates disable costing.
let loadTableEstimates (connection: SqliteConnection) (tableNames: string seq) : Map<string, TableEstimate> =
    try
        use exists = connection.CreateCommand()
        exists.CommandText <- "SELECT 1 FROM sqlite_master WHERE name = 'sqlite_stat1' AND type = 'table';"
        if isNull (exists.ExecuteScalar()) then Map.empty else
        use command = connection.CreateCommand()
        command.CommandText <- "SELECT stat FROM sqlite_stat1 WHERE tbl = @tableName;"
        let tableParameter = command.Parameters.Add("@tableName", SqliteType.Text)
        tableNames |> Seq.distinct |> Seq.choose (fun table ->
            tableParameter.Value <- table
            use reader = command.ExecuteReader()
            let mutable estimate = 0L
            while reader.Read() do
                if not (reader.IsDBNull 0) then
                    let stat = reader.GetString 0
                    let endOfCount = stat.IndexOf(' ')
                    let first = if endOfCount < 0 then stat else stat.Substring(0, endOfCount)
                    match Int64.TryParse(first, Globalization.NumberStyles.None, Globalization.CultureInfo.InvariantCulture) with
                    | true, count when count > estimate -> estimate <- count
                    | _ -> ()
            if estimate > 0L then Some(table, { Rows = estimate; PayloadBytes = lazy (samplePayloadBytes connection table) }) else None) |> Map.ofSeq
    with :? SqliteException -> Map.empty
