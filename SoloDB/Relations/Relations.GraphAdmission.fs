module internal SoloDatabase.RelationsGraphAdmission

open System
open System.Collections.Generic
open SoloDatabase
open SoloDatabase.Utils
open NativeSQLiteTools
open RelationsTypes
open RelationsSchemaBuilder
open RelationsEntitySync

let private isTerminal table = table = "SoloDBFileHeader" || table = "SoloDBDirectoryHeader"

type private Direction = Outgoing | Incoming

[<Struct>]
type private LinkLayout = {
    Metadata: RelationMetadataRow
    LinkTable: string
    FromColumn: string
    ToColumn: string
}

type private Layouts = Dictionary<struct(Direction * string), LinkLayout array>

let private loader (connection: NativeSqliteConnection) (layouts: Layouts) =
    let metadata direction table =
        let key = struct(direction, table)
        match layouts.TryGetValue key with
        | true, rows -> rows
        | _ ->
            let rows =
                match direction with
                | Incoming -> readMetadataByTarget connection table
                | Outgoing -> readAllMetadataByOwner connection table
            let resolved =
                rows |> Array.map (fun row ->
                    if isTerminal row.TargetCollection then
                        { Metadata = row; LinkTable = ""; FromColumn = ""; ToColumn = "" }
                    else
                        let link, owner, target = metadataLinkLayout connection row (stringToRelationKind row.RefKind)
                        let fromColumn, toColumn =
                            match direction with
                            | Incoming -> target, owner
                            | Outgoing -> owner, target
                        { Metadata = row; LinkTable = link; FromColumn = fromColumn; ToColumn = toColumn })
            layouts.Add(key, resolved)
            resolved
    let load (cache: Dictionary<struct(string * int64), HashSet<struct(string * int64)>>) direction (nodes: HashSet<struct(string * int64)>) =
        let groups = Dictionary<string, ResizeArray<int64>>(StringComparer.Ordinal)
        for (struct(table, id) as node) in nodes do
            if not (cache.ContainsKey node) then
                cache.Add(node, HashSet<struct(string * int64)>())
                if not (isTerminal table) then
                    match groups.TryGetValue table with
                    | true, ids -> ids.Add id
                    | _ -> let ids = ResizeArray<int64>() in ids.Add id; groups.Add(table, ids)
        for KeyValue(table, ids) in groups do
            for layout in metadata direction table do
                let row = layout.Metadata
                let terminal = isTerminal row.TargetCollection
                if not (direction = Incoming && terminal) then
                    let targetTable =
                        match direction with
                        | Incoming -> row.OwnerCollection
                        | Outgoing -> row.TargetCollection
                    for offset in 0 .. 500 .. ids.Count - 1 do
                        let count = min 500 (ids.Count - offset)
                        let placeholders = String.Join(",", [|for i in 0 .. count-1 -> "@id" + string i|])
                        let sql =
                            if terminal then
                                let column = if row.TargetCollection = "SoloDBFileHeader" then "FileId" else "DirectoryId"
                                $"SELECT OwnerId,{column} FROM SoloDBFileSystemReference WHERE OwnerCollection=@owner AND Property=@property AND {column} IS NOT NULL AND OwnerId IN ({placeholders})"
                            else
                                $"SELECT {layout.FromColumn},{layout.ToColumn} FROM {quoteIdentifier layout.LinkTable} WHERE {layout.FromColumn} IN ({placeholders})"
                        let mutable statement = connection.OpenStatement(sql)
                        try
                            let first =
                                if terminal then
                                    statement.BindText(1, table)
                                    statement.BindText(2, row.PropertyName)
                                    3
                                else 1
                            for i = 0 to count-1 do statement.BindInt64(first+i, ids.[offset+i])
                            while statement.Step() do
                                let source = struct(table, statement.Row.GetInt64(0))
                                cache.[source].Add(struct(targetTable, statement.Row.GetInt64(1))) |> ignore
                            statement.Dispose()
                        with error -> statement.Abort(error); reraise()
    load

/// Capture physical owners in bounded batches before the operation changes them.
let captureMany (tx: RelationTxContext) (ownerIds: seq<int64>) =
    let missing = HashSet<struct(string * int64)>()
    for id in ownerIds do
        let key = struct(tx.OwnerTable, id)
        if not (tx.Preparation.GraphInitial.ContainsKey key) then missing.Add key |> ignore
    if missing.Count <> 0 then
        let adjacency = Dictionary<struct(string * int64), HashSet<struct(string * int64)>>()
        loader tx.Connection (Layouts()) adjacency Outgoing missing
        for KeyValue(key, edges) in adjacency do tx.Preparation.GraphInitial.Add(key, edges)

let capture (tx: RelationTxContext) ownerId = captureMany tx [ownerId]

/// Newly inserted rows have no pre-operation membership, even when callbacks
/// populate their references before the inserting API returns.
let inserted (tx: RelationTxContext) ownerId =
    let key = struct(tx.OwnerTable, ownerId)
    if not (tx.Preparation.GraphInitial.ContainsKey key) then
        tx.Preparation.GraphInitial.Add(key, HashSet<struct(string * int64)>())

/// Validate the completed operation under its existing SQLite writer ownership.
/// Load physical adjacency in bounded batches, then share depth states across
/// roots and diamonds. A cycle supplies a walk of any length.
let validate (tx: RelationTxContext) =
    let initial = tx.Preparation.GraphInitial
    if initial.Count <> 0 then
        let outgoing = Dictionary<struct(string * int64), HashSet<struct(string * int64)>>()
        let incoming = Dictionary<struct(string * int64), HashSet<struct(string * int64)>>()
        let load = loader tx.Connection (Layouts())
        let next (cache: Dictionary<struct(string * int64), HashSet<struct(string * int64)>>) direction frontier =
            load cache direction frontier
            let result = HashSet<struct(string * int64)>()
            for node in frontier do result.UnionWith cache.[node]
            result
        let changed = HashSet<struct(string * int64)>()
        let owners = HashSet<struct(string * int64)>(initial.Keys)
        load outgoing Outgoing owners
        for KeyValue(node, before) in initial do
            if not (outgoing.[node].IsSubsetOf before) then changed.Add node |> ignore
        // Prefetch only reachable adjacency, up to the admitted bound. Equal
        // nodes at the same depth are merged without discarding longer walks.
        let mutable frontier = changed
        let mutable depth = 0
        while depth <= maxRelationDepth && frontier.Count <> 0 do
            frontier <- next outgoing Outgoing frontier
            depth <- depth + 1
        let heights = Dictionary<struct(struct(string * int64) * int), int>()
        let rec height node remaining =
            if remaining = 0 || isTerminal (let struct(table, _) = node in table) then 0
            else
                let key = struct(node, remaining)
                match heights.TryGetValue key with
                | true, value -> value
                | _ ->
                    let mutable value = 0
                    for child in outgoing.[node] do
                        value <- max value (1 + height child (remaining-1))
                    heights.Add(key, value)
                    value
        let ancestors = Dictionary<int, HashSet<struct(string * int64)>>()
        let reject () =
            raise (InvalidOperationException($"Reference mutation creates a cycle or a path exceeding {maxRelationDepth} reference hops."))
        for node in changed do
            let depth = height node (maxRelationDepth+1)
            if depth > maxRelationDepth then reject()
            let remaining = maxRelationDepth+1-depth
            match ancestors.TryGetValue remaining with
            | true, roots -> roots.Add node |> ignore
            | _ -> let roots = HashSet<struct(string * int64)>() in roots.Add node |> ignore; ancestors.Add(remaining, roots)
        // Ancestors only need their incoming membership. There is no second
        // descent from every ancestor and no scan of unrelated graph components.
        for KeyValue(remaining, roots) in ancestors do
            let mutable frontier = roots
            let mutable steps = 0
            while steps < remaining && frontier.Count <> 0 do
                frontier <- next incoming Incoming frontier
                steps <- steps + 1
            if frontier.Count <> 0 then reject()
