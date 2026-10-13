namespace SoloDatabase

open System
open System.Collections.Generic

[<Struct>]
type internal NativeBlobColumn =
    { Name: string; PhysicalName: string; OriginName: string; Rowid: bool; Stored: bool }
[<Struct>]
type internal NativeBlobSource =
    { Schema: string; Table: string; Columns: NativeBlobColumn[]; Magic: NativeBlobColumn[] }

/// Native catalog reads share statement ownership, fault handling and the caller's
/// cumulative budget. Probe borrows detect callback reentrancy, not persisted versions.
[<Sealed; AllowNullLiteral>]
type internal NativeBlobCatalog(cache: NativeStatementCache, initialBudget: NativeExecutionBudget) =
    let mutable budget = initialBudget
    let mutable expectedBorrows = cache.BeginSchemaProbe()
    let callbacks = cache.Database.CallbackEpoch
    let mutable disposed = false
    let equal = NativeBlobIdentity.equalIdentifier
    let quote (name: string) = "\"" + name.Replace("\"", "\"\"") + "\""
    let openScope sql =
        expectedBorrows <- expectedBorrows + 1L
        new NativeStatementScope(cache, NativeStatementSource.Text sql, 0, budget, false)
    let finish (scope: byref<NativeStatementScope>) =
        budget <- scope.Budget
        scope.Dispose()
    let labels (names: string[]) (source: NativeBlobSource) =
        if names.Length <> source.Columns.Length then ValueNone
        elif names |> Array.mapi (fun i name -> names |> Array.take i |> Array.exists (equal name)) |> Array.exists id then ValueNone
        else ValueSome { source with Columns=Array.map2 (fun name column -> {column with Name=name}) names source.Columns; Magic=Array.empty }
    member _.Budget with get() = budget and set value = budget <- value
    member _.Coherent = not disposed && cache.SchemaProbeBorrows = expectedBorrows && cache.Database.CallbackEpoch = callbacks
    member _.SourceVersion(schema: string) =
        let mutable scope = openScope ("PRAGMA " + quote schema + ".schema_version")
        try
            if not (scope.Step()) then raise (InvalidOperationException("SQLite did not return its schema version."))
            let version = scope.Row.GetInt64(0)
            finish &scope
            version
        with error -> scope.Abort(error); reraise()
    member private _.FindObject(schema: string, name: string) =
        // PRAGMA resolves identifiers inside SQLite without invoking replaceable
        // application collations. Restrict discovery to this one schema.
        let mutable scope = openScope ("PRAGMA " + quote schema + ".table_list(" + quote name + ")")
        let metadata =
            try
                let result =
                    if scope.Step() then
                        let row = scope.Row
                        ValueSome(struct(row.GetString(1),row.GetString(2),row.GetInt64(4)=0L))
                    else ValueNone
                finish &scope
                result
            with error -> scope.Abort(error); reraise()
        match metadata with
        | ValueSome(struct(table,"table",ordinary)) -> ValueSome(struct(table,"table",null,ordinary))
        | ValueSome(struct(view,"view",_)) ->
            // Compare the canonical stored spelling as bytes. BINARY and NOCASE
            // are caller-replaceable collations and cannot own catalog identity.
            let mutable scope = openScope ("SELECT sql FROM " + quote schema + ".sqlite_schema WHERE CAST(name AS BLOB)=CAST(@name AS BLOB) AND CAST(type AS BLOB)=x'76696577'")
            try
                scope.BindText(1,view)
                let result =
                    if scope.Step() && not (scope.Row.IsDBNull(0)) then
                        ValueSome(struct(view,"view",scope.Row.GetString(0),false))
                    else ValueNone
                finish &scope
                result
            with error -> scope.Abort(error); reraise()
        | _ -> ValueNone
    member private _.ReadColumns(schema: string, table: string) =
        let mutable scope = openScope "SELECT name,type,pk,hidden FROM pragma_table_xinfo(@table,@schema) ORDER BY cid"
        let columns = ResizeArray<struct(string*string*int64*int64)>()
        try
            scope.BindText(1,table); scope.BindText(2,schema)
            while scope.Step() do
                let row=scope.Row
                columns.Add(struct(row.GetString(0),row.GetString(1),row.GetInt64(2),row.GetInt64(3)))
            finish &scope
            columns.ToArray()
        with error -> scope.Abort(error); reraise()
    member private this.Table(schema: string, table: string, ordinary: bool) =
        let columns = this.ReadColumns(schema,table)
        let mutable scope = openScope "SELECT EXISTS(SELECT 1 FROM pragma_index_list(@table,@schema) WHERE CAST(origin AS BLOB)=x'706b')"
        try
            scope.BindText(1,table); scope.BindText(2,schema)
            let indexed = if scope.Step() then scope.Row.GetInt64(0)<>0L else true
            finish &scope
            if not ordinary then ValueNone else
            let primary = columns |> Array.filter (fun struct(_,_,pk,_) -> pk>0L)
            let alias =
                if indexed || primary.Length<>1 then null else
                let struct(name,kind,pk,hidden) = primary.[0]
                if pk=1L && hidden=0L && equal kind "INTEGER" then name else null
            let selected =
                columns |> Array.choose (fun struct(name,_,_,hidden) ->
                    if hidden=1L then None else Some {Name=name;PhysicalName=name;OriginName=name;Rowid=equal name alias;Stored=hidden=0L})
            let magic =
                [|"rowid";"_rowid_";"oid"|] |> Array.choose (fun name ->
                    if columns |> Array.exists (fun struct(existing,_,_,_) -> equal name existing) then None else
                    Some {Name=name;PhysicalName=name;OriginName=(if isNull alias then "rowid" else alias);Rowid=true;Stored=false})
            ValueSome {Schema=schema;Table=table;Columns=selected;Magic=magic}
        with error -> scope.Abort(error); reraise()
    member private this.Lineage(schema: string, table: string, ordinary: bool, projection: NativeBlobIdentity.Projection[]) =
        if not ordinary then ValueNone else
        let names =
            if projection |> Array.exists (fun item -> item.Column.IsNone) then
                this.ReadColumns(schema,table) |> Array.choose (fun struct(name,_,_,hidden) -> if hidden=1L then None else Some name)
            else projection |> Array.map (fun item -> item.Column.Value)
        let columns = names |> Array.map (fun name -> {Name=name;PhysicalName=name;OriginName=null;Rowid=false;Stored=false})
        ValueSome {Schema=schema;Table=table;Columns=columns;Magic=Array.empty}
    member this.Physical(source: NativeBlobSource) =
        match this.Table(source.Schema,source.Table,true) with
        | ValueNone -> ValueNone
        | ValueSome table ->
            let columns = Array.zeroCreate source.Columns.Length
            let mutable valid = true
            for i = 0 to source.Columns.Length-1 do
                let forwarded = source.Columns.[i]
                let found =
                    match table.Columns |> Array.tryFind (fun column -> equal column.Name forwarded.PhysicalName) with
                    | Some column -> Some column
                    | _ -> table.Magic |> Array.tryFind (fun column -> equal column.Name forwarded.PhysicalName)
                match found with
                | Some column -> columns.[i] <- {column with Name=forwarded.Name}
                | None -> valid <- false
            if valid then ValueSome {source with Columns=columns} else ValueNone
    member this.Resolve(physicalSchema: string, root: NativeBlobIdentity.Query) =
        let mutable work = 0
        let rec resolve depth context bindings visited (query: NativeBlobIdentity.Query) =
            work <- work + 1
            if depth>32 || work>2048 then ValueNone else
            let environment = query.Bindings |> List.map (fun binding -> binding,query.Bindings@bindings)
            let findBinding name =
                match environment |> List.tryFind (fun (binding,_) -> equal binding.Name name) with
                | Some found -> Some found
                | _ -> bindings |> List.tryFind (fun binding -> equal binding.Name name) |> Option.map (fun binding -> binding,bindings)
            let source =
                match query.Input with
                | NativeBlobIdentity.Derived inner -> resolve (depth+1) context (query.Bindings@bindings) visited inner
                | NativeBlobIdentity.Named(schema,name) ->
                    match if isNull schema then findBinding name else None with
                    | Some(binding,scope) ->
                        if visited |> List.exists (fun marker -> Object.ReferenceEquals(marker,box binding)) then ValueNone else
                        match resolve (depth+1) context scope (box binding::visited) binding.Query with
                        | ValueSome source when binding.Columns.Length>0 -> labels binding.Columns source
                        | other -> other
                    | None ->
                        let candidates =
                            if not (isNull schema) then
                                if equal schema "temp" || equal schema physicalSchema then [|schema|] else Array.empty
                            elif not (isNull context) then [|context|]
                            elif equal physicalSchema "temp" then [|"temp"|]
                            else [|"temp";physicalSchema|]
                        let mutable found = ValueNone
                        let mutable i = 0
                        while found.IsNone && i<candidates.Length do
                            let schema=candidates.[i]
                            match this.FindObject(schema,name) with
                            | ValueSome metadata -> found <- ValueSome(struct(schema,metadata))
                            | _ -> ()
                            i<-i+1
                        match found with
                        | ValueNone -> ValueNone
                        | ValueSome(struct(schema,struct(table,kind,_,ordinary))) when kind="table" -> this.Lineage(schema,table,ordinary,query.Projection)
                        | ValueSome(struct(schema,struct(view,_,definition,_))) ->
                            let marker = schema + "." + view
                            if visited |> List.exists (function :? string as old -> equal old marker | _ -> false) then ValueNone else
                            match NativeBlobIdentity.tryQuery definition with
                            | ValueNone -> ValueNone
                            | ValueSome inner ->
                                match resolve (depth+1) (if equal schema "temp" then null else schema) [] (box marker::visited) inner with
                                | ValueNone -> ValueNone
                                | ValueSome source ->
                                    let names=this.ReadColumns(schema,view) |> Array.choose (fun struct(name,_,_,hidden) -> if hidden=1L then None else Some name)
                                    labels names source
            match source with
            | ValueNone -> ValueNone
            | ValueSome source ->
                let projected = ResizeArray<NativeBlobColumn>()
                let mutable valid = true
                for item in query.Projection do
                    match item.Column with
                    | ValueNone -> projected.AddRange(source.Columns)
                    | ValueSome name ->
                        let column =
                            match source.Columns |> Array.tryFind (fun column -> equal column.Name name) with
                            | Some column -> Some column
                            | _ -> source.Magic |> Array.tryFind (fun column -> equal column.Name name)
                        match column with
                        | None -> valid <- false
                        | Some column -> projected.Add({column with Name=item.Label})
                if valid then ValueSome {source with Columns=projected.ToArray();Magic=Array.empty} else ValueNone
        resolve 0 null [] [] root
    member _.Dispose() =
        if not disposed then disposed <- true; cache.EndSchemaProbe()
    interface IDisposable with member this.Dispose() = this.Dispose()
