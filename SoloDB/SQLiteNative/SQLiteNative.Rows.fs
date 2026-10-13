namespace SoloDatabase

open System
open SQLitePCL

/// Public dynamic reader. Internal known operations own their statement scopes directly.
[<Sealed; AllowNullLiteral>]
type SqlRows internal (initial: NativeSqlScript, exclusive: bool) =
    let mutable script = initial
    let database = initial.Database
    let generation = initial.Generation
    let mutable disposed = false
    let token =
        if exclusive then
            database.ReaderToken <- database.ReaderToken + 1L
            database.ReaderActive <- true
        database.ReaderToken
    member _.Row = script.Row
    member internal _.StatementHandle = if script.FieldCount > 0 then script.Statement.Handle else null
    member internal _.Columns = script.Statement.Columns
    member internal _.HasCurrentRow = script.HasCurrentRow
    member internal _.HasPendingRow = script.HasPendingRow
    member internal _.GetDeclaredType(ordinal: int) =
        if ordinal < 0 || ordinal >= script.FieldCount then raise (ArgumentOutOfRangeException("ordinal"))
        raw.sqlite3_column_decltype(script.Statement.Handle, ordinal).utf8_to_string()
    member internal _.GetStorageClass(ordinal: int) =
        if ordinal < 0 || ordinal >= script.FieldCount then raise (ArgumentOutOfRangeException("ordinal"))
        raw.sqlite3_column_type(script.Statement.Handle, ordinal)
    member internal _.DescribeColumn(ordinal: int) = NativeColumnMetadata.describe database script.Statement.Handle ordinal
    member internal _.OpenBlobStream(ordinal: int, row: SqlRow, result: NativeBlobResult) =
        if row.IsDBNull ordinal then raise (InvalidOperationException($"SQL NULL at ordinal {ordinal}."))
        NativeColumnStreams.openStream(database, script.Statement.Handle, ordinal, row.GetBlob, row.GetInt64, script.TryBlobSource(result))
    member _.FieldCount = script.FieldCount
    member _.GetName(ordinal: int) = script.Statement.Columns.Names.[ordinal]
    member _.GetOrdinal(name: string) =
        let ordinal = script.Statement.Columns.Ordinal(name)
        if ordinal < 0 then invalidArg "name" ($"Column '{name}' was not returned.")
        ordinal
    member _.RecordsAffected = script.RecordsAffected
    member internal this.NextResult(result: NativeBlobResult) =
        try script.NextResult(result)
        with error ->
            if not (isNull result) then result.Dispose()
            this.Abort(error)
            reraise()
    member this.NextResult() = this.NextResult(null)
    member this.Read() =
        try script.Read()
        with error -> this.Abort(error); reraise()
    member internal this.Consume() =
        try script.Consume()
        with error -> this.Abort(error); reraise()
    member this.Abort(error: exn) =
        script.Abort(error)
        try this.Dispose() with _ -> ()
    member _.Dispose() =
        if not disposed then
            disposed <- true
            try script.Dispose()
            finally
                if exclusive && database.Generation = generation && database.ReaderToken = token then database.ReaderActive <- false
    interface IDisposable with member this.Dispose() = this.Dispose()
