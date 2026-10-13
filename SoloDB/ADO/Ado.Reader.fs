namespace SoloDatabase.Data.Sqlite

open System
open System.Collections
open System.Data
open System.Data.Common
open System.IO
open SoloDatabase

module internal AdoReaderAccess =
    let inline guard (session: AdoSession) version closed action =
        session.Check(version)
        AdoSessionAccess.guard session (fun () ->
            if closed then raise (InvalidOperationException("The data reader is closed."))
            action())
    let inline advance session (rows: SqlRows) (admission: NativeBlobResult) version closed action =
        guard session version closed (fun () ->
            try action()
            with error -> admission.Dispose(); rows.Abort(error); reraise())
    let inline current session (rows: SqlRows) version closed positioned action =
        guard session version closed (fun () ->
            if not positioned then raise (InvalidOperationException("No data exists for the row."))
            action rows.Row)

/// A standard reader presenting one execution lease from the native SQL owner.
[<Sealed; AllowNullLiteral>]
type SqliteDataReader internal (session: AdoSession, rows: SqlRows, admission: NativeBlobResult, closeConnection: bool, completed: Action) as this =
    inherit DbDataReader()
    let version = session.Version
    let mutable closed = false
    let mutable positioned = false
    let mutable hasRows = rows.HasPendingRow
    let mutable recordsAffected = rows.RecordsAffected
    let mutable observedNames: string[] = null
    let mutable observedTypes: Type[] = null
    let storageType ordinal =
        let names = rows.Columns.Names
        if not (Object.ReferenceEquals(names, observedNames)) then
            observedNames <- names
            observedTypes <- null
        let kind = rows.GetStorageClass(ordinal)
        if kind = 5 then
            if not (isNull observedTypes) && not (isNull observedTypes.[ordinal]) then observedTypes.[ordinal]
            else
                let declared = rows.GetDeclaredType(ordinal)
                let declared = if isNull declared then "" else declared.ToUpperInvariant()
                if declared.Contains("INT") then typeof<int64>
                elif declared.Contains("CHAR") || declared.Contains("CLOB") || declared.Contains("TEXT") then typeof<string>
                elif declared.Contains("REAL") || declared.Contains("FLOA") || declared.Contains("DOUB") then typeof<double>
                elif declared.Contains("BLOB") || declared.Length = 0 then typeof<byte[]>
                else typeof<string>
        else
            let result =
                match kind with
                | 1 -> typeof<int64>
                | 2 -> typeof<double>
                | 3 -> typeof<string>
                | _ -> typeof<byte[]>
            if isNull observedTypes then observedTypes <- Array.zeroCreate names.Length
            observedTypes.[ordinal] <- result
            result
    let selectedStream ordinal =
        SqliteBlob.FromSelected(session, session.Native.OpenColumnStream(rows, ordinal, admission))
    let textReader ordinal = new StreamReader(selectedStream ordinal, Text.Encoding.UTF8)
    do session.Register(this)
    let validateRange offset bufferLength bufferOffset length =
        if offset < 0L then raise (ArgumentOutOfRangeException("dataOffset"))
        if bufferOffset < 0 || bufferOffset > bufferLength then raise (ArgumentOutOfRangeException("bufferOffset"))
        if length < 0 || length > bufferLength - bufferOffset then raise (ArgumentOutOfRangeException("length"))
    member _.Handle = if closed then null else AdoReaderAccess.guard session version closed (fun () -> rows.StatementHandle)
    override _.Depth = 0
    override _.IsClosed = closed
    override _.HasRows = AdoReaderAccess.guard session version closed (fun () -> hasRows)
    override _.FieldCount = AdoReaderAccess.guard session version closed (fun () -> rows.FieldCount)
    override _.VisibleFieldCount = this.FieldCount
    override _.RecordsAffected = if closed then recordsAffected else AdoReaderAccess.guard session version closed (fun () -> rows.RecordsAffected)
    override _.Read() = AdoReaderAccess.advance session rows admission version closed (fun () ->
        positioned <- rows.Read()
        positioned)
    override _.NextResult() = AdoReaderAccess.advance session rows admission version closed (fun () ->
        positioned <- false
        observedNames <- null
        observedTypes <- null
        let found = rows.NextResult(admission)
        hasRows <- found && rows.HasPendingRow
        found)
    override _.GetName(ordinal) = AdoReaderAccess.guard session version closed (fun () -> rows.GetName(ordinal))
    override _.GetOrdinal(name) = AdoReaderAccess.guard session version closed (fun () ->
        if isNull name then nullArg "name"
        let names = rows.Columns.Names
        let mutable result = rows.Columns.Ordinal(name)
        if result < 0 then
            let mutable matched: string = null
            for index = 0 to names.Length - 1 do
                if String.Equals(names.[index], name, StringComparison.OrdinalIgnoreCase) then
                    if not (isNull matched) && not (String.Equals(matched, names.[index], StringComparison.Ordinal)) then
                        raise (InvalidOperationException($"The name '{name}' is ambiguous between columns '{matched}' and '{names.[index]}'. Specify one using its exact case."))
                    matched <- names.[index]
                    result <- index
        if result < 0 then raise (ArgumentOutOfRangeException("name", name, null))
        result)
    override _.GetFieldType(ordinal) = AdoReaderAccess.guard session version closed (fun () -> storageType ordinal)
    override _.GetDataTypeName(ordinal) = AdoReaderAccess.guard session version closed (fun () ->
        let declared = rows.GetDeclaredType(ordinal)
        if not (String.IsNullOrEmpty declared) then
            let size = declared.IndexOf('(')
            if size < 0 then declared else declared.Substring(0, size)
        else
            match rows.GetStorageClass(ordinal) with
            | 1 -> "INTEGER"
            | 2 -> "REAL"
            | 3 -> "TEXT"
            | _ -> "BLOB")
    override _.IsDBNull(ordinal) = AdoReaderAccess.current session rows version closed positioned (fun row -> row.IsDBNull ordinal)
    override _.GetValue(ordinal) = AdoReaderAccess.current session rows version closed positioned (fun row -> row.GetValue ordinal)
    override _.GetValues(values) =
        if isNull values then nullArg "values"
        AdoReaderAccess.current session rows version closed positioned (fun row ->
            let count = min values.Length row.FieldCount
            for i = 0 to count - 1 do values.[i] <- row.GetValue i
            count)
    override _.Item with get(ordinal: int) = this.GetValue(ordinal)
    override _.Item with get(name: string) = this.GetValue(this.GetOrdinal(name))
    override _.GetBoolean(ordinal) = AdoReaderAccess.current session rows version closed positioned (fun row -> row.GetBoolean ordinal)
    override _.GetByte(ordinal) = AdoReaderAccess.current session rows version closed positioned (fun row -> row.GetByte ordinal)
    override _.GetInt16(ordinal) = AdoReaderAccess.current session rows version closed positioned (fun row -> row.GetInt16 ordinal)
    override _.GetInt32(ordinal) = AdoReaderAccess.current session rows version closed positioned (fun row -> row.GetInt32 ordinal)
    override _.GetInt64(ordinal) = AdoReaderAccess.current session rows version closed positioned (fun row -> row.GetInt64 ordinal)
    override _.GetFloat(ordinal) = AdoReaderAccess.current session rows version closed positioned (fun row -> row.GetFloat ordinal)
    override _.GetDouble(ordinal) = AdoReaderAccess.current session rows version closed positioned (fun row -> row.GetDouble ordinal)
    override _.GetDecimal(ordinal) = AdoReaderAccess.current session rows version closed positioned (fun row -> row.GetDecimal ordinal)
    override _.GetString(ordinal) = AdoReaderAccess.current session rows version closed positioned (fun row -> row.GetString ordinal)
    override _.GetChar(ordinal) = AdoReaderAccess.current session rows version closed positioned (fun row -> row.GetChar ordinal)
    override _.GetGuid(ordinal) = AdoReaderAccess.current session rows version closed positioned (fun row -> row.GetGuid ordinal)
    override _.GetDateTime(ordinal) = AdoReaderAccess.current session rows version closed positioned (fun row -> row.GetDateTime ordinal)
    member _.GetDateTimeOffset(ordinal: int) = AdoReaderAccess.current session rows version closed positioned (fun row -> SqlRowValue.ReadRaw<DateTimeOffset>(row, ordinal))
    member _.GetTimeSpan(ordinal: int) = AdoReaderAccess.current session rows version closed positioned (fun row -> SqlRowValue.ReadRaw<TimeSpan>(row, ordinal))
    override this.GetFieldValue<'T>(ordinal) =
        if typeof<'T>.Equals(typeof<obj>) then unbox<'T>(this.GetValue(ordinal))
        elif typeof<'T>.Equals(typeof<Stream>) then unbox<'T>(box (this.GetStream(ordinal)))
        elif typeof<'T>.Equals(typeof<TextReader>) then unbox<'T>(box (this.GetTextReader(ordinal)))
        else AdoReaderAccess.current session rows version closed positioned (fun row -> SqlRowValue.ReadRaw<'T>(row, ordinal))
    override _.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length) =
        AdoReaderAccess.current session rows version closed positioned (fun row ->
            if dataOffset < 0L then raise (ArgumentOutOfRangeException("dataOffset"))
            let size = row.BlobLength ordinal
            if isNull buffer then int64 size else
            validateRange dataOffset buffer.Length bufferOffset length
            let count = int (max 0L (min (int64 length) (int64 size - dataOffset)))
            if count > 0 then row.CopyBlob(ordinal, int dataOffset, Span(buffer, bufferOffset, count))
            int64 count)
    override _.GetChars(ordinal, dataOffset, buffer, bufferOffset, length) =
        AdoReaderAccess.current session rows version closed positioned (fun row ->
            if dataOffset < 0L then raise (ArgumentOutOfRangeException("dataOffset"))
            use text = textReader ordinal
            if isNull buffer then
                let mutable count = 0L
                while text.Read() <> -1 do count <- count + 1L
                count
            else
                validateRange dataOffset buffer.Length bufferOffset length
                let mutable position = 0L
                while position < dataOffset do
                    if text.Read() = -1 then raise (ArgumentOutOfRangeException("dataOffset", dataOffset, null))
                    position <- position + 1L
                int64 (text.Read(buffer, bufferOffset, length)))
    override _.GetStream(ordinal) = AdoReaderAccess.current session rows version closed positioned (fun _ -> selectedStream ordinal)
    override _.GetTextReader(ordinal) =
        AdoReaderAccess.current session rows version closed positioned (fun row ->
            if row.IsDBNull ordinal then new StringReader(String.Empty) :> TextReader
            else textReader ordinal :> TextReader)
    override _.GetEnumerator() = new DbEnumerator(this, false) :> IEnumerator
    override _.GetSchemaTable() = AdoReaderAccess.guard session version closed (fun () ->
        let table = new DataTable("SchemaTable")
        for name, t in [|"ColumnName", typeof<string>; "ColumnOrdinal", typeof<int>; "ColumnSize", typeof<int>;
                         "DataType", typeof<Type>; "DataTypeName", typeof<string>; "AllowDBNull", typeof<bool>;
                         "IsLong", typeof<bool>; "IsUnique", typeof<bool>;
                         "IsKey", typeof<bool>; "BaseCatalogName", typeof<string>; "BaseSchemaName", typeof<string>;
                         "BaseTableName", typeof<string>; "BaseColumnName", typeof<string>|] do
            table.Columns.Add(name, t) |> ignore
        for i = 0 to rows.FieldCount - 1 do
            let item = table.NewRow()
            item.["ColumnName"] <- rows.GetName i
            item.["ColumnOrdinal"] <- i
            item.["ColumnSize"] <- -1
            item.["DataType"] <- storageType i
            item.["DataTypeName"] <- this.GetDataTypeName i
            item.["IsLong"] <- false
            let facts = rows.DescribeColumn i
            if not (isNull facts.DatabaseName) then item.["BaseCatalogName"] <- facts.DatabaseName
            item.["BaseSchemaName"] <- ""
            if not (isNull facts.TableName) then item.["BaseTableName"] <- facts.TableName
            if not (isNull facts.OriginName) then item.["BaseColumnName"] <- facts.OriginName
            if facts.NotNull >= 0 then item.["AllowDBNull"] <- facts.NotNull = 0
            if facts.PrimaryKey >= 0 then item.["IsKey"] <- facts.PrimaryKey <> 0
            table.Rows.Add(item)
        table)
    override _.Close() =
        if not closed then
            closed <- true
            session.Unregister(this)
            positioned <- false
            observedNames <- null
            observedTypes <- null
            let mutable failure: exn = null
            try admission.Dispose() with error -> failure <- error
            try AdoErrors.convert rows.Dispose with error -> if isNull failure then failure <- error
            recordsAffected <- rows.RecordsAffected
            try completed.Invoke() with error -> if isNull failure then failure <- error
            if closeConnection && session.IsCurrent(version) then
                try session.Close() with error -> if isNull failure then failure <- error
            if not (isNull failure) then Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw()
    override _.Dispose(disposing) =
        if disposing then this.Close()
        base.Dispose(disposing)
