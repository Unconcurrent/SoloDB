namespace SoloDatabase

open System
open System.Globalization
open System.Text
open System.Diagnostics
open Microsoft.FSharp.NativeInterop
open SQLitePCL

#nowarn "9"

/// A typed, borrowed view of one native row. Advancing or releasing its lease expires the view.
[<Struct>]
type SqlRow =
    val private statement: NativePreparedStatement
    val private version: int64
    val private generation: int64
    internal new(statement, version, generation) = { statement = statement; version = version; generation = generation }
    member private this.Check() =
        if isNull (box this.statement) then raise (InvalidOperationException("There is no SQL row."))
        this.statement.Database.EnsureGeneration(this.generation)
        if this.version <> this.statement.RowVersion then
            raise (InvalidOperationException("The SQL row is no longer current."))
    member private this.Kind(ordinal: int) =
        this.Check()
        if ordinal < 0 || ordinal >= this.statement.ColumnCount then raise (ArgumentOutOfRangeException("ordinal"))
        raw.sqlite3_column_type(this.statement.Handle, ordinal)
    member private this.NonNull(ordinal: int) =
        if this.Kind ordinal = raw.SQLITE_NULL then
            raise (InvalidOperationException($"SQL NULL at ordinal {ordinal}."))
    member this.StorageClass(ordinal: int) = this.Kind ordinal
    member this.FieldCount = this.Check(); this.statement.ColumnCount
    member this.IsDBNull(ordinal: int) = this.Kind ordinal = raw.SQLITE_NULL
    member this.GetInt64(ordinal: int) = this.NonNull ordinal; raw.sqlite3_column_int64(this.statement.Handle, ordinal)
    member this.GetInt32(ordinal: int) = Checked.int (this.GetInt64 ordinal)
    member this.GetInt16(ordinal: int) = Checked.int16 (this.GetInt64 ordinal)
    member this.GetByte(ordinal: int) = Checked.byte (this.GetInt64 ordinal)
    member this.GetBoolean(ordinal: int) = this.GetInt64(ordinal) <> 0L
    member this.GetDouble(ordinal: int) = this.NonNull ordinal; raw.sqlite3_column_double(this.statement.Handle, ordinal)
    member this.GetFloat(ordinal: int) = single (this.GetDouble ordinal)
    member this.GetString(ordinal: int) =
        this.NonNull ordinal
        if this.statement.Database.UsesNativeUtf16 then RawSqliteFunctions.NativeSqlite.ColumnText(this.statement.Handle, ordinal)
        else NativeTextEncoding.copyConfiguredUtf8(raw.sqlite3_column_text(this.statement.Handle, ordinal), raw.sqlite3_column_bytes(this.statement.Handle, ordinal))
    // Internal consumers finish conversion inside their private producing borrow.
    // Do not mix text representations or retain these spans after that conversion.
    member internal this.CanBorrowUtf16 = this.Check(); this.statement.Database.UsesNativeUtf16
    member internal this.TextUtf16(ordinal: int) =
        this.NonNull ordinal
        let pointer = RawSqliteFunctions.NativeSqlite.sqlite3_column_text16(this.statement.Handle, ordinal)
        let bytes = RawSqliteFunctions.NativeSqlite.sqlite3_column_bytes16(this.statement.Handle, ordinal)
        ReadOnlySpan<char>(NativePtr.toVoidPtr(NativePtr.ofNativeInt<char> pointer), bytes / 2)
    member this.GetChar(ordinal: int) =
        if this.Kind ordinal = raw.SQLITE_TEXT then
            let text = this.GetString ordinal
            if text.Length = 1 then text.[0] else Checked.char (this.GetInt64 ordinal)
        else Checked.char (this.GetInt64 ordinal)
    member this.GetDecimal(ordinal: int) = Decimal.Parse(this.GetString ordinal, NumberStyles.Number ||| NumberStyles.AllowExponent, CultureInfo.InvariantCulture)
    member internal this.GetBigInteger(ordinal: int) =
        this.NonNull ordinal
        let row=this
        NativeValueConversion.bigInteger (row.Kind ordinal)
            (fun () -> row.GetInt64 ordinal) (fun () -> row.GetDouble ordinal) (fun () -> row.GetString ordinal)
    member this.GetBlob(ordinal: int) = this.NonNull ordinal; raw.sqlite3_column_blob(this.statement.Handle, ordinal).ToArray()
    member internal this.BlobLength(ordinal: int) =
        this.NonNull ordinal
        raw.sqlite3_column_bytes(this.statement.Handle, ordinal)
    member internal this.CopyBlob(ordinal: int, offset: int, destination: Span<byte>) =
        this.NonNull ordinal
        raw.sqlite3_column_blob(this.statement.Handle, ordinal).Slice(offset, destination.Length).CopyTo(destination)
    member this.GetGuid(ordinal: int) =
        if this.Kind ordinal = raw.SQLITE_BLOB then
            let bytes = this.GetBlob ordinal
            if bytes.Length = 16 then Guid(bytes) else Guid(Encoding.UTF8.GetString bytes)
        else Guid(this.GetString ordinal)
    member this.GetDateTime(ordinal: int) =
        let storage = this.Kind ordinal
        if storage = raw.SQLITE_INTEGER || storage = raw.SQLITE_FLOAT then NativeValueConversion.fromJulianDate (this.GetDouble ordinal)
        else DateTime.Parse(this.GetString ordinal, CultureInfo.InvariantCulture)
    member this.GetDateTimeOffset(ordinal: int) =
        let storage = this.Kind ordinal
        if storage = raw.SQLITE_INTEGER || storage = raw.SQLITE_FLOAT then DateTimeOffset(this.GetDateTime ordinal)
        else DateTimeOffset.Parse(this.GetString ordinal, CultureInfo.InvariantCulture)
    member this.GetDateOnly(ordinal: int) =
        let storage = this.Kind ordinal
        if storage = raw.SQLITE_INTEGER || storage = raw.SQLITE_FLOAT then DateOnly.FromDateTime(this.GetDateTime ordinal)
        else DateOnly.Parse(this.GetString ordinal, CultureInfo.InvariantCulture)
    member this.GetTimeOnly(ordinal: int) = TimeOnly.Parse(this.GetString ordinal, CultureInfo.InvariantCulture)
    member this.GetTimeSpan(ordinal: int) =
        let storage = this.Kind ordinal
        if storage = raw.SQLITE_INTEGER || storage = raw.SQLITE_FLOAT then TimeSpan.FromDays(this.GetDouble ordinal)
        else TimeSpan.Parse(this.GetString ordinal)

    member this.GetValue(ordinal: int) : obj =
        match this.Kind ordinal with
        | 1 -> box (this.GetInt64 ordinal)
        | 2 -> box (this.GetDouble ordinal)
        | 3 -> box (this.GetString ordinal)
        | 4 -> box (this.GetBlob ordinal)
        | _ -> DBNull.Value
    member internal this.GetNativeArray(ordinal: int) =
        this.NonNull ordinal
        let bytes = raw.sqlite3_column_blob(this.statement.Handle, ordinal)
        let owned = NativeArray.NativeArray.Alloc(bytes.Length)
        bytes.CopyTo(owned.Span)
        owned
