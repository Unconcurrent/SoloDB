#nowarn "9"

namespace SoloDatabase

open System
open System.Collections.Generic
open System.Text
open System.Diagnostics
open Microsoft.FSharp.NativeInterop
open System.Runtime.CompilerServices
open SQLitePCL

[<Struct>]
type internal NativeColumnDescription =
    { DatabaseName: string; TableName: string; OriginName: string
      DeclaredType: string; NotNull: int; PrimaryKey: int }

module internal NativeColumnMetadata =
    let describe (database: NativeDatabase) (handle: sqlite3_stmt) ordinal =
        if ordinal < 0 || ordinal >= raw.sqlite3_column_count handle then raise (ArgumentOutOfRangeException("ordinal"))
        let databaseName = raw.sqlite3_column_database_name(handle, ordinal).utf8_to_string()
        let tableName = raw.sqlite3_column_table_name(handle, ordinal).utf8_to_string()
        let origin = raw.sqlite3_column_origin_name(handle, ordinal).utf8_to_string()
        let mutable declared = raw.sqlite3_column_decltype(handle, ordinal).utf8_to_string()
        let mutable notNull = -1
        let mutable primary = -1
        if not (isNull databaseName || isNull tableName || isNull origin) then
            let mutable collation = null
            let mutable autoIncrement = 0
            database.Check(raw.sqlite3_table_column_metadata(database.Handle, databaseName, tableName, origin,
                                                            &declared, &collation, &notNull, &primary, &autoIncrement))
        { DatabaseName = databaseName; TableName = tableName; OriginName = origin
          DeclaredType = declared; NotNull = notNull; PrimaryKey = primary }

/// Column names describe the statement, while storage classes remain facts of each row.
[<Sealed>]
type internal NativeColumnLayout(names: string[]) =
    let ordinals = Dictionary<string, int>(names.Length, StringComparer.Ordinal)
    let mappings = Dictionary<struct (Type * int * int), obj>()
    do for i = 0 to names.Length - 1 do ordinals.[names.[i]] <- i
    member _.Names = names
    member _.Count = names.Length
    member _.Ordinal(name: string) =
        match ordinals.TryGetValue name with true, value -> value | _ -> -1
    member _.Ordinal(name: string, first: int, finish: int) =
        let mutable ordinal = -1
        for i = first to finish - 1 do if names.[i] = name then ordinal <- i
        ordinal
    member _.Split(name: string) =
        let mutable ordinal = -1
        for i = 1 to names.Length - 1 do
            if String.Equals(names.[i], name, StringComparison.OrdinalIgnoreCase) then ordinal <- i
        if ordinal < 0 then invalidArg "splitOn" ($"Split column '{name}' was not returned after the first column.")
        ordinal
    member _.Mappings = mappings
    member _.Columns = ordinals :> IReadOnlyDictionary<string, int>

[<Sealed>]
type internal NativePreparedStatement(database: NativeDatabase, handle: sqlite3_stmt, text: string, bytes: byte[], startOffset: int, endOffset: int, cacheEligible: bool) =
    let mutable names: string[] = null
    let mutable unprefixedNames: string[] = null
    let mutable slots: Dictionary<string, int> = null
    let generation = database.Generation
    let mutable bindingTarget: NativeBindingTarget = Unchecked.defaultof<_>
    let mutable compiledCapture: NativeCompiledArguments = Unchecked.defaultof<_>
    let mutable dynamicCapture: NativeArgumentCapture = Unchecked.defaultof<_>
    let mutable typedCapture: obj = null
    let mutable bindingLayout: NativeArgumentLayout = Unchecked.defaultof<_>
    let mutable bindingOrdinals: int[] = Array.empty
    let mutable columns: NativeColumnLayout = Unchecked.defaultof<_>
    let mutable metadataVersion = -1
    let mutable resultShapeVersion = 0
    let mutable rowVersion = 0L
    let mutable columnCount = -1
    let mutable hasBindings = false
    let initializeNames () =
        names <- Array.init (raw.sqlite3_bind_parameter_count(handle)) (fun i -> raw.sqlite3_bind_parameter_name(handle, i + 1).utf8_to_string())
    let initializeParameters () =
        if isNull names then initializeNames()
        slots <- Dictionary<string, int>(names.Length, StringComparer.Ordinal)
        bindingTarget <- NativeBindingTarget(database, handle, names.Length)
        for i = 0 to names.Length - 1 do
            let name = names.[i]
            if not (isNull name) then
                slots.[name] <- i + 1
                if name.Length > 1 && (name.[0] = '@' || name.[0] = '$' || name.[0] = ':') then
                    let short = name.Substring(1)
                    match slots.TryGetValue short with
                    | true, _ -> slots.[short] <- -1
                    | _ -> slots.[short] <- i + 1
    member _.ParameterNames =
        if isNull names then initializeNames()
        names
    member _.UnprefixedParameterNames =
        if isNull names then initializeNames()
        if isNull unprefixedNames then
            unprefixedNames <- names |> Array.map (fun name ->
                if not (isNull name) && name.Length > 1 && (name.[0] = '@' || name.[0] = '$' || name.[0] = ':') then name.Substring(1)
                else name)
        unprefixedNames
    member _.Database = database
    member _.Handle = handle
    member _.EndOffset = endOffset
    member _.HasTail = endOffset < bytes.Length - 1 && bytes.[endOffset] <> 0uy
    member _.Sql = text
    member _.Bytes = bytes
    member _.CacheEligible = cacheEligible
    member _.Generation = generation
    member _.HasStatement = not (isNull handle) && not handle.IsInvalid
    member _.HasBindings with get() = hasBindings and set value = hasBindings <- value
    member _.ClearBindings() =
        if not hasBindings then raw.SQLITE_OK
        else
            let result = raw.sqlite3_clear_bindings(handle)
            if result = raw.SQLITE_OK then hasBindings <- false
            result
    member _.Description = Encoding.UTF8.GetString(bytes, startOffset, min 200 (endOffset - startOffset)).Trim()
    member val InUse = false with get, set
    member val Borrow = 0L with get, set
    member val Retired = false with get, set
    member val Cached = false with get, set
    member val Calls = 0L with get, set
    member _.Capture(parameters: obj) =
        match parameters with
        | :? ParameterValues as packet ->
            if isNull (box compiledCapture) then compiledCapture <- NativeCompiledArguments()
            compiledCapture.Capture(packet)
            compiledCapture :> INativeArgumentBinding
        | _ ->
            if isNull (box dynamicCapture) then dynamicCapture <- NativeArgumentCapture()
            dynamicCapture.Capture(parameters)
            dynamicCapture :> INativeArgumentBinding
    member _.Capture(plan: SqlParameterPlan<'Input, 'State>, input: 'Input) =
        let capture =
            match typedCapture with
            | :? SqlParameterCapture<'Input, 'State> as capture when Object.ReferenceEquals(capture.Plan, plan) -> capture
            | _ ->
                let capture = SqlParameterCapture(plan)
                typedCapture <- capture
                capture
        capture.Capture(input)
        capture :> INativeArgumentBinding
    member _.ClearCapture() =
        if not (isNull (box compiledCapture)) then (compiledCapture :> INativeArgumentBinding).Clear()
        if not (isNull (box dynamicCapture)) then dynamicCapture.Clear()
        match typedCapture with :? INativeArgumentBinding as capture -> capture.Clear() | _ -> ()
    member _.RowVersion = rowVersion
    member _.AdvanceRow() = rowVersion <- rowVersion + 1L
    member _.ObserveResultShape() =
        let version = raw.sqlite3_stmt_status(handle, 5 (* SQLITE_STMTSTATUS_REPREPARE *), 0)
        if resultShapeVersion <> version then
            resultShapeVersion <- version
            columnCount <- -1
    member private _.ReadColumnCount() =
        columnCount <- if isNull handle || handle.IsInvalid then 0 else raw.sqlite3_column_count handle
        columnCount
    member this.ColumnCount
        with [<MethodImpl(MethodImplOptions.AggressiveInlining)>] get() =
            if columnCount < 0 then this.ReadColumnCount() else columnCount
    member this.RefreshColumns() =
        let version = resultShapeVersion
        if isNull (box columns) || metadataVersion <> version then
            let count = this.ColumnCount
            columns <- NativeColumnLayout(Array.init count (fun i -> raw.sqlite3_column_name(handle, i).utf8_to_string()))
            metadataVersion <- version
    member this.Columns =
        this.RefreshColumns()
        columns
    member this.Bind(arguments: INativeArgumentBinding) =
        if isNull slots then initializeParameters()
        database.Check(this.ClearBindings())
        if not (Object.ReferenceEquals(bindingLayout, arguments.Layout)) then
            let supplied = arguments.Layout.Names
            let used = Array.zeroCreate<bool> names.Length
            let resolved = Array.zeroCreate<int> supplied.Length
            for i = 0 to supplied.Length - 1 do
                let name = supplied.[i]
                match slots.TryGetValue(name) with
                | true, -1 -> raise (InvalidOperationException($"SQL argument '{name}' is ambiguous. Supply its symbol prefix."))
                | true, ordinal ->
                    if used.[ordinal - 1] then
                        let message = $"SQL argument '{name}' is supplied more than once."
                        if arguments.Layout :? NativeOperationArgumentLayout then raise (InvalidOperationException(message))
                        else invalidArg "parameters" message
                    used.[ordinal - 1] <- true
                    resolved.[i] <- ordinal
                | _ -> ()
            if raw.sqlite3_stmt_isexplain(handle) = 0 && Array.exists not used then
                let missing = names |> Array.mapi (fun i name -> if used.[i] then None else Some name) |> Array.choose id
                raise (InvalidOperationException("Missing SQL arguments: " + String.Join(", ", missing)))
            bindingOrdinals <- resolved
            bindingLayout <- arguments.Layout
        hasBindings <- names.Length > 0
        bindingTarget.Begin(bindingOrdinals)
        try
            arguments.Bind(bindingTarget)
            bindingTarget.Finish()
        finally bindingTarget.End()
    member _.Dispose() =
        rowVersion <- rowVersion + 1L
        if not (isNull handle) && not handle.IsClosed && not handle.IsInvalid then
            let result = raw.sqlite3_finalize(handle)
            if database.IsOpen && database.Generation = generation then database.Check(result)
            elif result <> raw.SQLITE_OK then raise (NativeSqliteException.FromResult(null, result))
    interface IDisposable with member this.Dispose() = this.Dispose()
