namespace SoloDatabase

open System
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open SQLiteToolsMapper
open SQLiteToolsHandlerFaultState

module internal SQLiteToolsExecution =
    let internal rethrow (error: exn) : 'T =
        Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error).Throw()
        Unchecked.defaultof<'T>

    let internal readRow (rows: byref<NativeSqlScript>) =
        try rows.Read()
        with error -> rows.Abort(error); rethrow error

    let internal completeRows connection (rows: byref<NativeSqlScript>) =
        try raiseIfHandlerFaultRecorded connection
        with error -> rows.Abort(error); rethrow error

    let internal bindLease<'T> (rows: byref<NativeSqlScript>) start finish =
        try bindRange<'T> rows.Statement.Columns start finish
        with error -> rows.Abort(error); rethrow error

    let internal mapRow (rows: byref<NativeSqlScript>) (map: Func<SqlRow, 'T>) =
        try map.Invoke(rows.Row)
        with error -> rows.Abort(error); rethrow error

    let internal query<'T> (connection: NativeSqliteConnection) (source: NativeStatementSource) (parameters: obj) = seq {
        raiseIfHandlerFaultRecorded connection
        let mutable rows = connection.AcquireScript(source, parameters)
        try
            let mutable map: Func<SqlRow, 'T> = null
            while readRow &rows do
                if isNull map then map <- bindLease<'T> &rows 0 rows.FieldCount
                yield mapRow &rows map
            completeRows connection &rows
        finally rows.Dispose()
    }

    // A completed non-result prefix must advance through the existing script owner.
    // An empty SELECT is already the first result set and must not be skipped.
    let internal firstFromTail<'T> required (connection: NativeSqliteConnection) statement arguments =
        let mutable script = NativeSqlScript.Continue(connection.StatementCache, statement, arguments)
        try
            let result =
                if script.NextResult() && script.Read() then
                    ValueSome ((bind<'T> script.Statement.Columns 0).Invoke(script.Row))
                elif required then
                    Seq.head (Seq.empty<'T>) |> ValueSome
                else ValueNone
            script.Dispose()
            result
        with error -> script.Abort(error); rethrow error

    let private first<'T> required (connection: NativeSqliteConnection) (sql: string) (parameters: obj) =
        let mutable statement = connection.OpenStatement(sql)
        let mutable result = ValueNone
        try
            let entry = statement.Statement
            let arguments = entry.Capture(parameters)
            if entry.HasStatement then statement.Bind(arguments)
            if statement.Step() then result <- ValueSome ((bind<'T> statement.Statement.Columns 0).Invoke(statement.Row))
            elif entry.HasStatement && entry.ColumnCount = 0 && entry.HasTail then
                result <- firstFromTail<'T> required connection statement arguments
            elif required then Seq.head (Seq.empty<'T>) |> ignore
            if statement.IsCurrent && entry.HasTail then
                NativeSqlScript.CompleteTail(connection.StatementCache, statement, arguments)
            else statement.Dispose()
        with error -> statement.Abort(error); rethrow error
        result

    let internal execute (connection: NativeSqliteConnection) (sql: string) (parameters: obj) =
        withHandlerFaultWrap connection (fun () -> connection.ExecuteSql(sql, parameters))

    let internal queryFirst<'T> (connection: NativeSqliteConnection) sql parameters =
        withHandlerFaultWrap connection (fun () ->
            match first<'T> true connection sql parameters with
            | ValueSome value -> value
            | _ -> Seq.head (Seq.empty<'T>))

    let internal queryFirstOrDefault<'T> (connection: NativeSqliteConnection) sql parameters =
        withHandlerFaultWrap connection (fun () ->
            match first<'T> false connection sql parameters with
            | ValueSome value -> value
            | _ -> defaultOf<'T>())

    let internal openReader (connection: NativeSqliteConnection) sql parameters =
        let rows = connection.AcquireRows(sql, parameters, true, null)
        try
            raiseIfHandlerFaultRecorded connection
            rows
        with error -> rows.Abort(error); rethrow error

    let internal multiQuery<'T1, 'T2, 'TReturn> (this: NativeSqliteConnection) (sql: string) (map: Func<'T1, 'T2, 'TReturn>) (parameters: obj) (splitOn: string) =
        seq {
            raiseIfHandlerFaultRecorded this
            let mutable rows = this.AcquireScript(NativeStatementSource.Text sql, parameters)
            try
                let mutable split = -1
                let mutable first: Func<SqlRow, 'T1> = null
                let mutable second: Func<SqlRow, 'T2> = null
                while readRow &rows do
                    // Resolve only carriers used by this row. Normal function guards
                    // preserve failures without a try/with generator around each yield.
                    let value =
                        try
                            if isNull first then
                                split <- rows.Statement.Columns.Split(splitOn)
                                first <- bindLease<'T1> &rows 0 split
                            let row = rows.Row
                            let left = first.Invoke(row)
                            let right =
                                if row.IsDBNull(split) then Unchecked.defaultof<'T2>
                                else
                                    if isNull second then second <- bindLease<'T2> &rows split rows.FieldCount
                                    second.Invoke(row)
                            map.Invoke(left, right)
                        with error -> rows.Abort(error); rethrow error
                    yield value
                completeRows this &rows
            finally rows.Dispose()
        }
