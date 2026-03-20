namespace SoloDatabase

open System
open System.IO
open System.Threading
open System.Threading.Tasks
open Microsoft.Data.Sqlite
open SQLiteTools
open Utils

module internal ConnectionsTransactionHelpers =
    let internal ThrowOutsideEventContextUsage() =
        raise (InvalidOperationException(
            "Error: Event handler used a non-context SoloDB instance.\nReason: Event handlers must use the provided ctx (ISoloDB). Using another instance can lock the database.\nFix: Use the ctx parameter for all database operations inside handlers."))

    let internal eventHandlerScopeUnderflowMessage =
        "Event handler scope underflow detected. ExitEventHandlerScope was called without a matching EnterEventHandlerScope."

    let mutable private savepointCounter = 0L

    let internal takeHandlerFaultCommitException (connection: SqliteConnection) : exn option =
        match takeHandlerFault connection with
        | Some handlerEx ->
            let commitEx = InvalidOperationException(
                "Error: Transaction cannot commit because handler-scoped database work failed.
Reason: Event handlers run on the active connection while SAVEPOINT is suppressed, so swallowed database faults would otherwise leak partial side effects.
Fix: Let handler-side database faults abort the outer transaction, or avoid swallowing them.",
                handlerEx)
            commitEx.Data["SoloDB.HandlerScopedFault"] <- handlerEx
            Some commitEx
        | None -> None

    let cleanupSavepointRollback (conn: SqliteConnection) (sp: string) (ex: exn) =
        let faultDepth = tryGetRecordedHandlerFaultDepth conn ex
        let mutable rollbackFailure: exn option = None
        let mutable releaseFailure: exn option = None
        try conn.Execute(sprintf "ROLLBACK TO \"%s\";" sp) |> ignore with rb -> rollbackFailure <- Some rb
        try conn.Execute(sprintf "RELEASE \"%s\";" sp) |> ignore with rel -> releaseFailure <- Some rel
        match rollbackFailure, releaseFailure with
        | None, None ->
            match faultDepth with
            | Some depth -> clearNonSwallowedHandlerFaultsAtDepth conn depth
            | None -> ()
        | Some rb, Some rel ->
            let wrapped = InvalidOperationException("SAVEPOINT rollback cleanup failed: both ROLLBACK TO and RELEASE failed.", rb)
            wrapped.Data["SoloDB.SavepointReleaseException"] <- rel
            wrapped.Data["SoloDB.PrimaryException"] <- ex
            raise wrapped
        | Some rb, None ->
            let wrapped = InvalidOperationException("SAVEPOINT rollback cleanup failed: ROLLBACK TO failed.", rb)
            wrapped.Data["SoloDB.PrimaryException"] <- ex
            raise wrapped
        | None, Some rel ->
            let wrapped = InvalidOperationException("SAVEPOINT rollback cleanup failed: RELEASE failed.", rel)
            wrapped.Data["SoloDB.PrimaryException"] <- ex
            raise wrapped

    let rollbackBorrowedTransaction (conn: CachingDbConnection) (primaryEx: exn option ref) (cleanupEx: exn option ref) (ex: exn) =
        primaryEx := Some ex
        try conn.Execute "ROLLBACK;" |> ignore with rb -> cleanupEx := Some rb

    let commitOrRollbackBorrowedTransaction (conn: CachingDbConnection) (primaryEx: exn option ref) (cleanupEx: exn option ref) =
        match takeHandlerFaultCommitException conn with
        | Some ex -> rollbackBorrowedTransaction conn primaryEx cleanupEx ex
        | None -> conn.Execute "COMMIT;" |> ignore

    let cleanupBorrowedTransaction (conn: CachingDbConnection) (cleanupEx: exn option ref) =
        conn.InsideTransaction <- false
        clearHandlerFault conn
        try (conn :> IDisposable).Dispose() with ex ->
            match !cleanupEx with
            | None -> cleanupEx := Some ex
            | Some _ -> ()

    let withPooledTransactionCore isInHandlerScope runner f =
        if isInHandlerScope() then
            ThrowOutsideEventContextUsage()
        runner f

    let internal withSavepoint (conn: SqliteConnection) (f: SqliteConnection -> 'T) =
        let sp = $"sp_{Interlocked.Increment(&savepointCounter)}"
        conn.Execute(sprintf "SAVEPOINT \"%s\";" sp) |> ignore
        try
            let ret = f conn
            conn.Execute(sprintf "RELEASE \"%s\";" sp) |> ignore
            ret
        with ex ->
            cleanupSavepointRollback conn sp ex
            reraise()

    let internal withSavepointAsync (conn: SqliteConnection) (f: SqliteConnection -> Task<'T>) = task {
        let sp = $"sp_{Interlocked.Increment(&savepointCounter)}"
        conn.Execute(sprintf "SAVEPOINT \"%s\";" sp) |> ignore
        try
            let! ret = f conn
            conn.Execute(sprintf "RELEASE \"%s\";" sp) |> ignore
            return ret
        with ex ->
            cleanupSavepointRollback conn sp ex
            return reraiseAnywhere ex
    }

    let shouldSuppressSavepointInHandler (conn: SqliteConnection) =
        isInEventHandlerScope conn

    let internal beginImmediateWithRetry (connection: SqliteConnection) =
        clearHandlerFault connection
        let mutable attempt = 0
        let mutable started = false
        while not started && attempt < 5 do
            try
                connection.Execute("BEGIN IMMEDIATE;") |> ignore
                started <- true
            with :? SqliteException as se when (se.SqliteErrorCode = 5 || se.SqliteErrorCode = 6) && attempt < 4 ->
                attempt <- attempt + 1
                Thread.Sleep(25 * attempt)

    let internal resolveTxOutcome (primaryEx: exn option) (cleanupEx: exn option) : exn option =
        match primaryEx, cleanupEx with
        | Some p, Some c ->
            p.Data["SoloDB.CleanupException"] <- c
            Some p
        | Some p, None -> Some p
        | None, Some c -> Some c
        | None, None -> None
