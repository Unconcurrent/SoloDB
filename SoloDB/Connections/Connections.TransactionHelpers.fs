namespace SoloDatabase

open System
open System.IO
open System.Threading
open System.Threading.Tasks
open NativeSQLiteTools
open Utils

module internal ConnectionsTransactionHelpers =
    let internal ThrowOutsideEventContextUsage() =
        raise (InvalidOperationException(
            "Error: Event handler used a non-context SoloDB instance.\nReason: Event handlers must use the provided ctx (ISoloDB). Using another instance can lock the database.\nFix: Use the ctx parameter for all database operations inside handlers."))

    let internal eventHandlerScopeUnderflowMessage =
        "Event handler scope underflow detected. ExitEventHandlerScope was called without a matching EnterEventHandlerScope."

    let mutable private savepointCounter = 0L

    let internal takeHandlerFaultCommitException (connection: NativeSqliteConnection) : exn option =
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

    let private executeBoundary (connection: NativeSqliteConnection) sql =
        let mutable statement = connection.OpenStatement(sql)
        try
            statement.StepTransactionBoundary() |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()

    let cleanupSavepointRollback (conn: NativeSqliteConnection) (sp: string) (ex: exn) =
        let faultDepth = tryGetRecordedHandlerFaultDepth conn ex
        let mutable rollbackFailure: exn option = None
        let mutable releaseFailure: exn option = None
        try executeBoundary conn (sprintf "ROLLBACK TO \"%s\";" sp) with rb -> rollbackFailure <- Some rb
        try executeBoundary conn (sprintf "RELEASE \"%s\";" sp) with rel -> releaseFailure <- Some rel
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

    let rollbackBorrowedTransaction (conn: NativeCachingDbConnection) (primaryEx: exn option ref) (cleanupEx: exn option ref) (ex: exn) =
        primaryEx := Some ex
        try executeBoundary conn "ROLLBACK;" with rb -> cleanupEx := Some rb

    let commitOrRollbackBorrowedTransaction (conn: NativeCachingDbConnection) (primaryEx: exn option ref) (cleanupEx: exn option ref) =
        match takeHandlerFaultCommitException conn with
        | Some ex -> rollbackBorrowedTransaction conn primaryEx cleanupEx ex
        | None -> executeBoundary conn "COMMIT;"

    let cleanupBorrowedTransaction (conn: NativeCachingDbConnection) (cleanupEx: exn option ref) =
        conn.InsideTransaction <- false
        clearHandlerFault conn
        try (conn :> IDisposable).Dispose() with ex ->
            match !cleanupEx with
            | None -> cleanupEx := Some ex
            | Some _ -> ()

    /// Ends a dedicated (non-pooled) transaction connection. The connection is destroyed rather
    /// than returned to any pool, so any reference captured during the callback becomes unusable
    /// the moment the scope ends and can never serve unrelated work.
    let cleanupDedicatedTransaction (conn: NativeCachingDbConnection) (cleanupEx: exn option ref) =
        conn.InsideTransaction <- false
        clearHandlerFault conn
        try conn.DisposeReal() with ex ->
            match !cleanupEx with
            | None -> cleanupEx := Some ex
            | Some _ -> ()

    let withPooledTransactionCore isInHandlerScope runner f =
        if isInHandlerScope() then
            ThrowOutsideEventContextUsage()
        runner f

    let internal withSavepoint (conn: NativeSqliteConnection) (f: NativeSqliteConnection -> 'T) =
        let sp = $"sp_{Interlocked.Increment(&savepointCounter)}"
        executeBoundary conn (sprintf "SAVEPOINT \"%s\";" sp)
        try
            let ret = f conn
            executeBoundary conn (sprintf "RELEASE \"%s\";" sp)
            ret
        with ex ->
            cleanupSavepointRollback conn sp ex
            reraise()

    let internal withSavepointAsync (conn: NativeSqliteConnection) (f: NativeSqliteConnection -> Task<'T>) = task {
        let sp = $"sp_{Interlocked.Increment(&savepointCounter)}"
        executeBoundary conn (sprintf "SAVEPOINT \"%s\";" sp)
        try
            let! ret = f conn
            executeBoundary conn (sprintf "RELEASE \"%s\";" sp)
            return ret
        with ex ->
            cleanupSavepointRollback conn sp ex
            return reraiseAnywhere ex
    }

    let shouldSuppressSavepointInHandler (conn: NativeSqliteConnection) =
        isInEventHandlerScope conn

    let internal beginImmediateWithRetry (connection: NativeSqliteConnection) =
        clearHandlerFault connection
        executeBoundary connection "BEGIN IMMEDIATE;"

    let internal beginImmediateWithRetryAsync (connection: NativeSqliteConnection) = task {
        clearHandlerFault connection
        let! prepared = connection.OpenStatementAsync("BEGIN IMMEDIATE;")
        let mutable statement = prepared
        try
            let! struct (updated, _) = statement.StepTransactionBoundaryAsync()
            statement <- updated
            statement.Dispose()
        with error ->
            statement.Abort(error)
            return reraiseAnywhere error
    }

    let internal resolveTxOutcome (primaryEx: exn option) (cleanupEx: exn option) : exn option =
        match primaryEx, cleanupEx with
        | Some p, Some c ->
            p.Data["SoloDB.CleanupException"] <- c
            Some p
        | Some p, None -> Some p
        | None, Some c -> Some c
        | None, None -> None
