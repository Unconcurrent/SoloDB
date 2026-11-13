namespace SoloDatabase

/// Explicit checks of caller-supplied SQLite storage contracts.
[<AbstractClass; Sealed>]
type SqliteStorageDiagnostics private () =
    /// Exercises same-process pager locks and, for Wal, shared-memory coordination.
    /// Supply isolated test resources with no active database users: mapped test bytes
    /// are overwritten. Both main views must belong to the same test database; pass
    /// the same object twice for SingleProcessInMemory. Files/provider are borrowed;
    /// every acquired distinct session is closed once, including on failure.
    /// Provider calls must return normally; this synchronous check does not interrupt
    /// blocked caller code. Passing establishes only the exercised same-process cases,
    /// not multiprocess exclusion, backing coherence, crash recovery or durability.
    static member TestCoordination(coordination: ISqliteStorageCoordination, firstMain: ISqliteStorageFile,
                                  secondMain: ISqliteStorageFile, profile: SqliteStorageProfile) =
        if isNull coordination then nullArg "coordination"
        if isNull (box firstMain) then nullArg "firstMain"
        if isNull (box secondMain) then nullArg "secondMain"
        StorageValidation.profile profile
        if profile = SqliteStorageProfile.ReadOnly then invalidArg "profile" "Coordination diagnostics require Simple or Wal."
        let checks = StorageCoordinationChecks(coordination, profile)
        if StorageCoordinationPager.run checks firstMain secondMain && profile = SqliteStorageProfile.Wal then
            StorageCoordinationWal.run checks firstMain secondMain |> ignore
        checks.Result
