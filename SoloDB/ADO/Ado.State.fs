namespace SoloDatabase.Data.Sqlite

open System
open System.Data
open SoloDatabase

/// Presentation lifetime is separate from the physical connection and its native leases.
[<Sealed>]
type internal AdoSession(initial: NativeSqliteConnection, leaveOpen: bool, standalone: bool, sourceGuard: unit -> unit, initialPool: AdoPoolGroup) =
    let mutable native = initial
    let mutable active = not standalone && initial.State = SqliteConnectionState.Open
    let mutable pool = initialPool
    let mutable returned = false
    let mutable connectionString = initial.ConnectionString
    let mutable dataSource = initial.DataSource
    let mutable defaultTimeout = initial.DefaultTimeout
    let mutable registrations = ValueNone
    let mutable disposed = false
    let mutable version = 0L
    let mutable generation = initial.DatabaseOwner.Generation
    let mutable resources: Collections.Generic.HashSet<IDisposable> = null
    let mutable releaseWatch: Action<NativeTransactionWatch> = null
    let register resource =
        if isNull resources then resources <- Collections.Generic.HashSet<IDisposable>()
        resources.Add(resource) |> ignore
    let unregister resource = if not (isNull resources) then resources.Remove(resource) |> ignore
    let currentOwner () =
        not returned && native.DatabaseOwner.Generation = generation && (try sourceGuard(); true with _ -> false)
    let clearResources () =
        let mutable failure: exn = null
        while not (isNull resources) && resources.Count > 0 do
            let mutable enumerator = resources.GetEnumerator()
            let resource =
                try enumerator.MoveNext() |> ignore; enumerator.Current
                finally enumerator.Dispose()
            resources.Remove(resource) |> ignore
            try resource.Dispose() with error -> if isNull failure then failure <- error
        if not (isNull failure) then Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw()
    let acquire () =
        if returned then
            pool <- null
            native <- new NativeSqliteConnection(connectionString)
            returned <- false
            generation <- native.DatabaseOwner.Generation
            try
                match registrations with
                | ValueSome saved -> native.RestoreRegistrations(saved); registrations <- ValueNone
                | ValueNone -> ()
                native.DefaultTimeout <- defaultTimeout
            with _ ->
                native.Dispose()
                returned <- true
                reraise()
    let returnStandalone () =
        if not returned then
            defaultTimeout <- native.DefaultTimeout
            if isNull pool then native.Close()
            else
                registrations <- ValueSome native.Registrations
                try
                    native.DetachRegistrations() |> ignore
                    returned <- true
                    pool.Return(native)
                with _ ->
                    returned <- true
                    try native.Dispose() with _ -> ()
                    reraise()
    let closePresentation physicalClose =
        if active then
            active <- false
            version <- version + 1L
            let mutable failure: exn = null
            try clearResources() with error -> failure <- error
            try
                if physicalClose && currentOwner() then
                    if standalone then returnStandalone() else native.Close()
            with error -> if isNull failure then failure <- error
            if not (isNull failure) then Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw()
    member this.WatchTransaction() =
        this.Check()
        if isNull releaseWatch then releaseWatch <- Action<NativeTransactionWatch>(fun watch -> unregister watch)
        let watch = native.DatabaseOwner.WatchTransaction(releaseWatch)
        register watch
        watch
    member internal _.Register(resource: IDisposable) = register resource
    member internal _.Unregister(resource: IDisposable) = unregister resource
    new(initial, leaveOpen, standalone, sourceGuard) = new AdoSession(initial, leaveOpen, standalone, sourceGuard, null)
    member _.Native =
        if not disposed then acquire()
        native
    member _.ConnectionString = connectionString
    member _.DataSource = dataSource
    member _.DefaultTimeout
        with get() = if standalone && returned then defaultTimeout else native.DefaultTimeout
        and set value =
            if value < 0 then raise (ArgumentOutOfRangeException("value"))
            defaultTimeout <- value
            if not returned then native.DefaultTimeout <- value
    member _.Version = version
    member _.IsCurrent(expected: int64) =
        (try sourceGuard(); true with _ -> false) && not disposed && active && version = expected && native.State = SqliteConnectionState.Open
        && native.DatabaseOwner.Generation = generation
    member _.State =
        sourceGuard()
        if active && not disposed && native.State = SqliteConnectionState.Open && native.DatabaseOwner.Generation = generation then ConnectionState.Open
        else ConnectionState.Closed
    member this.CheckSource() = sourceGuard()
    member this.Check(expected: int64) =
        sourceGuard()
        if disposed then raise (ObjectDisposedException("SqliteConnection"))
        if expected <> version || not active then raise (InvalidOperationException("The ADO connection presentation is closed or has expired."))
        native.DatabaseOwner.EnsureGeneration(generation)
    member this.Check() = this.Check(version)
    member internal _.AdmitOperation(acquireOwner: bool) =
        sourceGuard()
        if disposed then raise (ObjectDisposedException("SqliteConnection"))
        if active then native.DatabaseOwner.EnsureGeneration(generation)
        if acquireOwner then acquire()
        struct (native, native.DatabaseOwner.Generation, version)
    member internal _.IsOperationCurrent(struct (admitted: NativeSqliteConnection, admittedGeneration, admittedVersion)) =
        not returned && version = admittedVersion && Object.ReferenceEquals(native, admitted) && admitted.DatabaseOwner.Generation = admittedGeneration
        && (try sourceGuard(); true with _ -> false)
    member internal this.CompleteOperation(admitted) =
        if this.IsOperationCurrent(admitted) then
            let struct (owner, _, _) = admitted
            SQLiteToolsHandlerFaultState.raiseIfHandlerFaultRecorded owner
    member internal this.RecordOperationFault(admitted, error: exn) =
        if this.IsOperationCurrent(admitted) then
            let struct (owner, _, _) = admitted
            SQLiteToolsHandlerFaultState.tryRecordHandlerFault owner error
    member private this.Guard(action: unit -> 'T, acquireOwner: bool) : 'T =
        let admitted = this.AdmitOperation(acquireOwner)
        try
            let result = AdoErrors.convert action
            this.CompleteOperation(admitted)
            result
        with error -> this.RecordOperationFault(admitted, error); reraise()
    member this.Guard(action: unit -> 'T) : 'T = this.Guard(action, true)
    member this.GuardPresentation(action: unit -> 'T) : 'T = this.Guard(action, false)
    member this.Open() =
        sourceGuard()
        if disposed then raise (ObjectDisposedException("SqliteConnection"))
        if active then this.Check()
        else
            if standalone then
                let preferred = if returned then null else native
                if not returned then registrations <- ValueSome(native.DetachRegistrations())
                returned <- true
                let mutable acquired = false
                try
                    let struct (group, owner) = AdoConnectionPools.acquire connectionString preferred
                    pool <- group
                    native <- owner
                    returned <- false
                    acquired <- true
                    if not (isNull preferred) && not (Object.ReferenceEquals(preferred, owner)) then preferred.Dispose()
                    match registrations with
                    | ValueSome saved -> native.RestoreRegistrations(saved)
                    | ValueNone -> ()
                    native.DefaultTimeout <- defaultTimeout
                    native.Open()
                    registrations <- ValueNone
                with _ ->
                    if acquired then
                        registrations <- ValueSome native.Registrations
                        try native.DetachRegistrations() |> ignore with _ -> ()
                        returned <- true
                        try native.Dispose() with _ -> ()
                    elif not (isNull preferred) then
                        try preferred.Dispose() with _ -> ()
                    reraise()
            else
                native.Open()
            generation <- native.DatabaseOwner.Generation
            active <- true
    // Close ends the physical connection; disposing a borrowed presentation leaves its owner open.
    member _.Close() = closePresentation true
    member _.SetConnectionString(value: string) =
        if disposed then raise (ObjectDisposedException("SqliteConnection"))
        if active then raise (InvalidOperationException("The connection string cannot be changed while open."))
        if not standalone then raise (InvalidOperationException("A wrapped connection retains its native connection string."))
        if isNull value then nullArg "value"
        let options = SqliteConnectionStringBuilder(value)
        let replacement = new NativeSqliteConnection(options.ConnectionString)
        if not returned then native.Dispose()
        connectionString <- options.ConnectionString
        native <- replacement
        pool <- null
        returned <- false
        registrations <- ValueNone
        dataSource <- native.DataSource
        defaultTimeout <- options.DefaultTimeout
        native.DefaultTimeout <- defaultTimeout
        generation <- native.DatabaseOwner.Generation
        version <- version + 1L
    member this.Dispose() =
        if not disposed then
            if not standalone && not leaveOpen then
                if currentOwner() then
                    clearResources()
                    native.Dispose()
                closePresentation false
                disposed <- true
                registrations <- ValueNone
            else
                let mutable failure: exn = null
                try closePresentation standalone with error -> failure <- error
                if standalone && not active && not returned && not (isNull pool) then
                    try returnStandalone() with error -> if isNull failure then failure <- error
                disposed <- true
                try
                    if currentOwner() && (standalone || not leaveOpen) then
                        if isNull pool then native.Dispose() else pool.Return(native)
                with error -> if isNull failure then failure <- error
                registrations <- ValueNone
                if not (isNull failure) then Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw()

module internal AdoSessionAccess =
    let inline guard (session: AdoSession) action =
        if isNull (box session) then AdoErrors.convert action else
        let admitted = session.AdmitOperation(true)
        try
            let result = AdoErrors.convert action
            session.CompleteOperation(admitted)
            result
        with error -> session.RecordOperationFault(admitted, error); reraise()

/// Implemented only by public connection presentations; command association does not depend on concrete presentation types.
[<AllowNullLiteral>]
type internal IAdoSessionHost =
    abstract Session: AdoSession

module internal AdoSessionFactory =
    let createStandalone connectionString =
        let options = SqliteConnectionStringBuilder(connectionString)
        let native = new NativeSqliteConnection(options.ConnectionString)
        native.DefaultTimeout <- options.DefaultTimeout
        new AdoSession(native, false, true, ignore, null)
