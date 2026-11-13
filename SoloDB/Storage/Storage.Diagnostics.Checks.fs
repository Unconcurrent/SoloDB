namespace SoloDatabase

open System
open System.Runtime.InteropServices

[<Sealed>]
type private DiagnosticSession(session: ISqliteStorageCoordinationSession) =
    member _.Session = session
    member val Identity: obj = null with get,set
    member val Closed = false with get,set
    member val Slots = Array.zeroCreate<byte> 8

/// Owns sessions only; supplied byte files and the provider remain borrowed.
[<Sealed>]
type internal StorageCoordinationChecks(provider: ISqliteStorageCoordination, profile: SqliteStorageProfile) =
    let failures = ResizeArray<StorageCoordinationTestFailure>()
    let sessions = ResizeArray<DiagnosticSession>()
    // Stop dependent operations without mistaking a backend exception for our signal.
    let stopped = InvalidOperationException()
    let mutable step = "Coordination"
    let mutable expectedCode = Nullable<int>()
    let add message expected actual error =
        failures.Add(StorageCoordinationTestFailure(message, expected, actual, error))
    let exceptionFailure (error: exn) =
        let actual = match error with :? StorageNativeError as native -> Nullable(native.Code) | _ -> Nullable<int>()
        add (step + ": " + error.Message) expectedCode actual error
    let entry (session: ISqliteStorageCoordinationSession) =
        let mutable index = 0
        while index < sessions.Count && not(Object.ReferenceEquals(sessions.[index].Session, session)) do index <- index + 1
        sessions.[index]
    member _.Call(name: string, expected: Nullable<int>, action: unit -> 'T) =
        step <- name
        expectedCode <- expected
        action()
    member this.Require(name, message, condition: unit -> bool) =
        if not(this.Call(name, Nullable<int>(), condition)) then
            add (name + ": " + message) (Nullable<int>()) (Nullable<int>()) null
            raise stopped
    member _.ExpectCode(name: string, expected: int, actual: int) =
        // BUSY extensions retain contention meaning; other extended errors remain exact.
        if actual <> expected && not(expected = 5 && actual &&& 255 = 5) then
            add ($"{name}: expected SQLite result {expected}; received {actual}.") (Nullable expected) (Nullable actual) null
            raise stopped
    member this.Code(name, expected, action: unit -> int) =
        let actual = this.Call(name, Nullable expected, action)
        this.ExpectCode(name, expected, actual)
    member this.Open(name, main: ISqliteStorageFile) =
        let session = this.Call(name, Nullable<int>(), fun () -> provider.Open(main, profile))
        this.Require(name, "The provider returned no session.", fun () -> not(isNull session))
        this.Require(name, "The provider reused a live session instead of returning a distinct session.", fun () ->
            let mutable distinct = true
            for peer in sessions do if Object.ReferenceEquals(peer.Session, session) then distinct <- false
            distinct)
        let owned = DiagnosticSession(session)
        sessions.Add(owned)
        let identity = this.Call(name + " domain identity", Nullable<int>(), fun () -> session.DomainIdentity)
        this.Require(name, "A session must have a nonnull coordination-domain identity.", fun () -> not(isNull identity))
        owned.Identity <- identity
        this.Require(name, "Coordination-domain identity changed within one session.", fun () -> Object.Equals(identity, session.DomainIdentity))
        this.Require(name, "The supplied views do not share a compatible coordination domain.", fun () -> Object.Equals(sessions.[0].Identity, identity))
        session
    member this.Close(name, session) =
        let owned = entry session
        let before = failures.Count
        this.ReleaseSlots(owned)
        owned.Closed <- true
        this.Code(name, 0, session.Close)
        if failures.Count <> before then raise stopped
    member private this.ReleaseSlots(owned: DiagnosticSession) =
        // Native SQLite clients release WAL grants before xShmUnmap. Keep that
        // ordering even when a later scenario step fails.
        for slot = 0 to 7 do
            let held = owned.Slots.[slot]
            if held <> 0uy then
                owned.Slots.[slot] <- 0uy
                step <- $"Release owned WAL slot {slot}"
                expectedCode <- Nullable 0
                try
                    let result = owned.Session.LockSharedMemory(slot, 1, if held = 1uy then 5 else 9)
                    if result <> 0 then add ($"{step}: expected SQLite result 0; received {result}.") (Nullable 0) (Nullable result) null
                with error -> exceptionFailure error
    member this.Slot(name, session: ISqliteStorageCoordinationSession, offset, count, flags, expected) =
        let actual = this.Call(name, Nullable expected, fun () -> session.LockSharedMemory(offset, count, flags))
        if actual = 0 then
            let owned = entry session
            for slot = offset to offset + count - 1 do
                owned.Slots.[slot] <- if flags &&& 1 <> 0 then 0uy elif flags &&& 8 <> 0 then 2uy else 1uy
        this.ExpectCode(name, expected, actual)
    member this.Unmap(name, session: ISqliteStorageCoordinationSession, delete) =
        let before = failures.Count
        this.ReleaseSlots(entry session)
        if failures.Count <> before then raise stopped
        this.Code(name, 0, fun () -> session.UnmapSharedMemory(delete))
    member this.Reserved(name, session: ISqliteStorageCoordinationSession, expected: bool) =
        let result = this.Call(name, Nullable 0, session.CheckReservedLock)
        this.ExpectCode(name, 0, result.ResultCode)
        this.Require(name, $"Expected reserved-lock presence {expected}; received {result.IsReserved}.", fun () -> result.IsReserved = expected)
    member this.LockState(name, session: ISqliteStorageCoordinationSession, expected: int) =
        this.Call(name, Nullable<int>(), fun () ->
            let output = Marshal.AllocHGlobal(4)
            try
                Marshal.WriteInt32(output, -1)
                let code = session.FileControl(1, output)
                if code = 0 then
                    this.Require(name, $"Expected pager lock level {expected}; received {Marshal.ReadInt32(output)}.", fun () -> Marshal.ReadInt32(output) = expected)
                else this.ExpectCode(name, 12, code)
            finally Marshal.FreeHGlobal(output))
    member this.Map(name, session: ISqliteStorageCoordinationSession, region, extend) =
        let result = this.Call(name, Nullable 0, fun () -> session.MapSharedMemory(region, 32768, extend))
        this.ExpectCode(name, 0, result.ResultCode)
        this.Require(name, "An existing or extended WAL region has no mapped address.", fun () -> result.Address <> 0n)
        result.Address
    member this.Run(action: unit -> unit) =
        let previous = failures.Count
        sessions.Clear()
        try
            try action()
            with error -> if not(Object.ReferenceEquals(error, stopped)) then exceptionFailure error
        finally
            for i = sessions.Count - 1 downto 0 do
                let owned = sessions.[i]
                if not owned.Closed then
                    this.ReleaseSlots(owned)
                    owned.Closed <- true
                    step <- "Close remaining session"
                    expectedCode <- Nullable 0
                    try
                        let result = owned.Session.Close()
                        if result <> 0 then add ($"{step}: expected SQLite result 0; received {result}.") (Nullable 0) (Nullable result) null
                    with error -> exceptionFailure error
        failures.Count = previous
    member _.Result = StorageCoordinationTestResult(failures.ToArray())
