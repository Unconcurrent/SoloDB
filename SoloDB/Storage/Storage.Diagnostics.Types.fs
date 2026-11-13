namespace SoloDatabase

open System
open System.Collections.Generic

/// One failed coordination operation or expectation, retaining an original backend exception.
[<Sealed>]
type StorageCoordinationTestFailure internal (message: string, expected: Nullable<int>, actual: Nullable<int>, error: exn) =
    member _.Message = message
    member _.ExpectedResultCode = expected
    member _.ActualResultCode = actual
    member _.Exception = error

/// An immutable report for the requested same-process coordination scenarios.
[<Sealed>]
type StorageCoordinationTestResult internal (failures: StorageCoordinationTestFailure[]) =
    let failures = Array.AsReadOnly(failures)
    member _.Passed = failures.Count = 0
    member _.Failures = failures :> IReadOnlyList<StorageCoordinationTestFailure>
    /// Throws AggregateException on failure, retaining original backend exceptions.
    member _.ThrowIfFailed() =
        if failures.Count <> 0 then
            let errors = Array.zeroCreate<exn> failures.Count
            for i = 0 to failures.Count - 1 do
                let failure = failures.[i]
                errors.[i] <- if isNull failure.Exception then InvalidOperationException(failure.Message) else failure.Exception
            raise(AggregateException("SQLite storage coordination checks failed.", errors))
