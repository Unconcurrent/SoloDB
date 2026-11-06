namespace SoloDatabase

open System
open System.IO
open System.Threading

/// Owns one leased cursor and preserves the supplied Stream's flushing behavior.
[<Sealed>]
type internal StreamStorageFile(stream: Stream, leaveOpen: bool, writable: bool) =
    let state =
        if isNull stream then nullArg "stream"
        StorageObjects.get stream
    let gate = state.Gate
    let mutable disposed = false
    let mutable zeros = Array.empty<byte>
#if NETSTANDARD2_0
    let mutable buffer = Array.empty<byte>
    let ensureBuffer length =
        if buffer.Length < length then buffer <- Array.zeroCreate length
#endif
    do
        lock gate (fun () ->
            if state.Closing then raise(ObjectDisposedException("stream"))
            if not stream.CanSeek || not stream.CanRead || (writable && not stream.CanWrite) then
                invalidArg "stream" "Storage requires a readable, seekable Stream and write access for a writable profile."
            state.References <- state.References + 1)
    let failure (error: exn) =
        let kind = match error with :? StorageIOException as error -> error.Kind | _ -> StorageFailureKind.IoError
        StorageFailure(kind, error)
    let check () = if disposed then raise(ObjectDisposedException("SqliteStorage"))
    let readOnly () = StorageFailure(StorageFailureKind.ReadOnly, null)
    let length () =
        let value = stream.Length
        if value < 0L then raise(InvalidDataException("The Stream returned a negative length."))
        value
    let zeroFrom start ending =
        if start < ending then
            if zeros.Length = 0 then zeros <- Array.zeroCreate 8192
            stream.Position <- start
            let mutable position = start
            while position < ending do
                let count = int(min (int64 zeros.Length) (ending - position))
#if NETSTANDARD2_0
                stream.Write(zeros, 0, count)
#else
                stream.Write(ReadOnlySpan<byte>(zeros, 0, count))
#endif
                position <- position + int64 count
    member internal _.StreamIdentity = stream
    member internal _.Gate = gate
    interface ISqliteStorageFile with
        member _.ReadAt(position, destination) =
            Monitor.Enter gate
            try
                try
                    check()
                    StorageValidation.bounds position destination.Length
                    if destination.IsEmpty || position >= length() then StorageReadResult(0, null)
                    else
                        stream.Position <- position
#if NETSTANDARD2_0
                        ensureBuffer destination.Length
                        let count = stream.Read(buffer, 0, destination.Length)
                        if count < 0 || count > destination.Length then
                            raise(InvalidDataException("The Stream returned an invalid read count."))
                        Span<byte>(buffer, 0, count).CopyTo(destination)
#else
                        let count = stream.Read(destination)
                        if count < 0 || count > destination.Length then
                            raise(InvalidDataException("The Stream returned an invalid read count."))
#endif
                        StorageReadResult(count, null)
                with error -> StorageReadResult(0, failure error)
            finally Monitor.Exit gate
        member _.WriteAt(position, source) =
            Monitor.Enter gate
            try
                try
                    check()
                    StorageValidation.bounds position source.Length
                    if not writable then readOnly()
                    else
                        if not source.IsEmpty then
                            zeroFrom (length()) position
                            stream.Position <- position
#if NETSTANDARD2_0
                            ensureBuffer source.Length
                            source.CopyTo(Span<byte>(buffer, 0, source.Length))
                            stream.Write(buffer, 0, source.Length)
#else
                            stream.Write(source)
#endif
                        null
                with error -> failure error
            finally Monitor.Exit gate
        member _.GetLength() =
            Monitor.Enter gate
            try
                try check(); StorageLengthResult(length(), null)
                with error -> StorageLengthResult(0L, failure error)
            finally Monitor.Exit gate
        member _.SetLength(length) =
            Monitor.Enter gate
            try
                try
                    check()
                    if length < 0L then raise(ArgumentOutOfRangeException("length"))
                    if not writable then readOnly()
                    else
                        let previous = stream.Length
                        if previous < 0L then raise(InvalidDataException("The Stream returned a negative length."))
                        stream.SetLength(length)
                        zeroFrom previous length
                        null
                with error -> failure error
            finally Monitor.Exit gate
        member _.Flush() =
            Monitor.Enter gate
            try
                try
                    check()
                    stream.Flush()
                    match stream with
                    | :? FileStream as file when file.CanWrite -> file.Flush(true)
                    | _ -> ()
                    null
                with error -> failure error
            finally Monitor.Exit gate
    interface IDisposable with
        member _.Dispose() =
            Monitor.Enter gate
            try
                if not disposed then
                    disposed <- true
                    zeros <- Array.empty
#if NETSTANDARD2_0
                    buffer <- Array.empty
#endif
                    state.CloseRequested <- state.CloseRequested || not leaveOpen
                    state.References <- state.References - 1
                    if state.References = 0 && state.CloseRequested then
                        state.CloseRequested <- false
                        state.Closing <- true
                        try stream.Dispose()
                        finally state.Closing <- false
            finally Monitor.Exit gate
