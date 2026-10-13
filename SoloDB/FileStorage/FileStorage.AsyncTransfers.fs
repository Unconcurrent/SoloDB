namespace SoloDatabase

open System
open System.Buffers
open System.IO
open System.Threading

/// A synchronous callback owns its active SQLite statement and reentrant dispatch lock.
/// External I/O may finish elsewhere; database stream work stays on the callback thread.
module internal FileStorageAsyncTransfers =
    let copyOnCallbackThread (source: Stream) (destination: Stream) bufferSize =
        if isNull destination then nullArg (nameof destination)
        if not source.CanRead then
            if not source.CanWrite then raise (ObjectDisposedException(nameof source))
            raise (NotSupportedException("The source stream does not support reading."))
        if not destination.CanWrite then
            if not destination.CanRead then raise (ObjectDisposedException(nameof destination))
            raise (NotSupportedException("The destination stream does not support writing."))
        let buffer = ArrayPool<byte>.Shared.Rent(bufferSize)
        try
            let mutable copying = true
            while copying do
                let read =
                    #if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
                    source.ReadAsync(buffer.AsMemory(0, bufferSize), CancellationToken.None).AsTask().GetAwaiter().GetResult()
                    #else
                    source.ReadAsync(buffer, 0, bufferSize, CancellationToken.None).GetAwaiter().GetResult()
                    #endif
                if read = 0 then copying <- false
                else
                    #if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
                    destination.WriteAsync(buffer.AsMemory(0, read), CancellationToken.None).AsTask().GetAwaiter().GetResult()
                    #else
                    destination.WriteAsync(buffer, 0, read, CancellationToken.None).GetAwaiter().GetResult()
                    #endif
        finally ArrayPool<byte>.Shared.Return(buffer)
