module MemoryTools

open System.Text

#nowarn "9"

open System
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open Microsoft.FSharp.NativeInterop
open System.Buffers

type AllocatorOf<'T when 'T: unmanaged> =
    static member New(count: uint) =        
        let ptr = NativeMemory.AllocZeroed (unativeint count, unativeint sizeof<'T>)
        new NativeMemoryHandle<'T>(NativePtr.ofVoidPtr ptr, int count)
    static member internal Free(ptr: nativeptr<'T>) =
        NativeMemory.Free (NativePtr.toVoidPtr ptr)
    

and [<Struct>] NativeMemoryHandle<'T when 'T: unmanaged> =    
    val private ptr: nativeptr<'T>
    val mutable private disposed: bool
    val mutable private len: int

    new(ptr, len) =
        { ptr = ptr; disposed = false; len = len}


    member private this.ThrowIfDisposed() =
        if this.disposed then raise(ObjectDisposedException("MemoryHandle"))

    member this.Span =
        this.ThrowIfDisposed()
        Span<'T>((NativePtr.toVoidPtr this.ptr), this.len)

    member this.Length = 
        this.ThrowIfDisposed()
        this.len

    member this.DownsizeTo(newLen) =
        this.ThrowIfDisposed()
        if newLen <= this.len then
            this.len <- newLen
        else failwithf "New size larger than current."

    member this.Dispose() =
        if not this.disposed then
            AllocatorOf<'T>.Free (this.ptr)
            this.disposed <- true
        ()

    interface IDisposable with
        override this.Dispose() =
            this.Dispose()


// Offsets for metadata
let private capacityOffset = 0
let private countOffset = sizeof<int>
let private nextFreeOffsetOffset = 2 * sizeof<int>

[<Struct; IsByRefLike>]
type StackDictionary = 
    val mutable memory: Span<byte>
    val mutable data: Span<byte>

    new(memory: Span<byte>) =
        let mutable span = memory.Slice(capacityOffset, sizeof<int>)
        BitConverter.TryWriteBytes(span, memory.Length) |> ignore

        {
            memory = memory
            data = memory.Slice (3 * sizeof<int>)
        }

    member private this.Capacity
        with get() = BitConverter.ToInt32(this.memory.Slice(capacityOffset, sizeof<int>))
    
    member private this.Count
        with get() = BitConverter.ToInt32(this.memory.Slice(countOffset, sizeof<int>))
        and set(value: int) = BitConverter.TryWriteBytes(this.memory.Slice(countOffset, sizeof<int>), value) |> ignore

    member private this.NextFreeOffset
        with get() = BitConverter.ToInt32(this.memory.Slice(nextFreeOffsetOffset, sizeof<int>))
        and set(value: int) = BitConverter.TryWriteBytes(this.memory.Slice(nextFreeOffsetOffset, sizeof<int>), value) |> ignore

    member this.Add(struct (key: string, value: int)) =
        let keyLength = Encoding.UTF8.GetByteCount(key)
        let requiredSize = sizeof<int> + sizeof<int> + keyLength

        if this.NextFreeOffset + requiredSize > this.Capacity then
            raise (InvalidOperationException("StackDictionary is full."))

        this.WriteInt(struct (this.NextFreeOffset, keyLength))
        let mutable offset = this.NextFreeOffset + sizeof<int>
        this.WriteInt(struct (offset, value))
        offset <- offset + sizeof<int>
        let copyCount = Encoding.UTF8.GetBytes(key, this.data.Slice(offset, keyLength))

        this.NextFreeOffset <- this.NextFreeOffset + requiredSize
        this.Count <- this.Count + 1
        ()

    member this.TryGetValue(key: string) =
        let keyLength = Encoding.UTF8.GetByteCount(key)
        let keyBytes = Span<byte>((NativePtr.stackalloc<byte> keyLength) |> NativePtr.toVoidPtr, keyLength)
        let copyCount = Encoding.UTF8.GetBytes(key, keyBytes)

        let mutable offset = 0
        let mutable found = false
        let mutable value = 0
        let mutable i = 0

        while not found && i < this.Count do
            let storedKeyLength = this.ReadInt(offset)
            offset <- offset + sizeof<int>
            let storedValue = this.ReadInt(offset)
            offset <- offset + sizeof<int>

            if storedKeyLength = keyLength then
                let storedKeySpan = this.data.Slice(offset, storedKeyLength)
                if storedKeySpan.SequenceEqual keyBytes then
                    value <- storedValue
                    found <- true

            offset <- offset + storedKeyLength
            i <- i + 1

        if found then ValueSome value else ValueNone

    member private this.WriteInt(struct (offset: int, value: int)) =
        BitConverter.TryWriteBytes(this.data.Slice(offset, sizeof<int>), value) |> ignore

    member private this.ReadInt(offset: int) =
        BitConverter.ToInt32(this.data.Slice(offset, sizeof<int>))