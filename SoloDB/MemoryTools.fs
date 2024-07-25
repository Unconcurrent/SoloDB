module MemoryTools

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
