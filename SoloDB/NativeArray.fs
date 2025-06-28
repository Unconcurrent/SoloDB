module internal NativeArray

open System
open System.Runtime.InteropServices
open Microsoft.FSharp.NativeInterop

#nowarn "9"

[<Struct>]
type NativeArray private (ptr: nativeptr<byte>, len: int32) =
    member this.Length = len
    member this.Span = Span<byte>(NativePtr.toVoidPtr ptr, int len)

    [<DefaultValue(false)>]
    val mutable private Disposed: bool

    static member Empty = 
        let mutable empty = new NativeArray(NativePtr.nullPtr, 0)
        empty.Disposed <- true
        empty

    static member internal Alloc(len: int32) =
        let mutable ptr = Marshal.AllocHGlobal (nativeint len) |> NativePtr.ofNativeInt<byte>

        new NativeArray(ptr, len)

    member this.Dispose() =
        if not this.Disposed then
            this.Disposed <- true
            Marshal.FreeHGlobal (ptr |> NativePtr.toNativeInt)

    interface IDisposable with
        member this.Dispose() =
            this.Dispose()