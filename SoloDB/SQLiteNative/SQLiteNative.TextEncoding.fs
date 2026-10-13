namespace SoloDatabase

open System
open System.Text
open System.Runtime.CompilerServices
open Microsoft.FSharp.NativeInterop
open SQLitePCL

#nowarn "9"

/// UTF16 APIs interpret leading byte-order marks and malformed surrogate sequences.
/// Preserve the established UTF8 replacement behavior for those inputs.
module internal NativeTextEncoding =
    let copyUtf8(pointer: nativeint, bytes: int) =
        if bytes = 0 then String.Empty
        else Encoding.UTF8.GetString(NativePtr.ofNativeInt<byte> pointer, bytes)

    let copyConfiguredUtf8(text: utf8z, bytes: int) =
        let pointer = Unsafe.AsPointer(&Unsafe.AsRef(&text.GetPinnableReference())) |> NativePtr.ofVoidPtr<byte> |> NativePtr.toNativeInt
        copyUtf8(pointer, bytes)

    let emptyText = [|0uy|]
    let requiresUtf8 (value: ReadOnlySpan<char>) =
        let mutable needed = value.Length > 0 && (value.[0] = '\uFEFF' || value.[0] = '\uFFFE')
        let mutable i = 0
        while not needed && i < value.Length do
            let current = value.[i]
            if Char.IsHighSurrogate(current) then
                if i + 1 < value.Length && Char.IsLowSurrogate(value.[i + 1]) then i <- i + 2
                else needed <- true
            elif Char.IsLowSurrogate(current) then needed <- true
            else i <- i + 1
        needed
