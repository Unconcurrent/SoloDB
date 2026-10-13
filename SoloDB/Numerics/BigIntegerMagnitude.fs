namespace SoloDatabase.Numerics

open System
open System.Numerics
open System.Runtime.CompilerServices
open System.Runtime.InteropServices

[<Struct; StructLayout(LayoutKind.Sequential)>]
type internal BigIntegerLayout =
    val Sign: int
    val Limbs: uint32[]
    new(sign,limbs) = { Sign = sign; Limbs = limbs }

/// Transfers completed native magnitude storage into an immutable result.
[<AbstractClass; Sealed>]
type internal BigIntegerMagnitude private () =
    static member internal Reference(value: uint32[]): byref<uint32> =
        &MemoryMarshal.GetReference(Span<uint32>(value))

    static member internal Finish(small: uint32, limbs: uint32[], count: int, negative: bool) =
        if count = 0 then BigInteger.Zero
        elif count = 1 then
            let value = int64 (if isNull limbs then small else limbs[0])
            BigInteger(if negative then -value else value)
        else
            let mutable limbs = limbs
            if limbs.Length <> count then Array.Resize(&limbs,count)
            let mutable source = BigIntegerLayout(1,limbs)
            let value = Unsafe.As<BigIntegerLayout,BigInteger>(&source)
            if negative then BigInteger.Negate(value) else value
