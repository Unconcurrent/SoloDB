namespace SoloDatabase.Numerics

open System
open System.Numerics
open System.Runtime.CompilerServices
open System.Runtime.InteropServices

[<Struct; StructLayout(LayoutKind.Sequential)>]
type private BigIntegerLayout =
    static let nativeWords =
        // This constructor reuses the runtime's cached, non-inline magnitude.
        let mutable value = BigInteger(Int32.MinValue)
        Unsafe.As<BigInteger,BigIntegerLayout>(&value).Limbs :? unativeint[]

    val Sign: int
    val Limbs: Array
    new(sign,limbs) = { Sign = sign; Limbs = limbs }
    static member NativeWords = nativeWords

// Owns the managed sign/reference layout shared by the supported BigInteger
// implementations. The backing array uses UInt32 or native-width unsigned words.
[<AbstractClass; Sealed>]
type internal BigIntegerMagnitude private () =
    static member internal Read(value: BigInteger) =
        let mutable value = value
        let source = &Unsafe.As<BigInteger,BigIntegerLayout>(&value)
        let limbs = source.Limbs
        let count =
            if isNull limbs then 1
            elif BigIntegerLayout.NativeWords && IntPtr.Size = 8 then
                let words = limbs :?> unativeint[]
                Checked.(+) (Checked.(*) (words.Length - 1) 2)
                    (if uint64 words[words.Length - 1] >>> 32 = 0UL then 1 else 2)
            else limbs.Length
        struct(source.Sign,limbs,count)

    static member internal Copy(sign: int, limbs: Array, count: int, destination: uint32[]) =
        if isNull limbs then
            destination[0] <- uint32 (if sign < 0 then -int64 sign else int64 sign)
        elif not BigIntegerLayout.NativeWords then Array.Copy(limbs,destination,count)
        else
            let words = limbs :?> unativeint[]
            if IntPtr.Size = 8 then
                for index = 0 to count / 2 - 1 do
                    let word = uint64 words[index]
                    destination[index * 2] <- uint32 word
                    destination[index * 2 + 1] <- uint32 (word >>> 32)
                if count % 2 <> 0 then destination[count - 1] <- uint32 words[count / 2]
            else
                for index = 0 to count - 1 do destination[index] <- uint32 words[index]

    static member internal Reference(value: uint32[]): byref<uint32> =
        &MemoryMarshal.GetReference(Span<uint32>(value))

    static member internal Finish(small: uint32, limbs: uint32[], count: int, negative: bool) =
        if count = 0 then BigInteger.Zero
        elif count = 1 then
            let value = int64 (if isNull limbs then small else limbs[0])
            BigInteger(if negative then -value else value)
        else
            let limbs: Array =
                if BigIntegerLayout.NativeWords then
                    if IntPtr.Size = 8 then
                        let words = Array.zeroCreate<unativeint> (count / 2 + count % 2)
                        for index = 0 to count / 2 - 1 do
                            words[index] <- unativeint (uint64 limbs[index * 2] ||| (uint64 limbs[index * 2 + 1] <<< 32))
                        if count % 2 <> 0 then words[words.Length - 1] <- unativeint limbs[count - 1]
                        words :> Array
                    else
                        let words = Array.zeroCreate<unativeint> count
                        for index = 0 to count - 1 do words[index] <- unativeint limbs[index]
                        words :> Array
                else
                    let mutable limbs = limbs
                    if limbs.Length <> count then Array.Resize(&limbs,count)
                    limbs :> Array
            let mutable source = BigIntegerLayout(1,limbs)
            let value = Unsafe.As<BigIntegerLayout,BigInteger>(&source)
            if negative then BigInteger.Negate(value) else value
