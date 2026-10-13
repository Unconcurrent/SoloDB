namespace SoloDatabase.Numerics

open System
open System.Numerics
open System.Runtime.CompilerServices

[<Struct>]
type internal BigIntegerRadixDigits =
    { Chunks: uint32[]
      Count: int
      Sign: int }

/// Fixed decimal-radix preparation borrows immutable managed magnitude limbs.
[<AbstractClass; Sealed>]
type internal BigIntegerRadix private () =
    static member inline private Prepare(value: BigInteger, radix: uint64, digits: int, extra: int) =
        let mutable value = value
        let source = &Unsafe.As<BigInteger,BigIntegerLayout>(&value)
        let sign = source.Sign
        if sign = 0 then { Chunks = null; Count = 0; Sign = 0 }
        else
            let limbs = source.Limbs
            let sourceCount = if isNull limbs then 1 else limbs.Length
            let capacity = Checked.int (Checked.(+) (int64 sourceCount * 10L / int64 digits + 1L) (int64 extra))
            let chunks = Array.zeroCreate<uint32> capacity
            if isNull limbs then
                chunks[0] <- uint32 (if sign < 0 then -int64 sign else int64 sign)
            else Array.Copy(limbs, chunks, sourceCount)
            let mutable remaining = sourceCount
            let mutable used = 0
            // Binary quotient occupies the front; completed remainders occupy the back.
            // 1E9 >= 2^29 and 1E8 >= 2^26 bound their combined live word count
            // below the existing ten-decimal-digits-per-source-word capacity estimate.
            while remaining <> 0 do
                let mutable carry = 0UL
                for index = remaining - 1 downto 0 do
                    let current = (carry <<< 32) ||| uint64 chunks[index]
                    let quotient = current / radix
                    chunks[index] <- uint32 quotient
                    carry <- current - quotient * radix
                while remaining <> 0 && chunks[remaining - 1] = 0u do
                    remaining <- remaining - 1
                chunks[capacity - 1 - used] <- uint32 carry
                used <- used + 1
            Array.Reverse(chunks, capacity - used, used)
            Array.Copy(chunks, capacity - used, chunks, 0, used)
            Array.Clear(chunks, used, capacity - used)
            { Chunks = chunks; Count = used; Sign = sign }

    static member internal Raw(value: BigInteger) = BigIntegerRadix.Prepare(value,1000000000UL,9,0)
    static member internal Stored(value: BigInteger) = BigIntegerRadix.Prepare(value,100000000UL,8,1)
