namespace SoloDatabase

open System
open System.Runtime.CompilerServices
open System.Runtime.InteropServices

module private Hashing =
    [<Literal>]
    let P1 = 2654435761u
    [<Literal>]
    let P2 = 2246822519u
    [<Literal>]
    let P3 = 3266489917u
    [<Literal>]
    let P4 = 668265263u
    [<Literal>]
    let P5 = 374761393u

    let inline private rotl (x:uint32) (r:int) : uint32 =
        (x <<< r) ||| (x >>> (32 - r))

    let inline private rotl64 (x:uint64) (r:int) : uint64 =
        (x <<< r) ||| (x >>> (64 - r))

    let inline private readU32 (s: ReadOnlySpan<byte>) (offset:int) : uint32 =
        Unsafe.ReadUnaligned<uint>(&Unsafe.AsRef(&s.[offset]))

    let inline private readU64 (s: ReadOnlySpan<byte>) (offset:int) : uint64 =
        Unsafe.ReadUnaligned<uint64>(&Unsafe.AsRef(&s.[offset]))

    let inline private round (acc:uint32) (input:uint32) : uint32 =
        let acc = acc + input * P2
        let acc = rotl acc 13
        acc * P1

    let inline private merge (acc:uint32) (v:uint32) : uint32 =
        let acc = acc ^^^ (round 0u v)
        acc * P1 + P4


    let internal xxHash32 (data: ReadOnlySpan<byte>) (seed:uint32) : uint32 =
        let len = data.Length
        let mutable i = 0
        let mutable h32 = 0u

        if len >= 16 then
            let mutable v1 = seed + P1 + P2
            let mutable v2 = seed + P2
            let mutable v3 = seed + 0u
            let mutable v4 = seed - P1

            let limit = len - 16
            while i <= limit do
                v1 <- round v1 (readU32 data i); i <- i + 4
                v2 <- round v2 (readU32 data i); i <- i + 4
                v3 <- round v3 (readU32 data i); i <- i + 4
                v4 <- round v4 (readU32 data i); i <- i + 4

            h32 <- rotl v1 1 + rotl v2 7 + rotl v3 12 + rotl v4 18
            h32 <- merge h32 v1
            h32 <- merge h32 v2
            h32 <- merge h32 v3
            h32 <- merge h32 v4
        else
            h32 <- seed + P5

        h32 <- h32 + uint32 len

        while i <= len - 4 do
            h32 <- h32 + readU32 data i * P3
            h32 <- rotl h32 17 * P4
            i <- i + 4

        while i < len do
            h32 <- h32 + uint32 data[i] * P5
            h32 <- rotl h32 11 * P1
            i <- i + 1

        h32 <- h32 ^^^ (h32 >>> 15)
        h32 <- h32 * P2
        h32 <- h32 ^^^ (h32 >>> 13)
        h32 <- h32 * P3
        h32 <- h32 ^^^ (h32 >>> 16)
        h32

    [<Literal>]
    let P1_64 = 11400714785074694791UL
    [<Literal>]
    let P2_64 = 14029467366897019727UL
    [<Literal>]
    let P3_64 = 1609587929392839161UL
    [<Literal>]
    let P4_64 = 9650029242287828579UL
    [<Literal>]
    let P5_64 = 2870177450012600261UL

    let inline private round64 (acc:uint64) (input:uint64) : uint64 =
        let acc = acc + input * P2_64
        let acc = rotl64 acc 31
        acc * P1_64

    let inline private merge64 (acc:uint64) (v:uint64) : uint64 =
        let acc = acc ^^^ (round64 0UL v)
        acc * P1_64 + P4_64

    let internal xxHash64 (data: ReadOnlySpan<byte>) (seed:uint64) : uint64 =
        let len = data.Length
        let mutable i = 0
        let mutable h64 = 0UL

        if len >= 32 then
            let mutable v1 = seed + P1_64 + P2_64
            let mutable v2 = seed + P2_64
            let mutable v3 = seed + 0UL
            let mutable v4 = seed - P1_64

            let limit = len - 32
            while i <= limit do
                v1 <- round64 v1 (readU64 data i); i <- i + 8
                v2 <- round64 v2 (readU64 data i); i <- i + 8
                v3 <- round64 v3 (readU64 data i); i <- i + 8
                v4 <- round64 v4 (readU64 data i); i <- i + 8

            h64 <- rotl64 v1 1 |> fun x -> x + rotl64 v2 7 + rotl64 v3 12 + rotl64 v4 18
            h64 <- merge64 h64 v1
            h64 <- merge64 h64 v2
            h64 <- merge64 h64 v3
            h64 <- merge64 h64 v4
        else
            h64 <- seed + P5_64

        h64 <- h64 + uint64 len

        while i <= len - 8 do
            h64 <- h64 ^^^ (round64 0UL (readU64 data i))
            h64 <- rotl64 h64 27 * P1_64 + P4_64
            i <- i + 8

        if i <= len - 4 then
            h64 <- h64 ^^^ (uint64 (readU32 data i)) * P1_64
            h64 <- rotl64 h64 23 * P2_64 + P3_64
            i <- i + 4

        while i < len do
            h64 <- h64 ^^^ (uint64 data[i]) * P5_64
            h64 <- rotl64 h64 11 * P1_64
            i <- i + 1

        h64 <- h64 ^^^ (h64 >>> 33)
        h64 <- h64 * P2_64
        h64 <- h64 ^^^ (h64 >>> 29)
        h64 <- h64 * P3_64
        h64 <- h64 ^^^ (h64 >>> 32)
        h64

    let internal xxHash32Chars (data: ReadOnlySpan<char>) (seed:uint32) : uint32 =
        xxHash32 (MemoryMarshal.AsBytes data) seed

    let internal xxHash64Chars (data: ReadOnlySpan<char>) (seed:uint64) : uint64 =
        xxHash64 (MemoryMarshal.AsBytes data) seed

    [<Literal>]
    let internal fnvPrimeA32Bit = 16777619u

    let inline internal fnvFastChars (text: ReadOnlySpan<char>) =
        let mutable hash = fnvPrimeA32Bit
        for i = 0 to text.Length - 1 do
            let ch = text[i]
            hash <- (hash ^^^ uint32 ch) * fnvPrimeA32Bit;
        hash

    let inline internal fnvFastCharsStr (text: string) =
        let mutable hash = fnvPrimeA32Bit
        for i = 0 to text.Length - 1 do
            let ch = text[i]
            hash <- (hash ^^^ uint32 ch) * fnvPrimeA32Bit;
        hash