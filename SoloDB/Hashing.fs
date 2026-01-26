namespace SoloDatabase

open System
open System.Runtime.CompilerServices

module private Hashing =
    let inline private rotl (x:uint32) (r:int) : uint32 =
        (x <<< r) ||| (x >>> (32 - r))

    let inline private readU32 (s: ReadOnlySpan<byte>) (offset:int) : uint32 =
        Unsafe.ReadUnaligned<uint>(&Unsafe.AsRef(&s.[offset]))

    let inline private round (acc:uint32) (input:uint32) : uint32 =
        let P1 = 2654435761u
        let P2 = 2246822519u
        let acc = acc + input * P2
        let acc = rotl acc 13
        acc * P1

    let inline private merge (acc:uint32) (v:uint32) : uint32 =
        let P1 = 2654435761u
        let P4 = 668265263u
        let acc = acc ^^^ (round 0u v)
        acc * P1 + P4

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

