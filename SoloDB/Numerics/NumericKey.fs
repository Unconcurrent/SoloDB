namespace SoloDatabase.Numerics

open System
open System.Numerics

[<Struct>]
type internal NumericKeyDigits =
    { Chunks: uint32[]
      PairOffset: int64
      PairCount: int
      Header: int
      ExponentMagnitude: uint64
      ExponentWidth: int
      Negative: bool }

/// Canonical base100 keys. Coefficient chunks are numerical working storage.
module internal NumericKey =
    let private radix = BigInteger(100000000)
    let private pairPowers = [| 1u; 100u; 10000u; 1000000u |]
    let private exponentPowers = Array.init 10 (fun n -> pown 100UL n)

    let private invalid () = raise(FormatException("The numerical key is not canonical."))

    let private symbol index =
        if index < 2 then char (32 + index)
        elif index < 59 then char (33 + index)
        elif index < 93 then char (34 + index)
        else char (161 + index - 93)

    let private digit value =
        let unit = int value
        if unit >= 32 && unit <= 33 then unit - 32
        elif unit >= 35 && unit <= 91 then unit - 33
        elif unit >= 93 && unit <= 126 then unit - 34
        elif unit >= 161 && unit <= 167 then unit - 161 + 93
        else invalid ()

    let private exponentMagnitude exponent =
        if exponent < 0L then uint64 (-(exponent + 1L)) + 1UL else uint64 exponent

    let private exponentWidth magnitude =
        let mutable magnitude = magnitude
        let mutable count = 1
        while magnitude >= 100UL do
            magnitude <- magnitude / 100UL
            count <- count + 1
        count

    let private prepareRadix (value: BigIntegerRadixDigits) (exponent: BigInteger) =
        if value.Count = 0 then
            { Chunks = null; PairOffset = 0L; PairCount = 0; Header = 44; ExponentMagnitude = 0UL; ExponentWidth = 0; Negative = false }
        else
            let chunks = value.Chunks
            let mutable used = value.Count
            if not exponent.IsEven then
                let mutable carry = 0u
                for index = 0 to used - 1 do
                    let word = chunks[index] * 10u + carry
                    chunks[index] <- word % 100000000u
                    carry <- word / 100000000u
                if carry <> 0u then
                    chunks[used] <- carry
                    used <- used + 1
            let top = chunks[used - 1]
            let leadingPairs = if top < 100u then 1 elif top < 10000u then 2 elif top < 1000000u then 3 else 4
            let pairs = int64 (used - 1) * 4L + int64 leadingPairs
            let mutable chunk = 0
            while chunks[chunk] = 0u do chunk <- chunk + 1
            let mutable offset = int64 chunk * 4L
            let mutable low = chunks[chunk]
            while low % 100u = 0u do
                low <- low / 100u
                offset <- offset + 1L
            let halfExponent =
                let quotient = exponent / 2I
                if exponent.Sign < 0 && not exponent.IsEven then quotient - BigInteger.One else quotient
            let exponent = int64 (halfExponent + BigInteger(pairs - 1L))
            let negative = value.Sign < 0
            let extended = exponent < -20L || exponent >= 20L
            let magnitude = if extended then exponentMagnitude exponent else 0UL
            let width = if extended then exponentWidth magnitude else 0
            let header =
                if not extended then (if negative then 22 - int exponent else 66 + int exponent)
                elif negative then (if exponent >= 20L then 2 else 43)
                else (if exponent >= 20L then 86 else 45)
            { Chunks = chunks; PairOffset = offset; PairCount = Checked.int (pairs - offset)
              Header = header; ExponentMagnitude = magnitude; ExponentWidth = width; Negative = negative }

    let prepare (value: ExactNumber) = prepareRadix (BigIntegerRadix.Stored value.Coefficient) value.Exponent
    let prepareInteger (value: BigInteger) = prepareRadix (BigIntegerRadix.Stored value) BigInteger.Zero

    let length (value: NumericKeyDigits) =
        if value.PairCount = 0 then 1
        else
            let header = if value.ExponentWidth > 0 then 2 + value.ExponentWidth else 1
            Checked.(+) header value.PairCount

    /// Reads prepared symbols without producing an intermediate key string.
    let unitAt (value: NumericKeyDigits) index =
        if value.PairCount = 0 then 'M'
        else
            let width = value.ExponentWidth
            let headerLength = if width > 0 then width + 2 else 1
            let complementExponent = value.Header = 2 || value.Header = 45
            if index = 0 then symbol value.Header
            elif index < headerLength then
                let original =
                    if index = 1 then width
                    else int ((value.ExponentMagnitude / exponentPowers[width - index + 1]) % 100UL)
                symbol (if complementExponent then 99 - original else original)
            else
                let position = index - headerLength
                let fromEnd = value.PairCount - position - 1
                let chunkPair = int64 fromEnd + value.PairOffset
                let pair = int ((value.Chunks[int (chunkPair / 4L)] / pairPowers[int (chunkPair % 4L)]) % 100u)
                symbol (if not value.Negative then pair elif fromEnd = 0 then 100 - pair else 99 - pair)

    let encode value =
        let prepared = prepare value
        if prepared.PairCount = 0 then "M"
        else
            let chars = Array.zeroCreate<char> (length prepared)
            for index = 0 to chars.Length - 1 do chars[index] <- unitAt prepared index
            String(chars)

    let utf8Length prepared =
        let symbols = length prepared
        let mutable count = symbols
        for index = 0 to symbols - 1 do
            if int(unitAt prepared index) >= 128 then count <- Checked.(+) count 1
        count

    let decode (text: string) =
        if text.Length = 0 then invalid ()
        let header = digit text[0]
        if header = 44 then
            if text.Length <> 1 then invalid ()
            ExactNumber.zero
        else
            let negative = header >= 2 && header <= 43
            let mutable offset = 1
            let mutable exponent = BigInteger.Zero
            if header >= 3 && header <= 42 then exponent <- BigInteger(22 - header)
            elif header >= 46 && header <= 85 then exponent <- BigInteger(header - 66)
            elif header = 2 || header = 43 || header = 45 || header = 86 then
                if text.Length < 2 then invalid ()
                let complemented = header = 2 || header = 45
                let lengthCode = digit text[1]
                let count = if complemented then 99 - lengthCode else lengthCode
                if count = 0 || text.Length - 2 < count then invalid ()
                offset <- 2 + count
                for index = 2 to offset - 1 do
                    let encoded = digit text[index]
                    let original = if complemented then 99 - encoded else encoded
                    if index = 2 && original = 0 then invalid ()
                    exponent <- exponent * 100I + BigInteger original
                if header = 43 || header = 45 then exponent <- -exponent
                if exponent >= -20I && exponent <= 19I then invalid ()
                if (header = 2 || header = 86) && exponent < 20I then invalid ()
                if (header = 43 || header = 45) && exponent > -21I then invalid ()
            else invalid ()
            if offset = text.Length then invalid ()
            let pairs = text.Length - offset
            let mutable coefficient = BigInteger.Zero
            let mutable chunk = 0u
            let mutable count = 0
            for index = offset to text.Length - 1 do
                let encoded = digit text[index]
                if negative then
                    if index = text.Length - 1 && encoded = 0 then invalid ()
                    if index = offset && pairs > 1 && encoded = 99 then invalid ()
                elif (index = offset || index = text.Length - 1) && encoded = 0 then invalid ()
                let original = if not negative then encoded elif index = text.Length - 1 then 100 - encoded else 99 - encoded
                chunk <- chunk * 100u + uint32 original
                count <- count + 1
                if count = 4 then
                    coefficient <- coefficient * radix + BigInteger chunk
                    chunk <- 0u
                    count <- 0
            if count > 0 then coefficient <- coefficient * BigInteger(pairPowers[count - 1] * 100u) + BigInteger chunk
            let exponent = BigInteger(int64 exponent)
            ExactNumber.create (if negative then -coefficient else coefficient) (2I * (exponent + 1I - BigInteger pairs))
