namespace SoloDatabase.Numerics

open System

/// Bounded unsigned arithmetic for native binary/decimal conversion.
module internal BinaryDecimalWords =
    let private powers = [|
        [| 5u |]
        [| 25u |]
        [| 625u |]
        [| 390625u |]
        [| 2264035265u; 35u |]
        [| 2242703233u; 762134875u; 1262u |]
        [| 3211403009u; 1849224548u; 3668416493u; 3913284084u; 1593091u |]
        [| 781532673u; 64985353u; 253049085u; 594863151u; 3553621484u; 3288652808u; 3167596762u; 2788392729u; 3911132675u; 590u |]
        [| 2553183233u; 3201533787u; 3638140786u; 303378311u; 1809731782u; 3477761648u; 3583367183u; 649228654u; 2915460784u; 487929380u; 1011012442u; 1677677582u; 3428152256u; 1710878487u; 1438394610u; 2161952759u; 4100910556u; 1608314830u; 349175u |]
    |]

    let length (value: Span<uint32>) count =
        let mutable count = count
        while count <> 0 && value[count - 1] = 0u do count <- count - 1
        count

    let bitLength (value: Span<uint32>) count =
        if count = 0 then 0 else
        let mutable top = value[count - 1]
        let mutable bits = 0
        while top <> 0u do
            top <- top >>> 1
            bits <- bits + 1
        (count - 1) * 32 + bits

    let set (value: Span<uint32>) word =
        value.Clear()
        value[0] <- uint32 word
        value[1] <- uint32 (word >>> 32)
        if value[1] <> 0u then 2 elif value[0] <> 0u then 1 else 0

    let power5 (value: Span<uint32>) count exponent (temporary: Span<uint32>) =
        let mutable count = count
        for bit = 0 to powers.Length - 1 do
            if exponent &&& (1 <<< bit) <> 0 then
                let factor = powers[bit]
                temporary.Clear()
                for left = 0 to count - 1 do
                    let mutable carry = 0UL
                    for right = 0 to factor.Length - 1 do
                        let position = left + right
                        let product = uint64 value[left] * uint64 factor[right] + uint64 temporary[position] + carry
                        temporary[position] <- uint32 product
                        carry <- product >>> 32
                    temporary[left + factor.Length] <- uint32 carry
                count <- length temporary (count + factor.Length)
                temporary.CopyTo value
        count

    let shift (value: Span<uint32>) count bits (destination: Span<uint32>) =
        destination.Clear()
        let whole = bits / 32
        let partial = bits % 32
        for index = 0 to count - 1 do
            let word = uint64 value[index] <<< partial
            destination[index + whole] <- destination[index + whole] ||| uint32 word
            if partial <> 0 then destination[index + whole + 1] <- uint32(word >>> 32)
        length destination (count + whole + (if partial = 0 then 0 else 1))

    let private shiftedWord (value: Span<uint32>) count index bits =
        let source = index - bits / 32
        let partial = bits % 32
        let low = if source >= 0 && source < count then uint64 value[source] <<< partial else 0UL
        let high = if partial <> 0 && source > 0 && source <= count then uint64 value[source - 1] >>> (32 - partial) else 0UL
        uint32(low ||| high)

    let compare (left: Span<uint32>) leftCount (right: Span<uint32>) rightCount bits =
        let count = max leftCount (rightCount + bits / 32 + (if bits % 32 = 0 then 0 else 1))
        let mutable result = 0
        let mutable index = count - 1
        while result = 0 && index >= 0 do
            let a = if index < leftCount then left[index] else 0u
            let b = shiftedWord right rightCount index bits
            if a < b then result <- -1 elif a > b then result <- 1
            index <- index - 1
        result

    let private subtract (left: Span<uint32>) leftCount (right: Span<uint32>) rightCount bits =
        let mutable borrow = 0UL
        for index = 0 to leftCount - 1 do
            let a = uint64 left[index]
            let b = uint64(shiftedWord right rightCount index bits) + borrow
            left[index] <- uint32(a - b)
            borrow <- if a < b then 1UL else 0UL
        length left leftCount

    let private multiplyWord (value: Span<uint32>) count factor (destination: Span<uint32>) =
        destination.Clear()
        let mutable carry = 0UL
        for index = 0 to count - 1 do
            let product = uint64 value[index] * factor + carry
            destination[index] <- uint32 product
            carry <- product >>> 32
        destination[count] <- uint32 carry
        length destination (count + 1)

    /// The admitted scaled quotient has at most 64 bits. Normalization limits
    /// each leading-word estimate to at most two downward corrections.
    let divide (numerator: Span<uint32>) numeratorCount (denominator: Span<uint32>) denominatorCount (temporary: Span<uint32>) =
        let normalization = 32 - (bitLength denominator denominatorCount - (denominatorCount - 1) * 32)
        let denominatorCount =
            let count = shift denominator denominatorCount normalization temporary
            temporary.CopyTo denominator
            count
        let mutable numeratorCount = shift numerator numeratorCount normalization temporary
        temporary.CopyTo numerator
        let mutable quotient = 0UL
        for position = min 1 (numeratorCount - denominatorCount) downto 0 do
            let top = position + denominatorCount
            let high = if top < numeratorCount then uint64 numerator[top] else 0UL
            let low = if top - 1 < numeratorCount then uint64 numerator[top - 1] else 0UL
            let divisor = uint64 denominator[denominatorCount - 1]
            let mutable digit = min 0xffffffffUL (((high <<< 32) ||| low) / divisor)
            let mutable productCount = multiplyWord denominator denominatorCount digit temporary
            while compare numerator numeratorCount temporary productCount (position * 32) < 0 do
                digit <- digit - 1UL
                productCount <- subtract temporary productCount denominator denominatorCount 0
            numeratorCount <- subtract numerator numeratorCount temporary productCount (position * 32)
            quotient <- quotient ||| (digit <<< (32 * position))
        struct(quotient, numeratorCount, denominatorCount)

    let rounded (numerator: Span<uint32>) numeratorCount (denominator: Span<uint32>) denominatorCount temporary =
        let struct(value, remainderCount, denominatorCount) = divide numerator numeratorCount denominator denominatorCount temporary
        let comparison = compare denominator denominatorCount numerator remainderCount 1
        if comparison < 0 || comparison = 0 && value &&& 1UL <> 0UL then value + 1UL else value

    let ratio coefficient binaryExponent decimalExponent (numerator: Span<uint32>) (denominator: Span<uint32>) temporary =
        let mutable numeratorCount = set numerator coefficient
        let mutable denominatorCount = set denominator 1UL
        if decimalExponent < 0 then numeratorCount <- power5 numerator numeratorCount (-decimalExponent) temporary
        else denominatorCount <- power5 denominator denominatorCount decimalExponent temporary
        let binaryShift = binaryExponent - decimalExponent
        if binaryShift >= 0 then
            numeratorCount <- shift numerator numeratorCount binaryShift temporary
            temporary.CopyTo numerator
        else
            denominatorCount <- shift denominator denominatorCount (-binaryShift) temporary
            temporary.CopyTo denominator
        struct(numeratorCount, denominatorCount)
