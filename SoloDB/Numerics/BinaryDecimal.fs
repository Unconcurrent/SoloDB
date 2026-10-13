namespace SoloDatabase.Numerics

#nowarn "9"
open System
open Microsoft.FSharp.NativeInterop

[<Struct>]
type internal BinaryDecimalCoefficient = { Value: uint64; Exponent: int }

/// Exact target rounding and decimal interval selection, without text or tree conversion.
[<AbstractClass; Sealed>]
type internal BinaryDecimal private () =
    static member internal RoundBits(prefix: uint64, exponent: int64, negative: bool, width: int) =
        let precision,bias,minimum,maximum,capacity =
            match width with
            | 16 -> 11,15,-26L,4L,4
            | 32 -> 24,127,-64L,38L,7
            | 64 -> 53,1023,-342L,308L,28
            | _ -> invalidArg "width" "Unsupported binary format."
        let sign = if negative then 1UL <<< (width - 1) else 0UL
        let infinity = sign ||| (uint64(2 * bias + 1) <<< (precision - 1))
        if prefix = 0UL || exponent < minimum then sign
        elif exponent > maximum then infinity
        else
            let power = int exponent
            let topBit = BinaryDecimalScale.binaryFloor prefix
            let mutable targetExponent = topBit + ((power * 3483294) >>> 20)
            let interval = BinaryDecimalScale.integer prefix (-targetExponent) (-power)
            if interval.Floor = 0UL then targetExponent <- targetExponent - 1
            elif interval.Floor >= 2UL then targetExponent <- targetExponent + 1
            targetExponent <- max (1 - bias) targetExponent
            let compact = BinaryDecimalScale.integer prefix (precision - 1 - targetExponent) (-power)
            let mutable mantissa =
                if interval.Valid && compact.Valid then
                    compact.Floor + (if compact.Fraction > 2 || compact.Fraction = 2 && compact.Floor &&& 1UL <> 0UL then 1UL else 0UL)
                else
                    let struct(value,binary) = BinaryDecimal.RoundExact(prefix,power,precision,bias,capacity)
                    targetExponent <- binary
                    value
            if mantissa >= (1UL <<< precision) then
                mantissa <- mantissa >>> 1
                targetExponent <- targetExponent + 1
            let implicitBit = 1UL <<< (precision - 1)
            if targetExponent > bias then infinity
            elif mantissa < implicitBit then sign ||| mantissa
            else sign ||| (uint64(targetExponent + bias) <<< (precision - 1)) ||| (mantissa - implicitBit)

    static member private RoundExact(prefix,power,precision,bias,capacity) =
        let memory = NativePtr.stackalloc<uint32> (3 * capacity)
        let numerator = Span<uint32>(NativePtr.toVoidPtr memory, capacity)
        let denominator = Span<uint32>(NativePtr.toVoidPtr(NativePtr.add memory capacity), capacity)
        let temporary = Span<uint32>(NativePtr.toVoidPtr(NativePtr.add memory (2 * capacity)), capacity)
        let mutable n = BinaryDecimalWords.set numerator prefix
        let mutable d = BinaryDecimalWords.set denominator 1UL
        if power >= 0 then n <- BinaryDecimalWords.power5 numerator n power temporary
        else d <- BinaryDecimalWords.power5 denominator d (-power) temporary
        let mutable binary = BinaryDecimalWords.bitLength numerator n - BinaryDecimalWords.bitLength denominator d
        let below =
            if binary >= 0 then BinaryDecimalWords.compare numerator n denominator d binary < 0
            else BinaryDecimalWords.compare denominator d numerator n (-binary) > 0
        if below then binary <- binary - 1
        let targetExponent = max (1 - bias) (binary + power)
        if targetExponent > bias then struct(0UL,targetExponent) else
        let shift = precision - 1 - targetExponent + power
        if shift >= 0 then
            n <- BinaryDecimalWords.shift numerator n shift temporary
            temporary.CopyTo numerator
        else
            d <- BinaryDecimalWords.shift denominator d (-shift) temporary
            temporary.CopyTo denominator
        struct(BinaryDecimalWords.rounded numerator n denominator d temporary,targetExponent)

    static member private Scale(coefficient,exponent,scale) =
        let result = BinaryDecimalScale.integer coefficient exponent scale
        if result.Valid then result else
        let capacity = 28
        let memory = NativePtr.stackalloc<uint32> (3 * capacity)
        let numerator = Span<uint32>(NativePtr.toVoidPtr memory, capacity)
        let denominator = Span<uint32>(NativePtr.toVoidPtr(NativePtr.add memory capacity), capacity)
        let temporary = Span<uint32>(NativePtr.toVoidPtr(NativePtr.add memory (2 * capacity)), capacity)
        let struct(n,d) = BinaryDecimalWords.ratio coefficient exponent scale numerator denominator temporary
        let struct(value,remainder,denominatorCount) = BinaryDecimalWords.divide numerator n denominator d temporary
        let comparison = BinaryDecimalWords.compare denominator denominatorCount numerator remainder 1
        let fraction = if remainder = 0 then 0 elif comparison > 0 then 1 elif comparison = 0 then 2 else 3
        { Valid = true; Floor = value; Fraction = fraction }

    static member internal Coefficient(bits: uint64, width: int) =
        let fraction,bias,maximumDigits =
            match width with
            | 16 -> 10,15,5
            | 32 -> 23,127,9
            | 64 -> 52,1023,17
            | _ -> invalidArg "width" "Unsupported binary format."
        let field = int ((bits >>> fraction) &&& uint64(2 * bias + 1))
        let original = bits &&& ((1UL <<< fraction) - 1UL)
        let mantissa = if field = 0 then original else original ||| (1UL <<< fraction)
        if mantissa = 0UL then { Value = 0UL; Exponent = 0 } else
        let binary = (if field = 0 then 1 else field) - bias - fraction
        let struct(exact,value,scale) = BinaryDecimalScale.exactCoefficient mantissa binary
        if exact then { Value = value; Exponent = scale } else
        let topBit = BinaryDecimalScale.binaryFloor mantissa
        let floorBinary = topBit + binary
        let mutable floorDecimal = (floorBinary * 315653) >>> 20
        let result = BinaryDecimal.Scale(mantissa,binary,floorDecimal)
        if result.Floor = 0UL then floorDecimal <- floorDecimal - 1
        elif result.Floor >= 10UL then floorDecimal <- floorDecimal + 1
        let inclusive = mantissa &&& 1UL = 0UL
        let lower = mantissa * 4UL - (if original = 0UL && field > 1 then 1UL else 2UL)
        let upper = mantissa * 4UL + 2UL
        let mutable scale = floorDecimal + 1 - maximumDigits
        let result = BinaryDecimal.Scale(lower,binary - 2,scale)
        let mutable lowerCoefficient = result.Floor + (if result.Fraction <> 0 || not inclusive then 1UL else 0UL)
        let result = BinaryDecimal.Scale(upper,binary - 2,scale)
        let mutable upperCoefficient = result.Floor - (if result.Fraction = 0 && not inclusive then 1UL else 0UL)
        let mutable reducing = true
        while reducing do
            let low = lowerCoefficient / 10UL + (if lowerCoefficient % 10UL = 0UL then 0UL else 1UL)
            let high = upperCoefficient / 10UL
            if low <= high then
                lowerCoefficient <- low
                upperCoefficient <- high
                scale <- scale + 1
            else reducing <- false
        let result = BinaryDecimal.Scale(mantissa,binary,scale)
        let rounded = result.Floor + (if result.Fraction > 2 || result.Fraction = 2 && result.Floor &&& 1UL <> 0UL then 1UL else 0UL)
        let mutable value = max lowerCoefficient (min upperCoefficient rounded)
        while value % 10UL = 0UL do
            value <- value / 10UL
            scale <- scale + 1
        { Value = value; Exponent = scale }
