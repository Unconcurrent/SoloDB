namespace SoloDatabase.Numerics

open System
open System.Numerics
open System.Runtime.CompilerServices

/// A finite value; the exponent applies to the exact signed coefficient.
[<Struct>]
type internal ExactNumber =
    { Coefficient: BigInteger
      Exponent: BigInteger }

module internal ExactNumber =
    let private ten = BigInteger(10)
    let private billion = BigInteger(1000000000)
    let private wordMask = BigInteger(UInt32.MaxValue)
    let private maximum = (BigInteger.One <<< 96) - BigInteger.One
    let private powers = Array.init 29 (fun scale -> BigInteger.Pow(ten,scale))
    let zero = { Coefficient = BigInteger.Zero; Exponent = BigInteger.Zero }

    let create (coefficient: BigInteger) (exponent: BigInteger) =
        if coefficient.IsZero then zero
        else
            let mutable coefficient = coefficient
            let mutable exponent = exponent
            let mutable remainder = BigInteger.Zero
            let mutable quotient = BigInteger.DivRem(coefficient,billion,&remainder)
            while remainder.IsZero do
                coefficient <- quotient
                exponent <- exponent + 9I
                quotient <- BigInteger.DivRem(coefficient,billion,&remainder)
            quotient <- BigInteger.DivRem(coefficient,ten,&remainder)
            while remainder.IsZero do
                coefficient <- quotient
                exponent <- exponent + BigInteger.One
                quotient <- BigInteger.DivRem(coefficient,ten,&remainder)
            { Coefficient = coefficient; Exponent = exponent }

    let ofBigInteger value = create value BigInteger.Zero

    let ofDecimal (value: decimal) =
        let mutable value = value
        let source = &Unsafe.As<decimal,byte>(&value)
        let flags = Unsafe.ReadUnaligned<uint32>(&source)
        let high = Unsafe.ReadUnaligned<uint32>(&Unsafe.Add<byte>(&source,4))
        let low = Unsafe.ReadUnaligned<uint64>(&Unsafe.Add<byte>(&source,8))
        let coefficient = BigInteger(low) + (BigInteger(high) <<< 64)
        create (if flags >>> 31 = 0u then coefficient else -coefficient) (-BigInteger ((flags >>> 16) &&& 255u))

    let rec private power exponent =
        if exponent <= BigInteger Int32.MaxValue then BigInteger.Pow(ten,int exponent)
        else
            let half = power (exponent >>> 1)
            let square = half * half
            if exponent.IsEven then square else square * ten

    let private digits coefficient =
        let mutable value = BigInteger.Abs coefficient
        let mutable count = 0L
        while value >= billion do
            value <- value / billion
            count <- Checked.(+) count 9L
        let mutable tail = uint32 value
        while tail <> 0u do
            tail <- tail / 10u
            count <- count + 1L
        count

    let negate value = { value with Coefficient = -value.Coefficient }
    let abs value = { value with Coefficient = BigInteger.Abs value.Coefficient }

    let add left right =
        if left.Coefficient.IsZero then right
        elif right.Coefficient.IsZero then left
        else
            let exponent = min left.Exponent right.Exponent
            create
                (left.Coefficient * power (left.Exponent - exponent)
                 + right.Coefficient * power (right.Exponent - exponent)) exponent

    let subtract left right = add left (negate right)

    let multiply left right =
        if left.Coefficient.IsZero || right.Coefficient.IsZero then zero
        else create (left.Coefficient * right.Coefficient) (left.Exponent + right.Exponent)

    let compare left right =
        if left.Coefficient.Sign <> right.Coefficient.Sign then compare left.Coefficient.Sign right.Coefficient.Sign
        elif left.Coefficient.IsZero then 0
        elif left.Exponent = right.Exponent then left.Coefficient.CompareTo right.Coefficient
        else
            let leftDigits = digits left.Coefficient
            let rightDigits = digits right.Coefficient
            let position = (left.Exponent + BigInteger leftDigits).CompareTo(right.Exponent + BigInteger rightDigits)
            if position <> 0 then left.Coefficient.Sign * position
            else
                let exponent = min left.Exponent right.Exponent
                (left.Coefficient * power (left.Exponent - exponent)).CompareTo(
                    right.Coefficient * power (right.Exponent - exponent))

    let ratio value =
        if value.Exponent.Sign >= 0 then struct(value.Coefficient * power value.Exponent,BigInteger.One)
        else struct(value.Coefficient,power (-value.Exponent))

    /// Rounds once from the original ratio, including the exact final remainder.
    let roundRatio (numerator: BigInteger) (denominator: BigInteger) mode =
        if denominator.IsZero then raise(DivideByZeroException())
        let negative = numerator.Sign * denominator.Sign < 0
        let denominator = BigInteger.Abs denominator
        let mutable remainder = BigInteger.Zero
        let quotient = BigInteger.DivRem(BigInteger.Abs numerator,denominator,&remainder)
        let increment =
            match int mode with
            | 0 ->
                let halfway = (remainder <<< 1).CompareTo denominator
                halfway > 0 || halfway = 0 && not quotient.IsEven
            | 1 -> (remainder <<< 1) >= denominator
            | 2 -> false
            | 3 -> negative && not remainder.IsZero
            | 4 -> not negative && not remainder.IsZero
            | _ -> raise(ArgumentException("The rounding mode is not supported.",nameof mode))
        let magnitude = if increment then quotient + BigInteger.One else quotient
        if negative then -magnitude else magnitude

    let private quantizeDecimalRatio (numerator: BigInteger) (denominator: BigInteger) =
        if denominator.IsZero then raise(DivideByZeroException())
        let mutable scale = 28
        let mutable rounded = roundRatio (numerator * powers[scale]) denominator MidpointRounding.ToEven
        while BigInteger.Abs rounded > maximum && scale > 0 do
            scale <- scale - 1
            rounded <- roundRatio (numerator * powers[scale]) denominator MidpointRounding.ToEven
        if BigInteger.Abs rounded > maximum then raise(OverflowException())
        struct(rounded,scale)

    let private nativeDecimal (coefficient: BigInteger) scale =
        let magnitude = BigInteger.Abs coefficient
        let low = uint32 (magnitude &&& wordMask)
        let middle = uint32 ((magnitude >>> 32) &&& wordMask)
        let high = uint32 (magnitude >>> 64)
        Decimal(int low,int middle,int high,coefficient.Sign < 0,byte scale)

    let decimalRatio numerator denominator =
        let struct(coefficient,scale) = quantizeDecimalRatio numerator denominator
        nativeDecimal coefficient scale

    let toDecimal value =
        if value.Coefficient.IsZero then Decimal.Zero
        elif value.Exponent >= 29I then raise(OverflowException())
        elif BigInteger(digits value.Coefficient) + value.Exponent < -28I then Decimal.Zero
        else
            let struct(numerator,denominator) = ratio value
            let struct(coefficient,scale) = quantizeDecimalRatio numerator denominator
            let canonical = create coefficient (-BigInteger scale)
            if canonical.Exponent.Sign < 0 then
                nativeDecimal canonical.Coefficient (int (-canonical.Exponent))
            else
                nativeDecimal (canonical.Coefficient * powers[int canonical.Exponent]) 0

    let toBigInteger value =
        if value.Coefficient.IsZero then BigInteger.Zero
        elif value.Exponent.Sign >= 0 then value.Coefficient * power value.Exponent
        elif value.Exponent <= -BigInteger(digits value.Coefficient) then BigInteger.Zero
        else value.Coefficient / power (-value.Exponent)

    let private integerBounds (minimum: BigInteger) (maximum: BigInteger) =
        struct(ofBigInteger(minimum-1I),ofBigInteger(maximum+1I))
    let private signed8 = integerBounds (BigInteger(SByte.MinValue)) (BigInteger(SByte.MaxValue))
    let private unsigned8 = integerBounds 0I (BigInteger(Byte.MaxValue))
    let private signed16 = integerBounds (BigInteger(Int16.MinValue)) (BigInteger(Int16.MaxValue))
    let private unsigned16 = integerBounds 0I (BigInteger(UInt16.MaxValue))
    let private signed32 = integerBounds (BigInteger(Int32.MinValue)) (BigInteger(Int32.MaxValue))
    let private unsigned32 = integerBounds 0I (BigInteger(UInt32.MaxValue))
    let private signed64 = integerBounds (BigInteger(Int64.MinValue)) (BigInteger(Int64.MaxValue))
    let private unsigned64 = integerBounds 0I (BigInteger(UInt64.MaxValue))
    let private boundedInteger (struct(minimum,maximum)) value =
        if compare value minimum <= 0 || compare value maximum >= 0 then raise(OverflowException())
        toBigInteger value
    let toSByte value = sbyte(boundedInteger signed8 value)
    let toByte value = byte(boundedInteger unsigned8 value)
    let toInt16 value = int16(boundedInteger signed16 value)
    let toUInt16 value = uint16(boundedInteger unsigned16 value)
    let toInt32 value = int(boundedInteger signed32 value)
    let toUInt64Unchecked value =
        if value.Exponent >= 64I then 0UL
        else uint64(toBigInteger value &&& BigInteger UInt64.MaxValue)

    let toUInt32 value = uint32(boundedInteger unsigned32 value)
    let toInt64 value = int64(boundedInteger signed64 value)
    let toUInt64 value = uint64(boundedInteger unsigned64 value)

    let private bitLength (value: BigInteger) =
        let mutable value = value
        let mutable bits = 0
        while value > BigInteger(UInt32.MaxValue) do
            value <- value >>> 32
            bits <- Checked.(+) bits 32
        let mutable word = uint32 value
        while word <> 0u do
            word <- word >>> 1
            bits <- Checked.(+) bits 1
        bits

    /// Quantize the original ratio directly to the requested IEEE significand.
    let private binaryBits fractionBits exponentBits (decimalHigh: int) (decimalLow: int) value =
        let sign = if value.Coefficient.Sign < 0 then 1UL <<< (fractionBits + exponentBits) else 0UL
        let infinity = ((1UL <<< exponentBits) - 1UL) <<< fractionBits
        if value.Coefficient.IsZero then 0UL
        else
            let position = value.Exponent + BigInteger(digits value.Coefficient)
            if position > BigInteger decimalHigh then sign ||| infinity
            elif position < BigInteger decimalLow then sign
            else
                let struct(numerator,denominator) = ratio (abs value)
                let mutable exponent = bitLength numerator - bitLength denominator
                let belowPower =
                    if exponent >= 0 then numerator < (denominator <<< exponent)
                    else (numerator <<< -exponent) < denominator
                if belowPower then exponent <- exponent - 1
                let bias = (1 <<< (exponentBits - 1)) - 1
                let minimum = 1 - bias
                let shift = fractionBits - max minimum exponent
                let significand =
                    if shift >= 0 then roundRatio (numerator <<< shift) denominator MidpointRounding.ToEven
                    else roundRatio numerator (denominator <<< -shift) MidpointRounding.ToEven
                let mutable significand = uint64 significand
                if significand = (1UL <<< (fractionBits + 1)) then
                    significand <- significand >>> 1
                    exponent <- exponent + 1
                if exponent > bias then sign ||| infinity
                elif exponent < minimum then sign ||| significand
                else sign ||| (uint64(exponent + bias) <<< fractionBits) ||| (significand - (1UL <<< fractionBits))

    let toHalfBits value = uint16(binaryBits 10 5 6 -9 value)

    let toDouble value =
        let mutable bits = binaryBits 52 11 310 -325 value
        Unsafe.As<uint64,double>(&bits)

    let toSingle value =
        let mutable bits = uint32(binaryBits 23 8 40 -47 value)
        Unsafe.As<uint32,single>(&bits)

    let private quotientRatio left right =
        let exponent = left.Exponent - right.Exponent
        if exponent.Sign >= 0 then struct(left.Coefficient * power exponent,right.Coefficient)
        else struct(left.Coefficient,right.Coefficient * power (-exponent))

    let divideDecimal left right =
        if right.Coefficient.IsZero then raise(DivideByZeroException())
        elif left.Coefficient.IsZero then Decimal.Zero
        else
            let position = BigInteger(digits left.Coefficient) - BigInteger(digits right.Coefficient)
                           + left.Exponent - right.Exponent
            if position >= 30I then raise(OverflowException())
            elif position <= -30I then Decimal.Zero
            else
                let struct(numerator,denominator) = quotientRatio left right
                decimalRatio numerator denominator

    let divideInteger left right =
        if right.Coefficient.IsZero then raise(DivideByZeroException())
        elif compare (abs left) (abs right) < 0 then BigInteger.Zero
        else
            let struct(numerator,denominator) = quotientRatio left right
            numerator / denominator

    let remainder left right =
        subtract left (multiply (ofBigInteger (divideInteger left right)) right)

    let ieeeRemainder left right =
        if right.Coefficient.IsZero then raise(DivideByZeroException())
        elif compare (abs left) (multiply (abs right) (create 5I -1I)) <= 0 then left
        else
            let struct(numerator,denominator) = quotientRatio left right
            let quotient = roundRatio numerator denominator MidpointRounding.ToEven
            subtract left (multiply (ofBigInteger quotient) right)

    let round value places mode =
        if places < 0 || places > 28 then raise(ArgumentOutOfRangeException(nameof places))
        if int mode < 0 || int mode > 4 then raise(ArgumentException("The rounding mode is not supported.",nameof mode))
        let exponent = -BigInteger places
        if value.Coefficient.IsZero || value.Exponent >= exponent then value
        elif value.Exponent < exponent - BigInteger(digits value.Coefficient) then
            match int mode,value.Coefficient.Sign with
            | 3,-1 -> create -BigInteger.One exponent
            | 4,1 -> create BigInteger.One exponent
            | _ -> zero
        else
            let struct(numerator,denominator) = ratio value
            create (roundRatio (numerator * powers[places]) denominator mode) exponent

    let truncate value =
        if value.Exponent.Sign >= 0 then value
        else ofBigInteger(toBigInteger value)

    let floor value =
        let integral = truncate value
        if value.Coefficient.Sign < 0 && compare value integral <> 0 then subtract integral (ofBigInteger BigInteger.One)
        else integral

    let ceiling value =
        let integral = truncate value
        if value.Coefficient.Sign > 0 && compare value integral <> 0 then add integral (ofBigInteger BigInteger.One)
        else integral

    let minimum left right = if compare left right <= 0 then left else right
    let maximumValue left right = if compare left right >= 0 then left else right
