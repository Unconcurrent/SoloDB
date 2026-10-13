namespace SoloDatabase.Numerics

open System
open System.Numerics

module internal DecimalEvaluation =
    let evaluate calculate =
        let mutable precision = 128
        let mutable answer = ValueNone
        while answer.IsNone do
            let context = DecimalMathContext(precision)
            match calculate context with
            | ValueNone -> precision <- Checked.(*) precision 2
            | ValueSome interval ->
                let rounded value =
                    try ValueSome(ExactNumber.decimalRatio value context.Bounds.Scale)
                    with :? OverflowException -> ValueNone
                match rounded interval.Low,rounded interval.High with
                | ValueSome low,ValueSome high when low = high -> answer <- ValueSome low
                | ValueNone,ValueNone when interval.Low.Sign = interval.High.Sign -> raise(OverflowException())
                | _ -> precision <- Checked.(*) precision 2
        answer.Value

    let exactUnary value calculate =
        evaluate(fun context -> calculate context (context.Bounds.Number value))

    let exactBinary left right calculate =
        evaluate(fun context -> calculate context (context.Bounds.Number left) (context.Bounds.Number right))

    let exponential (context: DecimalMathContext) value =
        let bounds = context.Bounds
        if ExactNumber.compare value (ExactNumber.ofBigInteger 128I) >= 0 then raise(OverflowException())
        elif ExactNumber.compare value (ExactNumber.ofBigInteger(-BigInteger(max 128 bounds.Precision))) <= 0 then
            ValueSome { Low = BigInteger.Zero; High = BigInteger.One }
        else ValueSome(context.Exponential(bounds.Number value))

    let exponentialProduct (context: DecimalMathContext) interval value =
        let low,high =
            if value.Coefficient.Sign >= 0 then interval.Low,interval.High else interval.High,interval.Low
        let low = ExactNumber.create (low*value.Coefficient) value.Exponent
        let high = ExactNumber.create (high*value.Coefficient) value.Exponent
        let bounds = context.Bounds
        if ExactNumber.compare low (ExactNumber.ofBigInteger(bounds.Scale*128I)) >= 0 then raise(OverflowException())
        elif ExactNumber.compare high (ExactNumber.ofBigInteger(-bounds.Scale*BigInteger(max 128 bounds.Precision))) <= 0 then
            ValueSome { Low = BigInteger.Zero; High = BigInteger.One }
        elif interval.Low.Sign <= 0 && interval.High.Sign >= 0 then ValueNone
        else ValueSome(context.Exponential(bounds.Multiply(interval,bounds.Number value)))

    let atan2 (context: DecimalMathContext) negativeY negativeX zeroX y x =
        let bounds = context.Bounds
        if zeroX then
            let result = bounds.DivideInteger(context.Pi,BigInteger(2))
            ValueSome(if negativeY then bounds.Negate result else result)
        else
            match bounds.Divide(y,x) with
            | ValueNone -> ValueNone
            | ValueSome ratio ->
                let result = context.Arctangent ratio
                ValueSome(
                    if not negativeX then result
                    elif negativeY then bounds.Subtract(result,context.Pi)
                    else bounds.Add(result,context.Pi))

    let logarithm (context: DecimalMathContext) (value: ExactNumber) =
        let bounds = context.Bounds
        let coefficient = bounds.Integer(value.Coefficient)
        match context.Logarithm coefficient with
        | ValueNone -> ValueNone
        | ValueSome coefficientLog when value.Exponent.IsZero -> ValueSome coefficientLog
        | ValueSome coefficientLog ->
            match context.Logarithm(bounds.Integer(10I)) with
            | ValueNone -> ValueNone
            | ValueSome decade -> ValueSome(bounds.Add(coefficientLog,bounds.MultiplyInteger(decade,value.Exponent)))

    /// Native halfway values have bounded reduced numerators and denominators.
    /// Decide rational roots using prime valuations before materializing scale powers.
    let exactRationalPower value exponent =
        if ExactNumber.compare (ExactNumber.abs exponent) (ExactNumber.ofBigInteger 128I) > 0 then ValueNone
        else
            let mutable coefficient = BigInteger.Abs value.Coefficient
            let coefficientBits = IntegerBounds.bitLength coefficient
            let valuationBound = BigInteger.Abs value.Exponent + BigInteger coefficientBits
            let boundBits = IntegerBounds.bitLength valuationBound
            let exponentBits = IntegerBounds.bitLength(BigInteger.Abs exponent.Coefficient)
            if -exponent.Exponent > BigInteger exponentBits + BigInteger boundBits then ValueNone
            else
                let struct(numerator,denominator) = ExactNumber.ratio exponent
                let divisor = BigInteger.GreatestCommonDivisor(BigInteger.Abs numerator,denominator)
                let numerator,denominator = numerator/divisor,denominator/divisor
                if BigInteger.Abs numerator > 128I then ValueNone
                else
                    let mutable twos = BigInteger.Zero
                    while coefficient.IsEven do
                        coefficient <- coefficient >>> 1
                        twos <- twos + BigInteger.One
                    let mutable fives = BigInteger.Zero
                    let mutable remainder = BigInteger.Zero
                    let mutable quotient = BigInteger.DivRem(coefficient,5I,&remainder)
                    while remainder.IsZero do
                        coefficient <- quotient
                        fives <- fives + BigInteger.One
                        quotient <- BigInteger.DivRem(coefficient,5I,&remainder)
                    let twos,fives = twos+value.Exponent,fives+value.Exponent
                    if twos % denominator <> BigInteger.Zero || fives % denominator <> BigInteger.Zero then ValueNone
                    else
                        let root =
                            if coefficient.IsOne || denominator.IsOne then ValueSome coefficient
                            elif denominator >= BigInteger(IntegerBounds.bitLength coefficient) then ValueNone
                            else
                                let degree = int denominator
                                let root = IntegerBounds.root coefficient degree
                                if BigInteger.Pow(root,degree) = coefficient then ValueSome root else ValueNone
                        match root with
                        | ValueNone -> ValueNone
                        | ValueSome root when numerator.Sign < 0 && not root.IsOne -> ValueNone
                        | ValueSome root ->
                            let twos,fives = twos/denominator*numerator,fives/denominator*numerator
                            let scale = min twos fives
                            let difference = BigInteger.Abs(twos-fives)
                            let coefficient = BigInteger.Pow(root,int(BigInteger.Abs numerator))
                            if scale >= 29I then raise(OverflowException())
                            elif scale + BigInteger(IntegerBounds.bitLength coefficient) + difference < -28I then ValueSome Decimal.Zero
                            else
                                let factor = if twos > fives then 2I else 5I
                                let coefficient = coefficient * BigInteger.Pow(factor,int difference)
                                let coefficient = if value.Coefficient.Sign < 0 && not numerator.IsEven then -coefficient else coefficient
                                ValueSome(ExactNumber.toDecimal(ExactNumber.create coefficient scale))
