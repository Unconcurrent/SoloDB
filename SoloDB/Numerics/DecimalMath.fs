namespace SoloDatabase

open System
open System.Numerics
open SoloDatabase.Numerics

/// Exact operands enter the interval owner before native result rounding.
[<AbstractClass; Sealed>]
type internal ExactDecimalMath private () =
    static member private Compare(value, native) = ExactNumber.compare value (ExactNumber.ofDecimal native)
    static member private Domain(name) = raise(ArgumentOutOfRangeException(name,"The value is outside the function's real domain."))
    /// The ratio of a circle's circumference to its diameter.
    static member PI = DecimalEvaluation.evaluate(fun context -> ValueSome context.Pi)
    /// The base of the natural logarithm.
    static member E = ExactDecimalMath.Exp(ExactNumber.ofDecimal 1M)

    static member Sqrt(value: ExactNumber) =
        if ExactDecimalMath.Compare(value,0M) < 0 then ExactDecimalMath.Domain(nameof value)
        elif ExactDecimalMath.Compare(value,0M) = 0 then 0M
        else
            match DecimalEvaluation.exactRationalPower value (ExactNumber.ofDecimal 0.5M) with
            | ValueSome result -> result
            | ValueNone -> DecimalEvaluation.exactUnary value (fun context value -> context.Bounds.SquareRoot value)

    static member Exp(value: ExactNumber) =
        if ExactDecimalMath.Compare(value,0M) = 0 then 1M
        else DecimalEvaluation.evaluate(fun context -> DecimalEvaluation.exponential context value)

    static member Log(value: ExactNumber) =
        if ExactDecimalMath.Compare(value,0M) <= 0 then ExactDecimalMath.Domain(nameof value)
        elif ExactDecimalMath.Compare(value,1M) = 0 then 0M
        else DecimalEvaluation.evaluate(fun context -> DecimalEvaluation.logarithm context value)

    static member Log(value: ExactNumber, newBase: ExactNumber) =
        if ExactDecimalMath.Compare(value,0M) <= 0 then ExactDecimalMath.Domain(nameof value)
        elif ExactDecimalMath.Compare(newBase,0M) <= 0 || ExactDecimalMath.Compare(newBase,1M) = 0 then ExactDecimalMath.Domain(nameof newBase)
        elif value.Coefficient.IsOne && newBase.Coefficient.IsOne then
            ExactNumber.decimalRatio value.Exponent newBase.Exponent
        else
            DecimalEvaluation.evaluate(fun context ->
                match DecimalEvaluation.logarithm context value,DecimalEvaluation.logarithm context newBase with
                | ValueSome value,ValueSome basis -> context.Bounds.Divide(value,basis)
                | _ -> ValueNone)

    static member Log10(value: ExactNumber) =
        if ExactDecimalMath.Compare(value,0M) <= 0 then ExactDecimalMath.Domain(nameof value)
        elif value.Coefficient.IsOne then ExactNumber.toDecimal(ExactNumber.ofBigInteger value.Exponent)
        else ExactDecimalMath.Log(value,ExactNumber.ofDecimal 10M)
    static member Log2(value: ExactNumber) = ExactDecimalMath.Log(value,ExactNumber.ofDecimal 2M)

    static member IEEERemainder(value: ExactNumber, divisor: ExactNumber) =
        ExactNumber.toDecimal(ExactNumber.ieeeRemainder value divisor)

    static member Pow(value: ExactNumber, exponent: ExactNumber) =
        if ExactDecimalMath.Compare(exponent,0M) = 0 || ExactDecimalMath.Compare(value,1M) = 0 then 1M
        elif ExactDecimalMath.Compare(value,0M) = 0 then
            if ExactDecimalMath.Compare(exponent,0M) < 0 then raise(DivideByZeroException()) else 0M
        elif ExactDecimalMath.Compare(value,0M) < 0 && not exponent.Coefficient.IsZero && exponent.Exponent.Sign < 0 then ExactDecimalMath.Domain(nameof value)
        elif value.Coefficient = -BigInteger.One && value.Exponent.IsZero then
            if exponent.Exponent.Sign > 0 || exponent.Coefficient.IsEven then 1M else -1M
        else
            match DecimalEvaluation.exactRationalPower value exponent with
            | ValueSome result -> result
            | ValueNone ->
                let negative = ExactDecimalMath.Compare(value,0M) < 0 && exponent.Exponent.Sign = 0 && not exponent.Coefficient.IsEven
                DecimalEvaluation.evaluate(fun context ->
                    match DecimalEvaluation.logarithm context (ExactNumber.abs value) with
                    | ValueNone -> ValueNone
                    | ValueSome logarithm ->
                        match DecimalEvaluation.exponentialProduct context logarithm exponent with
                        | ValueNone -> ValueNone
                        | ValueSome result -> ValueSome(if negative then context.Bounds.Negate result else result))

    static member Sin(value: ExactNumber) =
        if ExactDecimalMath.Compare(value,0M) = 0 then 0M
        else DecimalEvaluation.exactUnary value (fun context value -> context.Circular(true,value))

    static member Cos(value: ExactNumber) =
        if ExactDecimalMath.Compare(value,0M) = 0 then 1M
        else DecimalEvaluation.exactUnary value (fun context value -> context.Circular(false,value))

    static member Tan(value: ExactNumber) =
        if ExactDecimalMath.Compare(value,0M) = 0 then 0M
        else
            DecimalEvaluation.exactUnary value (fun context value ->
                match context.Circular(true,value),context.Circular(false,value) with
                | ValueSome sine,ValueSome cosine -> context.Bounds.Divide(sine,cosine)
                | _ -> ValueNone)

    static member Atan(value: ExactNumber) =
        if ExactDecimalMath.Compare(value,0M) = 0 then 0M
        else
            let magnitude=ExactNumber.abs value
            DecimalEvaluation.evaluate(fun context ->
                let bounds=context.Bounds
                let result=
                    if ExactDecimalMath.Compare(magnitude,1M)>0 then
                        let inverse=bounds.Quotient(ExactNumber.ofBigInteger BigInteger.One,magnitude)
                        bounds.Subtract(bounds.DivideInteger(context.Pi,2I),context.Arctangent inverse)
                    else context.Arctangent(bounds.Number magnitude)
                ValueSome(if value.Coefficient.Sign<0 then bounds.Negate result else result))

    static member Atan2(y: ExactNumber, x: ExactNumber) =
        if ExactDecimalMath.Compare(y,0M) = 0 && ExactDecimalMath.Compare(x,0M) = 0 then 0M
        else
            DecimalEvaluation.evaluate(fun context ->
                let bounds=context.Bounds
                let magnitudeY,magnitudeX=ExactNumber.abs y,ExactNumber.abs x
                let angle=
                    if x.Coefficient.IsZero then bounds.DivideInteger(context.Pi,2I)
                    elif ExactNumber.compare magnitudeY magnitudeX>0 then
                        bounds.Subtract(bounds.DivideInteger(context.Pi,2I),context.Arctangent(bounds.Quotient(magnitudeX,magnitudeY)))
                    else context.Arctangent(bounds.Quotient(magnitudeY,magnitudeX))
                let angle=if x.Coefficient.Sign<0 then bounds.Subtract(context.Pi,angle) else angle
                ValueSome(if y.Coefficient.Sign<0 then bounds.Negate angle else angle))

    static member Asin(value: ExactNumber) =
        if ExactDecimalMath.Compare(value,-1M) < 0 || ExactDecimalMath.Compare(value,1M) > 0 then ExactDecimalMath.Domain(nameof value)
        elif ExactDecimalMath.Compare(value,0M) = 0 then 0M
        elif ExactDecimalMath.Compare(value,1M) = 0 || ExactDecimalMath.Compare(value,-1M) = 0 then
            DecimalEvaluation.evaluate(fun context ->
                ValueSome(context.Bounds.DivideInteger(context.Pi,BigInteger(if ExactDecimalMath.Compare(value,0M) < 0 then -2 else 2))))
        else
            DecimalEvaluation.exactUnary value (fun context value ->
                let bounds = context.Bounds
                match bounds.SquareRoot(bounds.Multiply(bounds.Subtract(bounds.One,value),bounds.Add(bounds.One,value))) with
                | ValueNone -> ValueNone
                | ValueSome root ->
                    match bounds.Divide(value,root) with
                    | ValueNone -> ValueNone
                    | ValueSome ratio -> ValueSome(context.Arctangent ratio))

    static member Acos(value: ExactNumber) =
        if ExactDecimalMath.Compare(value,-1M) < 0 || ExactDecimalMath.Compare(value,1M) > 0 then ExactDecimalMath.Domain(nameof value)
        elif ExactDecimalMath.Compare(value,1M) = 0 then 0M
        elif ExactDecimalMath.Compare(value,-1M) = 0 then ExactDecimalMath.PI
        else
            DecimalEvaluation.exactUnary value (fun context interval ->
                let bounds = context.Bounds
                match bounds.SquareRoot(bounds.Multiply(bounds.Subtract(bounds.One,interval),bounds.Add(bounds.One,interval))) with
                | ValueNone -> ValueNone
                | ValueSome root -> DecimalEvaluation.atan2 context false (ExactDecimalMath.Compare(value,0M) < 0) (ExactDecimalMath.Compare(value,0M) = 0) root interval)

    static member Sinh(value: ExactNumber) =
        if ExactDecimalMath.Compare(value,0M) = 0 then 0M
        else
            DecimalEvaluation.evaluate(fun context ->
                match DecimalEvaluation.exponential context (ExactNumber.abs value) with
                | ValueNone -> ValueNone
                | ValueSome exponential ->
                    match context.Bounds.Divide(context.Bounds.One,exponential) with
                    | ValueNone -> ValueNone
                    | ValueSome inverse ->
                        let result = context.Bounds.DivideInteger(context.Bounds.Subtract(exponential,inverse),BigInteger(2))
                        ValueSome(if ExactDecimalMath.Compare(value,0M) < 0 then context.Bounds.Negate result else result))

    static member Cosh(value: ExactNumber) =
        if ExactDecimalMath.Compare(value,0M) = 0 then 1M
        else
            DecimalEvaluation.evaluate(fun context ->
                match DecimalEvaluation.exponential context (ExactNumber.abs value) with
                | ValueNone -> ValueNone
                | ValueSome exponential ->
                    match context.Bounds.Divide(context.Bounds.One,exponential) with
                    | ValueNone -> ValueNone
                    | ValueSome inverse -> ValueSome(context.Bounds.DivideInteger(context.Bounds.Add(exponential,inverse),BigInteger(2))))

    static member Tanh(value: ExactNumber) =
        if ExactDecimalMath.Compare(value,0M) = 0 then 0M
        else
            DecimalEvaluation.evaluate(fun context ->
                match DecimalEvaluation.exponential context (ExactNumber.multiply (ExactNumber.abs value) (ExactNumber.ofBigInteger -2I)) with
                | ValueNone -> ValueNone
                | ValueSome exponential ->
                    match context.Bounds.Divide(context.Bounds.Subtract(context.Bounds.One,exponential),context.Bounds.Add(context.Bounds.One,exponential)) with
                    | ValueNone -> ValueNone
                    | ValueSome result -> ValueSome(if value.Coefficient.Sign<0 then context.Bounds.Negate result else result))

    static member Asinh(value: ExactNumber) =
        if ExactDecimalMath.Compare(value,0M) = 0 then 0M
        elif ExactDecimalMath.Compare(ExactNumber.abs value,1M)>0 then
            let magnitude=ExactNumber.abs value
            DecimalEvaluation.evaluate(fun context ->
                let bounds=context.Bounds
                let inverse=bounds.Quotient(ExactNumber.ofBigInteger BigInteger.One,magnitude)
                match bounds.SquareRoot(bounds.Add(bounds.One,bounds.Multiply(inverse,inverse))),DecimalEvaluation.logarithm context magnitude with
                | ValueSome root,ValueSome logarithm ->
                    match context.Logarithm(bounds.Add(bounds.One,root)) with
                    | ValueNone -> ValueNone
                    | ValueSome correction ->
                        let result=bounds.Add(logarithm,correction)
                        ValueSome(if value.Coefficient.Sign<0 then bounds.Negate result else result)
                | _ -> ValueNone)
        else
            DecimalEvaluation.exactUnary (ExactNumber.abs value) (fun context valueInterval ->
                let bounds = context.Bounds
                match bounds.SquareRoot(bounds.Add(bounds.Multiply(valueInterval,valueInterval),bounds.One)) with
                | ValueNone -> ValueNone
                | ValueSome root ->
                    match context.Logarithm(bounds.Add(valueInterval,root)) with
                    | ValueNone -> ValueNone
                    | ValueSome result -> ValueSome(if ExactDecimalMath.Compare(value,0M) < 0 then bounds.Negate result else result))

    static member Acosh(value: ExactNumber) =
        if ExactDecimalMath.Compare(value,1M) < 0 then ExactDecimalMath.Domain(nameof value)
        elif ExactDecimalMath.Compare(value,1M) = 0 then 0M
        else
            DecimalEvaluation.evaluate(fun context ->
                let bounds = context.Bounds
                let inverse=bounds.Quotient(ExactNumber.ofBigInteger BigInteger.One,value)
                match bounds.SquareRoot(bounds.Subtract(bounds.One,bounds.Multiply(inverse,inverse))),DecimalEvaluation.logarithm context value with
                | ValueSome root,ValueSome logarithm ->
                    match context.Logarithm(bounds.Add(bounds.One,root)) with
                    | ValueNone -> ValueNone
                    | ValueSome correction -> ValueSome(bounds.Add(logarithm,correction))
                | _ -> ValueNone)

    static member Atanh(value: ExactNumber) =
        if ExactDecimalMath.Compare(value,-1M) <= 0 || ExactDecimalMath.Compare(value,1M) >= 0 then ExactDecimalMath.Domain(nameof value)
        elif ExactDecimalMath.Compare(value,0M) = 0 then 0M
        else
            DecimalEvaluation.exactUnary value (fun context value ->
                let bounds = context.Bounds
                match context.Logarithm(bounds.Add(bounds.One,value)),context.Logarithm(bounds.Subtract(bounds.One,value)) with
                | ValueSome upper,ValueSome lower -> ValueSome(bounds.DivideInteger(bounds.Subtract(upper,lower),BigInteger(2)))
                | _ -> ValueNone)

    static member Degrees(value: ExactNumber) =
        if ExactDecimalMath.Compare(value,0M) = 0 then 0M
        else DecimalEvaluation.exactUnary value (fun context value -> context.Bounds.Divide(context.Bounds.MultiplyInteger(value,BigInteger(180)),context.Pi))

    static member Radians(value: ExactNumber) =
        if ExactDecimalMath.Compare(value,0M) = 0 then 0M
        else DecimalEvaluation.exactUnary value (fun context value -> ValueSome(context.Bounds.DivideInteger(context.Bounds.Multiply(value,context.Pi),BigInteger(180))))

/// Real elementary functions with native Decimal result rounding, including ties to even.
[<AbstractClass; Sealed>]
type DecimalMath private () =
    /// The ratio of a circle's circumference to its diameter.
    static member PI = ExactDecimalMath.PI
    /// The base of the natural logarithm.
    static member E = ExactDecimalMath.E
    static member Sqrt(value: decimal) = ExactDecimalMath.Sqrt(ExactNumber.ofDecimal value)
    static member Exp(value: decimal) = ExactDecimalMath.Exp(ExactNumber.ofDecimal value)
    static member Log(value: decimal) = ExactDecimalMath.Log(ExactNumber.ofDecimal value)
    static member Log10(value: decimal) = ExactDecimalMath.Log10(ExactNumber.ofDecimal value)
    static member Log2(value: decimal) = ExactDecimalMath.Log2(ExactNumber.ofDecimal value)
    static member Sin(value: decimal) = ExactDecimalMath.Sin(ExactNumber.ofDecimal value)
    static member Cos(value: decimal) = ExactDecimalMath.Cos(ExactNumber.ofDecimal value)
    static member Tan(value: decimal) = ExactDecimalMath.Tan(ExactNumber.ofDecimal value)
    static member Asin(value: decimal) = ExactDecimalMath.Asin(ExactNumber.ofDecimal value)
    static member Acos(value: decimal) = ExactDecimalMath.Acos(ExactNumber.ofDecimal value)
    static member Atan(value: decimal) = ExactDecimalMath.Atan(ExactNumber.ofDecimal value)
    static member Sinh(value: decimal) = ExactDecimalMath.Sinh(ExactNumber.ofDecimal value)
    static member Cosh(value: decimal) = ExactDecimalMath.Cosh(ExactNumber.ofDecimal value)
    static member Tanh(value: decimal) = ExactDecimalMath.Tanh(ExactNumber.ofDecimal value)
    static member Asinh(value: decimal) = ExactDecimalMath.Asinh(ExactNumber.ofDecimal value)
    static member Acosh(value: decimal) = ExactDecimalMath.Acosh(ExactNumber.ofDecimal value)
    static member Atanh(value: decimal) = ExactDecimalMath.Atanh(ExactNumber.ofDecimal value)
    static member Degrees(value: decimal) = ExactDecimalMath.Degrees(ExactNumber.ofDecimal value)
    static member Radians(value: decimal) = ExactDecimalMath.Radians(ExactNumber.ofDecimal value)
    static member Log(value: decimal, newBase: decimal) = ExactDecimalMath.Log(ExactNumber.ofDecimal value,ExactNumber.ofDecimal newBase)
    static member Pow(value: decimal, exponent: decimal) = ExactDecimalMath.Pow(ExactNumber.ofDecimal value,ExactNumber.ofDecimal exponent)
    static member Atan2(y: decimal, x: decimal) = ExactDecimalMath.Atan2(ExactNumber.ofDecimal y,ExactNumber.ofDecimal x)
    static member IEEERemainder(value: decimal, divisor: decimal) = ExactDecimalMath.IEEERemainder(ExactNumber.ofDecimal value,ExactNumber.ofDecimal divisor)
