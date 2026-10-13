namespace SoloDatabase.Numerics

open System
open System.Numerics

module private DecimalSeries =
    let magnitude (value: DecimalInterval) = max (BigInteger.Abs value.Low) (BigInteger.Abs value.High)

    let arctangent (context: DecimalIntervalContext) argument =
        let square = context.Multiply(argument,argument)
        let mutable power = argument
        let mutable sum = argument
        let mutable divisor = 3L
        let mutable subtract = true
        let mutable error = BigInteger.Zero
        let mutable complete = false
        while not complete do
            power <- context.Multiply(power,square)
            let term = context.DivideInteger(power,BigInteger divisor)
            error <- magnitude term
            if error <= BigInteger.One then complete <- true
            else
                sum <- if subtract then context.Subtract(sum,term) else context.Add(sum,term)
                subtract <- not subtract
                divisor <- Checked.(+) divisor 2L
        { Low = sum.Low-error; High = sum.High+error }

    let pi context =
        let first = arctangent context (context.Ratio(BigInteger.One,BigInteger(5)))
        let second = arctangent context (context.Ratio(BigInteger.One,BigInteger(239)))
        context.Subtract(context.MultiplyInteger(first,BigInteger(16)),context.MultiplyInteger(second,BigInteger(4)))

    /// For 1 <= y <= 2, z=(y-1)/(y+1) is at most one third.
    let logarithmUnit (context: DecimalIntervalContext) value =
        let z = (context.Divide(context.Subtract(value,context.One),context.Add(value,context.One))).Value
        let square = context.Multiply(z,z)
        let mutable power = z
        let mutable sum = z
        let mutable divisor = 3L
        let mutable error = BigInteger.Zero
        let mutable complete = false
        while not complete do
            power <- context.Multiply(power,square)
            let term = context.DivideInteger(power,BigInteger divisor)
            error <- magnitude term
            if error <= BigInteger.One then complete <- true
            else
                sum <- context.Add(sum,term)
                divisor <- Checked.(+) divisor 2L
        // Remaining denominators increase; the geometric tail is below twice the next term.
        context.MultiplyInteger({ Low = sum.Low; High = sum.High+2I*error },BigInteger(2))

    /// The positive argument is at most one eighth, so the tail is below twice the next term.
    let exponential (context: DecimalIntervalContext) value =
        let mutable term = context.One
        let mutable sum = context.One
        let mutable divisor = 1L
        let mutable error = BigInteger.Zero
        let mutable complete = false
        while not complete do
            let next = context.DivideInteger(context.Multiply(term,value),BigInteger divisor)
            error <- magnitude next
            if error <= BigInteger.One then complete <- true
            else
                sum <- context.Add(sum,next)
                term <- next
                divisor <- Checked.(+) divisor 1L
        { Low = sum.Low; High = sum.High+2I*error }

    /// The reduced argument is in [0, pi/2]; the alternating tail is bounded by its next term.
    let circular (context: DecimalIntervalContext) sine value =
        let square = context.Multiply(value,value)
        let mutable term = if sine then value else context.One
        let mutable sum = term
        let mutable index = if sine then 2L else 1L
        let mutable subtract = true
        let mutable error = BigInteger.Zero
        let mutable complete = false
        while not complete do
            let divisor = BigInteger(index)*BigInteger(Checked.(+) index 1L)
            let next = context.DivideInteger(context.Multiply(term,square),divisor)
            error <- magnitude next
            if error <= BigInteger.One then complete <- true
            else
                sum <- if subtract then context.Subtract(sum,next) else context.Add(sum,next)
                term <- next
                subtract <- not subtract
                index <- Checked.(+) index 2L
        { Low = sum.Low-error; High = sum.High+error }

[<Sealed>]
type internal DecimalMathContext(precision: int) =
    let bounds = DecimalIntervalContext(precision)
    let pi = lazy(DecimalSeries.pi bounds)
    let logTwo = lazy(DecimalSeries.logarithmUnit bounds (bounds.Integer(BigInteger(2))))
    member _.Bounds = bounds
    member _.Pi = pi.Value

    member _.Arctangent(value: DecimalInterval) =
        let rec point (x: BigInteger) =
            if x.Sign < 0 then bounds.Negate(point (-x))
            elif x.IsZero then bounds.Zero
            elif x = bounds.Scale then bounds.DivideInteger(pi.Value,BigInteger(4))
            elif x > bounds.Scale then
                let inverse = bounds.Ratio(bounds.Scale,x)
                let low,high = point inverse.Low,point inverse.High
                bounds.Subtract(bounds.DivideInteger(pi.Value,BigInteger(2)),{ Low = low.Low; High = high.High })
            elif x*2I >= bounds.Scale then
                let reduced = bounds.Ratio(x-bounds.Scale,x+bounds.Scale)
                bounds.Add(bounds.DivideInteger(pi.Value,BigInteger(4)),DecimalSeries.arctangent bounds reduced)
            else DecimalSeries.arctangent bounds { Low = x; High = x }
        let low,high = point value.Low,point value.High
        { Low = low.Low; High = high.High }

    member _.Logarithm(value: DecimalInterval) =
        if value.Low.Sign <= 0 then ValueNone
        else
            let point (x: BigInteger) =
                if x = bounds.Scale then bounds.Zero
                else
                    let mutable probe = x
                    let mutable exponent = 0
                    while probe >= bounds.Scale*2I do
                        probe <- probe >>> 1
                        exponent <- exponent+1
                    while probe < bounds.Scale do
                        probe <- probe <<< 1
                        exponent <- exponent-1
                    let reduced =
                        if exponent >= 0 then bounds.Ratio(x,bounds.Scale <<< exponent)
                        else bounds.Ratio(x <<< -exponent,bounds.Scale)
                    bounds.Add(DecimalSeries.logarithmUnit bounds reduced,
                               bounds.MultiplyInteger(logTwo.Value,BigInteger exponent))
            let low,high = point value.Low,point value.High
            ValueSome { Low = low.Low; High = high.High }

    member _.Exponential(value: DecimalInterval) =
        // e^128/2 exceeds the native Decimal range, including sinh/cosh's factor of two.
        if value.Low >= bounds.Scale*128I then raise(OverflowException())
        let rec point (x: BigInteger) =
            if x.IsZero then bounds.One
            elif x.Sign < 0 then
                if -x >= bounds.Scale*128I then
                    let exponent = -x/bounds.Scale
                    let high =
                        if exponent >= BigInteger precision then BigInteger.One
                        else IntegerBounds.ceilingDivide bounds.Scale (BigInteger.One <<< int exponent)
                    { Low = BigInteger.Zero; High = high }
                else (bounds.Divide(bounds.One,point (-x))).Value
            else
                let mutable exponent = 0
                while x*8I > (bounds.Scale <<< exponent) do
                    exponent <- exponent+1
                let reduced = bounds.Ratio(x,bounds.Scale <<< exponent)
                let mutable result = DecimalSeries.exponential bounds reduced
                for _ = 1 to exponent do result <- bounds.Multiply(result,result)
                result
        let low,high = point value.Low,point value.High
        { Low = low.Low; High = high.High }

    member _.Circular(sine: bool, value: DecimalInterval) =
        let halfPi = bounds.DivideInteger(pi.Value,BigInteger(2))
        let quotient = (bounds.Divide(value,halfPi)).Value
        let low = IntegerBounds.floorDivide quotient.Low bounds.Scale
        let high = IntegerBounds.floorDivide quotient.High bounds.Scale
        if low <> high then ValueNone
        else
            let reduced = bounds.Subtract(value,bounds.MultiplyInteger(halfPi,low))
            let quadrant = int (low % 4I)
            let quadrant = if quadrant < 0 then quadrant+4 else quadrant
            let selectedSine,negative =
                if sine then
                    match quadrant with 0 -> true,false | 1 -> false,false | 2 -> true,true | _ -> false,true
                else
                    match quadrant with 0 -> false,false | 1 -> true,true | 2 -> false,true | _ -> true,false
            let result = DecimalSeries.circular bounds selectedSine reduced
            ValueSome(if negative then bounds.Negate(result) else result)
