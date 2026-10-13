namespace SoloDatabase.Numerics

open System
open System.Numerics

/// Both integer endpoints use the context's common binary denominator.
[<Struct>]
type internal DecimalInterval =
    { Low: BigInteger
      High: BigInteger }

module internal IntegerBounds =
    let floorDivide (numerator: BigInteger) (denominator: BigInteger) =
        let mutable remainder = BigInteger.Zero
        let quotient = BigInteger.DivRem(numerator,denominator,&remainder)
        if remainder.Sign < 0 then quotient-BigInteger.One else quotient

    let ceilingDivide (numerator: BigInteger) (denominator: BigInteger) =
        -floorDivide (-numerator) denominator

    let bitLength (value: BigInteger) =
            let mutable size = value
            let mutable bits = 0
            while size > BigInteger(UInt32.MaxValue) do
                size <- size >>> 32
                bits <- Checked.(+) bits 32
            let mutable last = uint32 size
            while last <> 0u do
                last <- last >>> 1
                bits <- bits + 1
            bits

    let squareRoot (value: BigInteger) =
        if value.IsZero then BigInteger.Zero
        else
            let bits = bitLength value
            let mutable root = BigInteger.One <<< ((bits+1)/2)
            let mutable next = (root + value/root) >>> 1
            while next < root do
                root <- next
                next <- (root + value/root) >>> 1
            root

    let root (value: BigInteger) degree =
        if degree = 2 then squareRoot value
        elif value <= BigInteger.One then value
        elif degree >= bitLength value then BigInteger.One
        else
            let mutable low = BigInteger.Zero
            let bits = bitLength value
            let mutable high = BigInteger.One <<< ((bits - 1)/degree + 1)
            while high-low > BigInteger.One do
                let middle = (low+high) >>> 1
                if BigInteger.Pow(middle,degree) <= value then low <- middle else high <- middle
            low

[<Sealed>]
type internal DecimalIntervalContext(precision: int) =
    let scale = BigInteger.One <<< precision
    member _.Precision = precision
    member _.Scale = scale
    member _.Zero = { Low = BigInteger.Zero; High = BigInteger.Zero }
    member _.One = { Low = scale; High = scale }
    member _.Integer(value: BigInteger) = { Low = value*scale; High = value*scale }
    member _.Ratio(numerator: BigInteger, denominator: BigInteger) =
        let numerator,denominator = if denominator.Sign < 0 then -numerator,-denominator else numerator,denominator
        { Low = IntegerBounds.floorDivide (numerator*scale) denominator
          High = IntegerBounds.ceilingDivide (numerator*scale) denominator }
    member this.Number(value: ExactNumber) =
        if value.Coefficient.IsZero then this.Zero
        elif value.Exponent < -BigInteger precision - BigInteger(IntegerBounds.bitLength(BigInteger.Abs value.Coefficient)) then
            if value.Coefficient.Sign < 0 then { Low = -BigInteger.One; High = BigInteger.Zero }
            else { Low = BigInteger.Zero; High = BigInteger.One }
        else
            let struct(numerator,denominator) = ExactNumber.ratio value
            this.Ratio(numerator,denominator)
    /// The caller selects a quotient of magnitude at most one before scaling.
    member this.Quotient(numerator: ExactNumber, denominator: ExactNumber) =
        if numerator.Coefficient.IsZero then this.Zero
        else
            let exponent=numerator.Exponent-denominator.Exponent
            if exponent < -BigInteger precision - BigInteger(IntegerBounds.bitLength(BigInteger.Abs numerator.Coefficient)) then
                if numerator.Coefficient.Sign=denominator.Coefficient.Sign then {Low=BigInteger.Zero;High=BigInteger.One}
                else {Low= -BigInteger.One;High=BigInteger.Zero}
            elif exponent.Sign>=0 then
                this.Ratio(numerator.Coefficient*BigInteger.Pow(10I,int exponent),denominator.Coefficient)
            else this.Ratio(numerator.Coefficient,denominator.Coefficient*BigInteger.Pow(10I,int(-exponent)))
    member _.Negate(value: DecimalInterval) = { Low = -value.High; High = -value.Low }
    member _.Add(left: DecimalInterval, right: DecimalInterval) =
        { Low = left.Low+right.Low; High = left.High+right.High }
    member _.Subtract(left: DecimalInterval, right: DecimalInterval) =
        { Low = left.Low-right.High; High = left.High-right.Low }
    member _.Multiply(left: DecimalInterval, right: DecimalInterval) =
        let ac,ad = left.Low*right.Low,left.Low*right.High
        let bc,bd = left.High*right.Low,left.High*right.High
        { Low = IntegerBounds.floorDivide (min (min ac ad) (min bc bd)) scale
          High = IntegerBounds.ceilingDivide (max (max ac ad) (max bc bd)) scale }
    member _.MultiplyInteger(value: DecimalInterval, factor: BigInteger) =
        if factor.Sign < 0 then { Low = value.High*factor; High = value.Low*factor }
        else { Low = value.Low*factor; High = value.High*factor }
    member _.DivideInteger(value: DecimalInterval, divisor: BigInteger) =
        if divisor.Sign < 0 then
            { Low = IntegerBounds.floorDivide (-value.High) (-divisor)
              High = IntegerBounds.ceilingDivide (-value.Low) (-divisor) }
        else
            { Low = IntegerBounds.floorDivide value.Low divisor
              High = IntegerBounds.ceilingDivide value.High divisor }
    member _.Divide(left: DecimalInterval, right: DecimalInterval) =
        if right.Low <= BigInteger.Zero && right.High >= BigInteger.Zero then ValueNone
        else
            let bound (numerator: BigInteger) (denominator: BigInteger) =
                if denominator.Sign < 0 then -numerator,-denominator else numerator,denominator
            let low numerator denominator =
                let numerator,denominator = bound numerator denominator
                IntegerBounds.floorDivide (numerator*scale) denominator
            let high numerator denominator =
                let numerator,denominator = bound numerator denominator
                IntegerBounds.ceilingDivide (numerator*scale) denominator
            ValueSome
                { Low = min (min (low left.Low right.Low) (low left.Low right.High))
                            (min (low left.High right.Low) (low left.High right.High))
                  High = max (max (high left.Low right.Low) (high left.Low right.High))
                             (max (high left.High right.Low) (high left.High right.High)) }
    member _.SquareRoot(value: DecimalInterval) =
        if value.Low.Sign < 0 then ValueNone
        else
            let low = IntegerBounds.squareRoot (value.Low*scale)
            let highValue = value.High*scale
            let high = IntegerBounds.squareRoot highValue
            ValueSome { Low = low; High = if high*high = highValue then high else high+BigInteger.One }
