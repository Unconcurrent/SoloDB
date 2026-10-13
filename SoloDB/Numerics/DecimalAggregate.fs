namespace SoloDatabase.Numerics

open System
open System.Numerics
open SoloDatabase.Metadata.Generation

[<RequireQualifiedAccess>]
type internal DecimalAggregateKind =
    | Sum
    | Average
    | Minimum
    | Maximum

type internal ExactNumericOperation =
    | Add = 0 | Subtract = 1 | Multiply = 2 | Divide = 3 | Remainder = 4 | IeeeRemainder = 5
    | Negate = 6 | Absolute = 7 | Floor = 8 | Ceiling = 9 | Truncate = 10 | Sign = 11
    | Minimum = 12 | Maximum = 13 | Round = 14 | Cast = 15 | FromNumeric = 16
    | ToDouble = 17 | ToSingle = 18 | ToInt64 = 19 | ToInt32 = 20 | ToInt16 = 21 | ToSByte = 22
    | ToUInt64 = 23 | ToUInt32 = 24 | ToUInt16 = 25 | ToByte = 26
    | SquareRoot = 27 | Exponential = 28 | Logarithm = 29 | Log10 = 30 | Log2 = 31 | Power = 32
    | Sine = 33 | Cosine = 34 | Tangent = 35 | Asin = 36 | Acos = 37 | Atan = 38 | Atan2 = 39
    | Sinh = 40 | Cosh = 41 | Tanh = 42 | Asinh = 43 | Acosh = 44 | Atanh = 45
    | Degrees = 46 | Radians = 47 | Text = 48 | BinaryLogarithm = 49 | BinaryLog10 = 50 | NativeValue = 51
    | IntegerPower = 52 | StoredValue = 53 | NativeJsonValue = 54 | WriterValue = 55

module internal ExactNumericNames =
    let prefix = function
        | NumericKind.Decimal -> "DECIMAL_"
        | NumericKind.ArbitraryInteger -> "BIGINTEGER_"
        | _ -> invalidArg "domain" "An exact operation requires an exact numerical type."

    let scalar domain =
        match domain with
        | NumericKind.Decimal -> "DECIMAL"
        | NumericKind.ArbitraryInteger -> "BIGINTEGER"
        | _ -> invalidArg "domain" "An exact operation requires an exact numerical type."

    let operation = function
        | "ADD" -> ExactNumericOperation.Add | "SUB" -> ExactNumericOperation.Subtract
        | "MUL" -> ExactNumericOperation.Multiply | "DIV" -> ExactNumericOperation.Divide
        | "MOD" -> ExactNumericOperation.Remainder | "IEEE_REM" -> ExactNumericOperation.IeeeRemainder
        | "NEG" -> ExactNumericOperation.Negate | "ABS" -> ExactNumericOperation.Absolute
        | "FLOOR" -> ExactNumericOperation.Floor | "CEIL" -> ExactNumericOperation.Ceiling
        | "TRUNC" -> ExactNumericOperation.Truncate | "SIGN" -> ExactNumericOperation.Sign
        | "MIN" -> ExactNumericOperation.Minimum | "MAX" -> ExactNumericOperation.Maximum
        | "ROUND" -> ExactNumericOperation.Round | "CAST" -> ExactNumericOperation.Cast
        | "FROM_NUMERIC" -> ExactNumericOperation.FromNumeric
        | "NATIVE_VALUE" -> ExactNumericOperation.NativeValue
        | "STORED_VALUE" -> ExactNumericOperation.StoredValue
        | "NATIVE_JSON_VALUE" -> ExactNumericOperation.NativeJsonValue
        | "WRITER_VALUE" -> ExactNumericOperation.WriterValue
        | "TO_DOUBLE" -> ExactNumericOperation.ToDouble | "TO_SINGLE" -> ExactNumericOperation.ToSingle
        | "TO_INT64" -> ExactNumericOperation.ToInt64 | "TO_INT32" -> ExactNumericOperation.ToInt32
        | "TO_INT16" -> ExactNumericOperation.ToInt16 | "TO_SBYTE" -> ExactNumericOperation.ToSByte
        | "TO_UINT64" -> ExactNumericOperation.ToUInt64 | "TO_UINT32" -> ExactNumericOperation.ToUInt32
        | "TO_UINT16" -> ExactNumericOperation.ToUInt16 | "TO_BYTE" -> ExactNumericOperation.ToByte
        | "SQRT" -> ExactNumericOperation.SquareRoot | "EXP" -> ExactNumericOperation.Exponential
        | "LN" | "LOG" -> ExactNumericOperation.Logarithm
        | "LOG10" -> ExactNumericOperation.Log10 | "LOG2" -> ExactNumericOperation.Log2
        | "POW" -> ExactNumericOperation.Power | "POW_INTEGER" -> ExactNumericOperation.IntegerPower
        | "SIN" -> ExactNumericOperation.Sine
        | "COS" -> ExactNumericOperation.Cosine | "TAN" -> ExactNumericOperation.Tangent
        | "ASIN" -> ExactNumericOperation.Asin | "ACOS" -> ExactNumericOperation.Acos
        | "ATAN" -> ExactNumericOperation.Atan | "ATAN2" -> ExactNumericOperation.Atan2
        | "SINH" -> ExactNumericOperation.Sinh | "COSH" -> ExactNumericOperation.Cosh
        | "TANH" -> ExactNumericOperation.Tanh | "ASINH" -> ExactNumericOperation.Asinh
        | "ACOSH" -> ExactNumericOperation.Acosh | "ATANH" -> ExactNumericOperation.Atanh
        | "DEGREES" -> ExactNumericOperation.Degrees | "RADIANS" -> ExactNumericOperation.Radians
        | "TEXT" -> ExactNumericOperation.Text
        | "LN_BINARY" | "LOG_BINARY" -> ExactNumericOperation.BinaryLogarithm
        | "LOG10_BINARY" -> ExactNumericOperation.BinaryLog10
        | name -> invalidArg "operation" ("Unsupported exact operation: " + name)

    let aggregate kind domain =
        prefix domain +
            (match kind with
             | DecimalAggregateKind.Sum -> "SUM"
             | DecimalAggregateKind.Average -> "AVG"
             | DecimalAggregateKind.Minimum -> "MIN_AGG"
             | DecimalAggregateKind.Maximum -> "MAX_AGG")

/// The typed operation boundary rounds each accumulated sum before the next input.
type internal DecimalAggregate(kind: DecimalAggregateKind) =
    let mutable sum = Decimal.Zero
    let mutable count = 0L
    let mutable selected = ValueNone

    member _.Step(value: ExactNumber) =
        match kind with
        | DecimalAggregateKind.Sum | DecimalAggregateKind.Average ->
            sum <- ExactNumber.toDecimal(ExactNumber.add (ExactNumber.ofDecimal sum) value)
            if kind = DecimalAggregateKind.Average then count <- Checked.(+) count 1L
        | DecimalAggregateKind.Minimum ->
            selected <- ValueSome(match selected with ValueSome previous -> ExactNumber.minimum previous value | ValueNone -> value)
        | DecimalAggregateKind.Maximum ->
            selected <- ValueSome(match selected with ValueSome previous -> ExactNumber.maximumValue previous value | ValueNone -> value)

    member _.Finish() =
        match kind with
        | DecimalAggregateKind.Sum -> ValueSome sum
        | DecimalAggregateKind.Average ->
            if count = 0L then ValueNone
            else ValueSome(ExactNumber.divideDecimal (ExactNumber.ofDecimal sum) (ExactNumber.ofBigInteger(BigInteger count)))
        | DecimalAggregateKind.Minimum | DecimalAggregateKind.Maximum ->
            match selected with ValueSome value -> ValueSome(ExactNumber.toDecimal value) | ValueNone -> ValueNone
