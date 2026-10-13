namespace SoloDatabase

open System
open System.Numerics
open SoloDatabase.Metadata.Generation
open SoloDatabase.Numerics
open SoloDatabase.RawSqliteFunctions

/// Fixed operation identifiers preserve argument roles at the numerical callback owner.
module internal NumericalFunctions =
    let private read (value: SqliteRawValue) = NumericKey.decode(value.CopyToString())
    let private encodedDecimal value = NumericKey.encode(ExactNumber.ofDecimal value)
    let private encodedInteger value = NumericKey.encode(ExactNumber.ofBigInteger value)
    let private finish domain value =
        if domain = NumericKind.Decimal then encodedDecimal(ExactNumber.toDecimal value)
        else encodedInteger(ExactNumber.toBigInteger value)

    let private finishBinary domain (arguments: ReadOnlySpan<SqliteRawValue>) value =
        if arguments.Length=5 then
            let converted =
                match enum<ExactNumericOperation>(arguments[3].GetInt32()) with
                | ExactNumericOperation.ToUInt64 ->
                    if arguments[4].GetInt32()<>0 then ExactNumber.toUInt64 value
                    else ExactNumber.toUInt64Unchecked value
                | _ -> invalidArg "target" "A native unsigned operation target is required."
            encodedInteger(BigInteger converted)
        else finish domain value

    let private integerPower domain value exponent =
        let one=ExactNumber.ofBigInteger BigInteger.One
        let multiply left right =
            let result=ExactNumber.multiply left right
            if domain=NumericKind.Decimal then ExactNumber.ofDecimal(ExactNumber.toDecimal result)
            else ExactNumber.ofBigInteger(ExactNumber.toBigInteger result)
        let divide left right =
            if domain=NumericKind.Decimal then ExactNumber.ofDecimal(ExactNumber.divideDecimal left right)
            else ExactNumber.ofBigInteger(ExactNumber.divideInteger left right)
        let rec power count =
            if count=0 then one
            elif count=1 then value
            else
                let half=power(count/2)
                let square=multiply half half
                if count%2=0 then square else multiply square value
        if exponent>=0 then finish domain (power exponent)
        elif exponent=Int32.MinValue then finish domain (divide (divide one (power(exponent+1))) value)
        else finish domain (divide one (power exponent))

    let private nativeResult operation (context: byref<SqliteRawContext>) value =
        match operation with
        | ExactNumericOperation.ToDouble -> context.SetDouble(ExactNumber.toDouble value)
        | ExactNumericOperation.ToSingle -> context.SetDouble(double(ExactNumber.toSingle value))
        | ExactNumericOperation.ToInt64 -> context.SetInt64(ExactNumber.toInt64 value)
        | ExactNumericOperation.ToInt32 -> context.SetInt64(int64(ExactNumber.toInt32 value))
        | ExactNumericOperation.ToInt16 -> context.SetInt64(int64(ExactNumber.toInt16 value))
        | ExactNumericOperation.ToSByte -> context.SetInt64(int64(ExactNumber.toSByte value))
        | ExactNumericOperation.ToUInt64 ->
            let converted=ExactNumber.toUInt64 value
            context.SetText(NumericKey.encode(ExactNumber.ofBigInteger(BigInteger converted)))
        | ExactNumericOperation.ToUInt32 -> context.SetInt64(int64(ExactNumber.toUInt32 value))
        | ExactNumericOperation.ToUInt16 -> context.SetInt64(int64(ExactNumber.toUInt16 value))
        | ExactNumericOperation.ToByte -> context.SetInt64(int64(ExactNumber.toByte value))
        | _ -> invalidArg "operation" "A native numerical target is required."

    let private nativeValue (context: byref<SqliteRawContext>) (arguments: ReadOnlySpan<SqliteRawValue>) =
        let value=arguments[2]
        let operation=enum<ExactNumericOperation>(arguments[1].GetInt32())
        if value.IsNull then context.SetNull()
        elif operation=ExactNumericOperation.ToUInt64 then
            match NumericalJson.storedValue(value.CopyToString()) with
            | ValueSome number -> nativeResult operation &context number
            | ValueNone -> context.SetNull()
        elif value.IsInteger then context.SetInt64(value.GetInt64())
        elif value.IsFloat then context.SetDouble(value.GetDouble())
        else nativeResult operation &context (read value)

    let private jsonResult domain (arguments: ReadOnlySpan<SqliteRawValue>) (context: byref<SqliteRawContext>) value =
        match value with
        | ValueNone -> context.SetNull()
        | ValueSome value ->
            let target = enum<ExactNumericOperation>(arguments[1].GetInt32())
            if target = ExactNumericOperation.Cast then context.SetText(finish domain value)
            else nativeResult target &context value

    let private jsonValue domain consume (context: byref<SqliteRawContext>) (arguments: ReadOnlySpan<SqliteRawValue>) =
        let input = arguments[2]
        if input.IsNull then context.SetNull()
        else
            jsonResult domain arguments &context (consume(input.CopyToString()))

    let private storedValue domain (context: byref<SqliteRawContext>) arguments = jsonValue domain NumericalJson.storedValue &context arguments
    let private nativeJsonValue domain (context: byref<SqliteRawContext>) arguments = jsonValue domain NumericalJson.nativeValue &context arguments

    let private writerValue domain (context: byref<SqliteRawContext>) (arguments: ReadOnlySpan<SqliteRawValue>) =
        if arguments[2].IsNull then context.SetNull()
        else
            let text = arguments[2].CopyToString()
            let input = NumericalJson.readInput text
            if NumericalJson.isNullInput input then context.SetNull()
            else
                let inputType = JsonSerializator.JsonValue.Parse(arguments[3].CopyToString())
                                |> QuerySerializationDescriptor.resolveInputDescriptor
                let target = enum<ExactNumericOperation>(arguments[1].GetInt32())
                if QueryTranslatorSerialization.isStoredNumericalInput inputType then
                    jsonResult domain arguments &context (NumericalJson.storedInput input)
                elif target = ExactNumericOperation.Cast || target = ExactNumericOperation.ToUInt64 then
                    jsonResult domain arguments &context (NumericalJson.nativeInput input)
                else
                    // The original JSON token reaches SQLite scalar extraction unchanged.
                    // In particular, a String exponent retains the native CAST contract.
                    context.SetText(text)

    let private scalarInput domain (value: SqliteRawValue) =
        if value.IsInteger then ExactNumber.ofBigInteger(BigInteger(value.GetInt64()))
        elif value.IsFloat then
            if domain = NumericKind.Decimal then ExactNumber.ofDecimal(decimal(value.GetDouble()))
            else ExactNumber.ofBigInteger(BigInteger(value.GetDouble()))
        else NumericalJson.parse(value.CopyToString())

    let private unary domain (context: byref<SqliteRawContext>) (arguments: ReadOnlySpan<SqliteRawValue>) =
        if arguments[1].IsNull then context.SetNull()
        else
            let operation = enum<ExactNumericOperation>(arguments[0].GetInt32())
            if operation = ExactNumericOperation.Cast then context.SetText(finish domain (scalarInput domain arguments[1]))
            else
                let value = read arguments[1]
                match operation with
                | ExactNumericOperation.Sign -> context.SetInt32(value.Coefficient.Sign)
                | ExactNumericOperation.BinaryLogarithm -> context.SetDouble(BigInteger.Log(ExactNumber.toBigInteger value))
                | ExactNumericOperation.BinaryLog10 -> context.SetDouble(BigInteger.Log10(ExactNumber.toBigInteger value))
                | ExactNumericOperation.Text ->
                    context.SetText(if domain = NumericKind.Decimal then (ExactNumber.toDecimal value).ToString(Globalization.CultureInfo.InvariantCulture)
                                    else (ExactNumber.toBigInteger value).ToString(Globalization.CultureInfo.InvariantCulture))
                | ExactNumericOperation.ToDouble | ExactNumericOperation.ToSingle
                | ExactNumericOperation.ToInt64 | ExactNumericOperation.ToInt32 | ExactNumericOperation.ToInt16
                | ExactNumericOperation.ToSByte | ExactNumericOperation.ToUInt64 | ExactNumericOperation.ToUInt32
                | ExactNumericOperation.ToUInt16 | ExactNumericOperation.ToByte -> nativeResult operation &context value
                | ExactNumericOperation.FromNumeric -> context.SetText(finish domain value)
                | ExactNumericOperation.Negate -> context.SetText(finish domain (ExactNumber.negate value))
                | ExactNumericOperation.Absolute -> context.SetText(finish domain (ExactNumber.abs value))
                | ExactNumericOperation.Floor -> context.SetText(finish domain (ExactNumber.floor value))
                | ExactNumericOperation.Ceiling -> context.SetText(finish domain (ExactNumber.ceiling value))
                | ExactNumericOperation.Truncate -> context.SetText(finish domain (ExactNumber.truncate value))
                | ExactNumericOperation.Round ->
                    if arguments[2].IsNull || arguments[3].IsNull then context.SetNull()
                    else context.SetText(finish domain (ExactNumber.round value (arguments[2].GetInt32()) (enum<MidpointRounding>(arguments[3].GetInt32()))))
                | _ ->
                    let result =
                        match operation with
                        | ExactNumericOperation.SquareRoot -> ExactDecimalMath.Sqrt value
                        | ExactNumericOperation.Exponential -> ExactDecimalMath.Exp value
                        | ExactNumericOperation.Logarithm -> ExactDecimalMath.Log value
                        | ExactNumericOperation.Log10 -> ExactDecimalMath.Log10 value
                        | ExactNumericOperation.Log2 -> ExactDecimalMath.Log2 value
                        | ExactNumericOperation.Sine -> ExactDecimalMath.Sin value
                        | ExactNumericOperation.Cosine -> ExactDecimalMath.Cos value
                        | ExactNumericOperation.Tangent -> ExactDecimalMath.Tan value
                        | ExactNumericOperation.Asin -> ExactDecimalMath.Asin value
                        | ExactNumericOperation.Acos -> ExactDecimalMath.Acos value
                        | ExactNumericOperation.Atan -> ExactDecimalMath.Atan value
                        | ExactNumericOperation.Sinh -> ExactDecimalMath.Sinh value
                        | ExactNumericOperation.Cosh -> ExactDecimalMath.Cosh value
                        | ExactNumericOperation.Tanh -> ExactDecimalMath.Tanh value
                        | ExactNumericOperation.Asinh -> ExactDecimalMath.Asinh value
                        | ExactNumericOperation.Acosh -> ExactDecimalMath.Acosh value
                        | ExactNumericOperation.Atanh -> ExactDecimalMath.Atanh value
                        | ExactNumericOperation.Degrees -> ExactDecimalMath.Degrees value
                        | ExactNumericOperation.Radians -> ExactDecimalMath.Radians value
                        | _ -> invalidArg "operation" "The operation has no unary numerical role."
                    context.SetText(encodedDecimal result)

    let private binary domain (context: byref<SqliteRawContext>) (arguments: ReadOnlySpan<SqliteRawValue>) =
        if arguments[1].IsNull || arguments[2].IsNull then context.SetNull()
        else
            let operation = enum<ExactNumericOperation>(arguments[0].GetInt32())
            let left = read arguments[1]
            if operation = ExactNumericOperation.IntegerPower then
                context.SetText(integerPower domain left (arguments[2].GetInt32()))
            elif operation = ExactNumericOperation.Power && domain = NumericKind.ArbitraryInteger then
                context.SetText(encodedInteger(BigInteger.Pow(ExactNumber.toBigInteger left,arguments[2].GetInt32())))
            elif operation = ExactNumericOperation.BinaryLogarithm then
                context.SetDouble(BigInteger.Log(ExactNumber.toBigInteger left,arguments[2].GetDouble()))
            else
                let right = read arguments[2]
                let result =
                    match operation with
                    | ExactNumericOperation.Add -> finishBinary domain arguments (ExactNumber.add left right)
                    | ExactNumericOperation.Subtract -> finishBinary domain arguments (ExactNumber.subtract left right)
                    | ExactNumericOperation.Multiply -> finishBinary domain arguments (ExactNumber.multiply left right)
                    | ExactNumericOperation.Divide ->
                        if domain = NumericKind.Decimal then encodedDecimal(ExactNumber.divideDecimal left right)
                        else finishBinary domain arguments (ExactNumber.ofBigInteger(ExactNumber.divideInteger left right))
                    | ExactNumericOperation.Remainder -> finishBinary domain arguments (ExactNumber.remainder left right)
                    | ExactNumericOperation.IeeeRemainder -> finishBinary domain arguments (ExactNumber.ieeeRemainder left right)
                    | ExactNumericOperation.Minimum -> finishBinary domain arguments (ExactNumber.minimum left right)
                    | ExactNumericOperation.Maximum -> finishBinary domain arguments (ExactNumber.maximumValue left right)
                    | ExactNumericOperation.Logarithm -> encodedDecimal(ExactDecimalMath.Log(left,right))
                    | ExactNumericOperation.Power -> encodedDecimal(ExactDecimalMath.Pow(left,right))
                    | ExactNumericOperation.Atan2 -> encodedDecimal(ExactDecimalMath.Atan2(left,right))
                    | _ -> invalidArg "operation" "The operation has no binary numerical role."
                context.SetText result

    let register (connection: NativeSqliteConnection) =
        connection.RegisterNumericalScalar("NUMERIC_NORMALIZE",2,RawScalarFunc(fun context arguments ->
            if arguments[1].IsNull then context.SetNull()
            else
                match enum<NumericKind>(arguments[0].GetInt32()) with
                | NumericKind.Decimal | NumericKind.ArbitraryInteger ->
                    match NumericalJson.storedValue(arguments[1].CopyToString()) with
                    | ValueSome value -> context.SetText(NumericKey.encode value)
                    | ValueNone -> raise(FormatException("A numerical JSON value is required."))
                | _ -> invalidArg "kind" "An exact numerical type is required."),SqliteFunctionFlags.Deterministic)
        for domain in [NumericKind.Decimal;NumericKind.ArbitraryInteger] do
            let name = ExactNumericNames.scalar domain
            connection.RegisterNumericalScalar(name,2,RawScalarFunc(fun context arguments -> unary domain &context arguments),SqliteFunctionFlags.Deterministic)
            connection.RegisterNumericalScalar(name,4,RawScalarFunc(fun context arguments ->
                match enum<ExactNumericOperation>(arguments[0].GetInt32()) with
                | ExactNumericOperation.WriterValue -> writerValue domain &context arguments
                | _ -> unary domain &context arguments),SqliteFunctionFlags.Deterministic)
            connection.RegisterNumericalScalar(name,3,RawScalarFunc(fun context arguments ->
                match enum<ExactNumericOperation>(arguments[0].GetInt32()) with
                | ExactNumericOperation.NativeValue -> nativeValue &context arguments
                | ExactNumericOperation.StoredValue -> storedValue domain &context arguments
                | ExactNumericOperation.NativeJsonValue -> nativeJsonValue domain &context arguments
                | _ -> binary domain &context arguments),SqliteFunctionFlags.Deterministic)
            connection.RegisterNumericalScalar(name,5,RawScalarFunc(fun context arguments -> binary domain &context arguments),SqliteFunctionFlags.Deterministic)
            let finish value = finish domain value
            for kind in [DecimalAggregateKind.Sum;DecimalAggregateKind.Average;DecimalAggregateKind.Minimum;DecimalAggregateKind.Maximum] do
                if domain = NumericKind.Decimal then
                    let callback = NativeAggregateFunction<DecimalAggregate option,string>(None,
                        NativeAggregateStep(fun state arguments ->
                            if arguments[0].IsNull then state
                            else
                                let accumulator = match state with Some value -> value | None -> DecimalAggregate(kind)
                                accumulator.Step(read arguments[0])
                                Some accumulator),
                        Func<_,_>(fun state ->
                            let accumulator = match state with Some value -> value | None -> DecimalAggregate(kind)
                            match accumulator.Finish() with ValueSome value -> encodedDecimal value | ValueNone -> null),true)
                    connection.RegisterAggregate(ExactNumericNames.aggregate kind domain,1,callback,SqliteFunctionFlags.Deterministic)
                else
                    let callback = NativeAggregateFunction<struct(ExactNumber voption * int64),string>(struct(ValueNone,0L),
                        NativeAggregateStep(fun (struct(selected,count)) arguments ->
                            if arguments[0].IsNull then struct(selected,count)
                            else
                                let input = read arguments[0]
                                let result =
                                    match selected,kind with
                                    | ValueNone,_ -> input
                                    | ValueSome previous,DecimalAggregateKind.Minimum -> ExactNumber.minimum previous input
                                    | ValueSome previous,DecimalAggregateKind.Maximum -> ExactNumber.maximumValue previous input
                                    | ValueSome previous,_ -> ExactNumber.add previous input
                                struct(ValueSome result,Checked.(+) count 1L)),
                        Func<_,_>(fun (struct(selected,count)) ->
                            match selected,kind with
                            | ValueNone,DecimalAggregateKind.Sum -> "M"
                            | ValueNone,_ -> null
                            | ValueSome value,DecimalAggregateKind.Average -> encodedInteger(ExactNumber.divideInteger value (ExactNumber.ofBigInteger(BigInteger count)))
                            | ValueSome value,_ -> finish value),true)
                    connection.RegisterAggregate(ExactNumericNames.aggregate kind domain,1,callback,SqliteFunctionFlags.Deterministic)
