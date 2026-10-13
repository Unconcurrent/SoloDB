namespace SoloDatabase.JsonSerializator

open System
open System.Reflection
open System.Reflection.Emit
open System.Runtime.CompilerServices
open SoloDatabase
open SoloDatabase.Metadata.CodecGeneration
open SoloDatabase.Metadata.Generation
open SoloDatabase.Numerics

module internal JsonNumericRuntime =
    let rec private create (target: Type) (reader: Type) stored =
        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "direct JSON read" target
        let storedName = if stored then TypeSupport.StoredNumericReader target.FullName else None
        let coreName = (TypeSupport.NumericCore target.FullName).Value
        let core = if coreName = target.FullName then target else typeof<int>.Assembly.GetType(coreName,true)
        let adapter = not (core.Equals(target))
        let shape = (TypeSupport.Numeric coreName).Value
        let width = if shape.Native then IntPtr.Size * 8 else shape.Width
        let utf16 = typeof<Utf16Reader>.Equals(reader)
        let moduleValue = typeof<JsonRawPrimitive>.Module
        let offsetMethod index =
            typeof<Unsafe>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
            |> Array.find(fun method -> method.Name = "Add" && method.IsGenericMethodDefinition &&
                                        method.GetParameters().Length = 2 && method.GetParameters().[0].ParameterType.IsByRef &&
                                        method.GetParameters().[1].ParameterType.Equals(index))
        let offset = (offsetMethod typeof<unativeint>).MakeGenericMethod(typeof<byte>) :> MethodBase
        let api = { Int32 = typeof<int>; Int64 = typeof<int64>; UInt32 = typeof<uint32>; UInt64 = typeof<uint64>
                    OffsetByte = offset; InvalidNumber = typeof<FormatException>.GetConstructor([|typeof<string>|]) :> MethodBase
                    Overflow = typeof<OverflowException>.GetConstructor(Type.EmptyTypes) :> MethodBase
                    InvalidHex = typeof<Exception>.GetConstructor([|typeof<string>|]) :> MethodBase }
        let context = typeof<JsonDirectReader<obj,Utf8Reader>>.GetMethod("Invoke",BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic).GetParameters().[0].ParameterType
                            .GetElementType().GetGenericTypeDefinition().MakeGenericType(reader)
        let rawArguments = [|typeof<byte>.MakeByRefType();typeof<int>;typeof<int>.MakeByRefType()|]
        let flags = BindingFlags.NonPublic ||| BindingFlags.Static
        let magnitude() =
            let resize = typeof<Array>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                         |> Array.find(fun method -> method.Name = "Resize" && method.IsGenericMethodDefinition)
            { UInt32 = typeof<uint32>; Limbs = typeof<uint32[]>; Int32 = typeof<int>; UInt64 = typeof<uint64>
              Reference = typeof<uint32>.MakeByRefType()
              Offset = (offsetMethod typeof<unativeint>).MakeGenericMethod(typeof<uint32>) :> MethodBase
              ReferenceOf = typeof<BigIntegerMagnitude>.GetMethod("Reference",flags) :> MethodBase
              Resize = resize.MakeGenericMethod(typeof<uint32>) :> MethodBase }
        let compatibility =
            if (TypeSupport.NumericTextAdapter coreName).IsSome then JsonNumericScalarRuntime.compatibility core reader (context.MakeByRefType())
            else typeof<JsonRawPrimitive>.GetMethod("Read" + TypeSupport.NumericIntrinsicOperation coreName,flags).MakeGenericMethod(reader) :> MethodBase
        let input name = typeof<JsonNumericInput>.GetMethod(name,flags).MakeGenericMethod(reader) :> MethodBase
        let roundedBits width bits =
            JsonDefaultMethods.get bits typeof<BinaryDecimal> ("binary-rounding:" + string width) (fun () ->
                let method = DynamicMethod("RoundBinary" + string width,bits,[|typeof<uint64>;typeof<int64>;typeof<bool>|],moduleValue,true)
                let api = { Kernel = typeof<BinaryDecimal>.GetMethod("RoundBits",flags) :> MethodBase }
                CodecFloatingEmitter.bits (RuntimeCodecIL(method.GetILGenerator())) api width
                method)
        let quotedWrapper leaf =
            let method = DynamicMethod("ReadQuoted" + target.Name,core,Array.append rawArguments [|context.MakeByRefType()|],moduleValue,true)
            let quotedApi = { Result = core; Int32 = typeof<int>; OffsetByte = offset; Decimal = leaf
                              Consumed = input "Consumed"; Compatibility = compatibility }
            CodecQuotedNumberEmitter.reader (RuntimeCodecIL(method.GetILGenerator())) quotedApi utf16
            Some(method :> MethodBase)
        let leaf,hex,needsContext,quotedNumber =
            match shape.Kind with
            | NumericKind.Inferred ->
                let rounded = roundedBits 64 typeof<uint64>
                let reinterpret = typeof<Unsafe>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                                  |> Array.find(fun method -> method.Name = "As" && method.IsGenericMethodDefinition &&
                                                              method.GetGenericArguments().Length = 2 && method.GetParameters().Length = 1)
                let doubleType = typeof<double>
                let floating = { Result = doubleType; Bits = typeof<uint64>; Int32 = typeof<int>; Int64 = typeof<int64>; UInt64 = typeof<uint64>
                                 OffsetByte = offset; RoundedBits = rounded :> MethodBase
                                 ResultReference = reinterpret.MakeGenericMethod(typeof<uint64>,doubleType) :> MethodBase
                                 Ambiguous = typeof<JsonRawPrimitive>.GetMethod("ReadDouble",flags).MakeGenericMethod(reader) :> MethodBase
                                 InvalidNumber = api.InvalidNumber }
                let decimalApi = { Result = typeof<decimal>; Int32 = typeof<int>; Int64 = typeof<int64>; UInt64 = typeof<uint64>; OffsetByte = offset
                                   Constructor = typeof<decimal>.GetConstructor([|typeof<int>;typeof<int>;typeof<int>;typeof<bool>;typeof<byte>|]) :> MethodBase
                                   Overflow = api.Overflow
                                   InvalidNumber = api.InvalidNumber }
                let bigType = typeof<System.Numerics.BigInteger>
                let parameter = TypeSupport.NumericParameterResult target.FullName
                let inference = { Number = api; Decimal = decimalApi; Floating = floating; BigInteger = bigType
                                  BigReader = (if parameter then Unchecked.defaultof<MethodBase> else method bigType reader :> MethodBase)
                                  Parameter = (if parameter then Some(target.GetConstructor([|typeof<obj>;typeof<bool>|]) :> MethodBase) else None) }
                let leaf = DynamicMethod("ReadInferredDecimal",core,Array.append rawArguments [|context.MakeByRefType()|],moduleValue,true)
                CodecDynamicNumberEmitter.decimalReader (RuntimeCodecIL(leaf.GetILGenerator())) inference utf16
                let hex = DynamicMethod("ReadInferredHex",core,Array.append rawArguments [|context.MakeByRefType()|],moduleValue,true)
                CodecDynamicNumberEmitter.hexReader (RuntimeCodecIL(hex.GetILGenerator())) inference utf16
                leaf,hex,true,None
            | NumericKind.NumericText ->
                let facts = { Number = api; String = typeof<string>; Reference = typeof<byte>.MakeByRefType()
                              Construct = typeof<string>.GetConstructor([|typeof<char>;typeof<int>|]) :> MethodBase
                              StringReference = typeof<JsonNumericInput>.GetMethod("StringReference",flags) :> MethodBase }
                let arithmetic = magnitude()
                let leaf = DynamicMethod("ReadDecimalText",core,rawArguments,moduleValue,true)
                CodecNumericTextEmitter.decimalReader (RuntimeCodecIL(leaf.GetILGenerator())) facts utf16
                let hex = DynamicMethod("ReadHexText",core,rawArguments,moduleValue,true)
                CodecNumericTextEmitter.hexReader (RuntimeCodecIL(hex.GetILGenerator())) facts arithmetic typeof<uint32[]> utf16
                leaf,hex,false,None
            | NumericKind.Predicate ->
                let leaf = DynamicMethod("ReadDecimalBoolean",core,rawArguments,moduleValue,true)
                CodecNumericBooleanEmitter.decimalReader (RuntimeCodecIL(leaf.GetILGenerator())) api utf16
                let hex = DynamicMethod("ReadHexBoolean",core,rawArguments,moduleValue,true)
                CodecNumericBooleanEmitter.hexReader (RuntimeCodecIL(hex.GetILGenerator())) api utf16
                leaf,hex,false,None
            | NumericKind.ArbitraryInteger ->
                let facts = { Magnitude = magnitude(); Int64 = typeof<int64>; OffsetByte = offset
                              Construct = typeof<BigIntegerMagnitude>.GetMethod("Finish",flags) :> MethodBase
                              InvalidNumber = api.InvalidNumber; InvalidHex = api.InvalidHex }
                let leaf = DynamicMethod("ReadDecimal" + target.Name,core,rawArguments,moduleValue,true)
                CodecBigIntegerEmitter.decimalReader (RuntimeCodecIL(leaf.GetILGenerator())) facts utf16
                let hex = DynamicMethod("ReadHex" + target.Name,core,rawArguments,moduleValue,true)
                CodecBigIntegerEmitter.hexReader (RuntimeCodecIL(hex.GetILGenerator())) facts utf16
                leaf,hex,false,None
            | NumericKind.Integer ->
                let leaf hex =
                    let role = (if hex then "integer-hex:" else "integer-decimal:") + string width + ":" + string shape.Signed + ":" + string shape.Native
                    JsonDefaultMethods.get core reader role (fun () ->
                        let method = DynamicMethod((if hex then "ReadHex" else "ReadDecimal") + core.Name,core,rawArguments,moduleValue,true)
                        if hex then CodecIntegerEmitter.hexReader (RuntimeCodecIL(method.GetILGenerator())) api width shape.Signed utf16 shape.Native
                        else CodecIntegerEmitter.decimalReader (RuntimeCodecIL(method.GetILGenerator())) api width shape.Signed utf16 shape.Native
                        method)
                leaf false,leaf true,false,None
            | NumericKind.Binary ->
                let bitsType = if width = 64 then typeof<uint64> elif width = 16 then typeof<uint16> else typeof<uint32>
                let rounded = roundedBits width bitsType
                let reinterpret = typeof<Unsafe>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                                  |> Array.find(fun method -> method.Name = "As" && method.IsGenericMethodDefinition &&
                                                              method.GetGenericArguments().Length = 2 && method.GetParameters().Length = 1)
                let reinterpret = reinterpret.MakeGenericMethod(bitsType,core) :> MethodBase
                let floatingApi = { Result = core; Bits = bitsType; Int32 = typeof<int>; Int64 = typeof<int64>; UInt64 = typeof<uint64>
                                    OffsetByte = offset; RoundedBits = rounded :> MethodBase; ResultReference = reinterpret
                                    Ambiguous = compatibility; InvalidNumber = api.InvalidNumber }
                let leaf = JsonDefaultMethods.get core reader "binary-decimal" (fun () ->
                    let method = DynamicMethod("ReadDecimal" + core.Name,core,Array.append rawArguments [|context.MakeByRefType()|],moduleValue,true)
                    CodecFloatingReadEmitter.reader (RuntimeCodecIL(method.GetILGenerator())) floatingApi width utf16 false
                    method)
                let hex = JsonDefaultMethods.get core reader "binary-hex" (fun () ->
                    let method = DynamicMethod("ReadHex" + core.Name,core,rawArguments,moduleValue,true)
                    CodecFloatingReadEmitter.hex (RuntimeCodecIL(method.GetILGenerator())) api floatingApi width utf16
                    method)
                let quoted =
                    if adapter || storedName.IsSome then None
                    else
                        let quotedLeaf = JsonDefaultMethods.get core reader "binary-quoted" (fun () ->
                            let method = DynamicMethod("ReadQuotedDecimal" + core.Name,core,Array.append rawArguments [|context.MakeByRefType()|],moduleValue,true)
                            CodecFloatingReadEmitter.reader (RuntimeCodecIL(method.GetILGenerator())) floatingApi width utf16 true
                            method)
                        quotedWrapper (quotedLeaf :> MethodBase)
                leaf,hex,true,quoted
            | NumericKind.Decimal ->
                let decimalApi = { Result = core; Int32 = typeof<int>; Int64 = typeof<int64>; UInt64 = typeof<uint64>; OffsetByte = offset
                                   Constructor = typeof<decimal>.GetConstructor([|typeof<int>;typeof<int>;typeof<int>;typeof<bool>;typeof<byte>|]) :> MethodBase
                                   Overflow = api.Overflow
                                   InvalidNumber = api.InvalidNumber }
                let leaf = DynamicMethod("ReadDecimal",core,rawArguments,moduleValue,true)
                CodecDecimalEmitter.reader (RuntimeCodecIL(leaf.GetILGenerator())) decimalApi utf16 None
                let hex = DynamicMethod("ReadHexDecimal",core,rawArguments,moduleValue,true)
                CodecDecimalEmitter.hex (RuntimeCodecIL(hex.GetILGenerator())) api decimalApi utf16
                let quoted =
                    if adapter || storedName.IsSome then None
                    else
                        let quotedLeaf = DynamicMethod("ReadQuotedDecimal",core,Array.append rawArguments [|context.MakeByRefType()|],moduleValue,true)
                        CodecDecimalEmitter.reader (RuntimeCodecIL(quotedLeaf.GetILGenerator())) decimalApi utf16 (Some compatibility)
                        quotedWrapper (quotedLeaf :> MethodBase)
                leaf,hex,false,quoted
            | _ -> invalidArg "target" "Unsupported numeric format."
        let quoted =
            if shape.Kind = NumericKind.Integer && not adapter && storedName.IsNone then
                let method = DynamicMethod("ReadQuoted" + target.Name,core,Array.append rawArguments [|context.MakeByRefType()|],moduleValue,true)
                let quotedApi = { Int32 = typeof<int>; UInt32 = typeof<uint32>; UInt64 = typeof<uint64>; OffsetByte = offset; Compatibility = compatibility }
                CodecQuotedIntegerEmitter.reader (RuntimeCodecIL(method.GetILGenerator())) quotedApi width shape.Signed utf16 shape.Native
                Some(method :> MethodBase)
            else quotedNumber
        let targetCompatibility =
            if not adapter then compatibility
            else typeof<JsonRawPrimitive>.GetMethod("Read" + TypeSupport.NumericIntrinsicOperation target.FullName,flags).MakeGenericMethod(reader) :> MethodBase
        let mutable argument = core
        let after = TypeSupport.NumericFinish target.FullName |> Array.map(fun (owner,name,constructor) ->
            let owner = if owner = target.FullName then target else typeof<int>.Assembly.GetType(owner,true)
            let method =
                if constructor then owner.GetConstructor([|argument|]) :> MethodBase
                else owner.GetMethod(name,BindingFlags.Public ||| BindingFlags.Static,null,[|argument|],null) :> MethodBase
            argument <- if constructor then owner else (method :?> MethodInfo).ReturnType
            { Method = method; Constructor = constructor })
        let predicate =
            if TypeSupport.NumericPredicate target.FullName && shape.Kind <> NumericKind.Predicate then
                Some(core.GetMethod("op_Implicit",[|typeof<int>|]) :> MethodBase,
                     core.GetMethod("op_Inequality",[|core;core|]) :> MethodBase)
            else None
        let text =
            if TypeSupport.NumericText target.FullName && shape.Kind <> NumericKind.NumericText then
                Some(typeof<System.Globalization.CultureInfo>.GetProperty("InvariantCulture").GetMethod :> MethodBase,
                     core.GetMethod("ToString",[|typeof<IFormatProvider>|]) :> MethodBase)
            else None
        let storedQuoted =
            if stored then storedName |> Option.map(fun name ->
                let read = typeof<JsonStoredNumeric>.GetMethod(name,flags).MakeGenericMethod(if name="ReadKeyHalf" then [|core;reader|] else [|reader|])
                read :> MethodBase)
            else None
        let driver = DynamicMethod("ReadNumeric" + target.Name,target,[|context.MakeByRefType()|],moduleValue,true)
        let driverApi =
            { Result = target; Core = core; After = after; Predicate = predicate; Text = text; Int32 = typeof<int>; SourceReference = typeof<byte>.MakeByRefType(); PositionReference = typeof<int>.MakeByRefType()
              Begin = input "Begin"; Reference = input "Reference"; Length = input "Length"; Position = input "Position"
              Complete = input "Complete"; OffsetByte = offset; Decimal = leaf :> MethodBase; Hex = hex :> MethodBase; Quoted = (if storedQuoted.IsSome then None else quoted); StoredQuoted = storedQuoted; DecimalContext = needsContext
              HexContext = shape.Kind = NumericKind.Inferred
              Compatibility = targetCompatibility }
        CodecNumericDriverEmitter.reader (RuntimeCodecIL(driver.GetILGenerator())) driverApi utf16
        if TypeSupport.NumericText target.FullName then
            let text = DynamicMethod("ReadQuotedString",target,[|context.MakeByRefType()|],moduleValue,true)
            let readWord = typeof<Unsafe>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                           |> Array.find(fun method -> method.Name = "ReadUnaligned" && method.IsGenericMethodDefinition &&
                                                       method.GetParameters().Length = 1 && method.GetParameters().[0].ParameterType.Equals(typeof<byte>.MakeByRefType()))
            let api =
                { Int32 = typeof<int>; UInt64 = typeof<uint64>; SourceReference = typeof<byte>.MakeByRefType(); PositionReference = typeof<int>.MakeByRefType()
                  Begin = input "Begin"; Reference = input "Reference"; Length = input "Length"; Position = input "Position"
                  Complete = Some(input "Complete"); OffsetByte = offset
                  ReadWord = readWord.MakeGenericMethod(typeof<uint64>) :> MethodBase
                  Decode = typeof<JsonStringInput>.GetMethod((if utf16 then "Utf16" else "Utf8"),flags) :> MethodBase
                  Escaped = typeof<JsonStringInput>.GetMethod((if utf16 then "EscapedUtf16" else "EscapedUtf8"),flags) :> MethodBase
                  Compatibility = driver :> MethodBase }
            CodecStringEmitter.reader (RuntimeCodecIL(text.GetILGenerator())) api utf16
            text
        else driver

    and method (target: Type) (reader: Type) =
        JsonDefaultMethods.get target reader "numeric-read" (fun () -> create target reader false)

    let reader<'T,'R when 'R : struct and 'R :> IJsonReader>() =
        (method typeof<'T> typeof<'R>).CreateDelegate(typeof<JsonDirectReader<'T,'R>>) :?> JsonDirectReader<'T,'R>

    let storedMethod (target: Type) (reader: Type) =
        JsonDefaultMethods.get target reader (TypeSupport.StoredRole "numeric-read") (fun () -> create target reader true)

    let storedReader<'T,'R when 'R : struct and 'R :> IJsonReader>() =
        (storedMethod typeof<'T> typeof<'R>).CreateDelegate(typeof<JsonDirectReader<'T,'R>>) :?> JsonDirectReader<'T,'R>
