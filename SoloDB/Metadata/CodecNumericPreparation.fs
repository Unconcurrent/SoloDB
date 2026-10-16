namespace SoloDatabase.Metadata.CodecGeneration

open SoloDatabase.Metadata.Generation

type internal CodecNumericQuote = None = 0 | Integer = 1 | Binary = 2 | Decimal = 3 | Stored = 4

/// Directional preparation decisions precede either reflection or Cecil operand acquisition.
type internal CodecNumericReaderPreparation = {
    Core: string
    Leaf: NumericKind
    Width: int
    Signed: bool
    Native: bool
    DeferredWidth: bool
    Bits: string
    DecimalContext: bool
    HexContext: bool
    QuotedContext: bool
    Quote: CodecNumericQuote
    StoredReader: string option
    StoredCoreArgument: bool
    TargetCompatibility: bool
    ScalarAdapter: bool
    Finish: (string * string * bool)[]
    Predicate: bool
    Text: bool
    StringWrapper: bool
    ParameterResult: bool
    ReuseNative: bool
}

type internal CodecNumericWriterPreparation = {
    Core: string
    Family: NumericKind
    Width: int
    Signed: bool
    DeferredWidth: bool
    Bits: string
    Steps: (string * string * bool * string)[]
    Cast: bool
    AddressInput: bool
    StoredOperation: string option
}

module internal CodecNumericPreparation =
    let private width (shape: NumericShape) nativeSize =
        if shape.Native && Option.isSome nativeSize then Option.get nativeSize * 8 else shape.Width
    let private bits width = if width = 64 then "System.UInt64" elif width = 16 then "System.UInt16" else "System.UInt32"

    let reader definition stored nativeSize =
        let core = (TypeSupport.NumericCore definition).Value
        let shape = (TypeSupport.Numeric core).Value
        let adapter = core <> definition
        let storedReader = if stored then TypeSupport.StoredNumericReader definition else None
        let quote =
            if storedReader.IsSome then CodecNumericQuote.Stored
            elif adapter then CodecNumericQuote.None else
            match shape.Kind with
            | NumericKind.Integer -> CodecNumericQuote.Integer
            | NumericKind.Binary -> CodecNumericQuote.Binary
            | NumericKind.Decimal -> CodecNumericQuote.Decimal
            | _ -> CodecNumericQuote.None
        let width = width shape nativeSize
        { Core = core; Leaf = shape.Kind; Width = width; Signed = shape.Signed; Native = shape.Native
          DeferredWidth = shape.Native && Option.isNone nativeSize; Bits = bits width
          DecimalContext = shape.Kind = NumericKind.Binary || shape.Kind = NumericKind.Inferred
          HexContext = shape.Kind = NumericKind.Inferred
          QuotedContext = quote = CodecNumericQuote.Integer || quote = CodecNumericQuote.Binary || quote = CodecNumericQuote.Decimal
          Quote = quote; StoredReader = storedReader; StoredCoreArgument = storedReader = Some "ReadKeyHalf"
          TargetCompatibility = adapter; ScalarAdapter = (TypeSupport.NumericTextAdapter core).IsSome
          Finish = TypeSupport.NumericFinish definition
          Predicate = TypeSupport.NumericPredicate definition && shape.Kind <> NumericKind.Predicate
          Text = TypeSupport.NumericText definition && shape.Kind <> NumericKind.NumericText
          StringWrapper = TypeSupport.NumericText definition; ParameterResult = TypeSupport.NumericParameterResult definition
          ReuseNative = stored && storedReader.IsNone }

    let writer definition stored nativeSize =
        let core = (TypeSupport.NumericWriterCore definition).Value
        let shape = (TypeSupport.Numeric core).Value
        let width = width shape nativeSize
        { Core = core; Family = shape.Kind; Width = width; Signed = shape.Signed
          DeferredWidth = shape.Native && Option.isNone nativeSize; Bits = bits width
          Steps = TypeSupport.NumericWriterSteps definition; Cast = TypeSupport.NumericWriterCast definition
          AddressInput = shape.Kind = NumericKind.Decimal || shape.Kind = NumericKind.Binary
          StoredOperation = if stored then TypeSupport.StoredNumeric definition |> Option.map(fun struct(name,_) -> name) else None }
