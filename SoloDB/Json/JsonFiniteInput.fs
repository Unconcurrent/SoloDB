namespace SoloDatabase.JsonSerializator

open System
open System.Reflection
open System.Reflection.Emit
open SoloDatabase
open SoloDatabase.Metadata.CodecGeneration

[<Struct>]
type internal JsonMarkerRange =
    val Range: JsonSourceRange
    val Present: bool
    new(range,present) = { Range = range; Present = present }

[<Struct>]
type internal JsonEnvelopeRanges =
    val Values: JsonSourceRange
    val TypePresent: bool
    val ValuesPresent: bool
    new(values,typePresent,valuesPresent) = { Values = values; TypePresent = typePresent; ValuesPresent = valuesPresent }

type internal JsonFiniteCapture<'T,'R when 'R : struct and 'R :> IJsonReader> =
    delegate of byref<JsonReadContext<'R>> -> 'T

/// Finite captures retain grammar-owned root facts without an owning property map.
[<AbstractClass; Sealed>]
type internal JsonFiniteInput =
    static member Begin<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): bool =
        context.Cursor.codeUnitIndex <- context.Range.Start
        context.Cursor.decodeStrings <- false
        let start = context.Cursor.codeUnitIndex
        let token = JsonLexer.readNext &context.Cursor
        match token with
        | OpenBrace -> true
        | _ ->
            let mutable sink = JsonDiscardSink<'R>()
            JsonGrammar.fromToken<int,int,int,'R,JsonDiscardSink<'R>> &sink &context.Cursor start token false |> ignore
            JsonTupleInput.Complete &context
            false

    static member Api(reader: Type) =
        let flags = BindingFlags.Static ||| BindingFlags.Public ||| BindingFlags.NonPublic
        let operation name = typeof<JsonTupleInput>.GetMethod(name,flags).MakeGenericMethod(reader) :> MethodBase
        { Names = JsonNameRuntime.api reader; Range = typeof<JsonSourceRange>
          Capture = operation "Capture"; Complete = operation "Complete" }

    static member Publish<'T,'R when 'R : struct and 'R :> IJsonReader>(read: JsonFiniteCapture<'T,'R>): unit =
        JsonDefaultMethods.publish typeof<'T> typeof<'R> typeof<'R> "finite-object-capture" read

    static member private Create<'T,'R when 'R : struct and 'R :> IJsonReader>(marker: bool): MethodBase =
        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "finite JSON object capture" typeof<'T>
        let argument = typeof<JsonFiniteCapture<'T,'R>>.GetMethod("Invoke",BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic).GetParameters().[0].ParameterType
        let method = DynamicMethod("ReadFiniteObject",typeof<'T>,[|argument|],typeof<JsonFiniteInput>.Module,true)
        let il = RuntimeCodecIL(method.GetILGenerator())
        let construct = typeof<'T>.GetConstructors(BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic).[0] :> MethodBase
        CodecFiniteCaptureEmitter.reader il (JsonFiniteInput.Api typeof<'R>) marker construct
        method :> MethodBase

    static member Acquire<'T,'R when 'R : struct and 'R :> IJsonReader>(marker: bool): JsonFiniteCapture<'T,'R> =
        JsonDefaultMethods.callable<JsonFiniteCapture<'T,'R>> typeof<'T> typeof<'R> typeof<'R> "finite-object-capture"
            (fun () -> JsonFiniteInput.Create<'T,'R>(marker))

type internal JsonMarkerCapture<'R when 'R : struct and 'R :> IJsonReader> =
    static let read = JsonFiniteInput.Acquire<JsonMarkerRange,'R>(true)
    static member Read(context: byref<JsonReadContext<'R>>) = read.Invoke &context

type internal JsonEnvelopeCapture<'R when 'R : struct and 'R :> IJsonReader> =
    static let read = JsonFiniteInput.Acquire<JsonEnvelopeRanges,'R>(false)
    static member Read(context: byref<JsonReadContext<'R>>) = read.Invoke &context
