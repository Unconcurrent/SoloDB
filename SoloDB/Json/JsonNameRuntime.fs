namespace SoloDatabase.JsonSerializator

open System
open System.Reflection
open System.Runtime.CompilerServices
open SoloDatabase.Metadata.CodecGeneration

/// Maps lexical operands; the shared name emitter owns concrete recognition.
module internal JsonNameRuntime =
    let private flags = BindingFlags.Static ||| BindingFlags.NonPublic ||| BindingFlags.Public
    let private unsafeMethod name arguments =
        typeof<Unsafe>.GetMethods(flags)
        |> Array.find(fun method -> method.Name = name && method.IsGenericMethodDefinition &&
                                    method.GetParameters().Length = arguments && method.GetParameters().[0].ParameterType.IsByRef &&
                                    (arguments = 1 || method.GetParameters().[1].ParameterType.Equals(typeof<unativeint>)))
    let api (reader: Type) =
        let input name = typeof<JsonNumericInput>.GetMethod(name,flags).MakeGenericMethod(reader) :> MethodBase
        let operation name = typeof<JsonNameInput>.GetMethod(name,flags).MakeGenericMethod(reader) :> MethodBase
        { Int32 = typeof<int>; Boolean = typeof<bool>
          SourceReference = typeof<byte>.MakeByRefType(); PositionReference = typeof<int>.MakeByRefType()
          Begin = typeof<JsonNameInput>.GetMethod("Begin",flags).MakeGenericMethod(reader) :> MethodBase
          Reference = input "Reference"; Position = input "Position"; Length = input "Length"
          OffsetByte = (unsafeMethod "Add" 2).MakeGenericMethod(typeof<byte>) :> MethodBase
          Quoted = operation "Quoted"; Identifier = operation "Identifier"
          Invalid = typeof<JsonNameInput>.GetMethod("Invalid",flags) :> MethodBase
          Unterminated = typeof<JsonNameInput>.GetMethod("Unterminated",flags) :> MethodBase
          Unsupported = operation "Unsupported"; Colon = operation "Colon"; Value = operation "Value"
          Utf16 = reader.Equals(typeof<Utf16Reader>) }
