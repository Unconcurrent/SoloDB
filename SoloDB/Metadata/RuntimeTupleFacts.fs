namespace SoloDatabase

open System
open System.Reflection
open SoloDatabase.Metadata.CodecGeneration

/// Resolves original CLR tuple members without selecting a wire or SQL behavior.
module internal RuntimeTupleFacts =
    let rec model (target: Type): CodecTuple<Type,MethodBase,FieldInfo> =
        let arguments = TypeMetadata.GenericTypeArgCache.Get target
        let members = arguments |> Array.mapi (fun index element ->
            let name = if index = 7 then "Rest" else "Item" + string(index + 1)
            let memberValue =
                if target.IsValueType then
                    let field = target.GetField(name)
                    { Name = name; Type = element; Field = Some field; Getter = None; Setter = None; Static = false
                      Readable = true; Writable = true; Ignored = false; Many = false }
                else
                    { Name = name; Type = element; Field = None
                      Getter = Some(target.GetProperty(name).GetGetMethod() :> MethodBase); Setter = None; Static = false
                      Readable = true; Writable = false; Ignored = false; Many = false }
            { Member = memberValue; Rest = if index = 7 then Some(model element) else None })
        { Type = target; ValueType = target.IsValueType; Constructor = target.GetConstructor(arguments) :> MethodBase; Members = members }
