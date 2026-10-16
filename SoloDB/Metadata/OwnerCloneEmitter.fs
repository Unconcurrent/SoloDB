namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit
open SoloDatabase.Metadata.Generation

/// The replacement owner is fresh; scalar access keeps the original boxed source.
module internal OwnerCloneEmitter =
    let copy (il: CodecIL<'T,'M,'F>) owner valueType (members: CodecMember<'T,'M,'F>[]) =
        for memberValue in members do
            if TypeSupport.CloneProperty(memberValue.Field.IsSome,memberValue.Static,memberValue.Getter.IsSome,memberValue.Setter.IsSome,memberValue.Many) then
                il.Emit OpCodes.Ldarg_1
                il.EmitType((if valueType then OpCodes.Unbox else OpCodes.Castclass),owner)
                il.Emit OpCodes.Ldarg_0
                il.EmitType((if valueType then OpCodes.Unbox else OpCodes.Castclass),owner)
                il.EmitMethod((if valueType then OpCodes.Call else OpCodes.Callvirt),memberValue.Getter.Value)
                il.EmitMethod((if valueType then OpCodes.Call else OpCodes.Callvirt),memberValue.Setter.Value)
        il.Emit OpCodes.Ret
