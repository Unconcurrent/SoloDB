namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

/// One instruction policy for captured query members, calls and constructors.
module internal QueryOperationEmitter =
    let enumValue (il: CodecIL<'T,'M,'F>) owner underlying =
        il.Emit OpCodes.Ldarg_0
        il.EmitType(OpCodes.Unbox_Any,owner)
        il.EmitType(OpCodes.Box,underlying)
        il.Emit OpCodes.Ret
    let private receiver (il: CodecIL<'T,'M,'F>) owner valueType nullable =
        il.Emit OpCodes.Ldarg_0
        if nullable then
            let local = il.Local owner
            il.EmitType(OpCodes.Unbox_Any,owner)
            il.EmitLocal(OpCodes.Stloc,local)
            il.EmitLocal(OpCodes.Ldloca,local)
        else il.EmitType((if valueType then OpCodes.Unbox else OpCodes.Castclass),owner)

    let private arguments (il: CodecIL<'T,'M,'F>) referenceType referenceMethod argument (types: 'T[]) =
        for index in 0 .. types.Length-1 do
            il.EmitInt(OpCodes.Ldarg,argument)
            il.EmitInt(OpCodes.Ldc_I4,index)
            il.Emit OpCodes.Ldelem_Ref
            match referenceMethod types[index] with
            | Some address ->
                il.EmitType(OpCodes.Castclass,referenceType)
                il.EmitMethod(OpCodes.Call,address)
            | None -> il.EmitType(OpCodes.Unbox_Any,types[index])

    let call (il: CodecIL<'T,'M,'F>) referenceType referenceMethod ownerAddress owner ownerValueType nullable isStatic method (types: 'T[]) result resultValueType returnsVoid boxed =
        if not isStatic then
            match ownerAddress with
            | Some address ->
                let boxed, ready = il.Label(), il.Label()
                il.Emit OpCodes.Ldarg_0
                il.EmitType(OpCodes.Isinst,referenceType)
                il.Emit OpCodes.Dup
                il.EmitBranch(OpCodes.Brfalse,boxed)
                il.EmitMethod(OpCodes.Call,address)
                il.EmitBranch(OpCodes.Br,ready)
                il.Mark boxed
                il.Emit OpCodes.Pop
                receiver il owner ownerValueType nullable
                il.Mark ready
            | None -> receiver il owner ownerValueType nullable
        arguments il referenceType referenceMethod 1 types
        il.EmitMethod((if isStatic || ownerValueType then OpCodes.Call else OpCodes.Callvirt),method)
        if returnsVoid then il.Emit OpCodes.Ldnull
        elif boxed && resultValueType then il.EmitType(OpCodes.Box,result)
        il.Emit OpCodes.Ret

    let memberRead (il: CodecIL<'T,'M,'F>) owner ownerValueType nullable (memberValue: CodecMember<'T,'M,'F>) valueType boxedReceiver boxed =
        if not memberValue.Static then
            if boxedReceiver then receiver il owner ownerValueType nullable
            else il.EmitInt((if ownerValueType then OpCodes.Ldarga else OpCodes.Ldarg),0)
        match memberValue.Field, memberValue.Getter with
        | Some field, _ -> il.EmitField((if memberValue.Static then OpCodes.Ldsfld else OpCodes.Ldfld),field)
        | _, Some getter -> il.EmitMethod((if memberValue.Static || ownerValueType then OpCodes.Call else OpCodes.Callvirt),getter)
        | _ -> invalidOp "A query member requires a field or getter."
        if boxed && valueType then il.EmitType(OpCodes.Box,memberValue.Type)
        il.Emit OpCodes.Ret

    let memberWrite (il: CodecIL<'T,'M,'F>) owner ownerValueType (memberValue: CodecMember<'T,'M,'F>) boxedReceiver byrefReceiver boxedValue =
        if not memberValue.Static then
            if boxedReceiver then
                il.Emit OpCodes.Ldarg_0
                il.EmitType((if ownerValueType then OpCodes.Unbox else OpCodes.Castclass),owner)
            elif byrefReceiver then
                il.Emit OpCodes.Ldarg_0
                if not ownerValueType then il.Emit OpCodes.Ldind_Ref
            else il.EmitInt((if ownerValueType then OpCodes.Ldarga else OpCodes.Ldarg),0)
        il.Emit OpCodes.Ldarg_1
        if boxedValue then il.EmitType(OpCodes.Unbox_Any,memberValue.Type)
        match memberValue.Field, memberValue.Setter with
        | Some field, _ -> il.EmitField((if memberValue.Static then OpCodes.Stsfld else OpCodes.Stfld),field)
        | _, Some setter -> il.EmitMethod((if memberValue.Static || ownerValueType then OpCodes.Call else OpCodes.Callvirt),setter)
        | _ -> invalidOp "A query member requires a field or setter."
        il.Emit OpCodes.Ret

    let unionTest (il: CodecIL<'T,'M,'F>) owner ownerValueType tagMethod staticTag tag =
        if staticTag then
            il.Emit OpCodes.Ldarg_0
            il.EmitType(OpCodes.Unbox_Any,owner)
        else receiver il owner ownerValueType false
        il.EmitMethod((if staticTag || ownerValueType then OpCodes.Call else OpCodes.Callvirt),tagMethod)
        il.EmitInt(OpCodes.Ldc_I4,tag)
        il.Emit OpCodes.Ceq
        il.Emit OpCodes.Ret

    let construction (il: CodecIL<'T,'M,'F>) referenceType referenceMethod result valueType constructor (types: 'T[]) boxed =
        arguments il referenceType referenceMethod 0 types
        il.EmitMethod(OpCodes.Newobj,constructor)
        if boxed && valueType then il.EmitType(OpCodes.Box,result)
        il.Emit OpCodes.Ret

    let fieldAddress (il: CodecIL<'T,'M,'F>) referenceType owner ownerValueType ownerAddress field isStatic =
        if not isStatic then
            il.Emit OpCodes.Ldarg_0
            if ownerValueType then
                il.EmitType(OpCodes.Castclass,referenceType)
                il.EmitMethod(OpCodes.Call,ownerAddress)
            else il.EmitType(OpCodes.Castclass,owner)
        il.EmitField((if isStatic then OpCodes.Ldsflda else OpCodes.Ldflda),field)
        il.Emit OpCodes.Ret

    let boxAddress (il: CodecIL<'T,'M,'F>) owner =
        il.Emit OpCodes.Ldarg_0
        il.EmitType(OpCodes.Unbox,owner)
        il.Emit OpCodes.Ret

    let arrayAddress (il: CodecIL<'T,'M,'F>) owner element rank address =
        il.Emit OpCodes.Ldarg_0
        il.EmitType(OpCodes.Castclass,owner)
        for index in 0 .. rank-1 do
            il.Emit OpCodes.Ldarg_1
            il.EmitInt(OpCodes.Ldc_I4,index)
            il.Emit OpCodes.Ldelem_I4
        if rank = 1 then il.EmitType(OpCodes.Ldelema,element)
        else il.EmitMethod(OpCodes.Call,address)
        il.Emit OpCodes.Ret
