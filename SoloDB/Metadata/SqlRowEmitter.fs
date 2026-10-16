namespace SoloDatabase.Metadata.CodecGeneration

open System
open System.Reflection.Emit

/// Original SQL operations bound by either reflection or compiler facts.
type internal SqlRowEmitterApi<'T,'M> = {
    IsNull: 'M
    Read: Func<'T,'M>
    Uninitialized: Func<'T,'M>
    ValueType: Func<'T,bool>
    Nullable: Func<'T,bool>
    Name: Func<'T,string>
    TupleNull: 'M
}

module internal SqlRowEmitter =
    let private zero (il: CodecIL<'T,'M,'F>) typ =
        let value = il.Local typ
        il.EmitLocal(OpCodes.Ldloca,value)
        il.EmitType(OpCodes.Initobj,typ)
        il.EmitLocal(OpCodes.Ldloc,value)

    let private read (il: CodecIL<'T,'M,'F>) api typ (ordinal: Action<int>) position =
        il.Emit OpCodes.Ldarg_1
        ordinal.Invoke position
        il.EmitMethod(OpCodes.Call,api.Read.Invoke typ)

    let private isNull (il: CodecIL<'T,'M,'F>) api (ordinal: Action<int>) position =
        il.EmitInt(OpCodes.Ldarga,1)
        ordinal.Invoke position
        il.EmitMethod(OpCodes.Call,api.IsNull)

    let private absent (il: CodecIL<'T,'M,'F>) api (ordinal: Action<int>) position target =
        ordinal.Invoke position
        il.EmitInt(OpCodes.Ldc_I4,0)
        il.EmitBranch(OpCodes.Blt,target)
        isNull il api ordinal position
        il.EmitBranch(OpCodes.Brtrue,target)

    let members recordFields (members: CodecMember<'T,'M,'F>[]) fallbackFields =
        match recordFields with
        | Some names -> names |> Array.map(fun name -> members |> Array.find(fun memberValue -> memberValue.Field.IsNone && memberValue.Name = name))
        | None ->
            let properties = members |> Array.filter(fun memberValue -> memberValue.Field.IsNone && memberValue.Setter.IsSome)
            if properties.Length = 0 then fallbackFields else properties

    let objectRow (il: CodecIL<'T,'M,'F>) api (model: CodecModel<'T,'M,'F>)
                  (members: CodecMember<'T,'M,'F>[]) record constructor ordinal (available: Func<int,bool>) =
        if record then
            let values = members |> Array.map(fun memberValue -> il.Local memberValue.Type)
            for position = 0 to members.Length - 1 do
                if available.Invoke position then
                    let missing,complete = il.Label(),il.Label()
                    absent il api ordinal position missing
                    read il api members[position].Type ordinal position
                    il.EmitBranch(OpCodes.Br,complete)
                    il.Mark missing
                    zero il members[position].Type
                    il.Mark complete
                else zero il members[position].Type
                il.EmitLocal(OpCodes.Stloc,values[position])
            for value in values do il.EmitLocal(OpCodes.Ldloc,value)
            il.EmitMethod(OpCodes.Newobj,model.Constructor.Value)
        else
            let result = il.Local model.Type
            match constructor with
            | Some method -> il.EmitMethod(OpCodes.Newobj,method)
            | None when model.ValueType -> zero il model.Type
            | None -> il.EmitMethod(OpCodes.Call,api.Uninitialized.Invoke model.Type)
            il.EmitLocal(OpCodes.Stloc,result)
            for position = 0 to members.Length - 1 do
                if available.Invoke position then
                    let memberValue = members[position]
                    let missing = il.Label()
                    absent il api ordinal position missing
                    if not memberValue.Static then
                        il.EmitLocal((if model.ValueType then OpCodes.Ldloca else OpCodes.Ldloc),result)
                    read il api memberValue.Type ordinal position
                    match memberValue.Field,memberValue.Setter with
                    | Some field,_ -> il.EmitField((if memberValue.Static then OpCodes.Stsfld else OpCodes.Stfld),field)
                    | _,Some setter -> il.EmitMethod((if memberValue.Static || model.ValueType then OpCodes.Call else OpCodes.Callvirt),setter)
                    | _ -> invalidOp "A SQL row member requires a field or setter."
                    il.Mark missing
            il.EmitLocal(OpCodes.Ldloc,result)
        il.Emit OpCodes.Ret

    let tupleRow (il: CodecIL<'T,'M,'F>) api (model: CodecTuple<'T,'M,'F>) ordinal =
        let mutable position = 0
        let rec construct (tuple: CodecTuple<'T,'M,'F>) =
            for item in tuple.Members do
                match item.Rest with
                | Some rest -> construct rest
                | None ->
                    let index = position
                    position <- position + 1
                    let present,complete = il.Label(),il.Label()
                    let typ = item.Member.Type
                    isNull il api ordinal index
                    il.EmitBranch(OpCodes.Brfalse,present)
                    if api.ValueType.Invoke typ && not(api.Nullable.Invoke typ) then
                        il.EmitString(api.Name.Invoke typ)
                        il.EmitInt(OpCodes.Ldc_I4,index)
                        il.EmitMethod(OpCodes.Call,api.TupleNull)
                        il.Emit OpCodes.Throw
                    else zero il typ
                    il.EmitBranch(OpCodes.Br,complete)
                    il.Mark present
                    read il api typ ordinal index
                    il.Mark complete
            il.EmitMethod(OpCodes.Newobj,tuple.Constructor)
        construct model
        il.Emit OpCodes.Ret

    let scalarRow (il: CodecIL<'T,'M,'F>) api target ordinal =
        read il api target ordinal 0
        il.Emit OpCodes.Ret
