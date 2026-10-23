namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

module internal CodecEmitter =
    let internal boxedRawWriter (il: CodecIL<'T,'M,'F>) target valueType invoke =
        il.Emit OpCodes.Ldarg_0
        il.Emit OpCodes.Ldarg_1
        if valueType then il.EmitType(OpCodes.Unbox,target)
        else
            il.EmitType(OpCodes.Castclass,target)
            let value = il.Local target
            il.EmitLocal(OpCodes.Stloc,value)
            il.EmitLocal(OpCodes.Ldloca,value)
        il.Emit OpCodes.Ldarg_2
        il.Emit OpCodes.Ldarg_3
        il.EmitMethod(OpCodes.Callvirt,invoke)
        il.Emit OpCodes.Ret

    let internal callLocal (il: CodecIL<'T,'M,'F>) method index local =
        il.EmitLocal((if il.ByRefParameter(method,index) then OpCodes.Ldloca else OpCodes.Ldloc),local)

    let internal readLocal (il: CodecIL<'T,'M,'F>) method arguments local =
        arguments()
        if il.ResultParameter method then il.EmitLocal(OpCodes.Ldloca,local)
        il.EmitMethod(OpCodes.Call,method)
        if not(il.ResultParameter method) then il.EmitLocal(OpCodes.Stloc,local)

    let internal readValue (il: CodecIL<'T,'M,'F>) method target arguments =
        if il.ResultParameter method then
            let value = il.Local target
            readLocal il method arguments value
            il.EmitLocal(OpCodes.Ldloc,value)
        else
            arguments()
            il.EmitMethod(OpCodes.Call,method)

    let internal loadRawMember (il: CodecIL<'T,'M,'F>) valueType (memberValue: CodecMember<'T,'M,'F>) =
        if not memberValue.Static then
            if valueType then il.InputAddress() else il.InputValue()
        match memberValue.Field, memberValue.Getter with
        | Some field, _ -> il.EmitField((if memberValue.Static then OpCodes.Ldsfld else OpCodes.Ldfld),field)
        | _, Some getter -> il.EmitMethod((if valueType || memberValue.Static then OpCodes.Call else OpCodes.Callvirt),getter)
        | _ -> invalidOp "A stored JSON member has no getter."
    let writtenMembers (model: CodecModel<'T,'M,'F>) =
        model.Members
        |> Array.mapi (fun index memberValue -> index, memberValue)
        |> Array.choose (fun (index, memberValue) ->
            if memberValue.Readable &&
               (if memberValue.Field.IsSome then model.ValueType
                else not memberValue.Ignored && not memberValue.Many)
            then Some index else None)

    let internal loadMember (il: CodecIL<'T,'M,'F>) valueType argument (memberValue: CodecMember<'T,'M,'F>) =
        if not memberValue.Static then
            il.EmitInt((if valueType then OpCodes.Ldarga else OpCodes.Ldarg), argument)
        match memberValue.Field, memberValue.Getter with
        | Some field, _ -> il.EmitField((if memberValue.Static then OpCodes.Ldsfld else OpCodes.Ldfld), field)
        | _, Some getter -> il.EmitMethod((if valueType || memberValue.Static then OpCodes.Call else OpCodes.Callvirt), getter)
        | _ -> invalidOp "A stored JSON member has no getter."

    let internal typeToken (il: CodecIL<'T,'M,'F>) (api: CodecApi<'T,'M>) target =
        il.EmitType(OpCodes.Ldtoken,target)
        il.EmitMethod(OpCodes.Call,api.TypeFromHandle)

    let private fail (il: CodecIL<'T,'M,'F>) (api: CodecApi<'T,'M>) message =
        il.EmitString message
        il.EmitMethod(OpCodes.Newobj,api.ExceptionConstructor)
        il.Emit OpCodes.Throw

    let internal returnDefault (il: CodecIL<'T,'M,'F>) target =
        let result = il.Local target
        il.EmitLocal(OpCodes.Ldloca,result)
        il.EmitType(OpCodes.Initobj,target)
        il.EmitLocal(OpCodes.Ldloc,result)
        il.Emit OpCodes.Ret

    let getter (il: CodecIL<'T,'M,'F>) (model: CodecModel<'T,'M,'F>) memberIndex =
        QueryOperationEmitter.memberRead il model.Type model.ValueType false model.Members[memberIndex] false false false

    let boxedMemberGetter (il: CodecIL<'T,'M,'F>) owner ownerValueType (memberValue: CodecMember<'T,'M,'F>) valueType =
        QueryOperationEmitter.memberRead il owner ownerValueType false memberValue valueType true true

    let boxedGetter (il: CodecIL<'T,'M,'F>) (model: CodecModel<'T,'M,'F>) memberIndex valueType =
        boxedMemberGetter il model.Type model.ValueType model.Members[memberIndex] valueType

    let readerCall (il: CodecIL<'T,'M,'F>) isNull (operation: CodecReaderCall<'T,'M>) =
        if operation.NullDefault then
            let present = il.Label()
            il.Emit OpCodes.Ldarg_0
            il.EmitMethod(OpCodes.Callvirt,isNull)
            il.EmitBranch(OpCodes.Brfalse,present)
            returnDefault il operation.Result
            il.Mark present
        match operation.Before with
        | Some constructor -> il.EmitMethod(OpCodes.Newobj,constructor)
        | None -> ()
        il.Emit OpCodes.Ldarg_0
        match operation.Call with
        | Some method -> il.EmitMethod(OpCodes.Call,method)
        | None -> ()
        match operation.After with
        | Some constructor -> il.EmitMethod(OpCodes.Newobj,constructor)
        | None -> ()
        if operation.Cast then il.EmitType(OpCodes.Castclass,operation.Result)
        il.Emit OpCodes.Ret

    let writerCall (il: CodecIL<'T,'M,'F>) nullMethod (operation: CodecWriterCall<'T,'M>) =
        if not operation.ValueType then
            let present = il.Label()
            il.Emit OpCodes.Ldarg_0
            il.EmitBranch(OpCodes.Brtrue,present)
            il.EmitMethod(OpCodes.Call,nullMethod)
            il.Emit OpCodes.Ret
            il.Mark present
        il.Emit OpCodes.Ldarg_0
        if operation.Box then il.EmitType(OpCodes.Box,operation.Input)
        il.EmitMethod((if operation.Virtual then OpCodes.Callvirt else OpCodes.Call),operation.Call)
        il.Emit OpCodes.Ret

    let boxedRead (il: CodecIL<'T,'M,'F>) json result valueType method =
        il.Emit OpCodes.Ldarg_0
        il.EmitType(OpCodes.Castclass,json)
        il.EmitMethod(OpCodes.Call,method)
        if valueType then il.EmitType(OpCodes.Box,result)
        il.Emit OpCodes.Ret

    let boxedWrite (il: CodecIL<'T,'M,'F>) input method =
        il.Emit OpCodes.Ldarg_0
        il.EmitType(OpCodes.Unbox_Any,input)
        il.EmitMethod(OpCodes.Call,method)
        il.Emit OpCodes.Ret

    let construction (il: CodecIL<'T,'M,'F>) result valueType constructor (arguments: 'T[]) =
        QueryOperationEmitter.construction il result (fun _ -> None) result valueType constructor arguments true

    let typedConstruction (il: CodecIL<'T,'M,'F>) method argumentCount =
        for index = 0 to argumentCount - 1 do il.EmitInt(OpCodes.Ldarg,index)
        il.EmitMethod(OpCodes.Newobj,method)
        il.Emit OpCodes.Ret

    let defaultConstruction (il: CodecIL<'T,'M,'F>) result valueType constructor boxed =
        match constructor with
        | Some method -> il.EmitMethod(OpCodes.Newobj,method)
        | None ->
            let value = il.Local result
            il.EmitLocal(OpCodes.Ldloca,value)
            il.EmitType(OpCodes.Initobj,result)
            il.EmitLocal(OpCodes.Ldloc,value)
        if boxed && valueType then il.EmitType(OpCodes.Box,result)
        il.Emit OpCodes.Ret

    let directCall (il: CodecIL<'T,'M,'F>) method argumentCount =
        for index in 0 .. argumentCount - 1 do il.EmitInt(OpCodes.Ldarg,index)
        il.EmitMethod(OpCodes.Call,method)
        il.Emit OpCodes.Ret

    let nativeWidthCall (il: CodecIL<'T,'M,'F>) size argumentCount narrow wide =
        let useNarrow = il.Label()
        il.EmitMethod(OpCodes.Call,size)
        il.EmitInt(OpCodes.Ldc_I4,4)
        il.EmitBranch(OpCodes.Beq,useNarrow)
        directCall il wide argumentCount
        il.Mark useNarrow
        directCall il narrow argumentCount

    let private argumentFailure (il: CodecIL<'T,'M,'F>) (api: CodecApi<'T,'M>) reader message parameter =
        il.EmitString (if reader then "SoloDatabase.JsonSerializator.JsonDeserializerImpl`1" else "SoloDatabase.JsonSerializator.JsonSerializerImpl`1")
        il.EmitString message
        il.EmitString parameter
        il.EmitMethod(OpCodes.Newobj,api.ArgumentExceptionConstructor)
        il.EmitMethod(OpCodes.Newobj,api.InitializationExceptionConstructor)
        il.Emit OpCodes.Throw

    let internal indexedFailure il api reader signature =
        argumentFailure il api reader ("Incorrect number of arguments supplied for call to method '" + signature + "'") "property"

    let internal staticFieldFailure il api reader =
        argumentFailure il api reader "Static field requires null instance, non-static field requires non-null instance." "expression"

    let writer (il: CodecIL<'T,'M,'F>) (api: CodecApi<'T,'M>) (model: CodecModel<'T,'M,'F>) =
        match model.IndexedWrite with
        | Some signature -> indexedFailure il api false signature
        | None when model.ValueType && (model.Members |> Array.exists(fun memberValue -> memberValue.Field.IsSome && memberValue.Static)) -> staticFieldFailure il api false
        | None ->
            if not model.ValueType then
                let present = il.Label()
                il.Emit OpCodes.Ldarg_0
                il.EmitBranch(OpCodes.Brtrue,present)
                il.EmitMethod(OpCodes.Call,api.Null)
                il.Emit OpCodes.Ret
                il.Mark present
                if not model.Sealed then
                    let declared = il.Label()
                    il.EmitMethod(OpCodes.Call,api.EnsureStack)
                    il.Emit OpCodes.Ldarg_0
                    il.EmitMethod(OpCodes.Callvirt,api.GetType)
                    typeToken il api model.Type
                    il.Emit OpCodes.Ceq
                    il.EmitBranch(OpCodes.Brtrue,declared)
                    il.Emit OpCodes.Ldarg_0
                    il.EmitMethod(OpCodes.Call,api.SerializeActual)
                    il.Emit OpCodes.Ret
                    il.Mark declared
            let values = il.Local api.Dictionary
            il.EmitInt(OpCodes.Ldc_I4,16)
            il.EmitMethod(OpCodes.Newobj,api.DictionaryConstructor)
            il.EmitLocal(OpCodes.Stloc,values)
            for index in writtenMembers model do
                let memberValue = model.Members[index]
                il.EmitLocal(OpCodes.Ldloc,values)
                il.EmitString memberValue.Name
                loadMember il model.ValueType 0 memberValue
                il.EmitMethod(OpCodes.Call,api.Serialize memberValue.Type)
                il.EmitMethod(OpCodes.Callvirt,api.DictionaryAdd)
            il.EmitLocal(OpCodes.Ldloc,values)
            il.EmitMethod(OpCodes.Call,api.NewObject)
            il.Emit OpCodes.Ret


    let private unionReader (il: CodecIL<'T,'M,'F>) (api: CodecApi<'T,'M>) (model: CodecModel<'T,'M,'F>) =
        if model.Cases |> Array.exists (fun case -> case.Factory.IsNone) then
            il.EmitString "An index satisfying the predicate was not found in the collection."
            il.EmitMethod(OpCodes.Newobj,api.MissingFactoryConstructor)
            il.Emit OpCodes.Throw
        else
            let present = il.Label()
            il.Emit OpCodes.Ldarg_0
            il.EmitMethod((if api.RawReader then OpCodes.Call else OpCodes.Callvirt),api.IsNull)
            il.EmitBranch(OpCodes.Brfalse,present)
            returnDefault il model.Type
            il.Mark present
            let labels = model.Cases |> Array.map (fun _ -> il.Label())
            for ordinal = 0 to model.Cases.Length - 1 do
                il.Emit OpCodes.Ldarg_0
                il.EmitString ("Is" + model.Cases[ordinal].Name)
                il.EmitMethod(OpCodes.Call,api.UnionCaseSelected)
                il.EmitBranch(OpCodes.Brtrue,labels[ordinal])
            let tag = il.Local api.Int32
            il.Emit OpCodes.Ldarg_0
            il.EmitString "Tag"
            il.EmitMethod((if api.RawReader then OpCodes.Call else OpCodes.Callvirt),api.GetProperty)
            il.EmitMethod((if api.RawReader then OpCodes.Call else OpCodes.Callvirt),api.Deserialize api.Int32)
            il.EmitLocal(OpCodes.Stloc,tag)
            for ordinal = 0 to model.Cases.Length - 1 do
                il.EmitLocal(OpCodes.Ldloc,tag)
                il.EmitInt(OpCodes.Ldc_I4,model.Cases[ordinal].Tag)
                il.EmitBranch(OpCodes.Beq,labels[ordinal])
            fail il api ("Invalid Tag for type: " + model.RuntimeName)
            for ordinal = 0 to model.Cases.Length - 1 do
                let case = model.Cases[ordinal]
                il.Mark labels[ordinal]
                for name, target in case.Fields do
                    let value = il.Local target
                    let missing = il.Label()
                    il.Emit OpCodes.Ldarg_0
                    il.EmitString name
                    il.EmitMethod((if api.RawReader then OpCodes.Call else OpCodes.Callvirt),api.Contains)
                    il.EmitBranch(OpCodes.Brfalse,missing)
                    il.Emit OpCodes.Ldarg_0
                    il.EmitString name
                    il.EmitMethod((if api.RawReader then OpCodes.Call else OpCodes.Callvirt),api.GetProperty)
                    il.EmitMethod((if api.RawReader then OpCodes.Call else OpCodes.Callvirt),api.Deserialize target)
                    il.EmitLocal(OpCodes.Stloc,value)
                    il.Mark missing
                    il.EmitLocal(OpCodes.Ldloc,value)
                il.EmitMethod(OpCodes.Call,case.Factory.Value)
                if model.CastUnionResult then il.EmitType(OpCodes.Castclass,model.Type)
                il.Emit OpCodes.Ret

    let readerWithUnion (il: CodecIL<'T,'M,'F>) (api: CodecApi<'T,'M>) (model: CodecModel<'T,'M,'F>) readUnion =
        if model.IndexedRead.IsSome && not model.ConstructFromMembers then indexedFailure il api true model.IndexedRead.Value
        elif model.ValueType && not model.ConstructFromMembers && (model.Members |> Array.exists(fun memberValue -> memberValue.Field.IsSome && memberValue.Static)) then
            staticFieldFailure il api true
        elif model.ValueType && not model.ConstructFromMembers && (model.Members |> Array.exists(fun memberValue -> memberValue.Field.IsSome && not memberValue.Writable)) then
            argumentFailure il api true "Expression must be writeable" "left"
        elif model.Union then readUnion()
        elif not model.Abstract && not model.ValueType && not model.ConstructFromMembers &&
             not (model.Members |> Array.exists (fun memberValue -> memberValue.Writable && memberValue.Field.IsNone)) then
            il.EmitString "SoloDatabase.JsonSerializator.JsonDeserializerImpl`1"
            il.EmitString ("Could not deserialize the type " + model.Name + ", it does not have any public writable property.")
            il.EmitMethod(OpCodes.Newobj,api.ExceptionConstructor)
            il.EmitMethod(OpCodes.Newobj,api.InitializationExceptionConstructor)
            il.Emit OpCodes.Throw
        else
            if not model.Abstract then
                let present = il.Label()
                il.Emit OpCodes.Ldarg_0
                il.EmitMethod((if api.RawReader then OpCodes.Call else OpCodes.Callvirt),api.IsNull)
                il.EmitBranch(OpCodes.Brfalse,present)
                returnDefault il model.Type
                il.Mark present
            if not model.Sealed && not model.ValueType && not model.ConstructFromMembers then
                let declared = il.Label()
                il.Emit OpCodes.Ldarg_0
                il.EmitMethod(OpCodes.Call,api.ActualType model.Type)
                typeToken il api model.Type
                il.Emit OpCodes.Ceq
                il.EmitBranch(OpCodes.Brtrue,declared)
                il.Emit OpCodes.Ldarg_0
                il.Emit OpCodes.Ldarg_0
                il.EmitMethod(OpCodes.Call,api.ActualType model.Type)
                il.EmitMethod((if api.RawReader then OpCodes.Call else OpCodes.Callvirt),api.ToRuntimeObject)
                il.EmitType(OpCodes.Castclass,model.Type)
                il.Emit OpCodes.Ret

                il.Mark declared
            if model.Abstract then
                fail il api "Cannot serialize an abstract Type without the $type property from json."
            else
                let result = il.Local model.Type
                let values = il.Local api.ObjectDictionary
                let json = il.Local api.Json
                il.Emit OpCodes.Ldarg_0
                il.EmitMethod(OpCodes.Call,api.ObjectProperties)
                il.EmitLocal(OpCodes.Stloc,values)
                let readMember (memberValue: CodecMember<'T,'M,'F>) missing =
                    il.EmitLocal((if api.RawReader then OpCodes.Ldloca else OpCodes.Ldloc),values)
                    il.EmitString memberValue.Name
                    il.EmitLocal(OpCodes.Ldloca,json)
                    il.EmitMethod((if api.RawReader then OpCodes.Call else OpCodes.Callvirt),api.TryGetValue)
                    il.EmitBranch(OpCodes.Brfalse,missing)
                if model.ConstructFromMembers then
                    for index in model.ConstructorMembers do
                        let memberValue = model.Members[index]
                        let value = il.Local memberValue.Type
                        let missing = il.Label()
                        readMember memberValue missing
                        il.EmitLocal(OpCodes.Ldloc,json)
                        il.EmitMethod((if api.RawReader then OpCodes.Call else OpCodes.Callvirt),api.Deserialize memberValue.Type)
                        il.EmitLocal(OpCodes.Stloc,value)
                        il.Mark missing
                        il.EmitLocal(OpCodes.Ldloc,value)
                    il.EmitMethod(OpCodes.Newobj,model.Constructor.Value)
                else
                    match model.Constructor with
                    | Some constructor -> il.EmitMethod(OpCodes.Newobj,constructor)
                    | None when not model.ValueType -> il.EmitMethod(OpCodes.Call,api.Uninitialized model.Type)
                    | None ->
                        il.EmitLocal(OpCodes.Ldloca,result)
                        il.EmitType(OpCodes.Initobj,model.Type)
                        il.EmitLocal(OpCodes.Ldloc,result)
                    il.EmitLocal(OpCodes.Stloc,result)
                    for memberValue in model.Members do
                        if memberValue.Writable && (memberValue.Field.IsNone || model.ValueType) then
                            let missing = il.Label()
                            readMember memberValue missing
                            if not memberValue.Static then
                                il.EmitLocal((if model.ValueType then OpCodes.Ldloca else OpCodes.Ldloc),result)
                            il.EmitLocal(OpCodes.Ldloc,json)
                            il.EmitMethod((if api.RawReader then OpCodes.Call else OpCodes.Callvirt),api.Deserialize memberValue.Type)
                            match memberValue.Field, memberValue.Setter with
                            | Some field, _ -> il.EmitField((if memberValue.Static then OpCodes.Stsfld else OpCodes.Stfld),field)
                            | _, Some setter -> il.EmitMethod((if model.ValueType || memberValue.Static then OpCodes.Call else OpCodes.Callvirt),setter)
                            | _ -> invalidOp "A writable JSON member has no setter."
                            il.Mark missing
                    il.EmitLocal(OpCodes.Ldloc,result)
                il.Emit OpCodes.Ret

    let reader il api model = readerWithUnion il api model (fun () -> unionReader il api model)
