namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

module internal CodecRawEmitter =
    open CodecEmitter

    let internal directCall (il: CodecIL<'T,'M,'F>) operation fallback target arguments =
        let call = match operation target with Some call -> call | None -> fallback target
        arguments()
        il.EmitMethod(OpCodes.Call,call)

    let rawBody (il: CodecIL<'T,'M,'F>) (api: CodecApi<'T,'M>)
                (output: CodecRawWriterApi<'T,'M>) (model: CodecModel<'T,'M,'F>) =
        match model.IndexedWrite with
        | Some signature -> indexedFailure il api false signature
        | None when model.ValueType && (model.Members |> Array.exists(fun memberValue -> memberValue.Field.IsSome && memberValue.Static)) -> staticFieldFailure il api false
        | None ->
            let comma = il.Local output.Boolean
            il.Emit OpCodes.Ldarg_1
            il.Emit OpCodes.Ldarg_2
            il.EmitMethod(OpCodes.Call,output.BeginObject)
            let members = writtenMembers model
            let mutable names = Set.empty
            for index in members do
                let memberValue = model.Members[index]
                let next = il.Label()
                if memberValue.Name = "Id" && not(names.Contains memberValue.Name) then
                    let included = il.Label()
                    il.Emit OpCodes.Ldarg_2
                    il.EmitMethod(OpCodes.Call,output.OmitId)
                    il.EmitBranch(OpCodes.Brfalse,included)
                    loadMember il model.ValueType 0 memberValue
                    il.EmitMethod(OpCodes.Call,output.Discard memberValue.Type)
                    il.EmitBranch(OpCodes.Br,next)
                    il.Mark included
                if names.Contains memberValue.Name then
                    loadMember il model.ValueType 0 memberValue
                    il.EmitMethod(OpCodes.Call,output.Discard memberValue.Type)
                    il.EmitString ("An item with the same key has already been added. Key: " + memberValue.Name)
                    il.Emit OpCodes.Ldnull
                    il.EmitMethod(OpCodes.Newobj,api.ArgumentExceptionConstructor)
                    il.Emit OpCodes.Throw
                else
                    il.EmitLocal(OpCodes.Ldloc,comma)
                    il.Emit OpCodes.Ldarg_1
                    il.Emit OpCodes.Ldarg_2
                    il.EmitMethod(OpCodes.Call,output.Property (SoloDatabase.JsonSerializator.JsonStringLiteral.property memberValue.Name))
                    il.Emit OpCodes.Ldc_I4_1
                    il.EmitLocal(OpCodes.Stloc,comma)
                    let value = il.Local memberValue.Type
                    loadMember il model.ValueType 0 memberValue
                    il.EmitLocal(OpCodes.Stloc,value)
                    let arguments () =
                        il.EmitLocal(OpCodes.Ldloc,value)
                        il.Emit OpCodes.Ldarg_1
                        il.Emit OpCodes.Ldarg_2
                    match memberValue.Name, SoloDatabase.Metadata.Generation.TypeSupport.StoredTypeName model.RuntimeName with
                    | "$type", Some _ ->
                        let ordinary = il.Label()
                        let complete = il.Label()
                        il.Emit OpCodes.Ldarg_2; il.EmitMethod(OpCodes.Call,output.Polymorphic.IncludeType)
                        il.EmitBranch(OpCodes.Brfalse,ordinary)
                        il.EmitLocal(OpCodes.Ldloc,value); il.EmitMethod(OpCodes.Call,output.Discard memberValue.Type)
                        il.Emit OpCodes.Ldarg_1; il.Emit OpCodes.Ldarg_2
                        il.EmitMethod(OpCodes.Call,output.TypeLiteral model.Type)
                        il.EmitBranch(OpCodes.Br,complete)
                        il.Mark ordinary
                        directCall il output.Operation output.Serialize memberValue.Type arguments
                        il.Mark complete
                    | _ -> directCall il output.Operation output.Serialize memberValue.Type arguments
                names <- names.Add memberValue.Name
                il.Mark next
            match SoloDatabase.Metadata.Generation.TypeSupport.StoredTypeName model.RuntimeName with
            | Some _ when not(names.Contains "$type") ->
                let complete = il.Label()
                il.Emit OpCodes.Ldarg_2; il.EmitMethod(OpCodes.Call,output.Polymorphic.IncludeType)
                il.EmitBranch(OpCodes.Brfalse,complete)
                il.EmitLocal(OpCodes.Ldloc,comma); il.Emit OpCodes.Ldarg_1; il.Emit OpCodes.Ldarg_2
                il.EmitMethod(OpCodes.Call,output.Property(SoloDatabase.JsonSerializator.JsonStringLiteral.property "$type"))
                il.Emit OpCodes.Ldarg_1; il.Emit OpCodes.Ldarg_2
                il.EmitMethod(OpCodes.Call,output.TypeLiteral model.Type)
                il.Mark complete
            | _ -> ()
            il.Emit OpCodes.Ldarg_1; il.Emit OpCodes.Ldarg_2
            il.EmitMethod(OpCodes.Call,output.EndObject)
            il.Emit OpCodes.Ret


    let rawWriter (il: CodecIL<'T,'M,'F>) (api: CodecApi<'T,'M>)
                  (output: CodecRawWriterApi<'T,'M>) (model: CodecModel<'T,'M,'F>) body =
        match model.IndexedWrite with
        | Some signature -> indexedFailure il api false signature
        | None when model.ValueType && (model.Members |> Array.exists(fun memberValue -> memberValue.Field.IsSome && memberValue.Static)) -> staticFieldFailure il api false
        | None ->
            if not model.ValueType then
                let present = il.Label()
                il.Emit OpCodes.Ldarg_0
                il.EmitBranch(OpCodes.Brtrue,present)
                il.Emit OpCodes.Ldarg_1
                il.Emit OpCodes.Ldarg_2
                il.EmitMethod(OpCodes.Call,output.Null)
                il.Emit OpCodes.Ret
                il.Mark present
                if not model.Sealed then
                    CodecPolymorphicEmitter.dispatch il api output.Polymorphic model.Type (not model.Abstract) (fun () ->
                        il.Emit OpCodes.Ldarg_0; il.Emit OpCodes.Ldarg_1; il.Emit OpCodes.Ldarg_2
                        il.EmitMethod(OpCodes.Call,body))
            if model.ValueType || model.Sealed then
                il.Emit OpCodes.Ldarg_0
                il.Emit OpCodes.Ldarg_1
                il.Emit OpCodes.Ldarg_2
                il.EmitMethod(OpCodes.Call,body)
            il.Emit OpCodes.Ret


    let forwardReader (model: CodecModel<'T,'M,'F>) =
        let invalidMutable = not model.ConstructFromMembers &&
                             (model.IndexedRead.IsSome ||
                              (model.ValueType && (model.Members |> Array.exists(fun memberValue -> memberValue.Field.IsSome && (memberValue.Static || not memberValue.Writable)))) ||
                              (not model.ValueType && not(model.Members |> Array.exists(fun memberValue -> memberValue.Writable && memberValue.Field.IsNone))))
        not(model.Union || model.Abstract || invalidMutable)

    let rawReader (il: CodecIL<'T,'M,'F>) (api: CodecApi<'T,'M>) (input: CodecRawReaderApi<'T,'M>) (model: CodecModel<'T,'M,'F>) readUnion =
        if not(forwardReader model) then readerWithUnion il api model readUnion
        else
            let nameApi = input.Properties.Value
            let origin = il.Local api.Int32
            il.Emit OpCodes.Ldarg_0
            il.EmitMethod(OpCodes.Call,input.Start)
            il.EmitLocal(OpCodes.Stloc,origin)
            let present = il.Label()
            il.Emit OpCodes.Ldarg_0
            il.EmitMethod(OpCodes.Call,input.BeginObject)
            il.EmitBranch(OpCodes.Brtrue,present)
            returnDefault il model.Type
            il.Mark present
            let result = il.Local model.Type
            let locals = if model.ConstructFromMembers then model.ConstructorMembers |> Array.map(fun index -> index, il.Local model.Members[index].Type) else [||]
            if not model.ConstructFromMembers then
                match model.Constructor with
                | Some constructor -> il.EmitMethod(OpCodes.Newobj,constructor)
                | None when not model.ValueType -> il.EmitMethod(OpCodes.Call,api.Uninitialized model.Type)
                | None ->
                    il.EmitLocal(OpCodes.Ldloca,result)
                    il.EmitType(OpCodes.Initobj,model.Type)
                    il.EmitLocal(OpCodes.Ldloc,result)
                il.EmitLocal(OpCodes.Stloc,result)
            let first = il.Local input.Boolean
            il.Emit OpCodes.Ldc_I4_1
            il.EmitLocal(OpCodes.Stloc,first)
            let loop = il.Label()
            let finish = il.Label()
            il.Mark loop
            let readable =
                model.Members
                |> Array.mapi(fun index memberValue ->
                    let local = locals |> Array.tryFind(fun (memberIndex,_) -> memberIndex = index)
                    index,memberValue,local)
                |> Array.filter(fun (_,memberValue,local) ->
                    local.IsSome || (not model.ConstructFromMembers && memberValue.Writable && (memberValue.Field.IsNone || model.ValueType)))
                |> Array.groupBy(fun (_,memberValue,_) -> memberValue.Name)
            let targets = readable |> Array.map(fun (property,members) -> property,members,il.Label())
            let polymorphic = not model.Sealed && not model.ValueType
            let discriminator =
                if polymorphic then
                    match targets |> Array.tryFind(fun (property,_,_) -> property = "$type") with
                    | Some(_,_,target) -> Some(target,false)
                    | None -> Some(il.Label(),true)
                else None
            let unknown = il.Label()
            let names =
                let entries = targets |> Array.map(fun (property,_,target) -> property,target)
                match discriminator with
                | Some(target,true) -> Array.append entries [|"$type",target|]
                | _ -> entries
            CodecNameEmitter.recognize il nameApi first names unknown finish
            let completeName () =
                il.Emit OpCodes.Ldarg_0
                il.EmitMethod(OpCodes.Call,nameApi.Colon)
                il.Emit OpCodes.Ldarg_0
                il.EmitMethod(OpCodes.Call,nameApi.Value)
            for property, members, target in targets do
                il.Mark target
                completeName()
                if polymorphic && property = "$type" then
                    il.Emit OpCodes.Ldarg_0
                    il.EmitMethod(OpCodes.Call,input.CheckType model.Type)
                // Hidden inherited members retain separate conversions from the same borrowed value.
                let original = if members.Length > 1 then Some(il.Local api.Json) else None
                match original with
                | Some saved ->
                    il.Emit OpCodes.Ldarg_0
                    il.EmitType(OpCodes.Ldobj,api.Json)
                    il.EmitLocal(OpCodes.Stloc,saved)
                | None -> ()
                for offset = 0 to members.Length - 1 do
                    let _, memberValue, local = members[offset]
                    if offset > 0 then
                        il.Emit OpCodes.Ldarg_0
                        il.EmitLocal(OpCodes.Ldloc,original.Value)
                        il.EmitType(OpCodes.Stobj,api.Json)
                    let readValue () = directCall il input.Operation input.Read memberValue.Type (fun () -> il.Emit OpCodes.Ldarg_0)
                    match local with
                    | Some(_,value) ->
                        readValue()
                        il.EmitLocal(OpCodes.Stloc,value)
                    | None ->
                        if not memberValue.Static then il.EmitLocal((if model.ValueType then OpCodes.Ldloca else OpCodes.Ldloc),result)
                        readValue()
                        match memberValue.Field,memberValue.Setter with
                        | Some field,_ -> il.EmitField((if memberValue.Static then OpCodes.Stsfld else OpCodes.Stfld),field)
                        | _,Some setter -> il.EmitMethod((if model.ValueType || memberValue.Static then OpCodes.Call else OpCodes.Callvirt),setter)
                        | _ -> invalidOp "A writable JSON member has no setter."
                il.EmitBranch(OpCodes.Br,loop)
            match discriminator with
            | Some(target,true) ->
                il.Mark target
                completeName()
                il.Emit OpCodes.Ldarg_0
                il.EmitMethod(OpCodes.Call,input.CheckType model.Type)
                il.Emit OpCodes.Ldarg_0
                il.EmitMethod(OpCodes.Call,input.Skip)
                il.EmitBranch(OpCodes.Br,loop)
            | _ -> ()
            il.Mark unknown
            completeName()
            il.Emit OpCodes.Ldarg_0
            il.EmitMethod(OpCodes.Call,input.Skip)
            il.EmitBranch(OpCodes.Br,loop)
            il.Mark finish
            il.Emit OpCodes.Ldarg_0
            il.EmitLocal(OpCodes.Ldloc,origin)
            il.EmitMethod(OpCodes.Call,input.Complete)
            if model.ConstructFromMembers then
                for _,value in locals do il.EmitLocal(OpCodes.Ldloc,value)
                il.EmitMethod(OpCodes.Newobj,model.Constructor.Value)
            else il.EmitLocal(OpCodes.Ldloc,result)
            il.Emit OpCodes.Ret
