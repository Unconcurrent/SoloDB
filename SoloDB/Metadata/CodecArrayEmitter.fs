namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

module internal CodecArrayEmitter =
    let private loop (il: CodecIL<'T,'M,'F>) operation fallback element body =
        let call = match operation element with Some call -> call | None -> fallback element
        body (fun arguments -> arguments(); il.EmitMethod(OpCodes.Call,call))

    let rawArrayReader (il: CodecIL<'T,'M,'F>) (api: CodecApi<'T,'M>) (input: CodecRawReaderApi<'T,'M>) array element =
        let origin = il.Local api.Int32
        il.Emit OpCodes.Ldarg_0
        il.EmitMethod(OpCodes.Call,input.Start)
        il.EmitLocal(OpCodes.Stloc,origin)
        let wrapped = il.Local array
        let ordinary = il.Label()
        il.Emit OpCodes.Ldarg_0
        il.EmitLocal(OpCodes.Ldloca,wrapped)
        il.EmitMethod(OpCodes.Call,input.ArrayWrapper element)
        il.EmitBranch(OpCodes.Brfalse,ordinary)
        il.EmitLocal(OpCodes.Ldloc,wrapped)
        il.Emit OpCodes.Ret
        il.Mark ordinary
        let present = il.Label()
        il.Emit OpCodes.Ldarg_0
        il.EmitMethod(OpCodes.Call,input.BeginArray)
        il.EmitBranch(OpCodes.Brtrue,present)
        il.Emit OpCodes.Ldnull
        il.Emit OpCodes.Ret
        il.Mark present
        let values = il.Local array
        let count = il.Local api.Int32
        let finish = il.Label()
        il.Emit OpCodes.Ldarg_0
        il.Emit OpCodes.Ldc_I4_1
        il.EmitMethod(OpCodes.Call,input.NextElement)
        il.EmitBranch(OpCodes.Brfalse,finish)
        loop il input.Operation input.Read element (fun read ->
            let loop = il.Label()
            let completed = il.Label()
            il.Mark loop
            let capacity = il.Label()
            let grow = il.Label()
            il.EmitLocal(OpCodes.Ldloc,values)
            il.EmitBranch(OpCodes.Brfalse,grow)
            il.EmitLocal(OpCodes.Ldloc,count)
            il.EmitLocal(OpCodes.Ldloc,values)
            il.Emit OpCodes.Ldlen
            il.Emit OpCodes.Conv_I4
            il.EmitBranch(OpCodes.Blt,capacity)
            il.Mark grow
            il.EmitLocal(OpCodes.Ldloc,values)
            il.EmitMethod(OpCodes.Call,input.GrowArray element)
            il.EmitLocal(OpCodes.Stloc,values)
            il.Mark capacity
            il.EmitLocal(OpCodes.Ldloc,values)
            il.EmitLocal(OpCodes.Ldloc,count)
            read (fun () -> il.Emit OpCodes.Ldarg_0)
            il.EmitType(OpCodes.Stelem,element)
            il.EmitLocal(OpCodes.Ldloc,count)
            il.Emit OpCodes.Ldc_I4_1
            il.Emit OpCodes.Add
            il.EmitLocal(OpCodes.Stloc,count)
            il.Emit OpCodes.Ldarg_0
            il.Emit OpCodes.Ldc_I4_0
            il.EmitMethod(OpCodes.Call,input.NextElement)
            il.EmitBranch(OpCodes.Brfalse,completed)
            il.EmitBranch(OpCodes.Br,loop)
            il.Mark completed)
        il.Mark finish
        il.Emit OpCodes.Ldarg_0
        il.EmitLocal(OpCodes.Ldloc,origin)
        il.EmitMethod(OpCodes.Call,input.Complete)
        il.EmitLocal(OpCodes.Ldloc,values)
        il.EmitLocal(OpCodes.Ldloc,count)
        il.EmitMethod(OpCodes.Call,input.TrimArray element)
        il.Emit OpCodes.Ret

    let rawArrayWriter (il: CodecIL<'T,'M,'F>) int32 (output: CodecArrayWriterApi<'T,'M>) element =
        let present = il.Label()
        il.Emit OpCodes.Ldarg_0
        il.EmitBranch(OpCodes.Brtrue,present)
        il.Emit OpCodes.Ldarg_1
        il.Emit OpCodes.Ldarg_2
        il.EmitMethod(OpCodes.Call,output.Null)
        il.Emit OpCodes.Ret
        il.Mark present
        il.Emit OpCodes.Ldarg_1
        il.Emit OpCodes.Ldarg_2
        il.EmitMethod(OpCodes.Call,output.BeginArray)
        let index = il.Local int32
        let finish = il.Label()
        il.Emit OpCodes.Ldarg_0
        il.Emit OpCodes.Ldlen
        il.EmitBranch(OpCodes.Brfalse,finish)
        loop il output.Operation output.Serialize element (fun write ->
            let loop = il.Label()
            let completed = il.Label()
            il.Mark loop
            let value = il.Label()
            il.EmitLocal(OpCodes.Ldloc,index)
            il.EmitBranch(OpCodes.Brfalse,value)
            il.Emit OpCodes.Ldarg_1
            il.Emit OpCodes.Ldarg_2
            il.EmitMethod(OpCodes.Call,output.Separator)
            il.Mark value
            write (fun () ->
                il.Emit OpCodes.Ldarg_0
                il.EmitLocal(OpCodes.Ldloc,index)
                il.EmitType(OpCodes.Ldelem,element)
                il.Emit OpCodes.Ldarg_1
                il.Emit OpCodes.Ldarg_2)
            il.EmitLocal(OpCodes.Ldloc,index)
            il.Emit OpCodes.Ldc_I4_1
            il.Emit OpCodes.Add
            il.EmitLocal(OpCodes.Stloc,index)
            il.EmitLocal(OpCodes.Ldloc,index)
            il.Emit OpCodes.Ldarg_0
            il.Emit OpCodes.Ldlen
            il.Emit OpCodes.Conv_I4
            il.EmitBranch(OpCodes.Bge,completed)
            il.EmitBranch(OpCodes.Br,loop)
            il.Mark completed)
        il.Mark finish
        il.Emit OpCodes.Ldarg_1
        il.Emit OpCodes.Ldarg_2
        il.EmitMethod(OpCodes.Call,output.EndArray)
        il.Emit OpCodes.Ret
