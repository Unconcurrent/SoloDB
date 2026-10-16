namespace SoloDatabase.Metadata.CodecGeneration

open System
open System.Reflection.Emit

/// Reference-tuple defaults construct fresh Rest values and wrap each original constructor.
module internal SqlTupleDefaultEmitter =
    let prepare (model: CodecTuple<'T,'M,'F>) (valueType: Func<'T,bool>)
                (constructor: Func<'T,'M option>) exceptionType wrapper
                (define: Func<'T,Tuple<CodecIL<'T,'M,'F>,'M>>) =
        let zero (il: CodecIL<'T,'M,'F>) typ =
            let local = il.Local typ
            il.EmitLocal(OpCodes.Ldloca,local)
            il.EmitType(OpCodes.Initobj,typ)
            il.EmitLocal(OpCodes.Ldloc,local)
        let finish (il: CodecIL<'T,'M,'F>) typ method loadArguments =
            let result = il.Local typ
            il.BeginException()
            loadArguments()
            il.EmitMethod(OpCodes.Newobj,method)
            il.EmitLocal(OpCodes.Stloc,result)
            il.Catch exceptionType
            il.EmitMethod(OpCodes.Newobj,wrapper)
            il.Emit OpCodes.Throw
            il.EndException()
            il.EmitLocal(OpCodes.Ldloc,result)
            il.Emit OpCodes.Ret
        let rec tuple (current: CodecTuple<'T,'M,'F>) root =
            let il,method = define.Invoke current.Type
            if root && current.ValueType then
                zero il current.Type
                il.Emit OpCodes.Ret
            else
                let arguments = current.Members |> Array.map(fun item ->
                    let typ = item.Member.Type
                    let local = il.Local typ
                    match item.Rest with
                    | Some rest -> il.EmitMethod(OpCodes.Call,tuple rest false)
                    | None ->
                        match if valueType.Invoke typ then constructor.Invoke typ else None with
                        | Some create ->
                            let leafIL,leaf = define.Invoke typ
                            finish leafIL typ create (fun () -> ())
                            il.EmitMethod(OpCodes.Call,leaf)
                        | None -> zero il typ
                    il.EmitLocal(OpCodes.Stloc,local)
                    local)
                finish il current.Type current.Constructor (fun () ->
                    for argument in arguments do il.EmitLocal(OpCodes.Ldloc,argument))
            method
        tuple model true
