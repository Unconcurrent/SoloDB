namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

/// Original grammar operands for a generation-known object contract.
type internal CodecFiniteCaptureApi<'T,'M> = {
    Names: CodecNameApi<'T,'M>
    Range: 'T
    Capture: 'M
    Complete: 'M
}

module internal CodecFiniteCaptureEmitter =
    let names marker = if marker then [|"$dbrefTypedId",true|] else [|"$type",false;"$values",true|]

    /// The opening brace is already admitted. Names and slot requirements are cold facts.
    let capture (il: CodecIL<'T,'M,'F>) (api: CodecFiniteCaptureApi<'T,'M>) (names: (string * bool)[]) =
        let slots = names |> Array.map(fun (_,range) -> (if range then Some(il.Local api.Range) else None),il.Local api.Names.Boolean)
        let first = il.Local api.Names.Boolean
        il.Emit OpCodes.Ldc_I4_1; il.EmitLocal(OpCodes.Stloc,first)
        let next = il.Label()
        let unknown = il.Label()
        let complete = il.Label()
        let advance = il.Label()
        let labels = slots |> Array.map(fun _ -> il.Label())
        il.Mark next
        CodecNameEmitter.recognize il api.Names first (names |> Array.mapi(fun index (name,_) -> name,labels[index])) unknown complete
        let value() =
            il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Names.Colon)
            il.Emit OpCodes.Ldarg_0; il.Emit OpCodes.Ldc_I4_0
            il.EmitMethod(OpCodes.Call,api.Capture)
        for index = 0 to slots.Length - 1 do
            il.Mark labels[index]
            value()
            let range,present = slots[index]
            match range with Some local -> il.EmitLocal(OpCodes.Stloc,local) | None -> il.Emit OpCodes.Pop
            il.Emit OpCodes.Ldc_I4_1; il.EmitLocal(OpCodes.Stloc,present)
            il.EmitBranch(OpCodes.Br,advance)
        il.Mark unknown
        value(); il.Emit OpCodes.Pop
        il.Mark advance
        il.EmitBranch(OpCodes.Br,next)
        il.Mark complete
        il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Complete)
        slots

    let reader (il: CodecIL<'T,'M,'F>) api marker construct =
        let slots = capture il api (names marker)
        if marker then
            il.EmitLocal(OpCodes.Ldloc,(fst slots[0]).Value); il.EmitLocal(OpCodes.Ldloc,snd slots[0])
        else
            il.EmitLocal(OpCodes.Ldloc,(fst slots[1]).Value)
            il.EmitLocal(OpCodes.Ldloc,snd slots[0]); il.EmitLocal(OpCodes.Ldloc,snd slots[1])
        il.EmitMethod(OpCodes.Newobj,construct)
        il.Emit OpCodes.Ret
