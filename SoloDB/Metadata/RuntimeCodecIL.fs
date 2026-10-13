namespace SoloDatabase.Metadata.CodecGeneration

open System
open System.Reflection
open System.Reflection.Emit
open System.Collections.Generic

type internal RuntimeCodecIL(generator: ILGenerator) =
    inherit CodecIL<Type,MethodBase,FieldInfo>()
    let locals = List<LocalBuilder>()
    let labels = List<Label>()
    override _.Emit(code) = generator.Emit(code)
    override _.EmitInt(code, value) =
        if code.Equals(OpCodes.Unaligned) then generator.Emit(code,byte value)
        elif code.OperandType = OperandType.InlineVar || code.OperandType = OperandType.ShortInlineVar then
            generator.Emit(code, int16 value)
        else generator.Emit(code, value)
    override _.EmitInt64(value) = generator.Emit(OpCodes.Ldc_I8,value)
    override _.EmitFloat32(value) = generator.Emit(OpCodes.Ldc_R4,value)
    override _.EmitFloat64(value) = generator.Emit(OpCodes.Ldc_R8,value)
    override _.EmitString(value) = generator.Emit(OpCodes.Ldstr,value)
    override _.EmitType(code, value) = generator.Emit(code,value)
    override _.EmitMethod(code, value) =
        match value with
        | :? ConstructorInfo as constructor -> generator.Emit(code,constructor)
        | :? MethodInfo as method -> generator.Emit(code,method)
        | _ -> invalidArg "value" "A codec instruction requires a method or constructor."
    override _.EmitField(code, value) = generator.Emit(code,value)
    override _.EmitLocal(code, value) = generator.Emit(code,locals[value])
    override _.EmitBranch(code, value) = generator.Emit(code,labels[value])
    override _.EmitSwitch(values) = generator.Emit(OpCodes.Switch,values |> Array.map(fun value -> labels[value]))
    override _.Local(value) =
        let index = locals.Count
        locals.Add(generator.DeclareLocal(value))
        index
    override _.Label() =
        let index = labels.Count
        labels.Add(generator.DefineLabel())
        index
    override _.Mark(value) = generator.MarkLabel(labels[value])

    override _.BeginException() = generator.BeginExceptionBlock() |> ignore
    override _.Finally() = generator.BeginFinallyBlock()
    override _.Catch(target) = generator.BeginCatchBlock(target)
    override _.EndException() = generator.EndExceptionBlock()
