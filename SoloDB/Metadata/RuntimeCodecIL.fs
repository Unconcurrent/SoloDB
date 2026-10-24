namespace SoloDatabase.Metadata.CodecGeneration

open System
open System.Reflection
open System.Reflection.Emit
open System.Collections.Generic

type internal RuntimeCodecIL(generator: ILGenerator, ?input: Type) =
    inherit CodecIL<Type,MethodBase,FieldInfo>()
    let locals = List<LocalBuilder>()
    let labels = List<Label>()
    new(generator: ILGenerator) = RuntimeCodecIL(generator, ?input = None)
    override _.InputValue() =
        generator.Emit(OpCodes.Ldarg_0)
        match input with Some target when target.IsByRef -> generator.Emit(OpCodes.Ldobj,target.GetElementType()) | _ -> ()
    override _.InputAddress() =
        match input with Some target when target.IsByRef -> generator.Emit(OpCodes.Ldarg_0) | _ -> generator.Emit(OpCodes.Ldarga,0s)
    override _.ByRefParameter(method,index) = method.GetParameters().[index].ParameterType.IsByRef
    override _.ResultParameter(method) =
        let parameters = method.GetParameters()
        method :? MethodInfo && (method :?> MethodInfo).ReturnType.Equals(typeof<Void>) &&
        parameters.Length = 2 && parameters.[1].ParameterType.IsByRef
    override _.PropertyStorage(method) =
        match method with
        | :? MethodInfo as getter when not getter.IsStatic ->
            let body = getter.GetMethodBody()
            let bytes = if isNull body then null else body.GetILAsByteArray()
            let direct = not(isNull bytes) && bytes.Length = 7 && bytes[0] = 0x02uy && bytes[1] = 0x7buy && bytes[6] = 0x2auy
            if not direct then None else
            let field = getter.Module.ResolveField(BitConverter.ToInt32(bytes,2),getter.DeclaringType.GetGenericArguments(),getter.GetGenericArguments())
            if CodecMemberStorage.allowed(direct,getter.IsVirtual,getter.IsFinal,getter.DeclaringType.IsSealed,
                    field.IsStatic,field.IsInitOnly || field.IsLiteral,
                    field.DeclaringType.Equals(getter.DeclaringType),field.FieldType.Equals(getter.ReturnType),
                    (getter.GetMethodImplementationFlags() &&& MethodImplAttributes.Synchronized) <> enum 0) then Some field
            else None
        | _ -> None
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
