using System;
using System.Collections.Generic;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;
using EmitCode = System.Reflection.Emit.OpCode;

namespace SoloDatabase.Metadata.Tools;

internal sealed class CecilCodecIL : CodecIL<TypeReference, MethodReference, FieldReference>
{
    private static readonly Dictionary<short, OpCode> Codes = typeof(OpCodes).GetFields()
        .Where(field => field.FieldType == typeof(OpCode))
        .Select(field => (OpCode)field.GetValue(null)!)
        .ToDictionary(code => code.Value);
    private readonly ILProcessor il;
    private readonly List<Instruction> labels = new();
    private readonly Stack<(Instruction Start, Instruction End, List<ExceptionHandler> Handlers)> regions = new();
    private ModuleDefinition Module => il.Body.Method.Module;

    internal CecilCodecIL(MethodDefinition method) { il = method.Body.GetILProcessor(); }
    public override void Emit(EmitCode code) => il.Emit(Codes[code.Value]);
    public override void EmitInt(EmitCode code, int value)
    {
        var translated = Codes[code.Value];
        if (translated.Equals(OpCodes.Unaligned)) il.Emit(translated,(byte)value);
        else if (translated.OperandType is OperandType.InlineArg or OperandType.ShortInlineArg)
            il.Emit(translated, il.Body.Method.HasThis
                ? value == 0 ? il.Body.ThisParameter : il.Body.Method.Parameters[value - 1]
                : il.Body.Method.Parameters[value]);
        else il.Emit(translated, value);
    }
    public override void EmitInt64(long value) => il.Emit(OpCodes.Ldc_I8, value);
    public override void EmitFloat32(float value) => il.Emit(OpCodes.Ldc_R4, value);
    public override void EmitFloat64(double value) => il.Emit(OpCodes.Ldc_R8, value);
    public override void EmitString(string value) => il.Emit(OpCodes.Ldstr, value);
    public override void EmitType(EmitCode code, TypeReference value) => il.Emit(Codes[code.Value], Module.ImportReference(value));
    public override void EmitMethod(EmitCode code, MethodReference value) => il.Emit(Codes[code.Value], Module.ImportReference(value));
    public override void EmitField(EmitCode code, FieldReference value) => il.Emit(Codes[code.Value], Module.ImportReference(value));
    public override void EmitLocal(EmitCode code, int value) => il.Emit(Codes[code.Value], il.Body.Variables[value]);
    public override void EmitBranch(EmitCode code, int value) => il.Emit(Codes[code.Value], labels[value]);
    public override void EmitSwitch(int[] values) => il.Emit(OpCodes.Switch,values.Select(value => labels[value]).ToArray());
    public override int Local(TypeReference value)
    {
        var index = il.Body.Variables.Count;
        il.Body.Variables.Add(new VariableDefinition(Module.ImportReference(value)));
        il.Body.InitLocals = true;
        return index;
    }
    public override int Label()
    {
        var index = labels.Count;
        labels.Add(il.Create(OpCodes.Nop));
        return index;
    }
    public override void Mark(int value) => il.Append(labels[value]);
    public override void BeginException()
    {
        var start = il.Create(OpCodes.Nop); il.Append(start);
        regions.Push((start,il.Create(OpCodes.Nop),new List<ExceptionHandler>()));
    }
    public override void Catch(TypeReference target)
    {
        var region = regions.Peek();
        il.Emit(OpCodes.Leave,region.End);
        var start = il.Create(OpCodes.Nop); il.Append(start);
        if (region.Handlers.Count != 0) region.Handlers[region.Handlers.Count - 1].HandlerEnd = start;
        var handler = new ExceptionHandler(ExceptionHandlerType.Catch)
        { CatchType = Module.ImportReference(target), TryStart = region.Start,
          TryEnd = region.Handlers.Count == 0 ? start : region.Handlers[0].TryEnd, HandlerStart = start };
        region.Handlers.Add(handler);
    }
    public override void Finally()
    {
        var region = regions.Peek();
        il.Emit(OpCodes.Leave,region.End);
        var start = il.Create(OpCodes.Nop); il.Append(start);
        if (region.Handlers.Count != 0) region.Handlers[region.Handlers.Count - 1].HandlerEnd = start;
        region.Handlers.Add(new ExceptionHandler(ExceptionHandlerType.Finally)
        { TryStart = region.Start, TryEnd = region.Handlers.Count == 0 ? start : region.Handlers[0].TryEnd, HandlerStart = start });
    }
    public override void EndException()
    {
        var region = regions.Pop();
        if (region.Handlers[region.Handlers.Count - 1].HandlerType == ExceptionHandlerType.Finally) il.Emit(OpCodes.Endfinally);
        else il.Emit(OpCodes.Leave,region.End);
        il.Append(region.End);
        region.Handlers[region.Handlers.Count - 1].HandlerEnd = region.End;
        foreach(var handler in region.Handlers) il.Body.ExceptionHandlers.Add(handler);
    }
}
