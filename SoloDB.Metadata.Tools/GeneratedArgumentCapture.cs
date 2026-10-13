using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

internal static class GeneratedArgumentCapture
{
    internal static void Register(ModuleDefinition module, TypeDefinition owner, ILProcessor il,
        AssemblyDefinition product, GeneratedModel model, int index)
    {
        if (model.Facts.Shape != TypeShape.Object) return;
        var members = model.Facts.Members.Select((fact, ordinal) => (fact, ordinal)).Where(pair => !pair.fact.Field && pair.fact.Read).ToArray();
        var type = module.ImportReference(model.Type);
        il.Emit(OpCodes.Ldc_I4, members.Length); il.Emit(OpCodes.Newarr, module.TypeSystem.String);
        for (var ordinal = 0; ordinal < members.Length; ordinal++)
        {
            il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4, ordinal); il.Emit(OpCodes.Ldstr, members[ordinal].fact.Name); il.Emit(OpCodes.Stelem_Ref);
        }
        var read = new MethodDefinition("CaptureArguments" + index, MethodAttributes.Private | MethodAttributes.Static, module.TypeSystem.Void);
        read.Parameters.Add(new ParameterDefinition(type)); read.Parameters.Add(new ParameterDefinition(new ArrayType(module.TypeSystem.Object)));
        owner.Methods.Add(read); var body = read.Body.GetILProcessor();
        for (var ordinal = 0; ordinal < members.Length; ordinal++)
        {
            var member = model.Members[members[ordinal].ordinal];
            body.Emit(OpCodes.Ldarg_1); body.Emit(OpCodes.Ldc_I4, ordinal);
            if (model.Facts.ValueType) body.Emit(OpCodes.Ldarga_S, read.Parameters[0]); else body.Emit(OpCodes.Ldarg_0);
            body.Emit(model.Facts.ValueType ? OpCodes.Call : OpCodes.Callvirt, module.ImportReference(member.Getter!));
            if (member.Type.IsValueType) body.Emit(OpCodes.Box, module.ImportReference(member.Type));
            body.Emit(OpCodes.Stelem_Ref);
        }
        body.Emit(OpCodes.Ret);
        var action = new GenericInstanceType(new TypeReference("System", "Action`2", module, owner.BaseType.Scope));
        action.GenericArguments.Add(type); action.GenericArguments.Add(new ArrayType(module.TypeSystem.Object));
        var ctor = new MethodReference(".ctor", module.TypeSystem.Void, action) { HasThis = true };
        ctor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object)); ctor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
        il.Emit(OpCodes.Ldnull); il.Emit(OpCodes.Ldftn, read); il.Emit(OpCodes.Newobj, ctor);
        var register = new GenericInstanceMethod(module.ImportReference(product.MainModule.GetType("SoloDatabase.NativeArgumentMetadata").Methods.Single(method => method.Name == "Register")));
        register.GenericArguments.Add(type); il.Emit(OpCodes.Call, register);
    }
}
