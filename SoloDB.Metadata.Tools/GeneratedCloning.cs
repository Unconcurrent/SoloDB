using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

internal static class GeneratedCloning
{
    internal static void Register(ModuleDefinition module, TypeDefinition owner, ILProcessor il,
        AssemblyDefinition product, GeneratedModel model, int index)
    {
        if (model.Facts.Shape != TypeShape.Object || !(model.Facts.ValueType || model.Facts.ParameterlessConstructor)) return;
        var type = module.ImportReference(model.Type);
        var create = new MethodDefinition("CreateClone" + index, MethodAttributes.Private | MethodAttributes.Static, type);
        owner.Methods.Add(create);
        var body = create.Body.GetILProcessor();
        if (model.Constructor != null && model.Constructor.Parameters.Count == 0)
            body.Emit(OpCodes.Newobj, module.ImportReference(model.Constructor));
        else
        {
            create.Body.InitLocals = true;
            var value = new VariableDefinition(type); create.Body.Variables.Add(value);
            body.Emit(OpCodes.Ldloca, value); body.Emit(OpCodes.Initobj, type); body.Emit(OpCodes.Ldloc, value);
        }
        body.Emit(OpCodes.Ret);
        var copy = new MethodDefinition("CopyCloneScalars" + index, MethodAttributes.Private | MethodAttributes.Static, module.TypeSystem.Void);
        copy.Parameters.Add(new ParameterDefinition(type)); copy.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));
        owner.Methods.Add(copy); body = copy.Body.GetILProcessor();
        for (var ordinal = 0; ordinal < model.Members.Length; ordinal++)
        {
            var fact = model.Facts.Members[ordinal]; var member = model.Members[ordinal];
            if (fact.Field || fact.ManyReference || member.Getter == null || member.Setter == null) continue;
            body.Emit(OpCodes.Ldarg_1); body.Emit(model.Facts.ValueType ? OpCodes.Unbox : OpCodes.Castclass, type);
            if (model.Facts.ValueType) body.Emit(OpCodes.Ldarga_S, copy.Parameters[0]); else body.Emit(OpCodes.Ldarg_0);
            body.Emit(model.Facts.ValueType ? OpCodes.Call : OpCodes.Callvirt, module.ImportReference(member.Getter));
            body.Emit(model.Facts.ValueType ? OpCodes.Call : OpCodes.Callvirt, module.ImportReference(member.Setter));
        }
        body.Emit(OpCodes.Ret);
        Delegate(module, owner, il, "Func`1", create, type);
        Delegate(module, owner, il, "Action`2", copy, type, module.TypeSystem.Object);
        var register = new GenericInstanceMethod(module.ImportReference(product.MainModule.GetType("SoloDatabase.CollectionCloneMetadata")
            .Methods.Single(method => method.Name == "Register")));
        register.GenericArguments.Add(type); il.Emit(OpCodes.Call, register);
    }

    private static void Delegate(ModuleDefinition module, TypeDefinition owner, ILProcessor il, string name,
        MethodDefinition method, params TypeReference[] arguments)
    {
        var type = new GenericInstanceType(new TypeReference("System", name, module, owner.BaseType.Scope));
        foreach (var argument in arguments) type.GenericArguments.Add(argument);
        var constructor = new MethodReference(".ctor", module.TypeSystem.Void, type) { HasThis = true };
        constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object)); constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
        il.Emit(OpCodes.Ldnull); il.Emit(OpCodes.Ldftn, method); il.Emit(OpCodes.Newobj, constructor);
    }
}
