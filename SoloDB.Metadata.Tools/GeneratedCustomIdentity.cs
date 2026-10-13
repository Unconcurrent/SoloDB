using System;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;

namespace SoloDatabase.Metadata.Tools;

internal static class GeneratedCustomIdentity
{
    internal static void Register(ModuleDefinition module, TypeDefinition owner, ILProcessor il,
        AssemblyDefinition product, GeneratedModel model, int index)
    {
        var metadata = product.MainModule.GetType("SoloDatabase.CustomIdMetadata");
        var memberDefinition = metadata.Methods.Single(method => method.Name == "Member");
        var type = module.ImportReference(model.Type);
        var identities = model.Facts.Members.Select((fact, ordinal) => (fact, ordinal)).Where(pair => pair.fact.CustomIdentity).ToArray();
        il.Emit(OpCodes.Ldc_I4, identities.Length); il.Emit(OpCodes.Newarr, module.ImportReference(product.MainModule.GetType("SoloDatabase.CustomIdMember")));
        for (var ordinal = 0; ordinal < identities.Length; ordinal++)
        {
            var pair = identities[ordinal]; var member = model.Members[pair.ordinal]; var idType = module.ImportReference(member.Type);
            il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4, ordinal); il.Emit(OpCodes.Ldtoken, type);
            var systemType = module.ImportReference(memberDefinition.Parameters[0].ParameterType);
            var getType = new MethodReference("GetTypeFromHandle", systemType, systemType);
            getType.Parameters.Add(new ParameterDefinition(new TypeReference("System", "RuntimeTypeHandle", module, owner.BaseType.Scope, true)));
            il.Emit(OpCodes.Call, getType); il.Emit(OpCodes.Ldstr, pair.fact.Name);
            if (member.Getter == null) il.Emit(OpCodes.Ldnull);
            else
            {
                var read = Method(owner, "ReadCustomIdentity" + index + "_" + ordinal, idType, type);
                var body = read.Body.GetILProcessor();
                if (model.Facts.ValueType) body.Emit(OpCodes.Ldarga_S, read.Parameters[0]); else body.Emit(OpCodes.Ldarg_0);
                body.Emit(model.Facts.ValueType ? OpCodes.Call : OpCodes.Callvirt, module.ImportReference(member.Getter)); body.Emit(OpCodes.Ret);
                Delegate(module, owner, il, "Func`2", read, type, idType);
            }
            if (member.Setter == null) il.Emit(OpCodes.Ldnull);
            else
            {
                var write = Method(owner, "WriteCustomIdentity" + index + "_" + ordinal, module.TypeSystem.Void, module.TypeSystem.Object, idType);
                var body = write.Body.GetILProcessor(); body.Emit(OpCodes.Ldarg_0); body.Emit(model.Facts.ValueType ? OpCodes.Unbox : OpCodes.Castclass, type);
                body.Emit(OpCodes.Ldarg_1); body.Emit(model.Facts.ValueType ? OpCodes.Call : OpCodes.Callvirt, module.ImportReference(member.Setter)); body.Emit(OpCodes.Ret);
                Delegate(module, owner, il, "Action`2", write, module.TypeSystem.Object, idType);
            }
            var create = Method(owner, "CreateIdentityGenerator" + index + "_" + ordinal, module.TypeSystem.Object);
            var code = create.Body.GetILProcessor();
            var constructor = member.IdentityGenerator ?? throw new InvalidOperationException("The original identity generator constructor is absent: " + pair.fact.Name);
            code.Emit(OpCodes.Newobj, module.ImportReference(constructor));
            if (constructor.DeclaringType.IsValueType) code.Emit(OpCodes.Box, module.ImportReference(constructor.DeclaringType));
            code.Emit(OpCodes.Ret); Delegate(module, owner, il, "Func`1", create, module.TypeSystem.Object);
            var make = new GenericInstanceMethod(module.ImportReference(memberDefinition));
            make.GenericArguments.Add(type); make.GenericArguments.Add(idType); il.Emit(OpCodes.Call, make); il.Emit(OpCodes.Stelem_Ref);
        }
        var register = new GenericInstanceMethod(module.ImportReference(metadata.Methods.Single(method => method.Name == "Register")));
        register.GenericArguments.Add(type); il.Emit(OpCodes.Call, register);
        var indexes = model.Facts.Members.Where(member => member.Indexed).ToArray();
        var indexType = product.MainModule.GetType("SoloDatabase.DeclaredIndex");
        il.Emit(OpCodes.Ldc_I4, indexes.Length); il.Emit(OpCodes.Newarr, module.ImportReference(indexType));
        for (var ordinal = 0; ordinal < indexes.Length; ordinal++)
        {
            var fact = indexes[ordinal]; var member = model.Members[model.Facts.Members.ToList().IndexOf(fact)];
            il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4, ordinal); il.Emit(OpCodes.Ldstr, fact.Name); il.Emit(OpCodes.Ldtoken, module.ImportReference(member.Type));
            var systemType = module.ImportReference(memberDefinition.Parameters[0].ParameterType);
            var getType = new MethodReference("GetTypeFromHandle", systemType, systemType);
            getType.Parameters.Add(new ParameterDefinition(new TypeReference("System", "RuntimeTypeHandle", module, owner.BaseType.Scope, true)));
            il.Emit(OpCodes.Call, getType); il.Emit(fact.IndexUnique ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0); il.Emit(fact.CustomIdentity ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Newobj, module.ImportReference(indexType.Methods.Single(method => method.IsConstructor))); il.Emit(OpCodes.Stelem_Ref);
        }
        var schema = new GenericInstanceMethod(module.ImportReference(product.MainModule.GetType("SoloDatabase.SchemaMetadata").Methods.Single(method => method.Name == "RegisterIndexes")));
        schema.GenericArguments.Add(type); il.Emit(OpCodes.Call, schema);
    }
    private static MethodDefinition Method(TypeDefinition owner, string name, TypeReference result, params TypeReference[] arguments)
    {
        var method = new MethodDefinition(name, MethodAttributes.Private | MethodAttributes.Static, result);
        foreach (var argument in arguments) method.Parameters.Add(new ParameterDefinition(argument));
        owner.Methods.Add(method); return method;
    }
    private static void Delegate(ModuleDefinition module, TypeDefinition owner, ILProcessor il, string name, MethodDefinition method, params TypeReference[] arguments)
    {
        var type = new GenericInstanceType(new TypeReference("System", name, module, owner.BaseType.Scope));
        foreach (var argument in arguments) type.GenericArguments.Add(argument);
        var constructor = new MethodReference(".ctor", module.TypeSystem.Void, type) { HasThis = true };
        constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object)); constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
        il.Emit(OpCodes.Ldnull); il.Emit(OpCodes.Ldftn, method); il.Emit(OpCodes.Newobj, constructor);
    }
}
