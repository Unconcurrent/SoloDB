using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;

namespace SoloDatabase.Metadata.Tools;

internal static class GeneratedRelations
{
    internal static void Register(ModuleDefinition module, TypeDefinition owner, ILProcessor il,
        AssemblyDefinition product, GeneratedModel model, int index)
    {
        var type = module.ImportReference(model.Type);
        foreach (var capability in new[] { ("SoloDatabase.CollectionMetadata", "Register") })
        {
            var root = new GenericInstanceMethod(module.ImportReference(product.MainModule.GetType(capability.Item1).Methods.Single(method => method.Name == capability.Item2)));
            root.GenericArguments.Add(type); il.Emit(OpCodes.Call, root);
        }
        var metadata = product.MainModule.GetType("SoloDatabase.RelationMetadata");
        if (model.UnionCases != null)
        {
            il.Emit(model.UnionHostedReferences ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
            var union = new GenericInstanceMethod(module.ImportReference(metadata.Methods.Single(method => method.Name == "RegisterUnion")));
            union.GenericArguments.Add(type); il.Emit(OpCodes.Call, union);
        }
        var memberDefinition = metadata.Methods.Single(method => method.Name == "Member");
        var memberType = module.ImportReference(product.MainModule.GetType("SoloDatabase.RelationMember"));
        var members = model.Facts.Members.Select((fact, ordinal) => (fact, ordinal)).Where(pair => pair.fact.Reference).ToArray();
        il.Emit(OpCodes.Ldc_I4, members.Length); il.Emit(OpCodes.Newarr, memberType);
        for (var ordinal = 0; ordinal < members.Length; ordinal++)
        {
            var pair = members[ordinal]; var member = model.Members[pair.ordinal]; var referenceType = module.ImportReference(member.Type);
            il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4, ordinal);
            il.Emit(OpCodes.Ldtoken, type);
            var getType = new MethodReference("GetTypeFromHandle", module.ImportReference(memberDefinition.Parameters[0].ParameterType), module.ImportReference(memberDefinition.Parameters[0].ParameterType));
            getType.Parameters.Add(new ParameterDefinition(new TypeReference("System", "RuntimeTypeHandle", module, owner.BaseType.Scope, true)));
            il.Emit(OpCodes.Call, getType); il.Emit(OpCodes.Ldstr, pair.fact.Name);
            if (member.Getter == null) il.Emit(OpCodes.Ldnull);
            else
            {
                var read = Method(owner, "ReadReference" + index + "_" + ordinal, referenceType, type);
                QueryOperationEmitter.memberRead(new CecilCodecIL(read),type,model.Facts.ValueType,false,
                    GeneratedCodec.Model(model).Members[pair.ordinal],member.Type.IsValueType,false,false);
                Delegate(module, owner, il, "Func`2", type, referenceType, read);
            }
            if (member.Setter == null) il.Emit(OpCodes.Ldnull);
            else
            {
                var write = Method(owner, "WriteReference" + index + "_" + ordinal, module.TypeSystem.Void, module.TypeSystem.Object, referenceType);
                QueryOperationEmitter.memberWrite(new CecilCodecIL(write),type,model.Facts.ValueType,
                    GeneratedCodec.Model(model).Members[pair.ordinal],true,false,false);
                Delegate(module, owner, il, "Action`2", module.TypeSystem.Object, referenceType, write);
            }
            il.Emit(pair.fact.Ignored ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
            if (!pair.fact.HasReferencePolicy) il.Emit(OpCodes.Ldnull);
            else
            {
                il.Emit(OpCodes.Ldc_I4, pair.fact.OnDelete); il.Emit(OpCodes.Ldc_I4, pair.fact.OnOwnerDelete);
                il.Emit(pair.fact.Unique ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0); il.Emit(OpCodes.Ldc_I4, pair.fact.OrderBy);
                il.Emit(OpCodes.Newobj, module.ImportReference(product.MainModule.GetType("SoloDatabase.RelationPolicy").Methods.Single(method => method.IsConstructor)));
            }
            var create = new GenericInstanceMethod(module.ImportReference(memberDefinition));
            create.GenericArguments.Add(type); create.GenericArguments.Add(referenceType); il.Emit(OpCodes.Call, create); il.Emit(OpCodes.Stelem_Ref);
        }
        var register = new GenericInstanceMethod(module.ImportReference(metadata.Methods.Single(method => method.Name == "Register")));
        register.GenericArguments.Add(type); il.Emit(OpCodes.Call, register);
    }

    private static MethodDefinition Method(TypeDefinition owner, string name, TypeReference result, params TypeReference[] arguments)
    {
        var method = new MethodDefinition(name, MethodAttributes.Private | MethodAttributes.Static, result);
        foreach (var argument in arguments) method.Parameters.Add(new ParameterDefinition(argument));
        owner.Methods.Add(method); return method;
    }
    private static void Delegate(ModuleDefinition module, TypeDefinition owner, ILProcessor il, string name, TypeReference input, TypeReference result, MethodDefinition method)
    {
        var type = new GenericInstanceType(new TypeReference("System", name, module, owner.BaseType.Scope));
        type.GenericArguments.Add(input); type.GenericArguments.Add(result);
        var ctor = new MethodReference(".ctor", module.TypeSystem.Void, type) { HasThis = true };
        ctor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object)); ctor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
        il.Emit(OpCodes.Ldnull); il.Emit(OpCodes.Ldftn, method); il.Emit(OpCodes.Newobj, ctor);
    }
}
