using System;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using Microsoft.FSharp.Core;
using SoloDatabase.Metadata.CodecGeneration;

namespace SoloDatabase.Metadata.Tools;

internal static class GeneratedCustomIdentity
{
    internal static void Register(ModuleDefinition module, TypeDefinition owner, ILProcessor il,
        AssemblyDefinition product, GeneratedModel model, int index, GeneratedModel[] models)
    {
        var metadata = product.MainModule.GetType("SoloDatabase.CustomIdMetadata");
        var memberDefinition = metadata.Methods.Single(method => method.Name == "Member");
        var type = module.ImportReference(model.Type);
        var identities = model.OriginalProperties!.Where(property => property.Identity != null && !property.Member.Static).ToArray();
        il.Emit(OpCodes.Ldc_I4, identities.Length); il.Emit(OpCodes.Newarr, module.ImportReference(product.MainModule.GetType("SoloDatabase.CustomIdMember")));
        for (var ordinal = 0; ordinal < identities.Length; ordinal++)
        {
            var declaration = identities[ordinal]; var member = declaration.Member; var generator = declaration.Identity!; var idType = module.ImportReference(member.Type);
            var indexed = SoloDatabase.Metadata.Generation.TypeSupport.IndexedProperty(member.Getter?.Value.Parameters.Count ?? 0,member.Setter?.Value.Parameters.Count ?? 0);
            il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4, ordinal); il.Emit(OpCodes.Ldtoken,module.ImportReference(member.Getter?.Value.DeclaringType ?? member.Setter?.Value.DeclaringType ?? type));
            var systemType = module.ImportReference(memberDefinition.Parameters[0].ParameterType);
            var getType = new MethodReference("GetTypeFromHandle", systemType, systemType);
            getType.Parameters.Add(new ParameterDefinition(new TypeReference("System", "RuntimeTypeHandle", module, owner.BaseType.Scope, true)));
            il.Emit(OpCodes.Call, getType); il.Emit(OpCodes.Ldstr, member.Name);
            if (member.Getter == null || indexed) il.Emit(OpCodes.Ldnull);
            else
            {
                var read = Method(owner, "ReadCustomIdentity" + index + "_" + ordinal, idType, module.TypeSystem.Object);
                QueryOperationEmitter.memberRead(new CecilCodecIL(read),type,model.Facts.ValueType,false,
                    member,idType.IsValueType,true,false);
                Delegate(module, owner, il, "Func`2", read, module.TypeSystem.Object, idType);
            }
            if (member.Setter == null || indexed) il.Emit(OpCodes.Ldnull);
            else
            {
                var write = Method(owner, "WriteCustomIdentity" + index + "_" + ordinal, module.TypeSystem.Void, module.TypeSystem.Object, idType);
                QueryOperationEmitter.memberWrite(new CecilCodecIL(write),type,model.Facts.ValueType,
                    member,true,false,false);
                Delegate(module, owner, il, "Action`2", write, module.TypeSystem.Object, idType);
            }
            var create = Method(owner, "CreateIdentityGenerator" + index + "_" + ordinal, module.TypeSystem.Object);
            var constructor = generator.Constructor;
            if (generator.Type == null) { create.Body.GetILProcessor().Emit(OpCodes.Ldnull); create.Body.GetILProcessor().Emit(OpCodes.Ret); }
            else CodecEmitter.defaultConstruction(new CecilCodecIL(create),module.ImportReference(generator.Type),
                generator.Type.IsValueType,constructor == null ? null! : FSharpOption<MethodReference>.Some(module.ImportReference(constructor)),true);
            if (generator.Type == null) il.Emit(OpCodes.Ldnull); else new GeneratedJsonAssembly.Api(module,product).TypeToken(il,generator.Type);
            il.Emit(generator.Family ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
            il.Emit(constructor != null ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
            il.Emit(member.Setter != null ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
            il.Emit(indexed ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
            Delegate(module, owner, il, "Func`1", create, module.TypeSystem.Object);
            var defaultValue = Method(owner,"DefaultCustomIdentity" + index + "_" + ordinal,module.TypeSystem.Object);
            var identityModel = models.FirstOrDefault(candidate => GeneratedJsonDefinitions.SameType(candidate.Type,idType));
            var defaultConstructor = idType.IsValueType ? identityModel?.DefaultConstructor : null;
            CodecEmitter.defaultConstruction(new CecilCodecIL(defaultValue),idType,idType.IsValueType,
                defaultConstructor == null ? null! : FSharpOption<MethodReference>.Some(module.ImportReference(defaultConstructor)),true);
            Delegate(module,owner,il,"Func`1",defaultValue,module.TypeSystem.Object);
            var make = new GenericInstanceMethod(module.ImportReference(memberDefinition));
            make.GenericArguments.Add(type); make.GenericArguments.Add(idType); il.Emit(OpCodes.Call, make); il.Emit(OpCodes.Stelem_Ref);
        }
        var register = new GenericInstanceMethod(module.ImportReference(metadata.Methods.Single(method => method.Name == "Register")));
        register.GenericArguments.Add(type); il.Emit(OpCodes.Call, register);
        GeneratedSchemaIndexes.Register(module,owner,il,product,model,index);
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
