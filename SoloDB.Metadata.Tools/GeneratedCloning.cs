using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.Generation;
using SoloDatabase.Metadata.CodecGeneration;
using Microsoft.FSharp.Core;

namespace SoloDatabase.Metadata.Tools;

internal static class GeneratedCloning
{
    internal static void Register(ModuleDefinition module, TypeDefinition owner, ILProcessor il,
        AssemblyDefinition product, GeneratedModel model, int index)
    {
        if (!CodecPreparation.clonePublication(model.Facts.Shape)) return;
        var type = module.ImportReference(model.Type);
        var members = model.OriginalProperties!.Select(property => property.Member).Where(member =>
            TypeSupport.CloneProperty(member.Field != null,member.Static,member.Getter != null,member.Setter != null,member.Many)).ToArray();
        bool Indexed(CodecMember<TypeReference,MethodReference,FieldReference> member) =>
            TypeSupport.IndexedProperty(member.Getter?.Value.Parameters.Count ?? 0,member.Setter?.Value.Parameters.Count ?? 0);
        var create = new MethodDefinition("CreateClone" + index, MethodAttributes.Private | MethodAttributes.Static, type);
        owner.Methods.Add(create);
        var constructible = !model.Facts.AbstractType && (model.Facts.ValueType || model.CloneConstructor != null);
        var constructor = !constructible || model.CloneConstructor == null ? null! : FSharpOption<MethodReference>.Some(module.ImportReference(model.CloneConstructor));
        CodecEmitter.defaultConstruction(new CecilCodecIL(create),type,model.Facts.ValueType,constructor,false);
        var copy = new MethodDefinition("CopyCloneScalars" + index, MethodAttributes.Private | MethodAttributes.Static, module.TypeSystem.Void);
        copy.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object)); copy.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));
        owner.Methods.Add(copy);
        OwnerCloneEmitter.copy(new CecilCodecIL(copy),type,model.Facts.ValueType,members.Where(member => !Indexed(member)).ToArray());
        Delegate(module, owner, il, "Func`1", create, type);
        Delegate(module, owner, il, "Action`2", copy, module.TypeSystem.Object, module.TypeSystem.Object);
        il.Emit(constructible ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
        il.Emit(members.Any(Indexed) ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
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
