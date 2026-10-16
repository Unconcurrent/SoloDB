using System;
using System.Linq;
using Microsoft.FSharp.Core;
using SoloDatabase.Metadata.CodecGeneration;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

internal static class GeneratedObjectSqlRows
{
    internal static void Register(ModuleDefinition module, TypeDefinition owner, ILProcessor il,
        AssemblyDefinition product, GeneratedModel model, int index, GeneratedModel[] models)
    {
        var type = module.ImportReference(model.Type);
        var shape = module.ImportReference(product.MainModule.GetType("SoloDatabase.SqlRowShape"));
        var row = module.ImportReference(product.MainModule.GetType("SoloDatabase.SqlRow"));
        var codec = GeneratedCodec.Model(model);
        var members = SqlRowEmitter.members(model.Facts.RecordType ? FSharpOption<string[]>.Some(model.Facts.RecordFields.ToArray()) : null!,
            model.Facts.RecordType ? codec.Members : model.OriginalProperties!.Select(property => property.Member).ToArray(),model.SqlFields);
        bool Indexed(int position) => TypeSupport.IndexedProperty(members[position].Getter?.Value.Parameters.Count ?? 0,
            members[position].Setter?.Value.Parameters.Count ?? 0);
        var mapper = new TypeDefinition("", "ObjectRow" + index, TypeAttributes.NestedPrivate | TypeAttributes.Sealed, owner.BaseType);
        owner.NestedTypes.Add(mapper);
        var ordinals = new FieldDefinition("ordinals", FieldAttributes.Private | FieldAttributes.InitOnly, new ArrayType(module.TypeSystem.Int32));
        mapper.Fields.Add(ordinals);
        var ctor = Method(mapper, ".ctor", module.TypeSystem.Void, false, ordinals.FieldType);
        ctor.Attributes |= MethodAttributes.SpecialName | MethodAttributes.RTSpecialName;
        var body = ctor.Body.GetILProcessor(); body.Emit(OpCodes.Ldarg_0);
        body.Emit(OpCodes.Call, new MethodReference(".ctor", module.TypeSystem.Void, owner.BaseType) { HasThis = true });
        body.Emit(OpCodes.Ldarg_0); body.Emit(OpCodes.Ldarg_1); body.Emit(OpCodes.Stfld, ordinals); body.Emit(OpCodes.Ret);
        var read = Method(mapper, "Read", type, false, row); read.Body.InitLocals = true;
        body = read.Body.GetILProcessor();
        var defaultConstructor = model.DefaultConstructor ?? (model.Constructor?.Parameters.Count == 0 ? model.Constructor : null);
        var constructor = defaultConstructor == null ? null! : FSharpOption<MethodReference>.Some(module.ImportReference(defaultConstructor));
        SqlRowEmitter.objectRow(new CecilCodecIL(read),GeneratedSqlRows.Api(module,product,models),codec,members,
            model.Facts.RecordType,constructor,new Action<int>(position => {
                body.Emit(OpCodes.Ldarg_0); body.Emit(OpCodes.Ldfld,ordinals);
                body.Emit(OpCodes.Ldc_I4,position); body.Emit(OpCodes.Ldelem_I4);
            }),new Func<int,bool>(position => TypeSupport.SqlRowWritable(members[position].Field != null,members[position].Writable,Indexed(position))));
        var factory = Method(owner, "BindObjectRow" + index, Function(module, owner, row, type), true, shape);
        body = factory.Body.GetILProcessor();
        body.Emit(OpCodes.Ldc_I4, members.Length); body.Emit(OpCodes.Newarr, module.TypeSystem.Int32);
        var ordinalMethod = module.ImportReference(product.MainModule.GetType("SoloDatabase.SqlRowShape").Methods.Single(method => method.Name == "BindMember"));
        for (var position = 0; position < members.Length; position++)
        {
            body.Emit(OpCodes.Dup); body.Emit(OpCodes.Ldc_I4, position); body.Emit(OpCodes.Ldarg_0);
            body.Emit(OpCodes.Ldstr, TypeSupport.SqlColumnName(model.Facts.RecordType,members[position].Name)); body.Emit(model.Facts.RecordType || members[position].Field == null || members[position].Writable ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
            body.Emit(Indexed(position) ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
            body.Emit(OpCodes.Callvirt, ordinalMethod); body.Emit(OpCodes.Stelem_I4);
        }
        body.Emit(OpCodes.Newobj, ctor); body.Emit(OpCodes.Ldftn, read);
        body.Emit(OpCodes.Newobj, DelegateConstructor(module, (GenericInstanceType)factory.ReturnType)); body.Emit(OpCodes.Ret);
        il.Emit(OpCodes.Ldnull); il.Emit(OpCodes.Ldftn, factory);
        il.Emit(OpCodes.Newobj, DelegateConstructor(module, Function(module, owner, shape, factory.ReturnType)));
        var mapping = product.MainModule.GetType("SoloDatabase.SqlRowMapping`1");
        var mappingType = new GenericInstanceType(module.ImportReference(mapping)); mappingType.GenericArguments.Add(type);
        var mappingCtor = module.ImportReference(mapping.Methods.Single(method => method.IsConstructor)); mappingCtor.DeclaringType = mappingType; il.Emit(OpCodes.Newobj, mappingCtor);
        var register = module.ImportReference(mapping.Methods.Single(method => method.Name == "Register")); register.DeclaringType = mappingType; il.Emit(OpCodes.Call, register);
    }

    private static MethodDefinition Method(TypeDefinition owner, string name, TypeReference result, bool isStatic, params TypeReference[] arguments)
    {
        var method = new MethodDefinition(name, MethodAttributes.Public | MethodAttributes.HideBySig | (isStatic ? MethodAttributes.Static : 0), result);
        foreach (var argument in arguments) method.Parameters.Add(new ParameterDefinition(argument)); owner.Methods.Add(method); return method;
    }
    private static GenericInstanceType Function(ModuleDefinition module, TypeDefinition owner, TypeReference input, TypeReference output)
    {
        var function = new GenericInstanceType(new TypeReference("System", "Func`2", module, owner.BaseType.Scope));
        function.GenericArguments.Add(input); function.GenericArguments.Add(output); return function;
    }
    private static MethodReference DelegateConstructor(ModuleDefinition module, GenericInstanceType type)
    {
        var ctor = new MethodReference(".ctor", module.TypeSystem.Void, type) { HasThis = true };
        ctor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object)); ctor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr)); return ctor;
    }
}
