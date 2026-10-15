using System;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

internal static class GeneratedArgumentCapture
{
    internal static void Register(GeneratedJsonDefinitions definitions, ModuleDefinition module, TypeDefinition owner,
        ILProcessor il, AssemblyDefinition product, GeneratedModel model)
    {
        if (model.Facts.Shape != TypeShape.Object || model.Facts.AbstractType) return;
        var members = GeneratedCodec.Model(model).Members.Where(member =>
            member.Field == null && TypeSupport.SqlArgumentProperty(true,member.Readable,member.Static,
                member.Getter != null && member.Getter.Value.Parameters.Count != 0)).ToArray();
        Register(definitions,module,owner,il,product,new GeneratedArgumentSource(model.Type,members));
    }

    internal static void Register(GeneratedJsonDefinitions definitions, ModuleDefinition module, TypeDefinition owner,
        ILProcessor il, AssemblyDefinition product, GeneratedArgumentSource source)
    {
        var type = module.ImportReference(source.Type);
        if (definitions.TryGet("sql-arguments",type,module.TypeSystem.Object,"",out _)) return;
        var read = new MethodDefinition("CaptureArguments" + owner.Methods.Count,
            MethodAttributes.Private | MethodAttributes.Static,module.TypeSystem.Void);
        read.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));
        read.Parameters.Add(new ParameterDefinition(new ArrayType(module.TypeSystem.Object)));
        owner.Methods.Add(read);
        definitions.Add("sql-arguments",type,module.TypeSystem.Object,"",read);
        var body = read.Body.GetILProcessor();
        for (var ordinal = 0; ordinal < source.Members.Length; ordinal++)
        {
            var member = source.Members[ordinal];
            var declaring = member.Getter.Value.DeclaringType;
            var getter = GeneratedJsonAssembly.MemberReader(definitions,module,owner,declaring,
                declaring.IsValueType,false,member,true,true,"Sql" + owner.Methods.Count);
            body.Emit(OpCodes.Ldarg_1); body.Emit(OpCodes.Ldc_I4,ordinal);
            body.Emit(OpCodes.Ldarg_0); body.Emit(OpCodes.Call,getter); body.Emit(OpCodes.Stelem_Ref);
        }
        body.Emit(OpCodes.Ret);
        il.Emit(OpCodes.Ldc_I4,source.Members.Length); il.Emit(OpCodes.Newarr,module.TypeSystem.String);
        for (var ordinal = 0; ordinal < source.Members.Length; ordinal++)
        {
            il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4,ordinal);
            il.Emit(OpCodes.Ldstr,source.Members[ordinal].Name); il.Emit(OpCodes.Stelem_Ref);
        }
        var action = new GenericInstanceType(new TypeReference("System","Action`2",module,owner.BaseType.Scope));
        action.GenericArguments.Add(module.TypeSystem.Object);
        action.GenericArguments.Add(new ArrayType(module.TypeSystem.Object));
        GeneratedJsonAssembly.Delegate(il,action,read);
        var register = new GenericInstanceMethod(module.ImportReference(product.MainModule.GetType("SoloDatabase.NativeArgumentMetadata")
            .Methods.Single(method => method.Name == "Register")));
        register.GenericArguments.Add(type); il.Emit(OpCodes.Call,register);
    }
}
