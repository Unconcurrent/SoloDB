using System;
using System.Linq;
using SoloDatabase.Metadata.Generation;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;

namespace SoloDatabase.Metadata.Tools;

// Direct string-key lookup shared by supplied member-path expression construction.
internal static class GeneratedStringIndexer
{
    internal static void Register(ModuleDefinition module, TypeDefinition owner, ILProcessor il,
        AssemblyDefinition product, GeneratedModel model, int index)
    {
        var api = new GeneratedJsonAssembly.Api(module,product);
        api.TypeToken(il,model.Type);
        var selected = GeneratedRowFields.Named(model,"Item",false);
        var property = selected < 0 ? null : model.OriginalProperties![selected].Member;
        var parameters = property == null ? Array.Empty<string>() : GeneratedRowFields.IndexParameters(property).Select(parameter => parameter.FullName).ToArray();
        var signature = TypeSupport.StringIndexer(parameters);
        var restricted = signature && property?.Getter != null && TypeSupport.QueryCallRestricted(false,property.Getter.Value.ReturnType is ByReferenceType,false);
        if (!signature || property?.Getter == null || restricted)
        {
            il.Emit(OpCodes.Ldnull); il.Emit(OpCodes.Ldnull);
        }
        else
        {
            var result = module.ImportReference(property!.Type);
            var target = module.ImportReference(property!.Getter!.Value);
            var method = new MethodDefinition("ReadStringIndexer" + owner.Methods.Count, MethodAttributes.Private | MethodAttributes.Static,module.TypeSystem.Object);
            method.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));
            method.Parameters.Add(new ParameterDefinition(module.TypeSystem.String));
            owner.Methods.Add(method);
            QueryOperationEmitter.stringIndexer(new CecilCodecIL(method),module.ImportReference(model.Type),
                model.Facts.ValueType,false,property!.Static,target,result,result.IsValueType);
            api.TypeToken(il,result);
            var function = new GenericInstanceType(new TypeReference("System","Func`3",module,owner.BaseType.Scope));
            function.GenericArguments.Add(module.TypeSystem.Object);function.GenericArguments.Add(module.TypeSystem.String);function.GenericArguments.Add(module.TypeSystem.Object);
            var constructor = new MethodReference(".ctor",module.TypeSystem.Void,function) { HasThis = true };
            constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
            il.Emit(OpCodes.Ldnull);il.Emit(OpCodes.Ldftn,method);il.Emit(OpCodes.Newobj,constructor);
        }
        il.Emit(selected == -2 ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
        il.Emit(signature && property?.Getter == null ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
        if (restricted) il.Emit(OpCodes.Ldstr,property!.Getter!.Value.Name); else il.Emit(OpCodes.Ldnull);
        il.Emit(OpCodes.Call,module.ImportReference(product.MainModule.GetType("SoloDatabase.ModelMetadata").Methods.Single(method => method.Name == "RegisterStringIndexer")));
    }
}
