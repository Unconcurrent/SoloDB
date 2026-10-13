using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;

namespace SoloDatabase.Metadata.Tools;

// Direct string-key lookup shared by supplied member-path expression construction.
internal static class GeneratedStringIndexer
{
    internal static void Register(ModuleDefinition module, TypeDefinition owner, ILProcessor il,
        AssemblyDefinition product, GeneratedModel model, int index)
    {
        var api = new GeneratedJsonAssembly.Api(module,product);
        api.TypeToken(il,model.Type);
        if (model.StringIndexer == null)
        {
            il.Emit(OpCodes.Ldnull); il.Emit(OpCodes.Ldnull);
        }
        else
        {
            var result = module.ImportReference(model.StringIndexerValue!);
            var target = module.ImportReference(model.StringIndexer);
            var method = new MethodDefinition("ReadStringIndexer" + owner.Methods.Count, MethodAttributes.Private | MethodAttributes.Static,module.TypeSystem.Object);
            method.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));
            method.Parameters.Add(new ParameterDefinition(module.TypeSystem.String));
            owner.Methods.Add(method);
            var body = method.Body.GetILProcessor();
            body.Emit(OpCodes.Ldarg_0);
            body.Emit(model.Facts.ValueType ? OpCodes.Unbox : OpCodes.Castclass,module.ImportReference(model.Type));
            body.Emit(OpCodes.Ldarg_1);
            body.Emit(model.Facts.ValueType ? OpCodes.Call : OpCodes.Callvirt,target);
            if (result.IsValueType) body.Emit(OpCodes.Box,result);
            body.Emit(OpCodes.Ret);
            api.TypeToken(il,result);
            var function = new GenericInstanceType(new TypeReference("System","Func`3",module,owner.BaseType.Scope));
            function.GenericArguments.Add(module.TypeSystem.Object);function.GenericArguments.Add(module.TypeSystem.String);function.GenericArguments.Add(module.TypeSystem.Object);
            var constructor = new MethodReference(".ctor",module.TypeSystem.Void,function) { HasThis = true };
            constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
            il.Emit(OpCodes.Ldnull);il.Emit(OpCodes.Ldftn,method);il.Emit(OpCodes.Newobj,constructor);
        }
        il.Emit(OpCodes.Call,module.ImportReference(product.MainModule.GetType("SoloDatabase.ModelMetadata").Methods.Single(method => method.Name == "RegisterStringIndexer")));
    }
}
