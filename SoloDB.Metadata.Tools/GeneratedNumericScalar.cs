using System;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;

namespace SoloDatabase.Metadata.Tools;

// Original target operands supply the finite scalar adapter emitted by the common macros.
internal static class GeneratedNumericScalar
{
    private static MethodDefinition Body(ModuleDefinition module,TypeDefinition owner,string name,TypeReference result,params TypeReference[] arguments)
    {
        var method = new MethodDefinition(name,MethodAttributes.Assembly | MethodAttributes.Static | MethodAttributes.HideBySig,module.ImportReference(result));
        method.Body.InitLocals = true;
        foreach(var argument in arguments) method.Parameters.Add(new ParameterDefinition(module.ImportReference(argument)));
        owner.Methods.Add(method);
        return method;
    }
    private static MethodReference Parse(ModuleDefinition module,TypeReference target)
    {
        var method = new MethodReference("Parse",target,target);
        method.Parameters.Add(new ParameterDefinition(module.TypeSystem.String));
        method.Parameters.Add(new ParameterDefinition(new TypeReference("System.Globalization","NumberStyles",module,target.Scope,true)));
        method.Parameters.Add(new ParameterDefinition(new TypeReference("System","IFormatProvider",module,target.Scope)));
        return method;
    }
    private static MethodReference Widen(ModuleDefinition module,TypeReference target)
    {
        var method = new MethodReference("op_Explicit",module.TypeSystem.Single,target);
        method.Parameters.Add(new ParameterDefinition(target));
        return method;
    }
    internal static MethodReference Compatibility(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,AssemblyDefinition product,TypeReference target,TypeReference reader,TypeReference context,string name)
    {
        if (definitions.TryGet("numeric-compatibility",target,reader,"",out var existing)) return existing;
        target = module.ImportReference(target);
        var api = new GeneratedJsonAssembly.Api(module,product);
        var method = Body(module,owner,name,target,new ByReferenceType(context));
        CodecScalarAdapterEmitter.reader(new CecilCodecIL(method),api.Operation("JsonRawPrimitive","ReadNumericText",target,reader),
            api.Operation("JsonNumericInput","Culture"),Parse(module,target));
        definitions.Add("numeric-compatibility",target,reader,"",method);
        return method;
    }
    internal static void RegisterTree(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,ILProcessor entry,AssemblyDefinition product,TypeReference target,int index)
    {
        target = module.ImportReference(target);
        var api = new GeneratedJsonAssembly.Api(module,product);
        var hasRead = definitions.TryGet("numeric-tree-read",target,api.Json,"",out var read);
        if (!hasRead) {
            read = Body(module,owner,"ReadNumericTree" + index,target,api.Json);
            definitions.Add("numeric-tree-read",target,api.Json,"",read);
            CodecScalarAdapterEmitter.reader(new CecilCodecIL(read),api.Metadata("NumericTreeText",target),api.Operation("JsonNumericInput","Culture"),Parse(module,target));
        }
        var hasWrite = definitions.TryGet("numeric-tree-write",target,api.Json,"",out var write);
        if (!hasWrite) { write = Body(module,owner,"WriteNumericTree" + index,api.Json,target); definitions.Add("numeric-tree-write",target,api.Json,"",write); }
        var decimalType = module.ImportReference(api.Operation("JsonRawPrimitive","ReadDecimal",
            module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.Utf8Reader"))).ReturnType);
        var toDecimal = new MethodReference("op_Explicit",decimalType,decimalType);
        toDecimal.Parameters.Add(new ParameterDefinition(module.TypeSystem.Single));
        var number = new MethodReference("NewNumber",api.Json,api.Json);
        number.Parameters.Add(new ParameterDefinition(decimalType));
        if (!hasWrite) CodecScalarAdapterEmitter.treeWriter(new CecilCodecIL(write),Widen(module,target),toDecimal,number);
        GeneratedJsonAssembly.Delegate(entry,api.Function(target,api.Json),write);
        GeneratedJsonAssembly.Delegate(entry,api.Function(api.Json,target),read);
        entry.Emit(OpCodes.Call,api.ShapeMethod("get_Scalar"));
        entry.Emit(OpCodes.Newobj,api.CodecConstructor(target));
        entry.Emit(OpCodes.Call,api.Metadata("RegisterGenerated",target));
    }
    internal static MethodDefinition Writer(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,AssemblyDefinition product,TypeReference target,TypeReference element,TypeReference output,string name)
    {
        return GeneratedOutputCodec.Number(definitions,module,owner,product,target,element,name);
    }

    internal static void RegisterRaw(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,ILProcessor entry,AssemblyDefinition product,TypeReference target,int index)
    {
        target = module.ImportReference(target);
        var api = new GeneratedJsonAssembly.Api(module,product);
        foreach(var encoding in new[] {"Utf8","Utf16"})
        {
            var reader = module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator." + encoding + "Reader"));
            var method = GeneratedNumericCodec.Reader(definitions,module,owner,product,target,reader,"ReadNumeric" + encoding + index);
            var callable = new GenericInstanceType(module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonDirectReader`2")));
            callable.GenericArguments.Add(target);callable.GenericArguments.Add(reader);
            GeneratedJsonAssembly.Delegate(entry,callable,method);
        }
        foreach(var encoding in new[] {"Utf8","Utf16"})
        {
            var element = encoding == "Utf8" ? module.TypeSystem.Byte : module.TypeSystem.Char;
            var output = module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.Json" + encoding + "Writer"));
            var method = Writer(definitions,module,owner,product,target,element,output,"WriteNumeric" + encoding + index);
            var callable = new GenericInstanceType(module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonDirectWriter`2")));
            callable.GenericArguments.Add(target);callable.GenericArguments.Add(element);
            GeneratedJsonAssembly.Delegate(entry,callable,method);
        }
        entry.Emit(OpCodes.Call,api.Metadata("RegisterRaw",target));
    }
}
