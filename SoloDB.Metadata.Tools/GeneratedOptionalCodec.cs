using System;
using System.Linq;
using Microsoft.FSharp.Core;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

// Resolves original constructors and closed operands; shared FSharp macros author the bodies.
internal static class GeneratedOptionalCodec
{
    private static string Scope(TypeReference type) => type.Scope is AssemblyNameReference assembly ? assembly.FullName
        : type.Scope is ModuleDefinition module ? module.Assembly.Name.FullName : type.Scope.Name;
    private static GeneratedModel? Fact(TypeReference target,GeneratedModel[] models) => models.FirstOrDefault(model => model.Type.FullName == target.FullName && Scope(model.Type) == Scope(target));
    internal static int Kind(TypeReference target,GeneratedModel[] models) => TypeSupport.RawAdaptation(
        target is GenericInstanceType generic ? generic.ElementType.FullName : target.FullName,
        Fact(target,models)?.Facts.Shape == TypeShape.Enum);
    private static TypeReference Payload(ModuleDefinition module,TypeReference target,GeneratedModel[] models) =>
        module.ImportReference(Kind(target,models) == 3 ? Fact(target,models)!.ElementType! : ((GenericInstanceType)target).GenericArguments[0]);
    private static GenericInstanceType Closed(ModuleDefinition module,AssemblyDefinition product,string name,params TypeReference[] arguments)
    {
        var result = new GenericInstanceType(module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator." + name)));
        foreach(var argument in arguments) result.GenericArguments.Add(module.ImportReference(argument));
        return result;
    }
    private static MethodDefinition Body(ModuleDefinition module,TypeDefinition owner,string name,TypeReference result,params TypeReference[] arguments)
    {
        var body = new MethodDefinition(name,MethodAttributes.Assembly | MethodAttributes.Static | MethodAttributes.HideBySig,module.ImportReference(result));
        body.Body.InitLocals = true;
        foreach(var argument in arguments) body.Parameters.Add(new ParameterDefinition(module.ImportReference(argument)));
        owner.Methods.Add(body); return body;
    }
    private static MethodReference Member(ModuleDefinition module,TypeReference target,string name,int kind)
    {
        var original=((GenericInstanceType)target).ElementType;
        var definition=new TypeReference(original.Namespace,original.Name,module,original.Scope,kind==2);
        var parameter=new GenericParameter("T",definition);definition.GenericParameters.Add(parameter);
        var member=new MethodReference(name,name==".ctor"?module.TypeSystem.Void:name=="get_HasValue"?module.TypeSystem.Boolean:parameter,module.ImportReference(target)) {HasThis=true};
        if(name==".ctor")member.Parameters.Add(new ParameterDefinition(parameter));
        return member;
    }
    private static MethodReference Transfer(ModuleDefinition module,AssemblyDefinition product,TypeReference source,TypeReference target)
    {
        var scope = product.MainModule.AssemblyReferences.Single(reference => reference.Name == "System.Runtime.CompilerServices.Unsafe");
        var type = module.ImportReference(new TypeReference("System.Runtime.CompilerServices","Unsafe",product.MainModule,scope));
        var method = new MethodReference("As",module.TypeSystem.Void,type);
        var input = new GenericParameter("TFrom",method); var output = new GenericParameter("TTo",method);
        method.GenericParameters.Add(input); method.GenericParameters.Add(output);
        method.ReturnType = new ByReferenceType(output); method.Parameters.Add(new ParameterDefinition(new ByReferenceType(input)));
        var result = new GenericInstanceMethod(method); result.GenericArguments.Add(module.ImportReference(source)); result.GenericArguments.Add(module.ImportReference(target));
        return result;
    }
    internal static MethodReference ReadCall(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,AssemblyDefinition product,TypeReference target,TypeReference reader,string name,GeneratedModel[] models)
    {
        var api = new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        if(definitions.Stored && TypeSupport.StoredNumeric(target.FullName) != null)
            return api.Operation("JsonStoredNumeric",TypeSupport.StoredNumeric(target.FullName).Value.Item2,reader);
        var primitive = TypeSupport.RawPrimitive(target.FullName);
        var sequence = target is GenericInstanceType closedSequence ? TypeSupport.SequenceDefinition(closedSequence.ElementType.FullName,false) : null;
        MethodReference direct = Kind(target,models) != 0 ? Reader(definitions,module,owner,product,target,reader,name,models)
            : sequence != null ? GeneratedSequenceCodec.Reader(definitions,module,owner,product,target,((GenericInstanceType)target).GenericArguments[0],reader,sequence.Value,false,name,models)
            : TypeSupport.RawNumericCore(target.FullName) != null ? GeneratedNumericCodec.Reader(definitions,module,owner,product,target,reader,name)
            : TypeSupport.FormattedReader(target.FullName).Length!=0 ? GeneratedFormattedCodec.Reader(definitions,module,owner,product,target,reader,name)
            : primitive.Length != 0 ? api.Operation("JsonRawPrimitive","Read" + primitive,reader)
            : target is ArrayType { Rank: 1 } array ? GeneratedRawCodec.ArrayReader(definitions,module,owner,product,array,reader,name,models)
            : api.Operation("JsonRawRead","ReadMember",target,reader);
        return direct;
    }
    internal static MethodDefinition Reader(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,AssemblyDefinition product,TypeReference target,TypeReference reader,string name,GeneratedModel[] models)
    {
        if (definitions.TryGet("optional-read",target,reader,"",out var existing)) return existing;
        var api = new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        var context = Closed(module,product,"JsonReadContext`1",reader);
        var body = Body(module,owner,definitions.MethodName(name),target,new ByReferenceType(context));
        definitions.Add("optional-read",target,reader,"",body);
        var payload = Payload(module,target,models);
        MethodReference Input(string operation) => api.Operation("JsonOptionalInput",operation,reader);
        var names = Kind(target,models) == 3 ? default : GeneratedPropertyCodec.Api(module,product,reader);
        var operands = new CodecOptionalReaderApi<TypeReference,MethodReference>(target,payload,context,module.TypeSystem.Boolean,module.TypeSystem.Int32,
            Input("Begin"),Input("Open"),Input("Capture"),Input("End"),Input("Flat"),Input("Restore"),api.Operation("JsonOptionalInput","Missing"),
            names,api.Operation("JsonRawInput","Skip",reader),
            ReadCall(definitions,module,owner,product,payload,reader,name + "Payload",models),ReadCall(definitions,module,owner,product,module.TypeSystem.Boolean,reader,name + "Flag",models),
            module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonPrimitiveShapeMismatch")),
            module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonScalarShapeMismatch")),
            api.Operation("JsonRawRead","PrimitiveRetry",payload,reader),api.Operation("JsonRawRead","ScalarRetry",payload,reader),
            api.Operation("JsonRawRead","PrimitiveRetry",module.TypeSystem.Boolean,reader),api.Operation("JsonRawRead","ScalarRetry",module.TypeSystem.Boolean,reader),
            Kind(target,models) == 3 ? null! : FSharpOption<MethodReference>.Some(Member(module,target,".ctor",Kind(target,models))),
            Kind(target,models) == 3 ? FSharpOption<MethodReference>.Some(Transfer(module,product,payload,target)) : null!);
        CodecOptionalEmitter.reader(new CecilCodecIL(body),operands); return body;
    }
    internal static MethodReference WriteCall(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,AssemblyDefinition product,TypeReference target,TypeReference encoding,TypeReference output,string name,GeneratedModel[] models)
    {
        if(definitions.Stored && Kind(target,models)==3)
            return WriteCall(definitions.NativeView,module,owner,product,target,encoding,output,name,models);
        var api = new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        MethodReference Member()
        {
            var method = module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonRawWrite`4").Methods.Single(value => value.Name == "Member"));
            method.DeclaringType = api.RawType("JsonRawWrite",target,encoding,output);
            return method;
        }
        var primitive = TypeSupport.RawPrimitive(target.FullName);
        var sequence = target is GenericInstanceType closedSequence ? TypeSupport.SequenceDefinition(closedSequence.ElementType.FullName,true) : null;
        MethodReference direct = Kind(target,models) != 0 ? Writer(definitions,module,owner,product,target,encoding,output,name,models)
            : sequence != null ? GeneratedSequenceCodec.Writer(definitions,module,owner,product,target,((GenericInstanceType)target).GenericArguments[0],encoding,output,target.IsValueType,
                ((GenericInstanceType)target).GenericArguments[0].FullName == "System.Byte",name,models)
            : TypeSupport.NumericWriterCore(target.FullName) != null
            ? GeneratedOutputCodec.Number(definitions,module,owner,product,target,encoding,name)
            : primitive == "String" ? GeneratedOutputCodec.String(definitions,module,owner,product,encoding,name)
            : primitive == "Boolean" ? GeneratedOutputCodec.Boolean(definitions,module,owner,product,encoding,name)
                    : primitive == "Guid" ? GeneratedOutputCodec.Guid(definitions,module,owner,product,target,encoding,name)
                    : primitive == "Bytes" ? GeneratedOutputCodec.Bytes(definitions,module,owner,product,target,encoding,name)
            : primitive.Length != 0 && primitive != "Type" ? api.Operation("JsonRawPrimitive","Write" + primitive,encoding,output)
            : Member();
        return direct;
    }
    internal static MethodDefinition Writer(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,AssemblyDefinition product,TypeReference target,TypeReference encoding,TypeReference output,string name,GeneratedModel[] models)
    {
        var api = new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        var representation = api.RawType("JsonRawOutput",encoding,output);
        if (definitions.TryGet("optional-write",target,representation,"",out var existing)) return existing;
        var span = ((ByReferenceType)product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonDirectWriter`2").Methods.Single(method => method.Name == "Invoke").Parameters[1].ParameterType).ElementType;
        var closedSpan = new GenericInstanceType(module.ImportReference(((GenericInstanceType)span).ElementType)); closedSpan.GenericArguments.Add(encoding);
        var body = Body(module,owner,definitions.MethodName(name),module.TypeSystem.Void,target,new ByReferenceType(closedSpan),new ByReferenceType(Closed(module,product,"JsonWriteState`1",encoding)));
        definitions.Add("optional-write",target,representation,"",body);
        var payload = Payload(module,target,models);
        var nullMethod = module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonRawOutput`3").Methods.Single(method => method.Name == "Null"));
        nullMethod.DeclaringType = api.RawType("JsonRawOutput",encoding,output);
        var operands = new CodecOptionalWriterApi<TypeReference,MethodReference>(target,payload,Kind(target,models) != 1,
            Kind(target,models) == 2 ? FSharpOption<MethodReference>.Some(Member(module,target,"get_HasValue",Kind(target,models))) : null!,
            Kind(target,models) == 3 ? null! : FSharpOption<MethodReference>.Some(Member(module,target,"get_Value",Kind(target,models))),
            Kind(target,models) == 3 ? FSharpOption<MethodReference>.Some(Transfer(module,product,target,payload)) : null!,nullMethod,
            WriteCall(definitions,module,owner,product,payload,encoding,output,name + "Payload",models));
        CodecOptionalEmitter.writer(new CecilCodecIL(body),operands); return body;
    }
    internal static void Register(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,ILProcessor entry,AssemblyDefinition product,TypeReference target,int index,GeneratedModel[] models)
    {
        var api = new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        TypeReference Type(string name) => module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator." + name));
        void Delegate(GenericInstanceType type,MethodDefinition body)
        {
            var constructor = new MethodReference(".ctor",module.TypeSystem.Void,type) { HasThis = true };
            constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object)); constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
            entry.Emit(OpCodes.Ldnull); entry.Emit(OpCodes.Ldftn,body); entry.Emit(OpCodes.Newobj,constructor);
        }
        foreach(var name in new[] { "Utf8Reader","Utf16Reader" })
        {
            var reader = Type(name); Delegate(Closed(module,product,"JsonDirectReader`2",target,reader),Reader(definitions,module,owner,product,target,reader,"ReadOptional" + name + index,models));
        }
        foreach(var name in new[] { "JsonUtf8Writer","JsonUtf16Writer" })
        {
            var encoding = name == "JsonUtf8Writer" ? module.TypeSystem.Byte : module.TypeSystem.Char;
            var output = Type(name); Delegate(Closed(module,product,"JsonDirectWriter`2",target,encoding),Writer(definitions,module,owner,product,target,encoding,output,"WriteOptional" + name + index,models));
        }
        entry.Emit(OpCodes.Call,api.Metadata("RegisterRaw",target));
    }
}
