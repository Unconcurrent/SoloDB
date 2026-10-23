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
            GeneratedChildCodec.ReadCall(definitions,module,owner,product,payload,reader,name + "Payload",models),GeneratedChildCodec.ReadCall(definitions,module,owner,product,module.TypeSystem.Boolean,reader,name + "Flag",models),
            module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonPrimitiveShapeMismatch")),
            module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonScalarShapeMismatch")),
            api.Operation("JsonRawRead","PrimitiveRetry",payload,reader),api.Operation("JsonRawRead","ScalarRetry",payload,reader),
            api.Operation("JsonRawRead","PrimitiveRetry",module.TypeSystem.Boolean,reader),api.Operation("JsonRawRead","ScalarRetry",module.TypeSystem.Boolean,reader),
            Kind(target,models) == 3 ? null! : FSharpOption<MethodReference>.Some(Member(module,target,".ctor",Kind(target,models))),
            Kind(target,models) == 3 ? FSharpOption<MethodReference>.Some(Transfer(module,product,payload,target)) : null!);
        CodecOptionalEmitter.reader(new CecilCodecIL(body),operands); return body;
    }
    internal static MethodDefinition Writer(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,AssemblyDefinition product,TypeReference target,TypeReference encoding,TypeReference output,string name,GeneratedModel[] models)
    {
        var api = new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        var representation = api.RawType("JsonRawOutput",encoding,output);
        if (definitions.TryGet("optional-write",target,representation,"",out var existing)) return existing;
        var span = ((ByReferenceType)product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonDirectWriter`2").Methods.Single(method => method.Name == "Invoke").Parameters[1].ParameterType).ElementType;
        var closedSpan = new GenericInstanceType(module.ImportReference(((GenericInstanceType)span).ElementType)); closedSpan.GenericArguments.Add(encoding);
        var body = Body(module,owner,definitions.MethodName(name),module.TypeSystem.Void,new ByReferenceType(target),new ByReferenceType(closedSpan),new ByReferenceType(Closed(module,product,"JsonWriteState`1",encoding)));
        definitions.Add("optional-write",target,representation,"",body);
        var payload = Payload(module,target,models);
        var il = new CecilCodecIL(body);
        var operands = new CodecOptionalWriterApi<TypeReference,MethodReference>(target,payload,Kind(target,models) != 1,
            Kind(target,models) == 2 ? FSharpOption<MethodReference>.Some(Member(module,target,"get_HasValue",Kind(target,models))) : null!,
            Kind(target,models) == 3 ? null! : FSharpOption<MethodReference>.Some(Member(module,target,"get_Value",Kind(target,models))),
            Kind(target,models) == 3 ? FSharpOption<MethodReference>.Some(Transfer(module,product,target,payload)) : null!,
            FSharpFunc<Unit,Unit>.FromConverter(new Converter<Unit,Unit>(_ => GeneratedOutputCodec.LiteralAction(definitions,module,product,encoding,il).Invoke("null"))),
            GeneratedChildCodec.WriteCall(definitions,module,owner,product,payload,encoding,output,name + "Payload",models));
        CodecOptionalEmitter.writer(il,operands); return body;
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
        entry.Emit(OpCodes.Call,api.Metadata("RegisterRawReaders",target));
        foreach(var outputKind in GeneratedJsonDefinitions.Outputs) {
            var writes = definitions.OutputView(outputKind);
            foreach(var encoding in new[] {module.TypeSystem.Byte,module.TypeSystem.Char}) {
                var output = writes.OutputType(module,product,encoding);
                Delegate(Closed(module,product,"JsonDirectWriter`2",target,encoding),Writer(writes,module,owner,product,target,encoding,output,"WriteOptional" + encoding.Name + index,models));
            }
            GeneratedJsonAssembly.BoxedWriters(definitions,module,owner,entry,product,target);
            entry.Emit(OpCodes.Call,api.Metadata("RegisterOutputWriters",target,writes.OutputType(module,product,module.TypeSystem.Byte),writes.OutputType(module,product,module.TypeSystem.Char)));
        }
        if (definitions.Stored) return;
        var kind = Kind(target,models);
        foreach(var name in TypeSupport.OptionalQueryMembers(kind))
        {
            var valueType = name == "HasValue" ? module.TypeSystem.Boolean : Payload(module,target,models);
            var member = new CodecMember<TypeReference,MethodReference,FieldReference>(name,valueType,null!,
                FSharpOption<MethodReference>.Some(Member(module,target,"get_" + name,kind)),null!,false,true,false,false,false);
            GeneratedJsonAssembly.RegisterMemberReader(definitions,module,owner,entry,product,target,kind == 2,kind == 2,member,"Optional" + index + name);
        }
    }
}
