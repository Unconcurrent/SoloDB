using System;
using System.Linq;
using Microsoft.FSharp.Core;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

// Maps the shared child-operation decision to existing closed generated bodies.
internal static class GeneratedChildCodec
{
    private static string Definition(TypeReference target) => target is GenericInstanceType generic ? generic.ElementType.FullName : target.FullName;

    internal static MethodReference ReadCall(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,
        AssemblyDefinition product,TypeReference target,TypeReference reader,string name,GeneratedModel[] models,bool required = true)
    {
        var api = new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        var selected = TypeSupport.RawChildReader(Definition(target),GeneratedOptionalCodec.Kind(target,models) == 3,
            target is ArrayType array ? array.Rank : 0,definitions.Stored);
        if(selected.IsMember) return required ? api.Operation("JsonRawRead","ReadMember",target,reader) : null!;
        if(selected.IsStoredNumeric) return api.Operation("JsonStoredNumeric",TypeSupport.StoredNumeric(target.FullName).Value.Item2,reader);
        if(selected is RawChildOperation.Adapted) return GeneratedOptionalCodec.Reader(definitions,module,owner,product,target,reader,name,models);
        if(selected is RawChildOperation.Sequence sequence)
            return GeneratedSequenceCodec.Reader(definitions,module,owner,product,target,((GenericInstanceType)target).GenericArguments[0],reader,sequence.Item,false,name,models);
        if(selected.IsNumeric) return GeneratedNumericCodec.Reader(definitions,module,owner,product,target,reader,name);
        if(selected.IsFormatted) return GeneratedFormattedCodec.Reader(definitions,module,owner,product,target,reader,name);
        if(selected is RawChildOperation.Primitive primitive) return api.Operation("JsonRawPrimitive","Read" + primitive.Item,reader);
        if(selected.IsArray) return GeneratedRawCodec.ArrayReader(definitions,module,owner,product,(ArrayType)target,reader,name,models);
        throw new InvalidOperationException("A writer operation cannot be bound as a reader.");
    }

    internal static MethodReference WriteCall(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,
        AssemblyDefinition product,TypeReference target,TypeReference encoding,TypeReference output,string name,GeneratedModel[] models,bool required = true)
    {
        var api = new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        var selected = TypeSupport.RawChildWriter(Definition(target),GeneratedOptionalCodec.Kind(target,models) == 3,
            target is ArrayType array ? array.Rank : 0,definitions.Stored);
        if(selected.IsMember)
        {
            if(!required) return null!;
            return Member(api,module,product,target,encoding,output);
        }
        if(selected.IsStoredNumeric || selected.IsNumeric) return GeneratedOutputCodec.Number(definitions,module,owner,product,target,encoding,name);
        if(selected is RawChildOperation.Adapted adapted)
            return GeneratedOptionalCodec.Writer(adapted.Item ? definitions.NativeView : definitions,module,owner,product,target,encoding,output,name,models);
        if(selected is RawChildOperation.Sequence)
            return GeneratedSequenceCodec.Writer(definitions,module,owner,product,target,((GenericInstanceType)target).GenericArguments[0],encoding,output,target.IsValueType,
                ((GenericInstanceType)target).GenericArguments[0].FullName == "System.Byte",name,models);
        if(selected.IsText) return GeneratedOutputCodec.String(definitions,module,owner,product,encoding,name);
        if(selected.IsBoolean) return GeneratedOutputCodec.Boolean(definitions,module,owner,product,encoding,name);
        if(selected.IsGuid) return GeneratedOutputCodec.Guid(definitions,module,owner,product,target,encoding,name);
        if(selected.IsBytes) return GeneratedOutputCodec.Bytes(definitions,module,owner,product,target,encoding,name);
        if(selected is RawChildOperation.Primitive primitive) return api.Operation("JsonRawPrimitive","Write" + primitive.Item,encoding,output);
        if(selected.IsArray) return ArrayWriter(definitions,module,owner,product,(ArrayType)target,encoding,output,name,models);
        throw new InvalidOperationException("A reader operation cannot be bound as a writer.");
    }

    private static MethodReference Member(GeneratedJsonAssembly.Api api,ModuleDefinition module,AssemblyDefinition product,
        TypeReference target,TypeReference encoding,TypeReference output)
    {
        var method = module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonRawWrite`4").Methods.Single(value => value.Name == "Member"));
        method.DeclaringType = api.RawType("JsonRawWrite",target,encoding,output);
        return method;
    }

    internal static MethodDefinition ArrayWriter(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,
        AssemblyDefinition product,ArrayType target,TypeReference encoding,TypeReference output,string name,GeneratedModel[] models)
    {
        var api = new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        var representation = api.RawType("JsonRawOutput",encoding,output);
        if(definitions.TryGet("array-write",target,representation,"",out var existing)) return existing;
        var originalSpan = (GenericInstanceType)((ByReferenceType)product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonDirectWriter`2")
            .Methods.Single(value => value.Name == "Invoke").Parameters[1].ParameterType).ElementType;
        var span = new GenericInstanceType(module.ImportReference(originalSpan.ElementType)); span.GenericArguments.Add(encoding);
        var method = new MethodDefinition(definitions.MethodName(name),MethodAttributes.Assembly | MethodAttributes.Static | MethodAttributes.HideBySig,module.TypeSystem.Void);
        method.Body.InitLocals = true;
        method.Parameters.Add(new ParameterDefinition(module.ImportReference(target)));
        method.Parameters.Add(new ParameterDefinition(new ByReferenceType(span)));
        var state = new GenericInstanceType(module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonWriteState`1"))); state.GenericArguments.Add(encoding);
        method.Parameters.Add(new ParameterDefinition(new ByReferenceType(state)));
        owner.Methods.Add(method);
        definitions.Add("array-write",target,representation,"",method);
        var serial = 0;
        FSharpFunc<TypeReference,FSharpOption<MethodReference>> operation = FSharpFunc<TypeReference,FSharpOption<MethodReference>>.FromConverter(
            new Converter<TypeReference,FSharpOption<MethodReference>>(child => WriteCall(definitions,module,owner,product,child,encoding,output,name + "Child" + serial++,models,false) is { } call
                ? FSharpOption<MethodReference>.Some(call) : null!));
        var serialize = FSharpFunc<TypeReference,MethodReference>.FromConverter(
            new Converter<TypeReference,MethodReference>(child => Member(api,module,product,child,encoding,output)));
        MethodReference Literal(string text,string suffix) => GeneratedOutputCodec.Constant(definitions,module,owner,product,encoding,text,name + suffix);
        var operands = new CodecArrayWriterApi<TypeReference,MethodReference>(operation,serialize,Literal("null","Null"),
            Literal("[","Begin"),Literal(",","Separator"),Literal("]","End"));
        CodecArrayEmitter.rawArrayWriter(new CecilCodecIL(method),module.TypeSystem.Int32,operands,target.ElementType);
        return method;
    }
}
