using System;
using System.Linq;
using Microsoft.FSharp.Core;
using Mono.Cecil;
using SoloDatabase.Metadata.CodecGeneration;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

internal static class GeneratedFormattedCodec
{
    internal static MethodDefinition Reader(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,
        AssemblyDefinition product,TypeReference target,TypeReference reader,string name)
    {
        if(definitions.TryGet("formatted-read",target,reader,"",out var existing))return existing;
        var api=new GeneratedJsonAssembly.Api(module,product);
        var kind=TypeSupport.FormattedReader(target.FullName);
        var utf16=reader.Name=="Utf16Reader";
        var encoding=utf16?"Utf16":"Utf8";
        var context=new GenericInstanceType(module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonReadContext`1")));
        context.GenericArguments.Add(module.ImportReference(reader));
        var contextRef=new ByReferenceType(context);
        MethodDefinition Body(string bodyName,params TypeReference[] arguments)
        {
            var method=new MethodDefinition(bodyName,MethodAttributes.Assembly|MethodAttributes.Static|MethodAttributes.HideBySig,module.ImportReference(target));
            method.Body.InitLocals=true;
            foreach(var argument in arguments)method.Parameters.Add(new ParameterDefinition(module.ImportReference(argument)));
            owner.Methods.Add(method);return method;
        }
        var method=Body(name,contextRef);
        definitions.Add("formatted-read",target,reader,"",method);
        MethodReference Input(string operation)=>api.Operation("JsonNumericInput",operation,reader);
        MethodReference Fallback(string operation)=>api.Operation("JsonFormattedInput",operation+encoding);
        var scope=product.MainModule.AssemblyReferences.Single(reference=>reference.Name=="System.Runtime.CompilerServices.Unsafe");
        var unsafeType=module.ImportReference(new TypeReference("System.Runtime.CompilerServices","Unsafe",product.MainModule,scope));
        var offset=new MethodReference("Add",module.TypeSystem.Void,unsafeType);
        var parameter=new GenericParameter("T",offset);offset.GenericParameters.Add(parameter);
        offset.ReturnType=new ByReferenceType(parameter);
        offset.Parameters.Add(new ParameterDefinition(new ByReferenceType(parameter)));offset.Parameters.Add(new ParameterDefinition(module.TypeSystem.UIntPtr));
        var add=new GenericInstanceMethod(offset);add.GenericArguments.Add(module.TypeSystem.Byte);
        MethodReference decode=Fallback(kind);
        if(kind=="Bytes")
        {
            if(!definitions.TryGet("base64-payload",target,reader,"",out var payload))
            {
                payload=Body(name+"Payload",contextRef,module.TypeSystem.Int32,module.TypeSystem.Int32);
                definitions.Add("base64-payload",target,reader,"",payload);
                var array=new TypeReference("System","Array",module,module.TypeSystem.Object.Scope);
                var empty=new MethodReference("Empty",module.TypeSystem.Void,array);
                var item=new GenericParameter("T",empty);empty.GenericParameters.Add(item);empty.ReturnType=new ArrayType(item);
                var emptyBytes=new GenericInstanceMethod(empty);emptyBytes.GenericArguments.Add(module.TypeSystem.Byte);
                var facts=new CodecBase64ReaderApi<TypeReference,MethodReference>(module.TypeSystem.Byte,module.ImportReference(target),module.TypeSystem.Int32,
                    new ByReferenceType(module.TypeSystem.Byte),Input("Reference"),api.Operation("JsonWriterRegion","BytesReference"),emptyBytes,add,Fallback(kind));
                CodecFormattedReaderEmitter.base64(new CecilCodecIL(payload),facts,utf16);
            }
            decode=payload;
        }
        var read=new MethodReference("ReadUnaligned",module.TypeSystem.Void,unsafeType);
        var wordType=new GenericParameter("T",read);read.GenericParameters.Add(wordType);read.ReturnType=wordType;
        read.Parameters.Add(new ParameterDefinition(new ByReferenceType(module.TypeSystem.Byte)));
        var word=new GenericInstanceMethod(read);word.GenericArguments.Add(module.TypeSystem.UInt64);
        var framing=new CodecStringApi<TypeReference,MethodReference>(module.TypeSystem.Int32,module.TypeSystem.UInt64,
            new ByReferenceType(module.TypeSystem.Byte),new ByReferenceType(module.TypeSystem.Int32),Input("Begin"),Input("Reference"),Input("Length"),Input("Position"),
            FSharpOption<MethodReference>.Some(Input("Complete")),add,word,decode,Fallback(kind+"Escaped"),api.Operation("JsonRawPrimitive","Read"+kind,reader));
        if(kind=="Guid")
        {
            var constructor=new MethodReference(".ctor",module.TypeSystem.Void,module.ImportReference(target)){HasThis=true};
            foreach(var type in new[]{module.TypeSystem.Int32,module.TypeSystem.Int16,module.TypeSystem.Int16}.Concat(Enumerable.Repeat(module.TypeSystem.Byte,8)))
                constructor.Parameters.Add(new ParameterDefinition(type));
            CodecFormattedReaderEmitter.guid(new CecilCodecIL(method),framing,constructor,utf16);
        }
        else CodecStringEmitter.reader(new CecilCodecIL(method),framing,utf16);
        return method;
    }
}
