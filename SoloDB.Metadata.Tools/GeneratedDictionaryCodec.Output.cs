using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.FSharp.Core;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

internal static partial class GeneratedDictionaryCodec
{
    private static MethodDefinition KeyFormat(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,
        TypeReference key,string name,GeneratedModel[] models)
    {
        if(definitions.TryGet("dictionary-key-format",key,module.TypeSystem.String,"",out var prepared))return prepared;
        var model=models.FirstOrDefault(m=>GeneratedJsonDefinitions.SameType(m.Type,key));
        var valueType=model?.Facts.ValueType??key.IsValueType;
        var formattable=model?.Facts.Formattable??false;
        var sealedType=model?.Facts.SealedType??false;
        var nullable=key is GenericInstanceType g && g.ElementType.FullName=="System.Nullable`1";
        var method=new MethodDefinition(definitions.MethodName(name),MethodAttributes.Assembly|MethodAttributes.Static|MethodAttributes.HideBySig,module.TypeSystem.String);
        method.Parameters.Add(new ParameterDefinition(module.ImportReference(key)));method.Body.InitLocals=true;owner.Methods.Add(method);
        definitions.Add("dictionary-key-format",key,module.TypeSystem.String,"",method);
        var api=new CodecDictionaryKeyApi<TypeReference,MethodReference>(module.TypeSystem.Object,module.TypeSystem.String,module.ImportReference(typeof(IFormattable)),
            module.ImportReference(typeof(CultureInfo).GetProperty("InvariantCulture")!.GetMethod!),module.ImportReference(typeof(IFormattable).GetMethod("ToString")!),module.ImportReference(typeof(object).GetMethod("ToString")!));
        CodecDictionaryEmitter.formatKey(new CecilCodecIL(method),api,key,valueType,formattable,
            TypeSupport.DictionaryFormatProbe(formattable,sealedType,nullable),key.MetadataType==MetadataType.String);
        return method;
    }
    internal static MethodDefinition Writer(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,
        AssemblyDefinition product,GeneratedModel model,TypeReference encoding,TypeReference output,string name,GeneratedModel[] models)
    {
        if(definitions.TryGet("dictionary-write",model.Type,encoding,output.FullName,out var prepared))return prepared;
        var api=new GeneratedJsonAssembly.Api(module,product,definitions.Stored);var arguments=Arguments(module,model);var key=arguments[0];var value=arguments[1];
        var nonGeneric=model.Facts.Shape==TypeShape.NonGenericDictionary;
        var readOnly=model.Facts.DictionaryReadOnly;
        var probe=TypeSupport.DictionaryReadOnlyProbe(readOnly,model.Facts.SealedType,nonGeneric);
        var state=Closed(module,module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonWriteState`1")),encoding);
        var method=new MethodDefinition(definitions.MethodName(name),MethodAttributes.Assembly|MethodAttributes.Static|MethodAttributes.HideBySig,module.TypeSystem.Void);
        method.Body.InitLocals=true;method.Parameters.Add(new ParameterDefinition(new ByReferenceType(module.ImportReference(model.Type))));
        method.Parameters.Add(new ParameterDefinition(new ByReferenceType(Closed(module,module.ImportReference(typeof(Span<>)),encoding))));
        method.Parameters.Add(new ParameterDefinition(new ByReferenceType(state)));owner.Methods.Add(method);
        definitions.Add("dictionary-write",model.Type,encoding,output.FullName,method);
        CodecDictionaryContract<TypeReference,MethodReference> Contract(Type definition,bool generic)
        {
            var contract=generic?Closed(module,module.ImportReference(definition),key,value):module.ImportReference(definition);
            var iterator=generic?Closed(module,module.ImportReference(typeof(IEnumerator<>)),key):module.ImportReference(typeof(IEnumerator));
            var enumerable=generic?Closed(module,module.ImportReference(typeof(IEnumerable<>)),key):module.ImportReference(typeof(IEnumerable));
            return new(contract,Bind(module,definition.GetProperty("Keys")!.GetMethod!,contract),Bind(module,definition.GetProperty("Item")!.GetMethod!,contract),
                Bind(module,(generic?typeof(IEnumerable<>):typeof(IEnumerable)).GetMethod("GetEnumerator")!,enumerable),iterator,
                module.ImportReference(typeof(IEnumerator).GetMethod("MoveNext")!),
                Bind(module,(generic?typeof(IEnumerator<>):typeof(IEnumerator)).GetProperty("Current")!.GetMethod!,iterator),
                module.ImportReference(typeof(IDisposable).GetMethod("Dispose")!),generic?null!:Some(module.ImportReference(typeof(IDisposable))));
        }
        MethodReference Output(string operation,params TypeReference[] types)
        {
            var original=product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonRawOutput`3").Methods.Single(m=>m.Name==operation);
            var bound=module.ImportReference(original);bound.DeclaringType=api.RawType("JsonRawOutput",encoding,output);
            if(types.Length==0)return bound;
            var generic=new GenericInstanceMethod(bound);foreach(var type in types)generic.GenericArguments.Add(module.ImportReference(type));return generic;
        }
        var operands=new CodecDictionaryWriterApi<TypeReference,MethodReference>(module.TypeSystem.Object,module.TypeSystem.String,module.TypeSystem.Boolean,key,value,module.ImportReference(typeof(HashSet<string>)),
            readOnly||probe?Some(Contract(typeof(IReadOnlyDictionary<,>),true)):null!,
            nonGeneric?Some(Contract(typeof(IDictionary),false)):!readOnly?Some(Contract(typeof(IDictionary<,>),true)):null!,probe,
            nonGeneric?module.ImportReference(typeof(object).GetMethod("ToString")!):KeyFormat(definitions,module,owner,key,name+"Key",models),nonGeneric,
            module.ImportReference(typeof(HashSet<string>).GetConstructor(Type.EmptyTypes)!),module.ImportReference(typeof(HashSet<string>).GetMethod("Add")!),
            module.ImportReference(typeof(string).GetMethod("op_Equality")!),module.ImportReference(typeof(string).GetMethod("Concat",new[]{typeof(string),typeof(string)})!),
            module.ImportReference(typeof(ArgumentException).GetConstructor(new[]{typeof(string)})!),Output("Property"),Output("TypeMember",model.Type,value),Output("EndObject",model.Type));
        var il = new CecilCodecIL(method);
        var forward=new CodecRawWriterApi<TypeReference,MethodReference>(module.TypeSystem.Boolean,
            Function<TypeReference,FSharpOption<MethodReference>>(child=>Some(GeneratedChildCodec.WriteCall(definitions,module,owner,product,child,encoding,output,name+"Value",models))),
            null!,GeneratedOutputCodec.LiteralAction(definitions,module,product,encoding,il),Output("SerializeActual"),null!,null!,
            Output("OmitId"),Function<TypeReference,MethodReference>(child=>
            {
                var bound=module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonRawWrite`4").Methods.Single(m=>m.Name=="Discard"));
                bound.DeclaringType=api.RawType("JsonRawWrite",child,encoding,output);return bound;
            }),Function<TypeReference,MethodReference>(child=>GeneratedChildCodec.WriteCall(definitions,module,owner,product,child,encoding,output,name+"Child",models)));
        CodecDictionaryEmitter.writer(il,forward,operands,model.Type,model.Facts.ValueType,nonGeneric);
        return method;
    }
}
