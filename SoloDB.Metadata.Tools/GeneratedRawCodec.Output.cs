using System;
using System.Linq;
using Microsoft.FSharp.Core;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

internal static partial class GeneratedRawCodec
{
    internal static MethodDefinition Writer(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,AssemblyDefinition product,
        GeneratedModel model,int index,GeneratedModel[] models,TypeReference encoding,TypeReference writer,string suffix,bool exact)
    {
        if(model.Type is ArrayType { Rank: 1 } rootArray)
            return GeneratedChildCodec.ArrayWriter(definitions,module,owner,product,rootArray,encoding,writer,"WriteArray"+suffix+index,models);
        var api = new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        var sequenceWriter = TypeSupport.RawCollectionWriter(model.Facts.Shape,model.Facts.AbstractType,model.Facts.NonGenericEnumerable);
        if (sequenceWriter == "SequenceWriter" ||
            (sequenceWriter == "NonGenericSequenceWriter" && !TypeSupport.RawPolymorphicFactory(model.Facts.Shape,model.Facts.AbstractType)))
        {
            var nonGeneric = sequenceWriter == "NonGenericSequenceWriter";
            var element = nonGeneric ? module.TypeSystem.Object : model.CollectionArguments[0];
            return GeneratedSequenceCodec.Writer(definitions,module,owner,product,model.Type,element,encoding,writer,
                model.Type.IsValueType,!nonGeneric && element.FullName == "System.Byte","WriteSequence" + suffix + index,models,nonGeneric);
        }
        MethodDefinition Body(string name,TypeReference result,params TypeReference[] parameters) {
            var method=new MethodDefinition(definitions.MethodName(name),MethodAttributes.Assembly|MethodAttributes.Static|MethodAttributes.HideBySig,module.ImportReference(result));
            method.Body.InitLocals=true; foreach(var parameter in parameters)method.Parameters.Add(new ParameterDefinition(module.ImportReference(parameter)));
            owner.Methods.Add(method);return method;
        }
        GenericInstanceType Closed(string name,params TypeReference[] arguments) {
            var result=new GenericInstanceType(module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator."+name)));
            foreach(var argument in arguments)result.GenericArguments.Add(module.ImportReference(argument));return result;
        }
        MethodReference Output(string name,TypeReference element,TypeReference output,params TypeReference[] arguments) {
            var result=module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonRawOutput`3").Methods.Single(method=>method.Name==name));
            result.DeclaringType=api.RawType("JsonRawOutput",element,output);
            if(arguments.Length==0)return result;var closed=new GenericInstanceMethod(result);
            foreach(var argument in arguments)closed.GenericArguments.Add(module.ImportReference(argument));return closed;
        }
        MethodReference Serialize(TypeReference target,TypeReference element,TypeReference output) {
            var result=module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonRawWrite`4").Methods.Single(method=>method.Name=="Member"));
            result.DeclaringType=api.RawType("JsonRawWrite",target,element,output);return result;
        }
        MethodReference Discard(TypeReference target,TypeReference element,TypeReference output) {
            var result=module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonRawWrite`4").Methods.Single(method=>method.Name=="Discard"));
            result.DeclaringType=api.RawType("JsonRawWrite",target,element,output);return result;
        }
            // Import the span identity from the product delegate, including its framework scope.
            var span = ((ByReferenceType)product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonDirectWriter`2").Methods.Single(value => value.Name == "Invoke").Parameters[1].ParameterType).ElementType;
            var spanDefinition = ((GenericInstanceType)span).ElementType;
            var closedSpan = new GenericInstanceType(module.ImportReference(spanDefinition)); closedSpan.GenericArguments.Add(encoding);
            var representation = api.RawType("JsonRawOutput",encoding,writer);
            var role = model.Facts.Shape==TypeShape.Tuple ? "tuple-write" : exact ? "model-exact-write" : "model-write";
            if (definitions.TryGet(role,model.Type,representation,"",out var prepared)) return prepared;
            var method = Body((exact ? "WriteMembers" : "Write") + suffix + index,module.TypeSystem.Void,module.ImportReference(model.Type),new ByReferenceType(closedSpan),new ByReferenceType(Closed("JsonWriteState`1",encoding)));
            definitions.Add(role,model.Type,representation,"",method);
            if (TypeSupport.RawPolymorphicFactory(model.Facts.Shape,model.Facts.AbstractType)) {
                var nullWriter=GeneratedOutputCodec.Constant(definitions,module,owner,product,encoding,"null","WriteNull"+suffix+index);
                CodecPolymorphicEmitter.abstractWriter(new CecilCodecIL(method),GeneratedCodec.Api(api),
                    Polymorphic(definitions,module,owner,product,model,models,encoding,writer,suffix),model.Type,nullWriter);
                return method;
            }
            if (model.Facts.Shape != TypeShape.Object && model.Facts.Shape != TypeShape.Tuple) {
                var il = method.Body.GetILProcessor();
                il.Emit(OpCodes.Ldarg_0); il.Emit(OpCodes.Ldarg_1); il.Emit(OpCodes.Ldarg_2);
                var direct=module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonRawWrite`4").Methods.Single(operation=>operation.Name=="Write"));
                direct.DeclaringType=api.RawType("JsonRawWrite",model.Type,encoding,writer);
                il.Emit(OpCodes.Call,direct); il.Emit(OpCodes.Ret);
                return method;
            }
            CodecRawWriterApi<TypeReference,MethodReference> raw = null!;
            var serial = 0;
            raw = new CodecRawWriterApi<TypeReference,MethodReference>(module.TypeSystem.Boolean,
                Function<TypeReference,FSharpOption<MethodReference>>(target => GeneratedChildCodec.WriteCall(definitions,module,owner,product,target,encoding,writer,"WriteChild"+suffix+index+"_"+serial++,models!,false) is { } operation ? FSharpOption<MethodReference>.Some(operation) : null!),
                Polymorphic(definitions,module,owner,product,model,models,encoding,writer,suffix),
                GeneratedOutputCodec.Constant(definitions,module,owner,product,encoding,"null","WriteNull"+suffix+index+"_"+serial++),Output("SerializeActual",encoding,writer),
                GeneratedOutputCodec.Constant(definitions,module,owner,product,encoding,"[","WriteBeginArray"+suffix+index+"_"+serial++),GeneratedOutputCodec.Constant(definitions,module,owner,product,encoding,",","WriteSeparator"+suffix+index+"_"+serial++),GeneratedOutputCodec.Constant(definitions,module,owner,product,encoding,"]","WriteEndArray"+suffix+index+"_"+serial++),
                GeneratedOutputCodec.Constant(definitions,module,owner,product,encoding,"{","WriteBeginObject"+suffix+index+"_"+serial++),Function<string,MethodReference>(text => GeneratedOutputCodec.Property(definitions,module,owner,product,encoding,text,"WriteLiteral" + suffix + index + "_" + serial++)),Function<TypeReference,MethodReference>(target => GeneratedOutputCodec.TypeLiteral(definitions,module,owner,product,target,encoding,"WriteTypeLiteral"+suffix+index+"_"+serial++)),
                GeneratedOutputCodec.Constant(definitions,module,owner,product,encoding,"}","WriteEndObject"+suffix+index+"_"+serial++),
                Output("OmitId",encoding,writer),Function<TypeReference,MethodReference>(target => Discard(target,encoding,writer)),
                Function<TypeReference,MethodReference>(target => Serialize(target,encoding,writer)));
            if (model.Facts.Shape == TypeShape.Tuple)
            {
                FieldReference StateField(string name)
                {
                    var field=module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonWriteState`1").Fields.Single(field=>field.Name==name));
                    field.DeclaringType=Closed("JsonWriteState`1",encoding);return field;
                }
                var scope=new CodecTupleWriterScope<TypeReference,FieldReference>(module.TypeSystem.Boolean,StateField("IncludeType"),StateField("OmitId"),
                    Function<TypeReference,bool>(target=>TypeSupport.TupleMemberScope(TypeSupport.RawPrimitive(target.FullName),GeneratedOptionalCodec.Kind(target,models))));
                CodecTupleEmitter.rawWriter(new CecilCodecIL(method),raw,scope,GeneratedCodec.Tuple(model,models!));
            }
            else if (exact) CodecRawEmitter.rawBody(new CecilCodecIL(method),GeneratedCodec.Api(api),raw,GeneratedCodec.Model(model));
            else CodecRawEmitter.rawWriter(new CecilCodecIL(method),GeneratedCodec.Api(api),raw,GeneratedCodec.Model(model),model.Facts.AbstractType ? null! : GeneratedRawCodec.Writer(definitions,module,owner,product,model,index,models,encoding,writer,suffix,true));
        return method;
    }

    internal static void RegisterFactoryWriters(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,
        ILProcessor entry,AssemblyDefinition product,GeneratedModel model,int index,GeneratedModel[] models)
    {
        var api=new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        void Writer(TypeReference encoding,string output,string suffix) {
            var writer=module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator."+output));
            var body=GeneratedRawCodec.Writer(definitions,module,owner,product,model,index,models,encoding,writer,suffix,false);
            var callable=new GenericInstanceType(module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonDirectWriter`2")));
            callable.GenericArguments.Add(module.ImportReference(model.Type));callable.GenericArguments.Add(module.ImportReference(encoding));
            var constructor=new MethodReference(".ctor",module.TypeSystem.Void,callable){HasThis=true};
            constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
            entry.Emit(OpCodes.Ldnull);entry.Emit(OpCodes.Ldftn,body);entry.Emit(OpCodes.Newobj,constructor);
        }
        Writer(module.TypeSystem.Byte,"JsonUtf8Writer","Utf8");
        Writer(module.TypeSystem.Char,"JsonUtf16Writer","Utf16");
        entry.Emit(OpCodes.Call,api.Metadata("RegisterRawWriters",model.Type));
    }

    private static CodecPolymorphicApi<TypeReference,MethodReference> Polymorphic(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,
        AssemblyDefinition product,GeneratedModel model,GeneratedModel[] models,TypeReference encoding,TypeReference output,string suffix)
    {
        var api=new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        var raw=api.RawType("JsonRawOutput",encoding,output);
        MethodReference Method(string name) {var result=module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonRawOutput`3").Methods.Single(method=>method.Name==name));result.DeclaringType=raw;return result;}
        var runtimeType=module.ImportReference(GeneratedCodec.Api(api).TypeFromHandle.ReturnType);
        var equals=new MethodReference("Equals",module.TypeSystem.Boolean,runtimeType){HasThis=true};equals.Parameters.Add(new ParameterDefinition(runtimeType));
        return new CodecPolymorphicApi<TypeReference,MethodReference>(runtimeType,equals,Method("IncludeType"),Method("SetIncludeType"),Method("Actual"),
            Function<TypeReference,CodecKnownWriter<TypeReference,MethodReference>[]>(declared=>models.Select((candidate,index)=>(candidate,index))
                .Where(item=>!item.candidate.Facts.AbstractType && !GeneratedJsonDefinitions.SameType(item.candidate.Type,declared) && item.candidate.AssignableTo.Any(target=>GeneratedJsonDefinitions.SameType(target,declared)))
                .Select(item=> {
                    var candidate=item.candidate;
                    var primitive=TypeSupport.RawPrimitive(candidate.Type.FullName);
                    var candidateDefinitions=definitions.Stored && TypeSupport.StoredNumeric(candidate.Type.FullName) != null
                        ? definitions.NativeView : definitions;
                    var body=GeneratedOptionalCodec.Kind(candidate.Type,models)!=0 || (primitive.Length!=0 && primitive!="Type")
                        ? GeneratedChildCodec.WriteCall(candidateDefinitions,module,owner,product,candidate.Type,encoding,output,"WriteKnown"+suffix+item.index,models)
                        : Writer(definitions,module,owner,product,candidate,item.index,models,encoding,output,suffix,true);
                    return new CodecKnownWriter<TypeReference,MethodReference>(candidate.Type,candidate.Facts.ValueType,body);
                }).ToArray()));
    }

}
