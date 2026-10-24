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
        if (!exact && model.Facts.ValueType && model.Facts.Shape == TypeShape.Object)
            return Writer(definitions,module,owner,product,model,index,models,encoding,writer,suffix,true);
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
            var method = Body((exact ? "WriteMembers" : "Write") + suffix + index,module.TypeSystem.Void,new ByReferenceType(module.ImportReference(model.Type)),new ByReferenceType(closedSpan),new ByReferenceType(Closed("JsonWriteState`1",encoding)));
            definitions.Add(role,model.Type,representation,"",method);
            var il = new CecilCodecIL(method, assembly => model.AccessAssemblies.Add(assembly));
            var literal = GeneratedOutputCodec.LiteralAction(definitions,module,product,encoding,il);
            if (TypeSupport.RawPolymorphicFactory(model.Facts.Shape,model.Facts.AbstractType)) {
                CodecPolymorphicEmitter.abstractWriter(il,GeneratedCodec.Api(api),
                    Polymorphic(definitions,module,product,encoding,writer),model.Type,
                    Function<Unit,Unit>(_ => literal.Invoke("null")));
                return method;
            }
            if (model.Facts.Shape != TypeShape.Object && model.Facts.Shape != TypeShape.Tuple) {
                var direct=module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonRawWrite`4").Methods.Single(operation=>operation.Name=="Write"));
                direct.DeclaringType=api.RawType("JsonRawWrite",model.Type,encoding,writer);
                CodecEmitter.directCall(new CecilCodecIL(method),direct,3);
                return method;
            }
            CodecRawWriterApi<TypeReference,MethodReference> raw = null!;
            var serial = 0;
            raw = new CodecRawWriterApi<TypeReference,MethodReference>(module.TypeSystem.Boolean,
                Function<TypeReference,FSharpOption<MethodReference>>(target => GeneratedChildCodec.WriteCall(definitions,module,owner,product,target,encoding,writer,"WriteChild"+suffix+index+"_"+serial++,models!,false) is { } operation ? FSharpOption<MethodReference>.Some(operation) : null!),
                Polymorphic(definitions,module,product,encoding,writer),
                literal,Output("SerializeActual",encoding,writer),
                GeneratedOutputCodec.PropertyAction(definitions,module,product,encoding,il),
                Function<TypeReference,MethodReference>(target => GeneratedOutputCodec.TypeLiteral(definitions,module,owner,product,target,encoding,"WriteTypeLiteral"+suffix+index+"_"+serial++)),
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
                CodecTupleEmitter.rawWriter(il,raw,scope,GeneratedCodec.Tuple(model,models!));
            }
            else if (exact) CodecRawEmitter.rawBody(il,GeneratedCodec.Api(api),raw,GeneratedCodec.Model(model));
            else CodecRawEmitter.rawWriter(il,GeneratedCodec.Api(api),raw,GeneratedCodec.Model(model),model.Facts.AbstractType ? null! : GeneratedRawCodec.Writer(definitions,module,owner,product,model,index,models,encoding,writer,suffix,true));
        return method;
    }

    internal static void RegisterFactoryWriters(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,
        ILProcessor entry,AssemblyDefinition product,GeneratedModel model,int index,GeneratedModel[] models)
    {
        var api=new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        foreach(var kind in GeneratedJsonDefinitions.Outputs) {
            var writes = definitions.OutputView(kind);
            foreach(var encoding in new[] {module.TypeSystem.Byte,module.TypeSystem.Char}) {
                var output = writes.OutputType(module,product,encoding);
                var body = GeneratedRawCodec.Writer(writes,module,owner,product,model,index,models,encoding,output,encoding.FullName == "System.Char" ? "Utf16" : "Utf8",false);
                var callable = new GenericInstanceType(module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonDirectWriter`2")));
                callable.GenericArguments.Add(module.ImportReference(model.Type));callable.GenericArguments.Add(module.ImportReference(encoding));
                GeneratedJsonAssembly.Delegate(entry,callable,body);
            }
            GeneratedJsonAssembly.BoxedWriters(definitions,module,owner,entry,product,model.Type);
            entry.Emit(OpCodes.Call,api.Metadata("RegisterOutputWriters",model.Type,writes.OutputType(module,product,module.TypeSystem.Byte),writes.OutputType(module,product,module.TypeSystem.Char)));
        }
    }

    private static CodecPolymorphicApi<TypeReference,MethodReference> Polymorphic(GeneratedJsonDefinitions definitions,ModuleDefinition module,
        AssemblyDefinition product,TypeReference encoding,TypeReference output)
    {
        var api=new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        var raw=api.RawType("JsonRawOutput",encoding,output);
        MethodReference Method(string name) {var result=module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonRawOutput`3").Methods.Single(method=>method.Name==name));result.DeclaringType=raw;return result;}
        var runtimeType=module.ImportReference(GeneratedCodec.Api(api).TypeFromHandle.ReturnType);
        var equals=new MethodReference("Equals",module.TypeSystem.Boolean,runtimeType){HasThis=true};equals.Parameters.Add(new ParameterDefinition(runtimeType));
        return new CodecPolymorphicApi<TypeReference,MethodReference>(runtimeType,equals,Method("IncludeType"),Method("SetIncludeType"),Method("Actual"));
    }
}
