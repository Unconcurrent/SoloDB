using System;
using System.Linq;
using Microsoft.FSharp.Core;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

// The shared model and tuple emitters target the same raw operations in both frontends.
internal static partial class GeneratedRawCodec
{
    private static FSharpFunc<T,R> Function<T,R>(Func<T,R> value) => FSharpFunc<T,R>.FromConverter(new Converter<T,R>(value));

    internal static MethodDefinition ArrayReader(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,
        AssemblyDefinition product,ArrayType target,TypeReference reader,string name,GeneratedModel[] models)
    {
        if (definitions.TryGet("array-read",target,reader,"",out var existing)) return existing;
        var api = new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        var context = new GenericInstanceType(module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonReadContext`1")));
        context.GenericArguments.Add(reader);
        var method = new MethodDefinition(definitions.MethodName(name),MethodAttributes.Assembly | MethodAttributes.Static | MethodAttributes.HideBySig,module.ImportReference(target));
        method.Body.InitLocals = true;
        method.Parameters.Add(new ParameterDefinition(new ByReferenceType(context)));
        owner.Methods.Add(method);
        definitions.Add("array-read",target,reader,"",method);
        MethodReference Input(string operation,params TypeReference[] arguments) => api.Operation("JsonRawInput",operation,arguments.Concat(new[] {reader}).ToArray());
        var forward = new CodecRawReaderApi<TypeReference,MethodReference>(module.TypeSystem.Boolean,
            Function<TypeReference,FSharpOption<MethodReference>>(element =>
                FSharpOption<MethodReference>.Some(GeneratedOptionalCodec.ReadCall(definitions,module,owner,product,element,reader,name+"Element",models))),
            Input("BeginArray"),Input("NextElement"),
            Function<TypeReference,MethodReference>(element => api.Operation("JsonRawInput","GrowArray",element)),
            Function<TypeReference,MethodReference>(element => api.Operation("JsonRawInput","TrimArray",element)),
            Function<TypeReference,MethodReference>(element => Input("ArrayWrapper",element)),Input("BeginObject"),Input("Start"),null!,Input("Skip"),Input("Complete"),
            Function<TypeReference,MethodReference>(element => Input("SelectType",element)),
            Function<TypeReference,MethodReference>(element => api.Operation("JsonRawRead","ReadMember",element,reader)));
        CodecArrayEmitter.rawArrayReader(new CecilCodecIL(method),GeneratedCodec.Api(api),forward,target,target.ElementType);
        return method;
    }

    internal static void Register(GeneratedJsonDefinitions definitions,ModuleDefinition module, TypeDefinition owner, ILProcessor entry,
        AssemblyDefinition product, GeneratedModel model, int index, GeneratedModel[]? models = null)
    {
        var api = new GeneratedJsonAssembly.Api(module, product,definitions.Stored);
        TypeReference Type(string name) => module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator." + name));
        GenericInstanceType Closed(string name, params TypeReference[] arguments)
        {
            var closed = new GenericInstanceType(Type(name));
            foreach (var argument in arguments) closed.GenericArguments.Add(module.ImportReference(argument));
            return closed;
        }
        MethodReference Method(string type, string name, params TypeReference[] arguments) => api.Operation(type, name, arguments);
        MethodDefinition Body(string name, TypeReference result, params TypeReference[] parameters)
        {
            var method = new MethodDefinition(definitions.MethodName(name),MethodAttributes.Assembly | MethodAttributes.Static | MethodAttributes.HideBySig,module.ImportReference(result));
            method.Body.InitLocals = true;
            foreach (var parameter in parameters) method.Parameters.Add(new ParameterDefinition(module.ImportReference(parameter)));
            owner.Methods.Add(method); return method;
        }
        void Delegate(GenericInstanceType type, MethodDefinition method)
        {
            var constructor = new MethodReference(".ctor",module.TypeSystem.Void,type) { HasThis = true };
            constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));
            constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
            entry.Emit(OpCodes.Ldnull); entry.Emit(OpCodes.Ldftn,method); entry.Emit(OpCodes.Newobj,constructor);
        }
        void Reader(string suffix, TypeReference reader)
        {
            var shape = model.Facts.Shape;
            var collection = TypeSupport.Collection(shape);
            if (shape == TypeShape.BsonDocument)
            {
                var create = Body("RawTreeConstruct" + suffix + index,model.Type,api.Json);
                var il = create.Body.GetILProcessor(); il.Emit(OpCodes.Ldarg_0); il.Emit(OpCodes.Newobj,module.ImportReference(model.Constructor!)); il.Emit(OpCodes.Ret);
                Delegate(api.Function(api.Json,model.Type),create);
                entry.Emit(OpCodes.Call,Method("JsonRawCollections","TreeReader",model.Type,reader));
                return;
            }
            if (TypeSupport.DictionaryShape(shape))
            {
                var body=GeneratedDictionaryCodec.Reader(definitions,module,owner,product,model,reader,"ReadDictionary"+suffix+index,models!);
                Delegate(Closed("JsonDirectReader`2",model.Type,reader),body);
                return;
            }
            if (collection)
            {
                var body=GeneratedSequenceCodec.CustomReader(definitions,module,owner,product,model,reader,"ReadSequence"+suffix+index,models!);
                Delegate(Closed("JsonDirectReader`2",model.Type,reader),body);
                return;
            }
            if (model.Type is ArrayType { Rank: 1 } rootArray)
            {
                var arrayMethod = ArrayReader(definitions,module,owner,product,rootArray,reader,"Read"+suffix+index,models!);
                Delegate(Closed("JsonDirectReader`2",model.Type,reader),arrayMethod);
                entry.Emit(OpCodes.Call,api.Operation("JsonRawInput","Wrap",model.Type,reader));
                return;
            }
            if(shape==TypeShape.Tuple && definitions.TryGet("tuple-read",model.Type,reader,"",out var tupleReader))
            {
                Delegate(Closed("JsonDirectReader`2",model.Type,reader),tupleReader);return;
            }
            var context = Closed("JsonReadContext`1", reader);
            var method = Body("Read" + suffix + index,module.ImportReference(model.Type),new ByReferenceType(context));
            MethodReference Input(string name, params TypeReference[] arguments) => Method("JsonRawInput",name,arguments.Concat(new[] { reader }).ToArray());
            if (model.Facts.Shape == TypeShape.Tuple)
            {
                definitions.Add("tuple-read",model.Type,reader,"",method);
                var tupleInput=new CodecTupleReaderApi<TypeReference,MethodReference>(context,
                    Type("JsonSourceRange"),module.TypeSystem.Boolean,module.TypeSystem.Int32,GeneratedPropertyCodec.Api(module,product,reader),
                    Method("JsonTupleInput","Begin",reader),Method("JsonTupleInput","NextArray",reader),
                    Method("JsonTupleInput","Capture",reader),Method("JsonTupleInput","Complete",reader),
                    Method("JsonReadCursor","child",reader),Method("JsonTupleInput","Missing"),
                    Type("JsonPrimitiveShapeMismatch"),Type("JsonScalarShapeMismatch"),
                    Function<TypeReference,MethodReference>(target=>Method("JsonRawRead","PrimitiveRetry",target,reader)),
                    Function<TypeReference,MethodReference>(target=>Method("JsonRawRead","ScalarRetry",target,reader)));
                CodecTupleEmitter.rawReader(new CecilCodecIL(method),tupleInput,
                    Function<TypeReference,FSharpOption<MethodReference>>(target=>FSharpOption<MethodReference>.Some(
                        GeneratedOptionalCodec.ReadCall(definitions,module,owner,product,target,reader,"ReadTuple"+suffix+index+"Child",models!))),
                    Function<TypeReference,MethodReference>(target=>Method("JsonRawRead","ReadMember",target,reader)),GeneratedCodec.Tuple(model,models!));
            }
            else
            {
                var tree = GeneratedCodec.Api(api);
                var raw = new CodecApi<TypeReference,MethodReference>(context,tree.Int32,tree.Dictionary,context,tree.Null,
                    Input("IsNull"),tree.GetType,tree.TypeFromHandle,tree.SerializeActual,
                    Function<TypeReference,MethodReference>(target => Input("ActualType",target)),Input("ToRuntimeObject"),
                    tree.DictionaryConstructor,tree.DictionaryAdd,tree.NewObject,Input("ObjectProperties"),Input("TryGetValue"),tree.Uninitialized,
                    tree.Serialize,Function<TypeReference,MethodReference>(target => Method("JsonRawRead","Child",target,reader)),
                    tree.ExceptionConstructor,tree.ArgumentExceptionConstructor,tree.InitializationExceptionConstructor,tree.MissingFactoryConstructor,
                    tree.UnionCaseSelected,Input("Contains"),Input("GetProperty"),tree.EnsureStack,true);
                CodecRawReaderApi<TypeReference,MethodReference> forward = null!;
                var serial = 0;
                MethodReference? ReadOperation(TypeReference target)
                {
                    if(definitions.Stored && TypeSupport.StoredNumeric(target.FullName) != null)
                        return Method("JsonStoredNumeric",TypeSupport.StoredNumeric(target.FullName).Value.Item2,reader);
                    if (GeneratedOptionalCodec.Kind(target,models!) == 0 && TypeSupport.RawPrimitive(target.FullName).Length == 0 && target is not ArrayType { Rank: 1 }) return null;
                    return GeneratedOptionalCodec.ReadCall(definitions,module,owner,product,target,reader,"ReadDefault"+suffix+index+"_"+serial++,models!);
                }
                var codecModel = GeneratedCodec.Model(model);
                var property = !CodecRawEmitter.forwardReader(codecModel) ? null
                    : FSharpOption<CodecNameApi<TypeReference,MethodReference>>.Some(GeneratedPropertyCodec.Api(module,product,reader));
                forward = new CodecRawReaderApi<TypeReference,MethodReference>(module.TypeSystem.Boolean,
                    Function<TypeReference,FSharpOption<MethodReference>>(target => ReadOperation(target) is { } operation ? FSharpOption<MethodReference>.Some(operation) : null!),
                    Input("BeginArray"),Input("NextElement"),
                    Function<TypeReference,MethodReference>(target => Method("JsonRawInput","GrowArray",target)),
                    Function<TypeReference,MethodReference>(target => Method("JsonRawInput","TrimArray",target)),
                    Function<TypeReference,MethodReference>(target => Input("ArrayWrapper",target)),
                    Input("BeginObject"),Input("Start"),property,Input("Skip"),Input("Complete"),
                    Function<TypeReference,MethodReference>(target => Input("SelectType",target)),
                    Function<TypeReference,MethodReference>(target => Method("JsonRawRead","ReadMember",target,reader)));
                var il = new CecilCodecIL(method);
                CodecRawEmitter.rawReader(il,raw,forward,codecModel!,Function<Unit,Unit>(_=> {
                    var unionInput=new CodecUnionReaderApi<TypeReference,MethodReference>(context,Type("JsonSourceRange"),
                        module.TypeSystem.Boolean,GeneratedPropertyCodec.Api(module,product,reader),
                        Method("JsonUnionInput","Begin",reader),
                        Method("JsonUnionInput","Capture",reader),Method("JsonTupleInput","Complete",reader),
                        Method("JsonReadCursor","child",reader),Method("JsonTupleInput","Missing"),
                        Type("JsonPrimitiveShapeMismatch"),Type("JsonScalarShapeMismatch"),
                        Function<TypeReference,MethodReference>(target=>Method("JsonRawRead","PrimitiveRetry",target,reader)),
                        Function<TypeReference,MethodReference>(target=>Method("JsonRawRead","ScalarRetry",target,reader)));
                    CodecUnionEmitter.reader(il,raw,unionInput,
                        Function<TypeReference,FSharpOption<MethodReference>>(target=>FSharpOption<MethodReference>.Some(
                            GeneratedOptionalCodec.ReadCall(definitions,module,owner,product,target,reader,"ReadUnion"+suffix+index+"Child",models!))),
                        forward.Read,codecModel!);
                    return null!;
                }));
            }
            Delegate(Closed("JsonDirectReader`2",model.Type,reader),method);
            if (model.Facts.Shape != TypeShape.Tuple) entry.Emit(OpCodes.Call,Input("Wrap",model.Type));
        }
        void Writer(string suffix, TypeReference encoding, TypeReference writer)
        {
            var shape = model.Facts.Shape;
            if (shape == TypeShape.BsonDocument)
            {
                var get = Body("RawTreeValue" + suffix + index,api.Json,model.Type);
                var il = get.Body.GetILProcessor(); il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Call,new MethodReference("get_Json",api.Json,module.ImportReference(model.Type)) { HasThis = true }); il.Emit(OpCodes.Ret);
                Delegate(api.Function(model.Type,api.Json),get);
                entry.Emit(OpCodes.Call,Method("JsonRawCollections","TreeWriter",model.Type,encoding,writer)); return;
            }
            if(TypeSupport.DictionaryShape(shape) && !TypeSupport.RawPolymorphicFactory(shape,model.Facts.AbstractType))
            {
                var body=GeneratedDictionaryCodec.Writer(definitions,module,owner,product,model,encoding,writer,"WriteDictionary"+suffix+index,models!);
                Delegate(Closed("JsonDirectWriter`2",model.Type,encoding),body);
                return;
            }
            var collectionWriter = TypeSupport.RawCollectionWriter(shape,model.Facts.AbstractType,model.Facts.NonGenericEnumerable);
            if (collectionWriter.Length != 0 && collectionWriter != "AbstractWriter")
            {
                if (collectionWriter == "SequenceWriter" || collectionWriter == "NonGenericSequenceWriter")
                {
                    var nonGeneric = collectionWriter == "NonGenericSequenceWriter";
                    var element = nonGeneric ? module.TypeSystem.Object : model.CollectionArguments[0];
                    var body = GeneratedSequenceCodec.Writer(definitions,module,owner,product,model.Type,element,encoding,writer,
                        model.Type.IsValueType,!nonGeneric && element.FullName == "System.Byte","WriteSequence" + suffix + index,models!,nonGeneric);
                    Delegate(Closed("JsonDirectWriter`2",model.Type,encoding),body);
                    return;
                }
                var types = new[] { model.Type }.Concat(model.CollectionArguments).Concat(new[] { encoding,writer }).ToArray();
                entry.Emit(OpCodes.Call,Method("JsonRawCollections",collectionWriter,types)); return;
            }
            var method = GeneratedRawCodec.Writer(definitions,module,owner,product,model,index,models!,encoding,writer,suffix,false);
            Delegate(Closed("JsonDirectWriter`2",model.Type,encoding),method);
        }
        Reader("Utf8",Type("Utf8Reader")); Reader("Utf16",Type("Utf16Reader"));
        Writer("Utf8",module.TypeSystem.Byte,Type("JsonUtf8Writer")); Writer("Utf16",module.TypeSystem.Char,Type("JsonUtf16Writer"));
        entry.Emit(OpCodes.Call,api.Metadata("RegisterRaw",model.Type));
        if (!model.Facts.AbstractType && model.Facts.Shape == TypeShape.Object) {
            var byteType=module.TypeSystem.Byte; var charType=module.TypeSystem.Char;
            var utf8=Type("JsonUtf8Writer"); var utf16=Type("JsonUtf16Writer");
            Delegate(Closed("JsonDirectWriter`2",model.Type,byteType),GeneratedRawCodec.Writer(definitions,module,owner,product,model,index,models!,byteType,utf8,"Utf8",true));
            Delegate(Closed("JsonDirectWriter`2",model.Type,charType),GeneratedRawCodec.Writer(definitions,module,owner,product,model,index,models!,charType,utf16,"Utf16",true));
            entry.Emit(OpCodes.Call,api.Metadata("RegisterWriterBodies",model.Type));
        }
    }
}
