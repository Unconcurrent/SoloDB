using System;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

// The second fixed wire contract reuses unchanged definitions from the same helper scope.
internal static class GeneratedStoredCodec
{
    private static void Numeric(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,
        ILProcessor entry,AssemblyDefinition product,TypeReference target,int index)
    {
        var api=new GeneratedJsonAssembly.Api(module,product,true);
        TypeReference Type(string name)=>module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator."+name));
        GenericInstanceType Callable(string name,TypeReference representation) {
            var closed=new GenericInstanceType(Type(name));closed.GenericArguments.Add(module.ImportReference(target));
            closed.GenericArguments.Add(module.ImportReference(representation));return closed;
        }
        foreach(var name in new[]{"Utf8Reader","Utf16Reader"}) {
            var reader=Type(name);
            GeneratedJsonAssembly.Delegate(entry,Callable("JsonDirectReader`2",reader),
                api.Operation("JsonStoredNumeric",TypeSupport.StoredNumeric(target.FullName).Value.Item2,reader));
        }
        foreach(var element in new[]{module.TypeSystem.Byte,module.TypeSystem.Char})
            GeneratedJsonAssembly.Delegate(entry,Callable("JsonDirectWriter`2",element),
                GeneratedOutputCodec.StoredNumber(definitions,module,owner,product,target,element,"WriteStoredNumber"+element.Name+index));
        entry.Emit(OpCodes.Call,api.Metadata("RegisterRaw",target));
    }

    internal static void Register(GeneratedJsonDefinitions native,ModuleDefinition module,TypeDefinition owner,
        ILProcessor entry,AssemblyDefinition product,GeneratedModel[] models,int startIndex)
    {
        var definitions=native.StoredView();
        var api=new GeneratedJsonAssembly.Api(module,product,true);
        var reader=module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.Utf8Reader"));
        foreach(var pair in new[]{("ReadDecimal",0),("ReadBigInteger",1)})
            Numeric(definitions,module,owner,entry,product,api.Operation("JsonStoredNumeric",pair.Item1,reader).ReturnType,startIndex+pair.Item2);
        for(var ordinal=0;ordinal<models.Length;ordinal++) {
            var model=models[ordinal];var target=module.ImportReference(model.Type);var shape=model.Facts.Shape;
            var index=startIndex+2+ordinal;
            if(TypeSupport.StoredNumeric(target.FullName) != null || target.FullName=="System.Object")continue;
            if(TypeSupport.StoredNumericReader(target.FullName)!=null) {
                GeneratedNumericCodec.Register(definitions,module,owner,entry,product,target,index);continue;
            }
            if(shape==TypeShape.Enum) {
                GeneratedOptionalCodec.Register(definitions,module,owner,entry,product,target,index,models);continue;
            }
            if(TypeSupport.RawPrimitive(target.FullName).Length!=0 || target.FullName=="SoloDatabase.JsonSerializator.JsonValue") {
                entry.Emit(OpCodes.Call,api.Metadata("RegisterStoredScalar",target));continue;
            }
            var adaptation=GeneratedOptionalCodec.Kind(target,models);
            if(adaptation!=0) {
                GeneratedOptionalCodec.Register(definitions,module,owner,entry,product,target,index,models);continue;
            }
            if(shape==TypeShape.DocumentReference || shape==TypeShape.TypedDocumentReference) {
                var arguments=((GenericInstanceType)target).GenericArguments.ToArray();
                entry.Emit(OpCodes.Call,api.Metadata(shape==TypeShape.DocumentReference?"RegisterStoredDocument":"RegisterStoredTypedDocument",arguments));continue;
            }
            if(shape==TypeShape.FileReference || shape==TypeShape.DirectoryReference) {
                entry.Emit(OpCodes.Call,api.Metadata(shape==TypeShape.FileReference?"RegisterStoredFileReference":"RegisterStoredDirectoryReference"));continue;
            }
            if(TypeSupport.ReferenceSequence(shape)!=null) {
                GeneratedSequenceCodec.RegisterReferenceReaders(definitions,module,owner,entry,product,model,index,models);
                GeneratedRawCodec.RegisterFactoryWriters(definitions,module,owner,entry,product,model,index,models);continue;
            }
            if(shape==TypeShape.Grouping || shape==TypeShape.GroupingImplementation) {
                GeneratedSequenceCodec.RegisterGroupingReaders(definitions,module,owner,entry,product,model,index,models);
                GeneratedRawCodec.RegisterFactoryWriters(definitions,module,owner,entry,product,model,index,models);continue;
            }
            if(TypeSupport.ForDefinition(shape,target.FullName).RawKind==RawCodecKind.Sequence && target is GenericInstanceType) {
                var abstractWriter=TypeSupport.RawPolymorphicFactory(shape,model.Facts.AbstractType);
                GeneratedSequenceCodec.Register(definitions,module,owner,entry,product,model,index,models,abstractWriter);
                if(abstractWriter)GeneratedRawCodec.RegisterFactoryWriters(definitions,module,owner,entry,product,model,index,models);
                continue;
            }
            GeneratedRawCodec.Register(definitions,module,owner,entry,product,model,index,models);
            if(target is ArrayType {Rank:1} array)
                GeneratedSequenceCodec.RegisterRangeArrayBodies(definitions,module,owner,entry,product,array.ElementType,
                    model.Facts.SupportsValuesWrapper,"ReadStoredArrayWrapper"+index,models);
        }
        if(models.Any(model=>model.Facts.Shape==TypeShape.DynamicObject || model.Facts.NonGenericEnumerable || model.Facts.Shape==TypeShape.NonGenericDictionary))
            entry.Emit(OpCodes.Call,api.Metadata("RegisterStoredDynamic"));
    }
}
