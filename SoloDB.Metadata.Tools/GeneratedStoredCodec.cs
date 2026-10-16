using System;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.Generation;
using SoloDatabase.Metadata.CodecGeneration;

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
        var dependencies=TypeSupport.StoredNumericDependencies();
        for(var dependency=0;dependency<dependencies.Length;dependency++)
            Numeric(definitions,module,owner,entry,product,api.Operation("JsonStoredNumeric",dependencies[dependency].Item2,reader).ReturnType,startIndex+dependency);
        for(var ordinal=0;ordinal<models.Length;ordinal++) {
            var model=models[ordinal];var target=module.ImportReference(model.Type);var shape=model.Facts.Shape;
            var index=startIndex+dependencies.Length+ordinal;
            var definition = target is GenericInstanceType original ? original.ElementType.FullName : target.FullName;
            var preparation = CodecPreparation.stored(definition,TypeSupport.ForDefinition(shape,definition),model.Facts.AbstractType);
            switch (preparation.Kind) {
                case CodecStoredKind.Reserved: continue;
                case CodecStoredKind.Numeric:
                    GeneratedNumericCodec.Register(definitions,module,owner,entry,product,target,index); continue;
                case CodecStoredKind.Optional:
                    GeneratedOptionalCodec.Register(definitions,module,owner,entry,product,target,index,models); continue;
                case CodecStoredKind.Scalar:
                    entry.Emit(OpCodes.Call,api.Metadata(preparation.Registration,target)); continue;
                case CodecStoredKind.Reference:
                    var arguments = preparation.Arguments == CodecArguments.Generic
                        ? ((GenericInstanceType)target).GenericArguments.ToArray() : Array.Empty<TypeReference>();
                    entry.Emit(OpCodes.Call,api.Metadata(preparation.Registration,arguments)); continue;
                case CodecStoredKind.ReferenceSequence:
                    GeneratedSequenceCodec.RegisterReferenceReaders(definitions,module,owner,entry,product,model,index,models);
                    continue;
                case CodecStoredKind.Grouping:
                    GeneratedSequenceCodec.RegisterGroupingReaders(definitions,module,owner,entry,product,model,index,models);
                    GeneratedRawCodec.RegisterFactoryWriters(definitions,module,owner,entry,product,model,index,models); continue;
                case CodecStoredKind.GroupingImplementation:
                    GeneratedSequenceCodec.RegisterGroupingReaders(definitions,module,owner,entry,product,model,index,models);
                    GeneratedSequenceCodec.RegisterWriters(definitions,module,owner,entry,product,target,model.CollectionArguments[preparation.ElementArgument],
                        model.Type.IsValueType,"WriteStoredGrouping"+index,models); continue;
                case CodecStoredKind.Sequence:
                    GeneratedSequenceCodec.Register(definitions,module,owner,entry,product,model,index,models,preparation.ReadersOnly);
                    if(preparation.ReadersOnly) GeneratedRawCodec.RegisterFactoryWriters(definitions,module,owner,entry,product,model,index,models);
                    continue;
                case CodecStoredKind.Body:
                    GeneratedRawCodec.Register(definitions,module,owner,entry,product,model,index,models);
                    if(preparation.ArrayBodies)
                        GeneratedSequenceCodec.RegisterRangeArrayBodies(definitions,module,owner,entry,product,((ArrayType)target).ElementType,
                            model.Facts.SupportsValuesWrapper,"ReadStoredArrayWrapper"+index,models);
                    break;
                default: throw new InvalidOperationException("Unknown stored codec preparation.");
            }

        }
        if(models.Any(model=>TypeSupport.StoredDynamic(model.Facts.Shape,model.Facts.NonGenericEnumerable)))
            entry.Emit(OpCodes.Call,api.Metadata("RegisterStoredDynamic"));
    }
}
