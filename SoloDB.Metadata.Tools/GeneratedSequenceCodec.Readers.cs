using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.FSharp.Core;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;

namespace SoloDatabase.Metadata.Tools;

internal static partial class GeneratedSequenceCodec
{
    internal static void RegisterReferenceReaders(GeneratedJsonDefinitions definitions, ModuleDefinition module, TypeDefinition owner,
        ILProcessor entry, AssemblyDefinition product, GeneratedModel model, int index, GeneratedModel[] models)
    {
        var api = new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        foreach (var name in new[] { "Utf8Reader", "Utf16Reader" })
        {
            var reader = module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator." + name));
            var body = CustomReader(definitions,module,owner,product,model,reader,"ReadReferences" + name + index,models);
            Delegate(entry,module,product,"JsonDirectReader`2",body,model.Type,reader);
        }
        entry.Emit(OpCodes.Call,api.Metadata("RegisterReferenceSequenceBodies",model.Type));
        var policy = SoloDatabase.Metadata.Generation.TypeSupport.ReferenceSequence(model.Facts.Shape)!.Value;
        var element = policy.Item3.Length == 0 ? ((GenericInstanceType)model.Type).GenericArguments[0] : module.ImportReference(product.MainModule.GetType(policy.Item3));
        RegisterWriters(definitions,module,owner,entry,product,model.Type,element,model.Type.IsValueType,"WriteReferences" + index,models);
        if (SoloDatabase.Metadata.Generation.TypeSupport.SequenceProducesResult(model.Facts.Shape))
            RegisterResultBodies(definitions,module,owner,entry,product,element,"ReferenceResult" + index,models);
    }

    internal static MethodDefinition GroupingReader(GeneratedJsonDefinitions definitions, ModuleDefinition module, TypeDefinition owner,
        AssemblyDefinition product, TypeReference target, TypeReference key, TypeReference element, TypeReference reader, string name, GeneratedModel[] models)
    {
        if (definitions.TryGet("grouping-read",target,reader,"",out var existing)) return existing;
        var api = new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        TypeReference Product(string type) => module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator." + type));
        var context = Closed(module,Product("JsonReadContext`1"),reader);
        var method = new MethodDefinition(definitions.MethodName(name),MethodAttributes.Assembly | MethodAttributes.Static | MethodAttributes.HideBySig,module.ImportReference(target));
        method.Body.InitLocals = true;
        method.Parameters.Add(new ParameterDefinition(new ByReferenceType(context)));
        owner.Methods.Add(method);
        definitions.Add("grouping-read",target,reader,"",method);
        var groupingDefinition = product.MainModule.GetType("SoloDatabase.JsonSerializator.Grouping`2");
        var grouping = Closed(module,module.ImportReference(groupingDefinition),key,element);
        var constructor = module.ImportReference(groupingDefinition.Methods.Single(value => value.IsConstructor && !value.IsStatic));
        constructor.DeclaringType = grouping;
        var items = new ArrayType(module.ImportReference(element));
        var operands = new CodecGroupingReaderApi<TypeReference,MethodReference>(context,items,key,
            api.Operation("JsonRawInput","SequenceNull",reader),api.Operation("JsonFiniteInput","Begin",reader),
            GeneratedFiniteCodec.Api(module,product,reader),
            api.Operation("JsonReadCursor","child",reader),api.Operation("JsonTupleInput","Missing"),
            Reader(definitions,module,owner,product,items,element,reader,"Array",false,name + "Items",models),
            GeneratedChildCodec.ReadCall(definitions,module,owner,product,key,reader,name + "Key",models),
            constructor,api.Operation("JsonRawCollections","FinishGrouping",key,element),
            Product("JsonPrimitiveShapeMismatch"),Product("JsonScalarShapeMismatch"),
            api.Operation("JsonRawRead","PrimitiveRetry",key,reader),api.Operation("JsonRawRead","ScalarRetry",key,reader));
        CodecSequenceEmitter.grouping(new CecilCodecIL(method),operands,target);
        return method;
    }

    internal static void RegisterGroupingReaders(GeneratedJsonDefinitions definitions, ModuleDefinition module, TypeDefinition owner,
        ILProcessor entry, AssemblyDefinition product, GeneratedModel model, int index, GeneratedModel[] models)
    {
        var api = new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        var arguments = model.CollectionArguments.Length == 2 ? model.CollectionArguments : ((GenericInstanceType)model.Type).GenericArguments.ToArray();
        var key = arguments[0];
        var element = arguments[1];
        foreach (var name in new[] { "Utf8Reader", "Utf16Reader" })
        {
            var reader = module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator." + name));
            var body = GroupingReader(definitions,module,owner,product,model.Type,key,element,reader,"ReadGrouping" + name + index,models);
            Delegate(entry,module,product,"JsonDirectReader`2",body,model.Type,reader);
        }
        entry.Emit(OpCodes.Call,api.Metadata("RegisterGroupingBodies",model.Type));
        RegisterResultBodies(definitions,module,owner,entry,product,element,"GroupingResult" + index,models);
    }

    internal static MethodDefinition CustomReader(GeneratedJsonDefinitions definitions, ModuleDefinition module, TypeDefinition owner,
        AssemblyDefinition product, GeneratedModel model, TypeReference reader, string name, GeneratedModel[] models)
    {
        var referencePolicy = SoloDatabase.Metadata.Generation.TypeSupport.ReferenceSequence(model.Facts.Shape);
        var reference = referencePolicy != null;
        var file = reference && referencePolicy!.Value.Item2;
        var role = reference ? "reference-sequence-read" : "custom-sequence-read";
        if (definitions.TryGet(role,model.Type,reader,"",out var existing)) return existing;
        var api = new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        TypeReference Product(string type) => module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator." + type));
        var element = reference
            ? file ? module.ImportReference(product.MainModule.GetType(referencePolicy!.Value.Item3))
                   : module.ImportReference(((GenericInstanceType)model.Type).GenericArguments[0])
            : module.ImportReference(model.CollectionArguments[0]);
        var context = Closed(module,Product("JsonReadContext`1"),reader);
        var method = new MethodDefinition(definitions.MethodName(name),MethodAttributes.Assembly | MethodAttributes.Static | MethodAttributes.HideBySig,module.ImportReference(model.Type));
        method.Body.InitLocals = true;
        method.Parameters.Add(new ParameterDefinition(new ByReferenceType(context)));
        owner.Methods.Add(method);
        definitions.Add(role,model.Type,reader,"",method);
        MethodReference Input(string operation) => api.Operation("JsonRawInput",operation,reader);
        var populate = reference ? referencePolicy!.Value.Item1 : model.Facts.PopulateCollection;
        var list = Closed(module,module.ImportReference(typeof(List<>)),element);
        var working = populate ? module.ImportReference(model.Type) : list;
        MethodReference construct;
        bool constructCall = false;
        if (!populate)
        {
            construct = module.ImportReference(typeof(List<>).GetConstructor(new[] {typeof(int)})!);
            construct.DeclaringType = working;
        }
        else if (reference)
        {
            var definition = product.MainModule.GetType(((GenericInstanceType)model.Type).ElementType.FullName);
            construct = module.ImportReference(definition.Methods.Single(value => value.IsConstructor && !value.IsStatic && value.Parameters.Count == 0));
            construct.DeclaringType = module.ImportReference(model.Type);
        }
        else if (model.Constructor != null) construct = module.ImportReference(model.Constructor);
        else
        {
            if (!definitions.TryGet("collection-construct",model.Type,model.Type,"",out var missing))
            {
                missing = new MethodDefinition(name + "Constructor",MethodAttributes.Assembly | MethodAttributes.Static | MethodAttributes.HideBySig,module.ImportReference(model.Type));
                owner.Methods.Add(missing);
                definitions.Add("collection-construct",model.Type,model.Type,"",missing);
                CodecSequenceEmitter.missingConstructor(new CecilCodecIL(missing),GeneratedCodec.Api(api).ExceptionConstructor,model.Type.Name);
            }
            construct = missing;
            constructCall = true;
        }
        var add = module.ImportReference((populate ? typeof(ICollection<>) : typeof(List<>)).GetMethod("Add")!);
        add.DeclaringType = populate ? Closed(module,module.ImportReference(typeof(ICollection<>)),element) : list;
        MethodReference? finish = !populate && model.Constructor != null ? module.ImportReference(model.Constructor) : null;
        MethodReference? nullConstruct = null;
        if (file)
        {
            var definition = product.MainModule.GetType(model.Type.FullName);
            finish = module.ImportReference(definition.Methods.Single(value => value.IsConstructor && !value.IsStatic && value.Parameters.Count == 1));
            nullConstruct = module.ImportReference(definition.Methods.Single(value => value.IsConstructor && !value.IsStatic && value.Parameters.Count == 0));
        }
        var operands = new CodecSequenceReaderApi<TypeReference,MethodReference>(context,module.TypeSystem.Int32,working,
            Input("SequenceNull"),nullConstruct == null ? null! : FSharpOption<MethodReference>.Some(nullConstruct),api.Operation("JsonRawCollections","SequenceContext",model.Type,reader),Input("SequenceCount"),Input("SequenceElement"),
            construct,constructCall,reference ? null! : FSharpOption<MethodReference>.Some(api.Operation("JsonRawCollections","AdmitCustomSequence",model.Type)),
            populate ? FSharpOption<TypeReference>.Some(Closed(module,module.ImportReference(typeof(IList<>)),element)) : null!,populate && model.Type.IsValueType,
            add,finish == null ? null! : FSharpOption<MethodReference>.Some(finish),
            file ? FSharpOption<MethodReference>.Some(api.Operation("JsonRawCollections","FinishEnumerable",element)) : null!,
            finish != null,!populate && finish == null,
            Product("JsonPrimitiveShapeMismatch"),Product("JsonScalarShapeMismatch"),api.Operation("JsonRawRead","PrimitiveRetry",element,reader),api.Operation("JsonRawRead","ScalarRetry",element,reader));
        CodecSequenceEmitter.reader(new CecilCodecIL(method),operands,
            FSharpFunc<TypeReference,FSharpOption<MethodReference>>.FromConverter(
                new Converter<TypeReference,FSharpOption<MethodReference>>(child => FSharpOption<MethodReference>.Some(
                    GeneratedChildCodec.ReadCall(definitions,module,owner,product,child,reader,name + "Element",models)))),
            FSharpFunc<TypeReference,MethodReference>.FromConverter(new Converter<TypeReference,MethodReference>(child => api.Operation("JsonRawRead","ReadMember",child,reader))),
            model.Type,element,!reference && model.Facts.SupportsValuesWrapper,false,!populate,false,false);
        return method;
    }
}
