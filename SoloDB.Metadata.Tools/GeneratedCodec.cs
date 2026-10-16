using System;
using System.Linq;
using Microsoft.FSharp.Core;
using Mono.Cecil;
using SoloDatabase.Metadata.CodecGeneration;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

internal static class GeneratedCodec
{
    private static FSharpOption<T> Some<T>(T? value) where T : class => value == null ? null! : FSharpOption<T>.Some(value);
    private static FSharpFunc<T, TResult> Function<T, TResult>(Func<T, TResult> function) =>
        FSharpFunc<T, TResult>.FromConverter(new Converter<T, TResult>(function));

    private static string ClrName(TypeReference type) => type is ArrayType array
        ? ClrName(array.ElementType) + "[" + new string(',', array.Rank - 1) + "]" : type.Name;

    internal static CodecModel<TypeReference, MethodReference, FieldReference> Model(GeneratedModel source)
    {
        var members = source.Members.Select((member, index) =>
        {
            var fact = source.Facts.Members[index];
            return new CodecMember<TypeReference, MethodReference, FieldReference>(fact.Name, member.Type,
                Some<FieldReference>(member.Field), Some(member.Getter), Some(member.Setter),
                member.StaticField || (member.Field is FieldDefinition field && field.IsStatic) || member.Getter?.HasThis == false || member.Setter?.HasThis == false,
                fact.Read, fact.Write, fact.Ignored, fact.ManyReference);
        }).ToArray();
        var cases = source.UnionCases?.Select(item => new CodecUnionCase<TypeReference, MethodReference>(item.Name,
            item.Tag, Some(item.Factory), item.FieldNames.Select((name, index) => System.Tuple.Create(name, item.FieldTypes[index])).ToArray())).ToArray()
            ?? Array.Empty<CodecUnionCase<TypeReference, MethodReference>>();
        return new CodecModel<TypeReference, MethodReference, FieldReference>(source.Type, ClrName(source.Type),
            source.Facts.RuntimeName, source.Facts.ValueType, source.Facts.SealedType, source.Facts.AbstractType,
            Some(source.Constructor), source.Facts.ConstructFromMembers,
            source.Facts.RecordType
                ? source.Facts.RecordFields.Select(name => System.Array.FindIndex(members, member => member.Name == name && member.Field == null)).ToArray()
                : source.Facts.Members.Select((member, index) => (member, index)).Where(pair => !pair.member.Field).Select(pair => pair.index).ToArray(),
            members, source.UnionCases != null, cases, source.UnionRoot != null && !source.Facts.ValueType,Some(source.Facts.IndexedRead),Some(source.Facts.IndexedWrite));
    }

    internal static CodecApi<TypeReference, MethodReference> Api(GeneratedJsonAssembly.Api api)
    {
        var module = api.Module;
        MethodReference Constructor(string name, params TypeReference[] parameters)
        {
            var type = new TypeReference("System", name, module, module.AssemblyReferences[0]);
            var method = new MethodReference(".ctor", module.TypeSystem.Void, type) { HasThis = true };
            foreach (var parameter in parameters) method.Parameters.Add(new ParameterDefinition(parameter));
            return method;
        }
        var exception = new TypeReference("System", "Exception", module, module.AssemblyReferences[0]);
        var runtimeHandle = new TypeReference("System", "RuntimeTypeHandle", module, module.AssemblyReferences[0], true);
        var typeFromHandle = new MethodReference("GetTypeFromHandle", api.Type, api.Type);
        typeFromHandle.Parameters.Add(new ParameterDefinition(runtimeHandle));
        var helpers = new TypeReference("System.Runtime.CompilerServices", "RuntimeHelpers", module, module.AssemblyReferences[0]);
        var ensureStack = new MethodReference("EnsureSufficientExecutionStack", module.TypeSystem.Void, helpers);
        return new CodecApi<TypeReference, MethodReference>(api.Json, module.TypeSystem.Int32, api.Dictionary,
            api.ObjectDictionary, api.JsonMethod("get_Null"), api.JsonMethod("get_IsNull"), api.ObjectMethod("GetType", api.Type),
            typeFromHandle, api.Metadata("SerializeActual"), Function<TypeReference, MethodReference>(type => api.Metadata("ActualType", type)),
            api.JsonMethod("ToObject", Array.Empty<TypeReference>(), 1),
            api.DictionaryMethod(".ctor", module.TypeSystem.Void, module.TypeSystem.Int32),
            api.DictionaryMethod("Add", module.TypeSystem.Void, api.KeyParameter, api.ValueParameter),
            api.JsonMethod("NewObject"), api.Metadata("ObjectProperties"),
            api.ObjectDictionaryMethod("TryGetValue", module.TypeSystem.Boolean, api.ObjectKeyParameter, new ByReferenceType(api.ObjectValueParameter)),
            Function<TypeReference, MethodReference>(type => api.Metadata("Uninitialized", type)),
            Function<TypeReference, MethodReference>(type => api.JsonMethod("Serialize", type)),
            Function<TypeReference, MethodReference>(type => api.JsonMethod("ToObject", type)),
            Constructor("Exception", module.TypeSystem.String),
            Constructor("ArgumentException", module.TypeSystem.String,module.TypeSystem.String),
            Constructor("TypeInitializationException", module.TypeSystem.String, exception),
            new MethodReference(".ctor", module.TypeSystem.Void,
                new TypeReference("System.Collections.Generic", "KeyNotFoundException", module, module.AssemblyReferences[0]))
                { HasThis = true, Parameters = { new ParameterDefinition(module.TypeSystem.String) } },
            api.Metadata("UnionCaseSelected"), api.JsonMethod("Contains"), api.JsonMethod("GetProperty"), ensureStack,false);
    }

    private static void CollectionWriter(MethodDefinition method, GeneratedModel model, GeneratedJsonAssembly.Api api)
    {
        var operation = TypeSupport.CollectionTreeWriter(model.Facts.Shape,model.Type,model.Facts.ValueType,model.Facts.AbstractType,
            model.CollectionArguments.Length == 1 && model.CollectionArguments[0].FullName == "System.Byte",model.CollectionArguments,
            new Func<string,TypeReference[],MethodReference>((name,types) => api.Helper(name,types)));
        CodecEmitter.writerCall(new CecilCodecIL(method),api.JsonMethod("get_Null"),operation);
    }

    private static void CollectionReader(MethodDefinition method, GeneratedModel model, GeneratedJsonAssembly.Api api)
    {
        var operation = TypeSupport.CollectionTreeReader(model.Facts.Shape,model.Type,Some(model.Constructor),model.Facts.AbstractType,model.Facts.PopulateCollection,
            model.CollectionArguments,new Func<string,TypeReference[],MethodReference>((name,types) => name == "MissingCollectionConstructor" ? api.Metadata(name,types) : api.Helper(name,types)));
        CodecEmitter.readerCall(new CecilCodecIL(method),api.JsonMethod("get_IsNull"),operation);
    }

    internal static void Writer(MethodDefinition method, GeneratedModel model, GeneratedJsonAssembly.Api api)
    {
        var operation = CodecPreparation.treeWriter(model.Facts.Shape,model.Facts.NonGenericEnumerable,model.Facts.AbstractType);
        if (operation == CodecTreeOperation.Collection) { CollectionWriter(method,model,api); return; }
        if (operation == CodecTreeOperation.NonGenericSequence || operation == CodecTreeOperation.Abstract)
        {
            CodecEmitter.writerCall(new CecilCodecIL(method),api.JsonMethod("get_Null"),
                new CodecWriterCall<TypeReference,MethodReference>(model.Type,model.Facts.ValueType,model.Facts.ValueType,
                    api.Helper(operation == CodecTreeOperation.Abstract ? "SerializeAbstract" : "SerializeNonGenericEnumerable"),false));
        }
        else if (operation == CodecTreeOperation.Bson)
        {
            var getter = new MethodReference("get_Json",api.Json,model.Type) { HasThis = true };
            CodecEmitter.writerCall(new CecilCodecIL(method),api.JsonMethod("get_Null"),
                new CodecWriterCall<TypeReference,MethodReference>(model.Type,false,false,getter,true));
        }
        else CodecEmitter.writer(new CecilCodecIL(method), Api(api), Model(model));
    }
    internal static void Reader(MethodDefinition method, GeneratedModel model, GeneratedJsonAssembly.Api api)
    {
        var instance = model.Type as GenericInstanceType;
        var selected = TypeSupport.TreeValueReader(instance?.ElementType.FullName ?? model.Type.FullName,model.Type,
            instance?.GenericArguments.ToArray() ?? Array.Empty<TypeReference>(),
            new Func<string,TypeReference[],MethodReference>((name,types) => api.Helper(name,types)));
        if (selected != null)
        {
            CodecEmitter.readerCall(new CecilCodecIL(method),api.JsonMethod("get_IsNull"),selected.Value);
            return;
        }
        var operation = CodecPreparation.treeReader(model.Facts.Shape);
        if (operation == CodecTreeOperation.Collection) { CollectionReader(method,model,api); return; }
        if (operation == CodecTreeOperation.Bson)
            CodecEmitter.readerCall(new CecilCodecIL(method),api.JsonMethod("get_IsNull"),
                new CodecReaderCall<TypeReference,MethodReference>(model.Type,null!,null!,Some(model.Constructor),false,false));
        else CodecEmitter.reader(new CecilCodecIL(method), Api(api), Model(model));
    }
    internal static void Construction(MethodDefinition method, GeneratedModel model, TypeReference[] arguments) =>
        CodecEmitter.construction(new CecilCodecIL(method),model.Type,model.Facts.ValueType,model.Constructor!,arguments);
    internal static void DefaultConstruction(MethodDefinition method, TypeReference type, MethodReference? constructor) =>
        CodecEmitter.defaultConstruction(new CecilCodecIL(method),type,true,Some(constructor),true);
    internal static void Getter(MethodDefinition method, GeneratedModel model, int member) =>
        CodecEmitter.getter(new CecilCodecIL(method), Model(model), member);
    internal static int[] WrittenMembers(GeneratedModel model) => CodecEmitter.writtenMembers(Model(model));
    internal static CodecTuple<TypeReference, MethodReference, FieldReference> Tuple(GeneratedModel model, GeneratedModel[] models)
    {
        var members = Model(model).Members;
        return new CodecTuple<TypeReference, MethodReference, FieldReference>(model.Type,model.Facts.ValueType,model.Constructor!,
            members.Select((member,index) => new CodecTupleMember<TypeReference, MethodReference, FieldReference>(member,
                index == 7 ? FSharpOption<CodecTuple<TypeReference, MethodReference, FieldReference>>.Some(
                    Tuple(models.First(candidate => GeneratedJsonDefinitions.SameType(candidate.Type,member.Type)),models)) : null!)).ToArray());
    }
    internal static void TupleWriter(MethodDefinition method, GeneratedModel model, GeneratedModel[] models, GeneratedJsonAssembly.Api api) =>
        CodecTupleEmitter.writer(new CecilCodecIL(method),api.Json,Function<TypeReference,MethodReference>(type => api.JsonMethod("Serialize",type)),Tuple(model,models));
    internal static void TupleReader(MethodDefinition method, GeneratedModel model, GeneratedModel[] models, GeneratedJsonAssembly.Api api) =>
        CodecTupleEmitter.reader(new CecilCodecIL(method),api.Metadata("TupleElement"),Function<TypeReference,MethodReference>(type => api.JsonMethod("ToObject",type)),Tuple(model,models));
    internal static void TupleGetter(MethodDefinition method, GeneratedModel model, GeneratedModel[] models, int index) =>
        CodecTupleEmitter.getter(new CecilCodecIL(method),false,Tuple(model,models),CodecTupleEmitter.leaves(Tuple(model,models))[index],false);
}
