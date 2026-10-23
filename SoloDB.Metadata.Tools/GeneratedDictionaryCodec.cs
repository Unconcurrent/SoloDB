using System;
using System.Linq;
using Microsoft.FSharp.Core;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

// Original closed operands mapped into the shared dictionary instruction owner.
internal static partial class GeneratedDictionaryCodec
{
    private static GenericInstanceType Closed(ModuleDefinition module, TypeReference definition, params TypeReference[] arguments)
    {
        var result = new GenericInstanceType(module.ImportReference(definition));
        foreach (var argument in arguments) result.GenericArguments.Add(module.ImportReference(argument));
        return result;
    }
    private static MethodReference Bind(ModuleDefinition module,System.Reflection.MethodBase original,TypeReference owner)
    {
        var method = module.ImportReference(original); method.DeclaringType = module.ImportReference(owner); return method;
    }
    private static FSharpOption<T> Some<T>(T value) => FSharpOption<T>.Some(value);
    private static FSharpFunc<T,R> Function<T,R>(Func<T,R> value) => FSharpFunc<T,R>.FromConverter(new Converter<T,R>(value));
    private static TypeReference[] Arguments(ModuleDefinition module,GeneratedModel model)
    {
        if (model.Facts.Shape == TypeShape.NonGenericDictionary) return new[] {module.TypeSystem.Object,module.TypeSystem.Object};
        return model.CollectionArguments.Length == 2 ? model.CollectionArguments.Select(module.ImportReference).ToArray()
            : ((GenericInstanceType)model.Type).GenericArguments.Select(module.ImportReference).ToArray();
    }
    private static void Delegate(ILProcessor entry,ModuleDefinition module,AssemblyDefinition product,
        string type,MethodReference body,params TypeReference[] arguments)
    {
        var delegateType = Closed(module,module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator."+type)),arguments);
        var constructor = new MethodReference(".ctor",module.TypeSystem.Void,delegateType) {HasThis=true};
        constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));
        constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
        entry.Emit(OpCodes.Ldnull);entry.Emit(OpCodes.Ldftn,body);entry.Emit(OpCodes.Newobj,constructor);
    }
    internal static void RegisterTree(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,
        ILProcessor entry,AssemblyDefinition product,GeneratedModel model,int index,GeneratedModel[] models)
    {
        var api=new GeneratedJsonAssembly.Api(module,product);
        var arguments=Arguments(module,model);
        var preparation=CodecPreparation.factory(model.Type.FullName,TypeSupport.ForDefinition(model.Facts.Shape,model.Type.FullName),model.Facts.AbstractType);
        var polymorphic=preparation.ReadersOnly;
        if(preparation.ConcreteReaderFactory.Length != 0)
        {
            var destination=models.FirstOrDefault(candidate=>candidate.Facts.Shape==TypeShape.Dictionary &&
                candidate.Type is GenericInstanceType dictionary && dictionary.GenericArguments.Count==2 &&
                dictionary.GenericArguments.Zip(arguments,GeneratedJsonDefinitions.SameType).All(equal=>equal));
            if(destination==null)
            {
                var concrete=Closed(module,module.ImportReference(typeof(System.Collections.Generic.Dictionary<,>)),arguments);
                var facts=new Model(concrete.FullName,concrete.FullName,TypeShape.Dictionary,false,false,true,null,Array.Empty<Member>()) {DictionaryReadOnly=true};
                destination=new GeneratedModel(facts,concrete,Bind(module,typeof(System.Collections.Generic.Dictionary<,>).GetConstructor(Type.EmptyTypes)!,concrete),Array.Empty<GeneratedMember>()) {CollectionArguments=arguments};
            }
            var concreteType=destination.Type;
            entry.Emit(OpCodes.Call,api.Metadata("Create"+preparation.ConcreteReaderFactory,arguments));entry.Emit(OpCodes.Call,api.Metadata("RegisterGenerated",concreteType));
            Register(definitions,module,owner,entry,product,destination,index,models,false);
        }
        entry.Emit(OpCodes.Call,api.Metadata("Create"+preparation.Factory,arguments));entry.Emit(OpCodes.Call,api.Metadata("RegisterGenerated",model.Type));
        Register(definitions,module,owner,entry,product,model,index,models,polymorphic);
        if(polymorphic) GeneratedRawCodec.RegisterFactoryWriters(definitions,module,owner,entry,product,model,index,models);
    }
    internal static void Register(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,
        ILProcessor entry,AssemblyDefinition product,GeneratedModel model,int index,GeneratedModel[] models,bool readersOnly)
    {
        var api = new GeneratedJsonAssembly.Api(module,product);
        foreach(var name in new[]{"Utf8Reader","Utf16Reader"})
        {
            var reader=module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator."+name));
            Delegate(entry,module,product,"JsonDirectReader`2",Reader(definitions,module,owner,product,model,reader,"ReadDictionary"+name+index,models),model.Type,reader);
        }
        entry.Emit(OpCodes.Call,api.Metadata("RegisterRawReaders",model.Type));
        if (!readersOnly)
            foreach(var kind in GeneratedJsonDefinitions.Outputs) {
                var writes = definitions.OutputView(kind);
                foreach(var encoding in new[]{module.TypeSystem.Byte,module.TypeSystem.Char}) {
                    var output = writes.OutputType(module,product,encoding);
                    Delegate(entry,module,product,"JsonDirectWriter`2",Writer(writes,module,owner,product,model,encoding,output,"WriteDictionary"+encoding.Name+index,models),model.Type,encoding);
                }
                GeneratedJsonAssembly.BoxedWriters(definitions,module,owner,entry,product,model.Type);
                entry.Emit(OpCodes.Call,api.Metadata("RegisterOutputWriters",model.Type,writes.OutputType(module,product,module.TypeSystem.Byte),writes.OutputType(module,product,module.TypeSystem.Char)));
            }
    }
}
