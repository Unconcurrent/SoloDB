using System;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;

namespace SoloDatabase.Metadata.Tools;

public static partial class GeneratedJsonAssembly
{
    internal sealed class Api
    {
        private readonly AssemblyDefinition product;
        private readonly TypeReference dictionary;
        private readonly TypeReference objectDictionary;
        public Api(ModuleDefinition module, AssemblyDefinition product, bool stored = false)
        {
            Module = module; this.product = product; Stored = stored;
            Json = ProductType("JsonValue"); StoredMember = ProductType("JsonStoredMember");
            var standard = module.AssemblyReferences[0];
            Type = new TypeReference("System", "Type", module, standard);
            dictionary = new TypeReference("System.Collections.Generic", "Dictionary`2", module, standard);
            dictionary.GenericParameters.Add(new GenericParameter("TKey", dictionary)); dictionary.GenericParameters.Add(new GenericParameter("TValue", dictionary));
            objectDictionary = new TypeReference("System.Collections.Generic", "IDictionary`2", module, standard);
            objectDictionary.GenericParameters.Add(new GenericParameter("TKey", objectDictionary)); objectDictionary.GenericParameters.Add(new GenericParameter("TValue", objectDictionary));
            Dictionary = Closed(dictionary, module.TypeSystem.String, Json); ObjectDictionary = Closed(objectDictionary, module.TypeSystem.String, Json);
        }
        public ModuleDefinition Module { get; }
        public bool Stored { get; }
        private TypeReference Contract => ProductType(Stored ? "JsonStoredContract" : "JsonNativeContract");
        public TypeReference Type { get; }
        public TypeReference Json { get; }
        public TypeReference StoredMember { get; }
        public GenericInstanceType Dictionary { get; }
        public GenericInstanceType ObjectDictionary { get; }
        public TypeReference KeyParameter => dictionary.GenericParameters[0];
        public TypeReference ValueParameter => dictionary.GenericParameters[1];
        public TypeReference ObjectKeyParameter => objectDictionary.GenericParameters[0];
        public TypeReference ObjectValueParameter => objectDictionary.GenericParameters[1];
        private TypeReference ProductType(string name) => Module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator." + name));
        public MethodReference ModelRegistration(string name) => Module.ImportReference(product.MainModule.GetType("SoloDatabase.ModelMetadata")
            .Methods.Single(method => method.Name == name));
        public MethodReference MemberRegistration(TypeReference owner, TypeReference value)
        {
            var method = new GenericInstanceMethod(Module.ImportReference(product.MainModule.GetType("SoloDatabase.QueryValueMetadata")
                .Methods.Single(method => method.Name == "RegisterMember")));
            method.GenericArguments.Add(Module.ImportReference(owner));
            method.GenericArguments.Add(Module.ImportReference(value));
            return method;
        }
        public MethodReference ConstructionRegistration() => Module.ImportReference(product.MainModule.GetType("SoloDatabase.QueryValueMetadata")
            .Methods.Single(method => method.Name == "RegisterGeneratedConstruction"));
        public MethodReference SequenceCodec(TypeReference type, TypeReference element) => ProductMethod("JsonMetadata","CreateSequence",new[] { type,element },2);
        public MethodReference Helper(string name, params TypeReference[] arguments) => ProductMethod("JsonImpl",name,arguments);
        public MethodReference Metadata(string name, params TypeReference[] arguments) => ProductMethod("JsonMetadata",
            Stored && name is "RegisterRaw" or "RegisterRawReaders" or "RegisterRawWriters" or "RegisterWriterBodies"
                or "RegisterSequenceWriterBodies" or "RegisterSequenceReaders" or "RegisterSequenceBodies"
                or "RegisterRangeArrayBodies" or "RegisterGroupingBodies" or "RegisterReferenceSequenceBodies"
                ? "RegisterStored" + name.Substring("Register".Length) : name, arguments);
        public MethodReference JsonMethod(string name, params TypeReference[] arguments) => ProductMethod("JsonValue", name, arguments);
        public MethodReference JsonMethod(string name, TypeReference[] arguments, int count) => ProductMethod("JsonValue", name, arguments, count);
        public MethodReference ShapeMethod(string name) => ProductMethod("JsonCodecShape", name, Array.Empty<TypeReference>());
        public MethodReference Operation(string owner, string name, params TypeReference[] arguments) => ProductMethod(owner,name,arguments);
        public GenericInstanceType RawType(string owner, params TypeReference[] arguments) =>
            Closed(ProductType(owner + "`" + (arguments.Length + 1)),arguments.Concat(new[] {Contract}).ToArray());
        private MethodReference ProductMethod(string owner, string name, TypeReference[] arguments, int? count = null)
        {
            var rawContract = owner is "JsonRawRead" or "JsonRawInput" or "JsonRawCollections";
            var definition = product.MainModule.GetType("SoloDatabase.JsonSerializator." + owner + (rawContract ? "`1" : ""));
            var methods = definition.Methods.Where(method =>
                method.Name == name && (method.IsPublic || method.IsAssembly) && method.GenericParameters.Count == arguments.Length && (!count.HasValue || method.Parameters.Count == count)).ToArray();
            if (methods.Length != 1) throw new InvalidOperationException("Generated API signature is not unique: " + owner + "." + name);
            var method = Module.ImportReference(methods[0]);
            if (rawContract)
                method.DeclaringType = Closed(Module.ImportReference(definition),Contract);
            if (arguments.Length == 0) return method;
            var closed = new GenericInstanceMethod(method); foreach (var argument in arguments) closed.GenericArguments.Add(Module.ImportReference(argument)); return closed;
        }
        public GenericInstanceType Function(TypeReference input, TypeReference output) => Closed(new TypeReference("System", "Func`2", Module, Module.AssemblyReferences[0]), input, output);
        public GenericInstanceType Function(TypeReference output) => Closed(new TypeReference("System", "Func`1", Module, Module.AssemblyReferences[0]), output);
        public MethodReference CodecConstructor(TypeReference type)
        {
            var definition = product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonCodec`1");
            var ctor = Module.ImportReference(definition.Methods.Single(method => method.IsConstructor && method.Parameters.Count == 3));
            ctor.DeclaringType = Closed(Module.ImportReference(definition), type); return ctor;
        }
        public MethodReference ObjectMethod(string name, TypeReference result) => Instance(name, Module.TypeSystem.Object, result);
        public MethodReference DictionaryMethod(string name, TypeReference result, params TypeReference[] arguments) => Instance(name, Dictionary, result, arguments);
        public MethodReference ObjectDictionaryMethod(string name, TypeReference result, params TypeReference[] arguments) => Instance(name, ObjectDictionary, result, arguments);
        public void TypeToken(ILProcessor il, TypeReference type)
        {
            il.Emit(OpCodes.Ldtoken, Module.ImportReference(type));
            var handle = new TypeReference("System", "RuntimeTypeHandle", Module, Module.AssemblyReferences[0], true);
            var method = new MethodReference("GetTypeFromHandle", Type, Type);
            method.Parameters.Add(new ParameterDefinition(handle)); il.Emit(OpCodes.Call, method);
        }
        private static MethodReference Instance(string name, TypeReference owner, TypeReference result, params TypeReference[] arguments)
        {
            var method = new MethodReference(name, result, owner) { HasThis = true };
            foreach (var argument in arguments) method.Parameters.Add(new ParameterDefinition(argument)); return method;
        }
        private GenericInstanceType Closed(TypeReference type, params TypeReference[] arguments)
        {
            var closed = new GenericInstanceType(type); foreach (var argument in arguments) closed.GenericArguments.Add(Module.ImportReference(argument)); return closed;
        }
    }
}
