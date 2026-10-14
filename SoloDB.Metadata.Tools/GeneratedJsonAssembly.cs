using System;
using System.Collections.Generic;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.Generation;
using SoloDatabase.Metadata.CodecGeneration;

namespace SoloDatabase.Metadata.Tools;

// Compiler-established original identities paired with shared serialization facts.
public sealed class GeneratedModel
{
    public GeneratedModel(Model facts, TypeReference type, MethodReference? constructor, GeneratedMember[] members)
    { Facts = facts; Type = type; Constructor = constructor; Members = members; }
    public Model Facts { get; }
    public TypeReference[] AssignableTo { get; set; } = Array.Empty<TypeReference>();
    // Unavailable original interface facts use the existing exact-type writer guard.
    public TypeReference[] UnknownInterfaces { get; set; } = Array.Empty<TypeReference>();
    public TypeReference Type { get; }
    public MethodReference? Constructor { get; }
    public MethodReference? DefaultConstructor { get; set; }
    public MethodReference? StringIndexer { get; set; }
    public TypeReference? StringIndexerValue { get; set; }
    public GeneratedMember[] Members { get; }
    public TypeReference[] CollectionArguments { get; set; } = Array.Empty<TypeReference>();
    public TypeReference? ElementType { get; set; }
    public TypeReference? SequenceElement { get; set; }
    public GeneratedUnionCase[]? UnionCases { get; set; }
    public TypeReference? UnionRoot { get; set; }
    public bool UnionHostedReferences { get; set; }
    public ISet<string> AccessAssemblies { get; } = new HashSet<string>(StringComparer.Ordinal);
}

public sealed class GeneratedUnionCase
{
    public GeneratedUnionCase(string name, int tag, MethodReference? factory, string[] fieldNames, TypeReference[] fieldTypes)
    { Name = name; Tag = tag; Factory = factory; FieldNames = fieldNames; FieldTypes = fieldTypes; }
    public string Name { get; }
    public int Tag { get; }
    public MethodReference? Factory { get; }
    public string[] FieldNames { get; }
    public TypeReference[] FieldTypes { get; }
}

public sealed class GeneratedMember
{
    public GeneratedMember(TypeReference type, MethodReference? getter, MethodReference? setter, FieldReference? field)
    { Type = type; Getter = getter; Setter = setter; Field = field; QueryWritable = setter != null; }
    public TypeReference Type { get; }
    public MethodReference? Getter { get; }
    public MethodReference? Setter { get; }
    public FieldReference? Field { get; }
    public bool QueryWritable { get; set; }
    public bool StaticField { get; set; }
    public MethodReference? IdentityGenerator { get; set; }
}

// Direct typed bodies for generative assemblies and inaccessible source operations.
public static partial class GeneratedJsonAssembly
{
    public static void BindTemplate(string template, string output, string identity, string name, string[] references)
    {
        using var resolver = new DefaultAssemblyResolver();
        foreach (var reference in references) resolver.AddSearchDirectory(System.IO.Path.GetDirectoryName(System.IO.Path.GetFullPath(reference))!);
        using var assembly = AssemblyDefinition.ReadAssembly(template, new ReaderParameters { ReadingMode = ReadingMode.Immediate, AssemblyResolver = resolver });
        var owner = assembly.MainModule.Types.Single(type => type.Namespace == "SoloDatabase.Generated" && type.Name == "Template");
        assembly.Name.Name = identity;
        assembly.MainModule.Name = identity + ".dll";
        owner.Name = name;
        HelperAssembly.Write(assembly, output, null);
    }

    public static void Surface(string path, string identity, string name)
    {
        using var assembly = HelperAssembly.Create(identity);
        var owner = Owner(assembly.MainModule, name);
        var entry = Method(owner, "register", assembly.MainModule.TypeSystem.Void);
        var il = entry.Body.GetILProcessor();
        il.Emit(OpCodes.Ldstr, "Checking-only SoloDB registration cannot execute. Build the project with executable compiler context.");
        var exception = new TypeReference("System", "InvalidOperationException", assembly.MainModule, owner.BaseType.Scope);
        var ctor = new MethodReference(".ctor", assembly.MainModule.TypeSystem.Void, exception) { HasThis = true };
        ctor.Parameters.Add(new ParameterDefinition(assembly.MainModule.TypeSystem.String));
        il.Emit(OpCodes.Newobj, ctor); il.Emit(OpCodes.Throw);
        HelperAssembly.Write(assembly, path, null);
    }

    public static void Write(string path, string identity, string name, AssemblyDefinition product, GeneratedModel[] models, TypeReference? existingAccessAttribute = null, GeneratedQueryOperation[]? queries = null, GeneratedQueryCall[]? queryCalls = null, GeneratedQueryWitness[]? queryWitnesses = null, GeneratedQueryLambda[]? queryLambdas = null, GeneratedProviderOperation[]? providerOperations = null, string[]? references = null)
    {
        using var assembly = HelperAssembly.Create(identity);
        var module = assembly.MainModule;
        var owner = Owner(module, name);
        var entry = Method(owner, "register", module.TypeSystem.Void);
        var il = entry.Body.GetILProcessor();
        if (queryWitnesses != null) GeneratedQueries.Preserve(module, entry, product, queryWitnesses);
        if (queryLambdas != null) GeneratedQueries.RegisterExpressions(module, il, product, queryLambdas);
        if (queries != null) GeneratedQueries.Register(module, il, product, queries);
        if (providerOperations != null) GeneratedQueries.RegisterProviders(module, il, product, providerOperations);
        if (queryCalls != null) GeneratedQueryCalls.Register(module, owner, il, product, queryCalls);
        RegisterModels(new GeneratedJsonDefinitions(references),module,owner,il,product,models);
        il.Emit(OpCodes.Ret);
        GuardRegistration(module, owner, entry);
        var access = models.SelectMany(model => model.AccessAssemblies).Concat(new[] { product.Name.Name }).Distinct().ToArray();
        if (access.Length != 0)
        {
            MethodReference? constructor = null;
            if (existingAccessAttribute != null)
            {
                constructor = new MethodReference(".ctor", module.TypeSystem.Void, module.ImportReference(existingAccessAttribute)) { HasThis = true };
                constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.String));
            }
            HelperAssembly.AllowAccess(module, access, owner.BaseType.Scope, constructor);
        }
        HelperAssembly.Write(assembly, path, null);
    }

    internal static void RegisterModels(GeneratedJsonDefinitions definitions,ModuleDefinition module, TypeDefinition owner, ILProcessor il,
        AssemblyDefinition product, GeneratedModel[] models, int startIndex = 0)
    {
        var api = new Api(module,product);
        if(models.Any(model=>model.Type is not ArrayType && model.SequenceElement?.FullName=="System.Byte"))
            GeneratedOutputCodec.RegisterSequenceBytes(definitions,module,owner,il,product);
        var finite = models.Select(model => TypeSupport.FiniteCaptureRoles(model.Facts.Shape)).ToArray();
        GeneratedFiniteCodec.Register(definitions,module,owner,il,product,finite.Any(value => value.Item1),finite.Any(value => value.Item2));
        var numericInputs = new HashSet<string>(models.Where(model => !TypeSupport.NumericInfrastructure(model.Type.FullName)).Select(model => model.Type.FullName),StringComparer.Ordinal);
        var dependencyIndex = startIndex + models.Length;
        foreach (var dependency in TypeSupport.NumericParameterDependencies())
        {
            if (!numericInputs.Add(dependency)) continue;
            var original = module.ImportReference(api.Operation("JsonRawPrimitive","Read" + TypeSupport.NumericIntrinsicOperation(dependency),
                module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.Utf8Reader"))).ReturnType);
            GeneratedNumericCodec.Register(definitions,module,owner,il,product,original,dependencyIndex++,intrinsicOnly: true);
        }
        foreach (var model in models)
            foreach (var dependency in TypeSupport.NumericDependencies(model.Type.FullName))
            {
                if (!numericInputs.Add(dependency)) continue;
                var original = module.ImportReference(api.Operation("JsonRawPrimitive","Read" + TypeSupport.NumericIntrinsicOperation(dependency),
                    module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.Utf8Reader"))).ReturnType);
                var intrinsicOnly = TypeSupport.NumericInfrastructure(dependency);
                if (!intrinsicOnly) il.Emit(OpCodes.Call,api.Metadata("RegisterScalarTree",original));
                GeneratedNumericCodec.Register(definitions,module,owner,il,product,original,dependencyIndex++,intrinsicOnly);
            }
        // Publish concrete primitive readers before factories acquire their scalar dependencies.
        for (var ordinal = 0; ordinal < models.Length; ordinal++)
        {
            var model = models[ordinal];
            if (TypeSupport.ForDefinition(model.Facts.Shape,model.Type.FullName).RawKind == RawCodecKind.Numeric)
            {
                if (TypeSupport.NumericTextAdapter(model.Type.FullName) != null)
                {
                    GeneratedNumericScalar.RegisterTree(definitions,module,owner,il,product,model.Type,startIndex + ordinal);
                    GeneratedNumericScalar.RegisterRaw(definitions,module,owner,il,product,model.Type,startIndex + ordinal);
                }
                else
                {
                    il.Emit(OpCodes.Call,api.Metadata("RegisterScalarTree",module.ImportReference(model.Type)));
                    GeneratedNumericCodec.Register(definitions,module,owner,il,product,model.Type,startIndex + ordinal);
                }
            }
        }
        for (var ordinal = 0; ordinal < models.Length; ordinal++)
        {
            var index = startIndex + ordinal;
            var model = models[ordinal];
            var type = module.ImportReference(model.Type);
            GeneratedStringIndexer.Register(module,owner,il,product,model,index);
            GeneratedIdentity.Register(module, owner, il, product, model, index);
            GeneratedArgumentCapture.Register(module, owner, il, product, model, index);
            GeneratedSqlRows.Register(module, owner, il, product, model, index, models);
            GeneratedRelations.Register(module, owner, il, product, model, index);
            GeneratedCloning.Register(module, owner, il, product, model, index);
            GeneratedCustomIdentity.Register(module, owner, il, product, model, index);
            il.Emit(model.Facts.SealedType ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
            il.Emit(model.Facts.RequiresTypeInformation ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Call, api.Metadata("RegisterType", type));
            var references = new GeneratedQueryReferences(module,product);
            if (type is ArrayType { Rank: > 1 } addressedArray) references.Array(owner,il,addressedArray);
            if (model.Facts.ValueType && model.Facts.Shape != TypeShape.Object) references.Box(owner,il,type);
            if (model.Facts.ValueType)
                RegisterDefaultConstruction(module,owner,il,product,type,model.DefaultConstructor,index);
            var closedArguments = type is GenericInstanceType generic ? generic.GenericArguments.ToArray() : Array.Empty<TypeReference>();
            il.Emit(OpCodes.Ldc_I4, closedArguments.Length);
            var runtimeType = new TypeReference("System","Type",module,owner.BaseType.Scope);
            il.Emit(OpCodes.Newarr,runtimeType);
            for(var argumentIndex = 0; argumentIndex < closedArguments.Length; argumentIndex++)
            {
                il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4,argumentIndex);
                api.TypeToken(il,closedArguments[argumentIndex]); il.Emit(OpCodes.Stelem_Ref);
            }
            il.Emit(OpCodes.Call,api.Metadata("RegisterTypeArguments",type));
            if (model.Facts.RecordType)
            {
                var fields = model.Facts.RecordFields;
                il.Emit(OpCodes.Ldc_I4, fields.Length); il.Emit(OpCodes.Newarr, module.TypeSystem.String);
                for (var fieldIndex = 0; fieldIndex < fields.Length; fieldIndex++)
                {
                    il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4, fieldIndex);
                    il.Emit(OpCodes.Ldstr, fields[fieldIndex]); il.Emit(OpCodes.Stelem_Ref);
                }
            }
            else il.Emit(OpCodes.Ldnull);
            il.Emit(OpCodes.Call,api.Metadata("RegisterRecordFields",type));
            RegisterFacts(il,model.Type,model.Facts.Members.Select(member => member.Name).ToArray(),model.Members.Select(member => member.Type).ToArray(),
                model.Facts.Members.Select(member => member.Field).ToArray(),model.SequenceElement,api);
            var decision = TypeSupport.ForDefinition(model.Facts.Shape,type.FullName);
            switch (decision.RegistrationKind)
            {
                case CodecRegistrationKind.Tuple:
                    GeneratedTuples.Register(definitions,module,owner,il,product,model,index,models); continue;
                case CodecRegistrationKind.Collection:
                    if (TypeSupport.ReferenceSequence(model.Facts.Shape) != null)
                    {
                        il.Emit(OpCodes.Call,api.Metadata("Create" + decision.Factory));
                        il.Emit(OpCodes.Call,api.Metadata("RegisterGenerated",type));
                        GeneratedSequenceCodec.RegisterReferenceReaders(definitions,module,owner,il,product,model,index,models);
                        continue;
                    }
                    if (model.Facts.Shape == TypeShape.Grouping)
                    {
                        il.Emit(OpCodes.Call,api.Metadata("CreateGrouping",((GenericInstanceType)type).GenericArguments.ToArray()));
                        il.Emit(OpCodes.Call,api.Metadata("RegisterGenerated",type));
                        GeneratedSequenceCodec.RegisterGroupingReaders(definitions,module,owner,il,product,model,index,models);
                        GeneratedRawCodec.RegisterFactoryWriters(definitions,module,owner,il,product,model,index,models);
                        continue;
                    }
                    if (decision.RawKind == RawCodecKind.Sequence)
                    {
                        var element = ((GenericInstanceType)type).GenericArguments[0];
                        il.Emit(OpCodes.Call,api.Metadata("Create" + decision.Factory,element));
                        il.Emit(OpCodes.Call,api.Metadata("RegisterGenerated",type));
                        GeneratedSequenceCodec.Register(definitions,module,owner,il,product,model,index,models,true);
                        GeneratedRawCodec.RegisterFactoryWriters(definitions,module,owner,il,product,model,index,models);
                        continue;
                    }
                    RegisterCollection(il,model,api);
                    if (TypeSupport.RawPolymorphicFactory(model.Facts.Shape,model.Facts.AbstractType))
                        GeneratedRawCodec.RegisterFactoryWriters(definitions,module,owner,il,product,model,index,models);
                    continue;
                case CodecRegistrationKind.Intrinsic:
                    if (decision.RawKind == RawCodecKind.Numeric) continue;
                    var arguments = decision.Arguments switch {
                        CodecArguments.None => Array.Empty<TypeReference>(),
                        CodecArguments.Target => new[] { type },
                        CodecArguments.Element => new[] { ((ArrayType)type).ElementType },
                        CodecArguments.Generic => ((GenericInstanceType)type).GenericArguments.ToArray(),
                        CodecArguments.TargetAndUnderlying => new[] { type,module.ImportReference(model.ElementType!) },
                        CodecArguments.TargetAndContract => new[] { type }.Concat(model.CollectionArguments).ToArray(),
                        _ => throw new InvalidOperationException("Unknown codec argument source.")
                    };
                    if (TypeSupport.ReferenceSequence(model.Facts.Shape) != null)
                    {
                        il.Emit(OpCodes.Call,api.Metadata("Create" + decision.Factory,arguments));
                        il.Emit(OpCodes.Call,api.Metadata("RegisterGenerated",type));
                        GeneratedSequenceCodec.RegisterReferenceReaders(definitions,module,owner,il,product,model,index,models);
                        continue;
                    }
                    if (model.Facts.Shape == TypeShape.GroupingImplementation)
                    {
                        il.Emit(OpCodes.Call,api.Metadata("CreateGroupingImplementation",arguments));
                        il.Emit(OpCodes.Call,api.Metadata("RegisterGenerated",type));
                        GeneratedSequenceCodec.RegisterGroupingReaders(definitions,module,owner,il,product,model,index,models);
                        GeneratedSequenceCodec.RegisterWriters(definitions,module,owner,il,product,model.Type,model.CollectionArguments[1],model.Type.IsValueType,"WriteGrouping" + index,models);
                        continue;
                    }
                    if (decision.RawKind == RawCodecKind.Sequence && model.Facts.Shape != TypeShape.Array)
                    {
                        il.Emit(OpCodes.Call,api.Metadata("Create" + decision.Factory,arguments));
                        il.Emit(OpCodes.Call,api.Metadata("RegisterGenerated",type));
                        GeneratedSequenceCodec.Register(definitions,module,owner,il,product,model,index,models,false);
                        continue;
                    }
                    if (decision.RawKind == RawCodecKind.Dictionary)
                    {
                        GeneratedDictionaryCodec.RegisterTree(definitions,module,owner,il,product,model,index,models);
                        continue;
                    }
                    if (decision.ArrayWrapper) il.Emit(model.Facts.SupportsValuesWrapper ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
                    var adaptation = GeneratedOptionalCodec.Kind(type,models);
                    il.Emit(OpCodes.Call,api.Metadata(TypeSupport.RawFactoryRegistration(model.Facts.Shape,model.Facts.AbstractType,decision.Registration) + (adaptation != 0 ? "Tree" : ""),arguments));
                    if(adaptation != 0) GeneratedOptionalCodec.Register(definitions,module,owner,il,product,type,index,models);
                    if (model.Facts.Shape == TypeShape.Array)
                    {
                        GeneratedRawCodec.Register(definitions,module,owner,il,product,model,index,models);
                        GeneratedSequenceCodec.RegisterRangeArrayBodies(definitions,module,owner,il,product,((ArrayType)model.Type).ElementType,
                            model.Facts.SupportsValuesWrapper,"ReadArrayWrapper" + index,models);
                    }
                    if (TypeSupport.RawPolymorphicFactory(model.Facts.Shape,model.Facts.AbstractType))
                        GeneratedRawCodec.RegisterFactoryWriters(definitions,module,owner,il,product,model,index,models);
                    continue;
                case CodecRegistrationKind.Model: break;
                default: throw new InvalidOperationException("Unknown codec registration kind.");
            }
            RegisterObject(definitions,module, owner, il, product, model, index,models);

        }
        GeneratedStoredCodec.Register(definitions,module,owner,il,product,models,startIndex+models.Length);
    }

    private static void GuardRegistration(ModuleDefinition module, TypeDefinition owner, MethodDefinition core)
    {
        core.Name = "RegisterCore";
        core.Attributes = MethodAttributes.Private | MethodAttributes.Static;
        var gate = new FieldDefinition("gate", FieldAttributes.Private | FieldAttributes.Static | FieldAttributes.InitOnly, module.TypeSystem.Object);
        var complete = new FieldDefinition("complete", FieldAttributes.Private | FieldAttributes.Static, module.TypeSystem.Boolean);
        owner.Fields.Add(gate); owner.Fields.Add(complete);
        var initialize = Method(owner, ".cctor", module.TypeSystem.Void);
        initialize.Attributes = MethodAttributes.Private | MethodAttributes.Static | MethodAttributes.SpecialName | MethodAttributes.RTSpecialName;
        var constructor = new MethodReference(".ctor", module.TypeSystem.Void, owner.BaseType) { HasThis = true };
        var initializeIL = initialize.Body.GetILProcessor();
        initializeIL.Emit(OpCodes.Newobj, constructor); initializeIL.Emit(OpCodes.Stsfld, gate); initializeIL.Emit(OpCodes.Ret);
        var entry = Method(owner, "register", module.TypeSystem.Void);
        var il = entry.Body.GetILProcessor();
        var monitor = new TypeReference("System.Threading", "Monitor", module, owner.BaseType.Scope);
        MethodReference Lock(string name)
        {
            var method = new MethodReference(name, module.TypeSystem.Void, monitor);
            method.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));
            return method;
        }
        var start = Instruction.Create(OpCodes.Ldsfld, complete);
        var release = Instruction.Create(OpCodes.Ldsfld, gate);
        var success = Instruction.Create(OpCodes.Nop);
        var end = Instruction.Create(OpCodes.Ret);
        il.Emit(OpCodes.Ldsfld, gate); il.Emit(OpCodes.Call, Lock("Enter"));
        il.Append(start); il.Emit(OpCodes.Brtrue, success);
        il.Emit(OpCodes.Call, core); il.Emit(OpCodes.Ldc_I4_1); il.Emit(OpCodes.Stsfld, complete);
        il.Append(success); il.Emit(OpCodes.Leave, end);
        il.Append(release); il.Emit(OpCodes.Call, Lock("Exit")); il.Emit(OpCodes.Endfinally); il.Append(end);
        entry.Body.ExceptionHandlers.Add(new ExceptionHandler(ExceptionHandlerType.Finally)
        { TryStart = start, TryEnd = release, HandlerStart = release, HandlerEnd = end });
    }

    private static TypeDefinition Owner(ModuleDefinition module, string name)
    {
        var type = new TypeDefinition("SoloDatabase.Generated", name, TypeAttributes.Public | TypeAttributes.Abstract | TypeAttributes.Sealed,
            new TypeReference("System", "Object", module, module.AssemblyReferences[0]));
        module.Types.Add(type); return type;
    }

    private static MethodDefinition Method(TypeDefinition owner, string name, TypeReference result, params TypeReference[] arguments)
    {
        var method = new MethodDefinition(name, MethodAttributes.Public | MethodAttributes.Static | MethodAttributes.HideBySig, result);
        foreach (var argument in arguments) method.Parameters.Add(new ParameterDefinition(argument));
        owner.Methods.Add(method); method.Body.InitLocals = true; return method;
    }

    internal static void Delegate(ILProcessor il, GenericInstanceType type, MethodReference method)
    {
        il.Emit(OpCodes.Ldnull); il.Emit(OpCodes.Ldftn, method);
        var ctor = new MethodReference(".ctor", il.Body.Method.Module.TypeSystem.Void, type) { HasThis = true };
        ctor.Parameters.Add(new ParameterDefinition(il.Body.Method.Module.TypeSystem.Object));
        ctor.Parameters.Add(new ParameterDefinition(il.Body.Method.Module.TypeSystem.IntPtr));
        il.Emit(OpCodes.Newobj, ctor);
    }

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
