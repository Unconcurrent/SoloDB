using System;
using System.Collections.Generic;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.Generation;
using SoloDatabase.Metadata.CodecGeneration;

namespace SoloDatabase.Metadata.Tools;

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

    public static void Surface(string path, string identity, string name) => Surface(path,identity,name,null);

    public static void Surface(string path,string identity,string name,string[]? originalPath)
    {
        using var assembly = HelperAssembly.Create(identity);
        var owner = Owner(assembly.MainModule, name);
        if (originalPath != null)
            owner.Fields.Add(new FieldDefinition("OriginalPath",FieldAttributes.Private | FieldAttributes.Static |
                FieldAttributes.Literal | FieldAttributes.HasDefault,assembly.MainModule.TypeSystem.String)
                { Constant = string.Join("\0",originalPath) });
        var entry = Method(owner, "register", assembly.MainModule.TypeSystem.Void);
        var il = entry.Body.GetILProcessor();
        il.Emit(OpCodes.Ldstr, "Checking-only SoloDB registration cannot execute. Build the project with executable compiler context.");
        var exception = new TypeReference("System", "InvalidOperationException", assembly.MainModule, owner.BaseType.Scope);
        var ctor = new MethodReference(".ctor", assembly.MainModule.TypeSystem.Void, exception) { HasThis = true };
        ctor.Parameters.Add(new ParameterDefinition(assembly.MainModule.TypeSystem.String));
        il.Emit(OpCodes.Newobj, ctor); il.Emit(OpCodes.Throw);
        HelperAssembly.Write(assembly, path, null);
    }

    public static void Write(string path, string identity, string name, AssemblyDefinition product, GeneratedModel[] models, TypeReference? existingAccessAttribute = null, GeneratedQueryOperation[]? queries = null, GeneratedQueryCall[]? queryCalls = null, GeneratedQueryWitness[]? queryWitnesses = null, GeneratedQueryLambda[]? queryLambdas = null, GeneratedProviderOperation[]? providerOperations = null, string[]? references = null, string[]? operationAccessAssemblies = null, GeneratedArgumentSource[]? argumentSources = null)
        => Write(path,identity,name,product,models,existingAccessAttribute,queries,queryCalls,queryWitnesses,
            queryLambdas,providerOperations,references,operationAccessAssemblies,argumentSources,null);

    internal static void Write(string path, string identity, string name, AssemblyDefinition product, GeneratedModel[] models,
        TypeReference? existingAccessAttribute, GeneratedQueryOperation[]? queries, GeneratedQueryCall[]? queryCalls,
        GeneratedQueryWitness[]? queryWitnesses, GeneratedQueryLambda[]? queryLambdas, GeneratedProviderOperation[]? providerOperations,
        string[]? references, string[]? operationAccessAssemblies, GeneratedArgumentSource[]? argumentSources, string[][]? aliases)
    {
        using var ownedResolver = references == null ? null : new DefaultAssemblyResolver();
        var resolver = (IAssemblyResolver?)ownedResolver ?? product.MainModule.AssemblyResolver;
        if (ownedResolver != null)
        {
            if (!string.IsNullOrEmpty(product.MainModule.FileName))
                ownedResolver.AddSearchDirectory(System.IO.Path.GetDirectoryName(System.IO.Path.GetFullPath(product.MainModule.FileName))!);
            foreach (var reference in references!)
                ownedResolver.AddSearchDirectory(System.IO.Path.GetDirectoryName(System.IO.Path.GetFullPath(reference))!);
        }
        using var imports = new TargetReflectionImports(references ?? Array.Empty<string>(),resolver,product.MainModule);
        using var assembly = HelperAssembly.Create(identity,resolver,imports);
        var module = assembly.MainModule;
        var owner = Owner(module, name);
        if (aliases != null) owner.Attributes = TypeAttributes.NotPublic | TypeAttributes.Abstract | TypeAttributes.Sealed;
        var entry = Method(owner, "register", module.TypeSystem.Void);
        var il = entry.Body.GetILProcessor();
        var definitions = new GeneratedJsonDefinitions(references);
        if (queryWitnesses != null) GeneratedQueries.Preserve(definitions,module, entry, product, queryWitnesses);
        if (queryLambdas != null) GeneratedQueries.RegisterExpressions(module, il, product, queryLambdas);
        if (queries != null) GeneratedQueries.Register(module, il, product, queries);
        if (providerOperations != null) GeneratedQueries.RegisterProviders(module, il, product, providerOperations);
        if (queryCalls != null) GeneratedQueryCalls.Register(module, owner, il, product, queryCalls);
        if (argumentSources != null)
            foreach (var source in argumentSources) GeneratedArgumentCapture.Register(definitions,module,owner,il,product,source);
        RegisterModels(definitions,module,owner,il,product,models);
        il.Emit(OpCodes.Ret);
        if (aliases == null) GuardRegistration(module,owner,entry);
        else
        {
            // Each authored alias keeps its own once/retry state. Only immutable
            // specialized method bodies are shared across those entrypoints.
            entry.Name = "RegisterCore";
            entry.Attributes = MethodAttributes.Assembly | MethodAttributes.Static;
            for (var index = 0; index < aliases.Length; index++)
            {
                var alias = aliases[index];
                var facade = Owner(module,alias[alias.Length - 1]);
                facade.Namespace += ".Aliases" + index;
                facade.Fields.Add(new FieldDefinition("OriginalPath",FieldAttributes.Private | FieldAttributes.Static |
                    FieldAttributes.Literal | FieldAttributes.HasDefault,module.TypeSystem.String)
                    { Constant = string.Join("\0",alias) });
                var call = Method(facade,"register",module.TypeSystem.Void);
                call.Body.GetILProcessor().Emit(OpCodes.Call,entry);
                call.Body.GetILProcessor().Emit(OpCodes.Ret);
                GuardRegistration(module,facade,call);
            }
        }
        var access = models.SelectMany(model => model.AccessAssemblies).Concat((argumentSources ?? Array.Empty<GeneratedArgumentSource>()).SelectMany(source => source.AccessAssemblies)).Concat(operationAccessAssemblies ?? Array.Empty<string>()).Concat(new[] { product.Name.Name }).Distinct().ToArray();
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
        if(models.Any(model=>TypeSupport.SequenceBytes(model.Type is ArrayType,model.SequenceElement?.FullName ?? "")))
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
            GeneratedArgumentCapture.Register(definitions,module,owner,il,product,model);
            GeneratedSqlRows.Register(module, owner, il, product, model, index, models);
            GeneratedRelations.Register(definitions,module, owner, il, product, model, index);
            GeneratedCloning.Register(definitions,module, owner, il, product, model, index);
            GeneratedCustomIdentity.Register(definitions,module, owner, il, product, model, index,models);
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
            il.Emit(model.Facts.StandardComparison ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Call,api.Metadata("RegisterStandardComparison",type));
            if (model.Facts.CustomComparison.HasValue)
            {
                il.Emit(model.Facts.CustomComparison.Value ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
                il.Emit(OpCodes.Call,api.Metadata("RegisterCustomComparison",type));
            }
            RegisterFacts(il,model,api);
            var memberWitnesses = model.Members.Select((member,index) => (
                    Owner: member.Getter?.DeclaringType ?? member.Setter?.DeclaringType ?? member.Field?.DeclaringType,
                    Name: model.Facts.Members[index].Name))
                .Where(member => member.Owner != null)
                .Select(member => new GeneratedQueryWitness(member.Name.Replace('.','#'),member.Owner!));
            var definition = type is GenericInstanceType original ? original.ElementType.FullName : type.FullName;
            var constructionWitnesses = TypeSupport.QueryConstructionMembers(model.Facts.Shape,definition)
                .Select(name => new GeneratedQueryWitness(name,type));
            GeneratedQueries.Preserve(definitions,module,il.Body.Method,product,memberWitnesses.Concat(constructionWitnesses).ToArray());
            var decision = TypeSupport.ForDefinition(model.Facts.Shape,type.FullName);
            var preparation = CodecPreparation.factory(definition,decision,model.Facts.AbstractType);
            var arguments = preparation.Arguments switch {
                CodecArguments.None => Array.Empty<TypeReference>(),
                CodecArguments.Target => new[] { type },
                CodecArguments.Element => new[] { ((ArrayType)type).ElementType },
                CodecArguments.Generic => closedArguments,
                CodecArguments.TargetAndUnderlying => new[] { type,module.ImportReference(model.ElementType!) },
                CodecArguments.TargetAndContract => new[] { type }.Concat(model.CollectionArguments).ToArray(),
                _ => throw new InvalidOperationException("Unknown codec argument source.")
            };
            void Factory() {
                il.Emit(OpCodes.Call,api.Metadata("Create" + preparation.Factory,arguments));
                il.Emit(OpCodes.Call,api.Metadata("RegisterGenerated",type));
            }
            switch (preparation.Kind)
            {
                case CodecFactoryKind.Numeric: continue; // Published before dependency-acquiring factories.
                case CodecFactoryKind.Tuple:
                    GeneratedTuples.Register(definitions,module,owner,il,product,model,index,models); continue;
                case CodecFactoryKind.ReferenceSequence:
                    Factory();
                    GeneratedSequenceCodec.RegisterReferenceReaders(definitions,module,owner,il,product,model,index,models); continue;
                case CodecFactoryKind.Grouping:
                    Factory();
                    GeneratedSequenceCodec.RegisterGroupingReaders(definitions,module,owner,il,product,model,index,models);
                    GeneratedRawCodec.RegisterFactoryWriters(definitions,module,owner,il,product,model,index,models); continue;
                case CodecFactoryKind.GroupingImplementation:
                    Factory();
                    GeneratedSequenceCodec.RegisterGroupingReaders(definitions,module,owner,il,product,model,index,models);
                    GeneratedSequenceCodec.RegisterWriters(definitions,module,owner,il,product,model.Type,model.CollectionArguments[preparation.ElementArgument],model.Type.IsValueType,"WriteGrouping" + index,models); continue;
                case CodecFactoryKind.Sequence:
                    Factory();
                    GeneratedSequenceCodec.Register(definitions,module,owner,il,product,model,index,models,preparation.ReadersOnly);
                    if (preparation.ReadersOnly) GeneratedRawCodec.RegisterFactoryWriters(definitions,module,owner,il,product,model,index,models);
                    continue;
                case CodecFactoryKind.Dictionary:
                    GeneratedDictionaryCodec.RegisterTree(definitions,module,owner,il,product,model,index,models); continue;
                case CodecFactoryKind.Collection:
                case CodecFactoryKind.Intrinsic:
                    if (preparation.ArrayWrapper) il.Emit(model.Facts.SupportsValuesWrapper ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
                    il.Emit(OpCodes.Call,api.Metadata(preparation.Registration,arguments));
                    if (preparation.Optional != 0) GeneratedOptionalCodec.Register(definitions,module,owner,il,product,type,index,models);
                    if (preparation.ArrayBodies)
                    {
                        GeneratedRawCodec.Register(definitions,module,owner,il,product,model,index,models);
                        GeneratedSequenceCodec.RegisterRangeArrayBodies(definitions,module,owner,il,product,((ArrayType)model.Type).ElementType,
                            model.Facts.SupportsValuesWrapper,"ReadArrayWrapper" + index,models);
                    }
                    if (preparation.ReadersOnly) GeneratedRawCodec.RegisterFactoryWriters(definitions,module,owner,il,product,model,index,models);
                    continue;
                case CodecFactoryKind.Model:
                    RegisterObject(definitions,module,owner,il,product,model,index,models); break;
                default: throw new InvalidOperationException("Unknown codec registration kind.");
            }

        }
        GeneratedStoredCodec.Register(definitions,module,owner,il,product,models,startIndex+models.Length);
    }

    private static void GuardRegistration(ModuleDefinition module, TypeDefinition owner, MethodDefinition core,MethodDefinition? entry = null,MethodReference? enter = null,MethodReference? exit = null)
    {
        core.Name = "RegisterCore";
        core.Attributes = MethodAttributes.Private | MethodAttributes.Static;
        var gate = new FieldDefinition("gate", FieldAttributes.Private | FieldAttributes.Static | FieldAttributes.InitOnly, module.TypeSystem.Object);
        var complete = new FieldDefinition("complete", FieldAttributes.Private | FieldAttributes.Static, module.TypeSystem.Boolean);
        owner.Fields.Add(gate); owner.Fields.Add(complete);
        var initialize = Method(owner, ".cctor", module.TypeSystem.Void);
        initialize.Attributes = MethodAttributes.Private | MethodAttributes.Static | MethodAttributes.SpecialName | MethodAttributes.RTSpecialName;
        var constructor = new MethodReference(".ctor", module.TypeSystem.Void, owner.BaseType) { HasThis = true };
        RegistrationEmitter.initialize(new CecilCodecIL(initialize),constructor,gate);
        entry ??= Method(owner, "register", module.TypeSystem.Void);
        var monitor = new TypeReference("System.Threading", "Monitor", module, owner.BaseType.Scope);
        MethodReference Lock(string name)
        {
            var method = new MethodReference(name, module.TypeSystem.Void, monitor);
            method.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));
            return method;
        }
        RegistrationEmitter.once(new CecilCodecIL(entry),gate,complete,enter ?? Lock("Enter"),exit ?? Lock("Exit"),core);
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

    internal static void BoxedWriters(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,
        ILProcessor entry,AssemblyDefinition product,TypeReference target)
    {
        GenericInstanceType Callable(string name,TypeReference element)
        {
            var type=new GenericInstanceType(module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator."+name)));
            type.GenericArguments.Add(module.ImportReference(target));type.GenericArguments.Add(element);return type;
        }
        var byteWriter=Callable("JsonDirectWriter`2",module.TypeSystem.Byte);
        var charWriter=Callable("JsonDirectWriter`2",module.TypeSystem.Char);
        var bytes=new VariableDefinition(byteWriter);var chars=new VariableDefinition(charWriter);
        entry.Body.Variables.Add(bytes);entry.Body.Variables.Add(chars);entry.Body.InitLocals=true;
        entry.Emit(OpCodes.Stloc,chars);entry.Emit(OpCodes.Stloc,bytes);
        entry.Emit(OpCodes.Ldloc,bytes);entry.Emit(OpCodes.Ldloc,chars);
        foreach(var element in new[]{module.TypeSystem.Byte,module.TypeSystem.Char})
        {
            var typed=element.MetadataType==MetadataType.Byte?byteWriter:charWriter;
            var runtime=new GenericInstanceType(module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonDirectRuntimeWriter`1")));
            runtime.GenericArguments.Add(element);
            if(!definitions.TryGet("boxed-writer",target,element,"",out var bridge))
            {
                var original=product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonDirectWriter`2").Methods.Single(method=>method.Name=="Invoke");
                var span=(GenericInstanceType)((ByReferenceType)original.Parameters[1].ParameterType).ElementType;
                var state=(GenericInstanceType)((ByReferenceType)original.Parameters[2].ParameterType).ElementType;
                TypeReference Closed(GenericInstanceType type)
                {
                    var closed=new GenericInstanceType(module.ImportReference(type.ElementType));closed.GenericArguments.Add(element);return new ByReferenceType(closed);
                }
                bridge=new MethodDefinition(definitions.MethodName("WriteBoxed"),MethodAttributes.Assembly|MethodAttributes.Static|MethodAttributes.HideBySig,module.TypeSystem.Void);
                foreach(var argument in new[]{(TypeReference)typed,module.TypeSystem.Object,Closed(span),Closed(state)})bridge.Parameters.Add(new ParameterDefinition(argument));
                owner.Methods.Add(bridge);definitions.Add("boxed-writer",target,element,"",bridge);
                var invoke=module.ImportReference(original);invoke.DeclaringType=typed;
                CodecEmitter.boxedRawWriter(new CecilCodecIL(bridge),target,target.IsValueType,invoke);
            }
            entry.Emit(OpCodes.Ldloc,element.MetadataType==MetadataType.Byte?bytes:chars);
            entry.Emit(OpCodes.Ldftn,bridge);
            var constructor=new MethodReference(".ctor",module.TypeSystem.Void,runtime){HasThis=true};
            constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
            entry.Emit(OpCodes.Newobj,constructor);
        }
    }

}
