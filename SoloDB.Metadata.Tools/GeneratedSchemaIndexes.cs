using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;

namespace SoloDatabase.Metadata.Tools;

// Declares callable factories; all construction instructions have one FSharp owner.
internal static class GeneratedSchemaIndexes
{
    internal static void Register(ModuleDefinition module,TypeDefinition owner,ILProcessor il,
        AssemblyDefinition product,GeneratedModel model,int index)
    {
        var metadata = product.MainModule.GetType("SoloDatabase.SchemaMetadata");
        var propertyType = module.ImportReference(product.MainModule.GetType("SoloDatabase.SchemaProperty"));
        var propertyConstructor = module.ImportReference(product.MainModule.GetType("SoloDatabase.SchemaProperty").Methods.Single(method => method.IsConstructor));
        var properties = model.OriginalProperties!.Where(property => !property.Member.Static).ToArray();
        var api = new GeneratedJsonAssembly.Api(module,product);
        var objectArray = new ArrayType(module.TypeSystem.Object);
        il.Emit(OpCodes.Ldc_I4,properties.Length); il.Emit(OpCodes.Newarr,propertyType);
        for (var ordinal = 0; ordinal < properties.Length; ordinal++)
        {
            var property = properties[ordinal];
            var factory = new MethodDefinition("CreateSchemaAttributes" + index + "_" + ordinal,
                MethodAttributes.Private | MethodAttributes.Static,objectArray);
            owner.Methods.Add(factory);
            if (property.DeferredSchema)
            {
                var body = factory.Body.GetILProcessor();
                body.Emit(OpCodes.Ldtoken,module.ImportReference(property.Member.Getter?.Value.DeclaringType ?? property.Member.Setter!.Value.DeclaringType));
                body.Emit(OpCodes.Ldtoken,module.ImportReference(property.Member.Getter?.Value ?? property.Member.Setter!.Value));
                body.Emit(OpCodes.Call,module.ImportReference(metadata.Methods.Single(method => method.Name == "UncompletedAttributes")));
                body.Emit(OpCodes.Ret);
            }
            else Emit(module,product,factory,property.Attributes);
            il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4,ordinal); il.Emit(OpCodes.Ldstr,property.Member.Name);
            api.TypeToken(il,property.Member.Type);
            var function = api.Function(objectArray);
            var constructor = new MethodReference(".ctor",module.TypeSystem.Void,function) { HasThis = true };
            constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));
            constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
            il.Emit(OpCodes.Ldnull); il.Emit(OpCodes.Ldftn,factory); il.Emit(OpCodes.Newobj,constructor);
            il.Emit(OpCodes.Newobj,propertyConstructor); il.Emit(OpCodes.Stelem_Ref);
        }
        var register = new GenericInstanceMethod(module.ImportReference(metadata.Methods.Single(method => method.Name == "RegisterIndexFactories")));
        register.GenericArguments.Add(module.ImportReference(model.Type)); il.Emit(OpCodes.Call,register);
    }

    private static void Emit(ModuleDefinition module,AssemblyDefinition product,MethodDefinition factory,
        AttributeConstruction<TypeReference,MethodReference,FieldReference>[] attributes)
    {
        var schema = product.MainModule.GetType("SoloDatabase.SchemaMetadata");
        var api = new GeneratedJsonAssembly.Api(module,product);
        var getType = new MethodReference("GetTypeFromHandle",api.Type,api.Type);
        getType.Parameters.Add(new ParameterDefinition(new TypeReference("System","RuntimeTypeHandle",module,module.TypeSystem.CoreLibrary,true)));
        AttributeConstructionEmitter.factory(new CecilCodecIL(factory),
            new AttributeEmitterApi<TypeReference,MethodReference>(module.TypeSystem.Object,new ArrayType(module.TypeSystem.Object),
                new TypeReference("System","Exception",module,module.TypeSystem.CoreLibrary),getType,
                module.ImportReference(schema.Methods.Single(method => method.Name == "NamedAssignmentError"))),attributes);
    }

    internal static bool HasUncompleted(ModuleDefinition module,AssemblyDefinition product)
    {
        bool Has(TypeDefinition type) => type.Methods.Any(method => Pending(method,product)) || type.NestedTypes.Any(Has);
        return module.Types.Any(Has);
    }
    private static bool Pending(MethodDefinition method,AssemblyDefinition product) => method.HasBody && method.Body.Instructions.Any(instruction =>
        instruction.OpCode == OpCodes.Call && instruction.Operand is MethodReference call && !call.HasThis && call.GenericParameters.Count == 0 &&
        call.FullName == "System.Object[] SoloDatabase.SchemaMetadata::UncompletedAttributes(System.RuntimeTypeHandle,System.RuntimeMethodHandle)" &&
        call.DeclaringType.Scope is AssemblyNameReference assembly && assembly.FullName == product.Name.FullName);

    // Only explicit compiler placeholders are completed, from actual emitted property metadata.
    internal static void Complete(AssemblyDefinition consumer,AssemblyDefinition product,AssemblyDefinition template,System.Action<string> access)
    {
        System.Collections.Generic.IEnumerable<MethodDefinition> Factories(TypeDefinition type) =>
            type.Methods.Where(method => Pending(method,product)).Concat(type.NestedTypes.SelectMany(Factories));
        string Signature(MethodReference method,System.Func<TypeReference,string> identity) =>
            method.Name + ":" + method.CallingConvention + ":" + method.HasThis + ":" + method.GenericParameters.Count + ":" +
            identity(method.ReturnType) + ":" + string.Join(";",method.Parameters.Select(parameter => identity(parameter.ParameterType)));
        (TypeReference Owner,MethodReference Accessor,string Key) Source(MethodDefinition method)
        {
            var body = method.Body.Instructions.Where(instruction => instruction.OpCode != OpCodes.Nop).ToArray();
            if (body.Length != 4 || body[0].OpCode != OpCodes.Ldtoken || body[0].Operand is not TypeReference owner ||
                body[1].OpCode != OpCodes.Ldtoken || body[1].Operand is not MethodReference accessor || body[2].OpCode != OpCodes.Call || body[3].OpCode != OpCodes.Ret)
                throw new System.InvalidOperationException("Generated schema source operands are incomplete: " + method.FullName);
            if (!GeneratedJsonDefinitions.SameType(owner,accessor.DeclaringType))
                throw new System.InvalidOperationException("Generated schema accessor has a different source owner: " + method.FullName);
            return (owner,accessor,method.Name + ":" + GeneratedJsonDefinitions.Identity(owner) + ":" + Signature(accessor,OriginalIdentity));
        }
        TypeReference Definition(TypeReference reference)
        {
            var scope = reference.Scope is AssemblyNameReference assembly ? assembly.FullName
                : reference.Scope is ModuleDefinition module ? module.Assembly.Name.FullName : reference.Scope.Name;
            return scope == consumer.Name.FullName
                ? consumer.MainModule.GetType(reference.FullName) ?? throw new System.InvalidOperationException("The current compiler image has no signature type: " + reference.FullName)
                : SourceTypeIdentity.Definition(consumer.MainModule.AssemblyResolver.Resolve(AssemblyNameReference.Parse(scope)),reference.FullName);
        }
        string OriginalIdentity(TypeReference reference) => GeneratedJsonDefinitions.SignatureIdentity(reference,Definition);
        var expected = template.MainModule.Types.SelectMany(Factories).Select(method => Source(method).Key).ToArray();
        var actual = consumer.MainModule.Types.SelectMany(Factories).Select(method => Source(method).Key).ToArray();
        if (expected.Except(actual,System.StringComparer.Ordinal).Any() || actual.Except(expected,System.StringComparer.Ordinal).Any())
            throw new System.InvalidOperationException("Current generated schema factories do not match the compiler template.");
        void Complete(TypeDefinition type)
        {
            foreach (var method in type.Methods.Where(method => Pending(method,product)))
            {
                var source = Source(method);
                var owner = source.Owner;
                var root = owner is GenericInstanceType closed ? closed.ElementType : owner;
                var sourceAssembly = root.Scope is AssemblyNameReference assembly ? assembly.FullName
                    : root.Scope is ModuleDefinition sourceModule ? sourceModule.Assembly.Name.FullName : root.Scope.Name;
                if (sourceAssembly != consumer.Name.FullName)
                    throw new System.InvalidOperationException("Generated schema source does not belong to the current compiler image: " + owner.FullName);
                var original = consumer.MainModule.GetType(root.FullName) ?? throw new System.InvalidOperationException("The current compiler image has no schema source type: " + root.FullName);
                var signature = Signature(source.Accessor,OriginalIdentity);
                var candidates = original.Properties.Where(property =>
                    (property.GetMethod != null && Signature(property.GetMethod,OriginalIdentity) == signature) ||
                    (property.SetMethod != null && Signature(property.SetMethod,OriginalIdentity) == signature)).ToArray();
                if (candidates.Length != 1)
                    throw new System.InvalidOperationException("The original schema property is missing or ambiguous: " + original.FullName + " " + signature);
                var attributes = OriginalAttributeFacts.Read(consumer.MainModule,candidates[0],access);
                method.Body = new Mono.Cecil.Cil.MethodBody(method);
                Emit(consumer.MainModule,product,method,attributes);
            }
            foreach (var child in type.NestedTypes) Complete(child);
        }
        foreach (var type in consumer.MainModule.Types) Complete(type);
        if (HasUncompleted(consumer.MainModule,product)) throw new System.InvalidOperationException("Generated schema factories remain incomplete.");
    }
}
