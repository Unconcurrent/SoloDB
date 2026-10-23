using System.Collections.Generic;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;

namespace SoloDatabase.Metadata.Tools;

public sealed class GeneratedQueryOperation
{
    public GeneratedQueryOperation(bool sequence, TypeReference source, TypeReference arguments, TypeReference result)
    { Sequence = sequence; Source = source; Arguments = arguments; Result = result; }
    public bool Sequence { get; }
    public TypeReference Source { get; }
    public TypeReference Arguments { get; }
    public TypeReference Result { get; }
}

public sealed class GeneratedProviderOperation
{
    public GeneratedProviderOperation(bool sequence, TypeReference source, TypeReference result)
    { Sequence = sequence; Source = source; Result = result; }
    public bool Sequence { get; }
    public TypeReference Source { get; }
    public TypeReference Result { get; }
}

public sealed class GeneratedQueryWitness
{
    public GeneratedQueryWitness(string signature, TypeReference type)
    { Signature = signature; Type = type; }
    public string Signature { get; }
    public TypeReference Type { get; }
}

public sealed class GeneratedQueryLambda
{
    public GeneratedQueryLambda(TypeReference type, TypeReference[] parameters)
        : this(type,parameters,true) { }
    public GeneratedQueryLambda(TypeReference type, TypeReference[] parameters, bool expression)
    { Type = type; Parameters = parameters; Expression = expression; }
    public TypeReference Type { get; }
    public TypeReference[] Parameters { get; }
    public bool Expression { get; }
    public string Identity => GeneratedJsonDefinitions.Identity(Type);
}

internal static class GeneratedQueries
{
    internal static void RegisterParameterFacts(ModuleDefinition module, ILProcessor il, AssemblyDefinition product,
        System.Collections.Generic.IEnumerable<TypeReference> parameters, System.Collections.Generic.HashSet<string> sequences,
        System.Collections.Generic.HashSet<string> carriers)
    {
        var api = new GeneratedJsonAssembly.Api(module,product);
        void RegisterCarrier(TypeReference parameter)
        {
            if (parameter.IsByReference || parameter.IsPointer || !carriers.Add(GeneratedJsonDefinitions.Identity(parameter))) return;
            var carrier = new GenericInstanceMethod(module.ImportReference(product.MainModule.GetType("SoloDatabase.QueryValueMetadata")
                .Methods.Single(method => method.Name == "RegisterQuotedParameter")));
            carrier.GenericArguments.Add(module.ImportReference(parameter)); il.Emit(OpCodes.Call,carrier);
        }
        RegisterCarrier(module.TypeSystem.Object);
        foreach (var type in parameters)
        {
            RegisterCarrier(type);
            TypeReference? element = type is ArrayType array ? array.ElementType :
                type is GenericInstanceType generic && generic.ElementType.FullName is
                    "System.Collections.Generic.IEnumerable`1" or "System.Linq.IOrderedEnumerable`1" or
                    "System.Linq.IQueryable`1" or "System.Linq.IOrderedQueryable`1" ? generic.GenericArguments[0] : null;
            if (element == null || !sequences.Add(GeneratedJsonDefinitions.Identity(type))) continue;
            api.TypeToken(il,type); api.TypeToken(il,element);
            il.Emit(OpCodes.Call,api.ModelRegistration("RegisterSequence"));
        }
    }
    private static void WitnessTarget(CustomAttribute attribute, ModuleDefinition module, TypeReference type)
    {
        // String targets survive the FSharp generated-assembly linker for nested generic types.
        var definition = type.GetElementType();
        var assembly = definition.Scope is AssemblyNameReference reference ? reference.Name : ((ModuleDefinition)definition.Scope).Assembly.Name.Name;
        attribute.ConstructorArguments.Add(new CustomAttributeArgument(module.TypeSystem.String,definition.FullName.Replace('/','.')));
        attribute.ConstructorArguments.Add(new CustomAttributeArgument(module.TypeSystem.String,assembly));
    }
    internal static void RegisterProviders(ModuleDefinition module, ILProcessor il, AssemblyDefinition product, GeneratedProviderOperation[] operations)
    {
        var api = product.MainModule.GetType("SoloDatabase.QueryMetadata");
        foreach (var operation in operations)
        {
            var method = new GenericInstanceMethod(module.ImportReference(api.Methods.Single(candidate =>
                candidate.Name == (operation.Sequence ? "RegisterSequence" : "RegisterResult"))));
            method.GenericArguments.Add(module.ImportReference(operation.Source));
            method.GenericArguments.Add(module.ImportReference(operation.Result));
            il.Emit(OpCodes.Call, method);
        }
    }

    internal static void RegisterExpressions(ModuleDefinition module, ILProcessor il, AssemblyDefinition product, GeneratedQueryLambda[] lambdas)
    {
        var sequenceFacts = new System.Collections.Generic.HashSet<string>(System.StringComparer.Ordinal);
        var carrierFacts = new System.Collections.Generic.HashSet<string>(System.StringComparer.Ordinal);
        var standard = il.Body.Method.DeclaringType.BaseType.Scope;
        var type = new TypeReference("System", "Type", module, standard);
        var fromHandle = new MethodReference("GetTypeFromHandle", type, type);
        fromHandle.Parameters.Add(new ParameterDefinition(new TypeReference("System", "RuntimeTypeHandle", module, standard, true)));
        foreach (var lambda in lambdas)
        {
            RegisterParameterFacts(module,il,product,lambda.Parameters,sequenceFacts,carrierFacts);
            if (!lambda.Expression) continue;
            il.Emit(OpCodes.Ldc_I4, lambda.Parameters.Length); il.Emit(OpCodes.Newarr, type);
            for (var index = 0; index < lambda.Parameters.Length; index++)
            {
                il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4, index);
                il.Emit(OpCodes.Ldtoken, module.ImportReference(lambda.Parameters[index])); il.Emit(OpCodes.Call, fromHandle);
                il.Emit(OpCodes.Stelem_Ref);
            }
            var register = new GenericInstanceMethod(module.ImportReference(product.MainModule.GetType("SoloDatabase.QueryValueMetadata")
                .Methods.Single(method => method.Name == "RegisterExpressionLambda")));
            register.GenericArguments.Add(module.ImportReference(lambda.Type)); il.Emit(OpCodes.Call, register);
        }
    }

    internal static void PreserveMembers(ModuleDefinition module, MethodDefinition entry, AssemblyDefinition product, TypeReference type, int members)
    {
        var attribute=product.MainModule.GetType("System.Diagnostics.CodeAnalysis.DynamicDependencyAttribute");
        var constructor=module.ImportReference(attribute.Methods.Single(method => method.IsConstructor && method.Parameters.Count == 3 && method.Parameters[0].ParameterType.FullName == "System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes"));
        var dependency=new CustomAttribute(constructor);
        // The enum belongs to the product reference, not the tooling runtime.
        ((DefaultAssemblyResolver)module.AssemblyResolver).AddSearchDirectory(System.IO.Path.GetDirectoryName(product.MainModule.FileName)!);
        dependency.ConstructorArguments.Add(new CustomAttributeArgument(constructor.Parameters[0].ParameterType,members));
        WitnessTarget(dependency,module,type);
        entry.CustomAttributes.Add(dependency);
    }

    internal sealed class Preservation
    {
        private readonly ModuleDefinition module;
        private readonly MethodDefinition entry;
        private readonly MethodReference constructor;
        private readonly HashSet<(object?,object?,object?)> values = new();

        internal Preservation(ModuleDefinition module,MethodDefinition entry,AssemblyDefinition product)
        {
            this.module = module;
            this.entry = entry;
            var attribute = product.MainModule.GetType("System.Diagnostics.CodeAnalysis.DynamicDependencyAttribute");
            constructor = module.ImportReference(attribute.Methods.Single(method => method.IsConstructor && method.Parameters.Count == 3 && method.Parameters[0].ParameterType.FullName == "System.String"));
            var signature = constructor.FullName;
            var scope = constructor.DeclaringType.Scope.ToString();
            foreach (var existing in entry.CustomAttributes)
                if (existing.Constructor.FullName == signature && existing.Constructor.DeclaringType.Scope.ToString() == scope &&
                    existing.ConstructorArguments.Count == 3)
                    values.Add((existing.ConstructorArguments[0].Value,existing.ConstructorArguments[1].Value,existing.ConstructorArguments[2].Value));
        }

        internal void Add(GeneratedQueryWitness[] witnesses)
        {
            foreach (var witness in witnesses)
            {
                var dependency = new CustomAttribute(constructor);
                dependency.ConstructorArguments.Add(new CustomAttributeArgument(module.TypeSystem.String,witness.Signature));
                WitnessTarget(dependency,module,witness.Type);
                var key = (dependency.ConstructorArguments[0].Value,dependency.ConstructorArguments[1].Value,dependency.ConstructorArguments[2].Value);
                if (values.Contains(key)) continue;
                entry.CustomAttributes.Add(dependency);
                values.Add(key);
            }
        }
    }

    internal static void Preserve(GeneratedJsonDefinitions definitions,ModuleDefinition module,MethodDefinition entry,
        AssemblyDefinition product,GeneratedQueryWitness[] witnesses)
        => definitions.Preservation(module,entry,product).Add(witnesses);

    internal static void Register(ModuleDefinition module, ILProcessor il, AssemblyDefinition product, GeneratedQueryOperation[] operations)
    {
        var api = product.MainModule.GetType("SoloDatabase.QueryMetadata");
        foreach (var operation in operations)
        {
            var method = new GenericInstanceMethod(module.ImportReference(api.Methods.Single(candidate =>
                candidate.Name == (operation.Sequence ? "RegisterCompiledSequence" : "RegisterCompiledResult"))));
            method.GenericArguments.Add(module.ImportReference(operation.Source));
            method.GenericArguments.Add(module.ImportReference(operation.Arguments));
            method.GenericArguments.Add(module.ImportReference(operation.Result));
            il.Emit(OpCodes.Call, method);
        }
    }
}
