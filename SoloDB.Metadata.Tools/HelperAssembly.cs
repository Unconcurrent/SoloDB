using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using Microsoft.Build.Framework;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

internal static class HelperAssembly
{
    internal static bool HasCompilerSymbols(ModuleDefinition module) => module.HasDebugHeader &&
        module.GetDebugHeader().Entries.Any(entry => entry.Directory.Type == ImageDebugType.CodeView ||
            entry.Directory.Type == ImageDebugType.EmbeddedPortablePdb);

    internal static void WriteContract(string output, string identity, string? keyFile, ITaskItem[] references)
    {
        // The reference contract is immutable across implementation refreshes.
        using var assembly = Create(identity);
        var method = EntryPoint(assembly.MainModule);
        var il = method.Body.GetILProcessor();
        il.Emit(OpCodes.Ldstr, "The generated SoloDB helper implementation was not delivered.");
        var module = assembly.MainModule;
        var standard = module.AssemblyReferences.First(reference => reference.Name == "netstandard");
        var exception = new TypeReference("System", "InvalidOperationException", module, standard);
        var constructor = new MethodReference(".ctor", method.ReturnType, exception) { HasThis = true };
        constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.String));
        il.Emit(OpCodes.Newobj, constructor);
        il.Emit(OpCodes.Throw);
        Write(assembly, output, keyFile);
    }

    internal static bool WriteImplementation(string input, string output, string identity, string? keyFile, ITaskItem[] references)
    {
        using var resolver = new DefaultAssemblyResolver();
        resolver.AddSearchDirectory(Path.GetDirectoryName(Path.GetFullPath(input))!);
        foreach (var reference in references) resolver.AddSearchDirectory(Path.GetDirectoryName(Path.GetFullPath(reference.ItemSpec))!);
        using var consumer = AssemblyDefinition.ReadAssembly(input, new ReaderParameters { ReadingMode = ReadingMode.Immediate, AssemblyResolver = resolver });
        if (!consumer.CustomAttributes.Any(attribute => attribute.AttributeType.FullName == "SoloDatabase.Generated.MetadataActivationAttribute"))
        {
            if (File.Exists(output)) File.Delete(output);
            return false;
        }
        using var helper = Create(identity,resolver);
        var module = helper.MainModule;
        var entry = EntryPoint(module);
        var runtimePath = references.Select(reference => reference.ItemSpec).First(path => Path.GetFileName(path) == "SoloDB.dll");
        using var product = AssemblyDefinition.ReadAssembly(runtimePath);
        var runtime = product.Name.Name;
        var il = entry.Body.GetILProcessor();
        var access = new HashSet<string>(StringComparer.Ordinal) { runtime };
        var definitions = new GeneratedJsonDefinitions(references.Select(reference=>reference.ItemSpec).ToArray());
        CSharpQueryOperations.RegisterArguments(definitions,consumer,product,module,entry.DeclaringType,il,name=>access.Add(name));
        CSharpCodecFacts.Register(definitions,consumer, product, module, entry.DeclaringType, il, name => access.Add(name));
        CSharpQueryOperations.Register(definitions,consumer, product, module, entry.DeclaringType, il, name => access.Add(name));
        if (access.Count != 0) AllowAccess(module, access, entry.DeclaringType.BaseType.Scope);
        il.Emit(OpCodes.Ret);
        Write(helper, output, keyFile);
        return true;
    }

    internal static (PropertyDefinition Property,TypeReference Owner)? ParentProperty(PropertyDefinition property,TypeReference owner)
    {
        var accessor = property.GetMethod ?? property.SetMethod;
        if (accessor == null || !accessor.IsVirtual) return null;
        foreach (var overridden in accessor.Overrides)
        {
            var original = overridden.Resolve();
            var inherited = original.DeclaringType.Properties.SingleOrDefault(candidate => candidate.GetMethod == original || candidate.SetMethod == original);
            if (inherited != null) return (inherited,OriginalArgumentSources.Close(overridden.DeclaringType,owner));
        }
        if (accessor.IsNewSlot) return null;
        var symbolic = property.DeclaringType.BaseType;
        for (var parent = symbolic == null ? null : OriginalArgumentSources.Close(symbolic,owner); parent != null;)
        {
            var definition = parent.Resolve();
            foreach (var candidate in definition.Properties)
            {
                var method = property.GetMethod != null ? candidate.GetMethod : candidate.SetMethod;
                if (method == null || !method.IsVirtual || method.Name != accessor.Name || method.Parameters.Count != accessor.Parameters.Count) continue;
                if (method.Parameters.Select(parameter => OriginalArgumentSources.Close(parameter.ParameterType,symbolic!))
                    .Zip(accessor.Parameters,(left,right) => SourceTypeIdentity.SameSignature(left,right.ParameterType,property.Module.AssemblyResolver)).All(equal => equal)) return (candidate,parent);
            }
            symbolic = definition.BaseType == null ? null : OriginalArgumentSources.Close(definition.BaseType,symbolic!);
            parent = definition.BaseType == null ? null : OriginalArgumentSources.Close(definition.BaseType,parent);
        }
        return null;
    }

    private static CustomAttribute? IdentityAttribute(PropertyDefinition property)
    {
        var own = property.CustomAttributes.SingleOrDefault(attribute => attribute.AttributeType.FullName == "SoloDatabase.Attributes.SoloId");
        if (own != null) return own;
        var parent = ParentProperty(property,property.DeclaringType);
        if (!parent.HasValue || parent.Value.Property.Name != property.Name) return null;
        var value = OriginalArgumentSources.Close(parent.Value.Property.PropertyType,parent.Value.Owner);
        return SourceTypeIdentity.SameSignature(value,property.PropertyType,property.Module.AssemblyResolver) ? IdentityAttribute(parent.Value.Property) : null;
    }

    internal static bool InheritedAttribute(TypeReference type,string name)
    {
        for (TypeReference? current = type; current != null;)
        {
            var definition = current.Resolve();
            if (definition.CustomAttributes.Any(attribute => attribute.AttributeType.FullName == name)) return true;
            current = definition.BaseType == null ? null : OriginalArgumentSources.Close(definition.BaseType,current);
        }
        return false;
    }

    internal static GeneratedIdGenerator? IdentityGenerator(ModuleDefinition module, PropertyDefinition property, Action<string> access)
    {
        var attribute = IdentityAttribute(property);
        if (attribute == null) return null;
        if (attribute.ConstructorArguments[0].Value is not TypeReference generator) return new GeneratedIdGenerator(null,null,false);
        var definition = generator.Resolve();
        var type = CSharpCodecFacts.ImportType(module,generator);
        access(definition.Module.Assembly.Name.Name);
        bool Family(TypeReference current) {
            var original = current.Resolve();
            return original.Interfaces.Any(contract => TypeSupport.IdentityGeneratorContract(contract.InterfaceType.FullName,contract.InterfaceType is ArrayType) || Family(contract.InterfaceType)) ||
                original.BaseType != null && Family(original.BaseType);
        }
        var constructor = definition.Methods.FirstOrDefault(method => method.IsConstructor && !method.IsStatic && method.IsPublic && method.Parameters.Count == 0);
        MethodReference? call = null;
        if (constructor != null) { call = module.ImportReference(constructor); call.DeclaringType = type; }
        return new GeneratedIdGenerator(type,call,Family(generator));
    }

    internal static void AllowAccess(ModuleDefinition module, IEnumerable<string> assemblies, IMetadataScope standard, MethodReference? constructor = null)
    {
        if (constructor == null)
        {
            var attribute = new TypeDefinition("System.Runtime.CompilerServices", "IgnoresAccessChecksToAttribute",
                TypeAttributes.NotPublic | TypeAttributes.Sealed, new TypeReference("System", "Attribute", module, standard));
            module.Types.Add(attribute);
            var definition = new MethodDefinition(".ctor", MethodAttributes.Public | MethodAttributes.HideBySig | MethodAttributes.SpecialName | MethodAttributes.RTSpecialName,
                module.TypeSystem.Void);
            definition.Parameters.Add(new ParameterDefinition(module.TypeSystem.String)); attribute.Methods.Add(definition);
            var baseConstructor = new MethodReference(".ctor", module.TypeSystem.Void, attribute.BaseType) { HasThis = true };
            var il = definition.Body.GetILProcessor(); il.Emit(OpCodes.Ldarg_0); il.Emit(OpCodes.Call, baseConstructor); il.Emit(OpCodes.Ret);
            constructor = definition;
        }
        foreach (var assembly in assemblies.OrderBy(name => name, StringComparer.Ordinal))
        {
            var granted = new CustomAttribute(constructor);
            granted.ConstructorArguments.Add(new CustomAttributeArgument(module.TypeSystem.String, assembly));
            module.Assembly.CustomAttributes.Add(granted);
        }
    }

    internal static AssemblyDefinition Create(string identity,IAssemblyResolver? resolver = null)
    {
        var parameters = new ModuleParameters { Kind = ModuleKind.Dll };
        if (resolver != null) parameters.AssemblyResolver = resolver;
        var assembly = AssemblyDefinition.CreateAssembly(new AssemblyNameDefinition(identity, new Version(1, 0, 0, 0)),
            identity + ".dll", parameters);
        // Build tooling runs on modern MSBuild; its runtime core library must not leak
        // into the generated netstandard2.0 helper.
        var module = assembly.MainModule;
        module.AssemblyReferences.Clear();
        var standard = new AssemblyNameReference("netstandard", new Version(2, 0, 0, 0))
        {
            PublicKeyToken = new byte[] { 0xcc, 0x7b, 0x13, 0xff, 0xcd, 0x2d, 0xdd, 0x51 }
        };
        module.AssemblyReferences.Add(standard);
        return assembly;
    }

    private static MethodDefinition EntryPoint(ModuleDefinition module)
    {
        var standard = module.AssemblyReferences.Single(reference => reference.Name == "netstandard");
        var owner = new TypeDefinition("SoloDatabase.Generated", "Registration",
            TypeAttributes.Public | TypeAttributes.Abstract | TypeAttributes.Sealed,
            new TypeReference("System", "Object", module, standard));
        module.Types.Add(owner);
        var entry = new MethodDefinition("Register", MethodAttributes.Public | MethodAttributes.Static | MethodAttributes.HideBySig,
            module.TypeSystem.Void);
        owner.Methods.Add(entry);
        return entry;
    }

    internal static void Write(AssemblyDefinition assembly, string path, string? keyFile)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);
        var parameters = new WriterParameters { DeterministicMvid = true, Timestamp = 0 };
        if (!string.IsNullOrEmpty(keyFile)) parameters.StrongNameKeyBlob = File.ReadAllBytes(keyFile);
        using var bytes = new MemoryStream();
        assembly.Write(bytes, parameters);
        var content = bytes.ToArray();
        if (!File.Exists(path) || !File.ReadAllBytes(path).SequenceEqual(content)) File.WriteAllBytes(path, content);
    }
}
