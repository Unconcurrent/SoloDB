using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using Microsoft.Build.Framework;
using Mono.Cecil;
using Mono.Cecil.Cil;

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
        using var helper = Create(identity);
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

    internal static MethodReference IdentityConstructor(ModuleDefinition module, PropertyDefinition property, out string assemblyName)
    {
        var attribute = property.CustomAttributes.Single(attribute => attribute.AttributeType.FullName == "SoloDatabase.Attributes.SoloId");
        var generator = (TypeReference)attribute.ConstructorArguments[0].Value;
        var definition = generator.Resolve();
        assemblyName = definition.Module.Assembly.Name.Name;
        var constructor = definition.Methods.Single(method => method.IsConstructor && !method.IsStatic && method.Parameters.Count == 0);
        var call = module.ImportReference(constructor);
        call.DeclaringType = CSharpCodecFacts.ImportType(module,generator);
        return call;
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

    internal static AssemblyDefinition Create(string identity)
    {
        var assembly = AssemblyDefinition.CreateAssembly(new AssemblyNameDefinition(identity, new Version(1, 0, 0, 0)),
            identity + ".dll", ModuleKind.Dll);
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
