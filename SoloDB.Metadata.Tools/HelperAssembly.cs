using System;
using System.IO;
using System.Xml;
using System.Xml.Linq;
using System.Linq;
using System.Collections.Generic;
using System.Security.Cryptography;
using Microsoft.Build.Framework;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

internal static class HelperAssembly
{
    internal sealed class OutputSymbolWriterProvider : ISymbolWriterProvider
    {
        private readonly ISymbolWriterProvider provider;
        private readonly string output;
        public OutputSymbolWriterProvider(ISymbolWriterProvider provider, string output)
        { this.provider = provider; this.output = output; }
        public ISymbolWriter GetSymbolWriter(ModuleDefinition module, string fileName) =>
            provider.GetSymbolWriter(module, output);
        public ISymbolWriter GetSymbolWriter(ModuleDefinition module, Stream stream) =>
            provider.GetSymbolWriter(module, stream);
    }

    internal static bool HasCompilerSymbols(ModuleDefinition module) => module.HasDebugHeader &&
        module.GetDebugHeader().Entries.Any(entry => entry.Directory.Type == ImageDebugType.CodeView ||
            entry.Directory.Type == ImageDebugType.EmbeddedPortablePdb);

    internal static void WriteContract(string output, string identity, string? keyFile, ITaskItem[] references)
        => WriteContract(output,identity,keyFile,references,false,false);

    internal static void WriteContract(string output, string identity, string? keyFile, ITaskItem[] references, bool publicSign, bool delaySign)
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
        Write(assembly, output, Signing(assembly,keyFile,publicSign,delaySign));
    }

    internal static bool WriteImplementation(string input, string output, string identity, string? keyFile, ITaskItem[] references)
        => WriteImplementation(input,output,identity,keyFile,references,false,false);

    internal static bool WriteImplementation(string input, string output, string identity, string? keyFile, ITaskItem[] references, bool publicSign, bool delaySign, ITaskItem[]? compilerReferences = null)
    {
        using var resolver = new DefaultAssemblyResolver();
        resolver.AddSearchDirectory(Path.GetDirectoryName(Path.GetFullPath(input))!);
        foreach (var reference in references.Concat(compilerReferences ?? Array.Empty<ITaskItem>())) resolver.AddSearchDirectory(Path.GetDirectoryName(Path.GetFullPath(reference.ItemSpec))!);
        using var consumer = AssemblyDefinition.ReadAssembly(input, new ReaderParameters { ReadingMode = ReadingMode.Immediate, AssemblyResolver = resolver });
        if (!consumer.CustomAttributes.Any(attribute => attribute.AttributeType.FullName == "SoloDatabase.Generated.MetadataActivationAttribute"))
        {
            if (File.Exists(output)) File.Delete(output);
            return false;
        }
        var runtimePath = references.Select(reference => reference.ItemSpec).First(path => Path.GetFileName(path) == "SoloDB.dll");
        using var product = AssemblyDefinition.ReadAssembly(runtimePath);
        var compilerPaths = (compilerReferences ?? references).Select(reference => reference.ItemSpec).ToArray();
        using var imports = new TargetReflectionImports(compilerPaths,resolver,product.MainModule);
        using var helper = Create(identity,resolver,imports);
        var module = helper.MainModule;
        var entry = EntryPoint(module);
        var runtime = product.Name.Name;
        var il = entry.Body.GetILProcessor();
        var access = new HashSet<string>(StringComparer.Ordinal) { runtime };
        var definitions = new GeneratedJsonDefinitions(compilerPaths);
        CSharpQueryOperations.RegisterArguments(definitions,consumer,product,module,entry.DeclaringType,il,name=>access.Add(name));
        CSharpCodecFacts.Register(definitions,consumer, product, module, entry.DeclaringType, il, name => access.Add(name));
        CSharpQueryOperations.Register(definitions,consumer, product, module, entry.DeclaringType, il, name => access.Add(name));
        if (access.Count != 0) AllowAccess(module, access, entry.DeclaringType.BaseType.Scope);
        il.Emit(OpCodes.Ret);
        Write(helper, output, Signing(helper,keyFile,publicSign,delaySign,consumer));
        return true;
    }

    private static string? Signing(AssemblyDefinition image,string? keyFile,bool publicSign,bool delaySign,AssemblyDefinition? consumer = null)
    {
        if (string.IsNullOrEmpty(keyFile) || (!publicSign && !delaySign)) return keyFile;
        var key = consumer?.Name.PublicKey;
        if (key == null)
        {
            key = File.ReadAllBytes(keyFile);
            // CAPI PRIVATEKEYBLOB inputs are also accepted by the CSharp compiler.
            if (key.Length >= 2 && key[0] == 7 && key[1] == 2) key = SigningPublicKey(key);
        }
        image.Name.PublicKey = key;
        var signed = consumer == null ? publicSign : (consumer.MainModule.Attributes & ModuleAttributes.StrongNameSigned) != 0;
        if (signed) image.MainModule.Attributes |= ModuleAttributes.StrongNameSigned;
        else image.MainModule.Attributes &= ~ModuleAttributes.StrongNameSigned;
        return null;
    }

    internal static byte[] SigningPublicKey(byte[] bytes)
    {
        using var rsa = new RSACryptoServiceProvider();
        rsa.ImportCspBlob(bytes);
        if (rsa.PublicOnly) throw new CryptographicException("Full signing requires a private key pair.");
        var key = rsa.ExportCspBlob(false);
        // CLR framing of the framework-acquired CAPI public key.
        using var buffer = new MemoryStream(12 + key.Length);
        using (var writer = new BinaryWriter(buffer,System.Text.Encoding.UTF8,true))
        {
            writer.Write(0x2400); writer.Write(0x8004); writer.Write(key.Length);
            writer.Write(key,0,4); writer.Write(0x2400); writer.Write(key,8,key.Length - 8);
        }
        return buffer.ToArray();
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

    internal static AssemblyDefinition Create(string identity,IAssemblyResolver? resolver = null,IReflectionImporterProvider? reflection = null)
    {
        var parameters = new ModuleParameters { Kind = ModuleKind.Dll, ReflectionImporterProvider = reflection };
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

    internal static MethodDefinition EntryPoint(ModuleDefinition module)
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
        void Compact(TypeDefinition type)
        {
            foreach (var method in type.Methods)
                if (method.HasBody) CompactBranches(method.Body);
            foreach (var nested in type.NestedTypes) Compact(nested);
        }
        foreach (var type in assembly.MainModule.Types) Compact(type);
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);
        var parameters = new WriterParameters { DeterministicMvid = true, Timestamp = 0 };
        if (!string.IsNullOrEmpty(keyFile)) parameters.StrongNameKeyBlob = File.ReadAllBytes(keyFile);
        using var bytes = new MemoryStream();
        assembly.Write(bytes, parameters);
        var content = bytes.ToArray();
        Publish(path, content);
    }

    private static void CompactBranches(MethodBody body)
    {
        var offset = 0;
        foreach (var instruction in body.Instructions)
        {
            instruction.Offset = offset;
            offset += instruction.GetSize();
        }
        // Use the original offsets throughout. Other contractions only reduce
        // these spans, so every admitted short branch remains in range.
        foreach (var instruction in body.Instructions)
        {
            if (instruction.OpCode.OperandType != OperandType.InlineBrTarget) continue;
            var distance = ((Instruction)instruction.Operand).Offset - (instruction.Offset + instruction.OpCode.Size + 4);
            if (distance < sbyte.MinValue || distance > sbyte.MaxValue) continue;
            instruction.OpCode = instruction.OpCode.Code switch {
                Code.Br => OpCodes.Br_S, Code.Brfalse => OpCodes.Brfalse_S, Code.Brtrue => OpCodes.Brtrue_S,
                Code.Beq => OpCodes.Beq_S, Code.Bge => OpCodes.Bge_S, Code.Bgt => OpCodes.Bgt_S,
                Code.Ble => OpCodes.Ble_S, Code.Blt => OpCodes.Blt_S, Code.Bne_Un => OpCodes.Bne_Un_S,
                Code.Bge_Un => OpCodes.Bge_Un_S, Code.Bgt_Un => OpCodes.Bgt_Un_S,
                Code.Ble_Un => OpCodes.Ble_Un_S, Code.Blt_Un => OpCodes.Blt_Un_S,
                Code.Leave => OpCodes.Leave_S, _ => instruction.OpCode
            };
        }
    }
    internal static void Publish(string path, byte[] content)
    {
        if (!File.Exists(path) || !File.ReadAllBytes(path).SequenceEqual(content)) File.WriteAllBytes(path, content);
    }

    internal static void Publish(string path, XElement state)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);
        using var bytes = new MemoryStream();
        using (var writer = XmlWriter.Create(bytes, new XmlWriterSettings { NewLineHandling = NewLineHandling.Entitize }))
            state.Save(writer);
        Publish(path, bytes.ToArray());
    }

}
