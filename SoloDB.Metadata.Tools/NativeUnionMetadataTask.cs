using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection.PortableExecutable;
using Microsoft.Build.Framework;
using Mono.Cecil;
using Mono.Cecil.Cil;

namespace SoloDatabase.Metadata.Tools;

/// Retains FSharp union field attributes in private native compiler inputs.
public sealed class PrepareNativeUnionMetadata : ITask
{
    public IBuildEngine BuildEngine { get; set; } = null!;
    public ITaskHost HostObject { get; set; } = null!;
    [Required] public string OutputDirectory { get; set; } = "";
    [Required] public ITaskItem[] References { get; set; } = Array.Empty<ITaskItem>();
    [Required] public ITaskItem[] Inputs { get; set; } = Array.Empty<ITaskItem>();
    [Output] public ITaskItem[] NativeReferences { get; private set; } = Array.Empty<ITaskItem>();
    [Output] public ITaskItem[] NativeInputs { get; private set; } = Array.Empty<ITaskItem>();

    private static IEnumerable<TypeDefinition> Types(TypeDefinition type)
    {
        yield return type;
        foreach (var nested in type.NestedTypes)
            foreach (var item in Types(nested)) yield return item;
    }
    private static bool Mapping(ICustomAttributeProvider owner, int kind) =>
        owner.CustomAttributes.Any(attribute => attribute.AttributeType.FullName == "Microsoft.FSharp.Core.CompilationMappingAttribute" &&
            attribute.ConstructorArguments.Count != 0 && (Convert.ToInt32(attribute.ConstructorArguments[0].Value) & 31) == kind);
    private static bool Union(TypeDefinition type) => Mapping(type, 1) || (type.IsNested && Union(type.DeclaringType));

    public bool Execute()
    {
        var assemblies = new Dictionary<string, AssemblyDefinition>(StringComparer.Ordinal);
        try
        {
            var paths = Inputs.Concat(References).Select(item => Path.GetFullPath(item.ItemSpec)).Distinct(StringComparer.Ordinal).ToArray();
            using var resolver = new DefaultAssemblyResolver();
            foreach (var directory in paths.Select(Path.GetDirectoryName).Distinct()) resolver.AddSearchDirectory(directory!);
            foreach (var path in paths)
            {
                using var stream = File.OpenRead(path);
                using var image = new PEReader(stream);
                var header = image.PEHeaders.PEHeader;
                if (header != null && header.CorHeaderTableDirectory.RelativeVirtualAddress == 0 && header.CorHeaderTableDirectory.Size == 0)
                    continue;
                assemblies.Add(path, AssemblyDefinition.ReadAssembly(path, new ReaderParameters { AssemblyResolver = resolver }));
            }
            // Authored dependencies and generated witnesses identify the selected union metadata.
            var targets = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
            foreach (var assembly in assemblies.Values.Where(assembly => assembly.Name.Name == "SoloDB" || assembly.MainModule.AssemblyReferences.Any(reference => reference.Name == "SoloDB")))
                foreach (var type in assembly.MainModule.Types.SelectMany(Types))
                    foreach (var method in type.Methods)
                        foreach (var attribute in method.CustomAttributes.Where(attribute => attribute.AttributeType.FullName == "System.Diagnostics.CodeAnalysis.DynamicDependencyAttribute"))
                        {
                            var arguments = attribute.ConstructorArguments;
                            string name, identity;
                            if (arguments.Count == 3 && arguments[1].Value is string named && arguments[2].Value is string namedAssembly)
                            {
                                name = named;
                                identity = namedAssembly;
                            }
                            else if (arguments.Count == 2 && arguments[1].Value is TypeReference target)
                            {
                                var definition = target is GenericInstanceType generic ? generic.ElementType : target;
                                name = definition.FullName.Replace('/', '.');
                                identity = definition.Scope switch
                                {
                                    AssemblyNameReference reference => reference.Name,
                                    ModuleDefinition module => module.Assembly.Name.Name,
                                    _ => ""
                                };
                                if (identity.Length == 0) continue;
                            }
                            else continue;
                            if (!targets.TryGetValue(identity, out var names)) targets.Add(identity, names = new HashSet<string>(StringComparer.Ordinal));
                            names.Add(name);
                        }
            var replacements = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var item in assemblies)
            {
                var assembly = item.Value;
                if (assembly.Name.Name == "FSharp.Core" || !targets.TryGetValue(assembly.Name.Name, out var names)) continue;
                bool Selected(TypeDefinition type) => names.Contains(type.FullName.Replace('/', '.')) || (type.IsNested && Selected(type.DeclaringType));
                var accessors = assembly.MainModule.Types.SelectMany(Types).Where(type => Selected(type) && Union(type))
                    .SelectMany(type => type.Properties.Where(property => Mapping(property, 4)))
                    .SelectMany(property => new[] { property.GetMethod, property.SetMethod })
                    .Where(method => method != null && !method.IsSpecialName).ToArray();
                if (accessors.Length == 0) continue;
                if (assembly.Name.HasPublicKey || (assembly.MainModule.Attributes & ModuleAttributes.StrongNameSigned) != 0)
                {
                    BuildEngine.LogErrorEvent(new BuildErrorEventArgs("SoloDB", "SOLO005", item.Key, 0, 0, 0, 0,
                        "Native quotation field metadata cannot be corrected for signed model assembly '" + assembly.Name.Name + "'. Its original assembly remains unchanged.", null, nameof(PrepareNativeUnionMetadata)));
                    return false;
                }
                // Re-read with symbols only for an assembly that actually needs correction.
                var readSymbols = HelperAssembly.HasCompilerSymbols(assembly.MainModule);
                using var corrected = AssemblyDefinition.ReadAssembly(item.Key, new ReaderParameters
                {
                    AssemblyResolver = resolver, ReadSymbols = readSymbols, SymbolReaderProvider = readSymbols ? new DefaultSymbolReaderProvider(false) : null
                });
                var tokens = new HashSet<int>(accessors.Select(method => method.MetadataToken.ToInt32()));
                foreach (var type in corrected.MainModule.Types.SelectMany(Types))
                    foreach (var method in type.Methods)
                        if (tokens.Contains(method.MetadataToken.ToInt32())) method.IsSpecialName = true;
                Directory.CreateDirectory(OutputDirectory);
                var output = Path.Combine(OutputDirectory, Path.GetFileName(item.Key));
                using var image = new MemoryStream();
                using var symbols = corrected.MainModule.SymbolReader is PortablePdbReader ? new MemoryStream() : null;
                corrected.Write(image, new WriterParameters
                {
                    DeterministicMvid = true, Timestamp = 0,
                    WriteSymbols = corrected.MainModule.HasSymbols,
                    SymbolWriterProvider = corrected.MainModule.HasSymbols
                        ? new HelperAssembly.OutputSymbolWriterProvider(corrected.MainModule.SymbolReader.GetWriterProvider(), output) : null,
                    SymbolStream = symbols
                });
                if (symbols != null) HelperAssembly.Publish(Path.ChangeExtension(output, ".pdb"), symbols.ToArray());
                HelperAssembly.Publish(output, image.ToArray());
                replacements.Add(item.Key, output);
            }
            void Replace(ITaskItem item)
            {
                if (replacements.TryGetValue(Path.GetFullPath(item.ItemSpec), out var output)) item.ItemSpec = output;
            }
            foreach (var item in References) Replace(item);
            foreach (var item in Inputs) Replace(item);
            NativeReferences = References;
            NativeInputs = Inputs;
            return true;
        }
        catch (Exception error)
        {
            BuildEngine.LogErrorEvent(new BuildErrorEventArgs("SoloDB", "SOLO005", "", 0, 0, 0, 0, error.ToString(), null, nameof(PrepareNativeUnionMetadata)));
            return false;
        }
        finally { foreach (var assembly in assemblies.Values) assembly.Dispose(); }
    }
}
