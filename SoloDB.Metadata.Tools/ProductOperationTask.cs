using System;
using System.IO;
using System.Linq;
using System.Xml.Linq;
using Microsoft.Build.Framework;
using Mono.Cecil;
using Mono.Cecil.Cil;

namespace SoloDatabase.Metadata.Tools;

public sealed class ProductOperationDependencies : ITask
{
    public IBuildEngine BuildEngine { get; set; } = null!;
    public ITaskHost HostObject { get; set; } = null!;
    [Required] public string InputFile { get; set; } = "";
    [Required] public string WorkingDirectory { get; set; } = "";
    [Output] public string[] Dependencies { get; private set; } = Array.Empty<string>();
    public bool Execute()
    {
        try
        {
            if (File.Exists(InputFile))
            {
                var state = XElement.Load(InputFile);
                var inputs = new FSharpCompilerInputs(state.Elements("argument").Select(value => value.Value).ToArray(),WorkingDirectory);
                Dependencies = inputs.Dependencies.Concat(state.Elements("dependency").Select(value => value.Value)).Distinct(StringComparer.Ordinal).ToArray();
            }
            return true;
        }
        catch (Exception error) { return FSharpBuildState.Error(BuildEngine,"SOLO005",InputFile,error); }
    }
}

public sealed class ProductOperationInputs : ITask
{
    public IBuildEngine BuildEngine { get; set; } = null!;
    public ITaskHost HostObject { get; set; } = null!;
    [Required] public string InputFile { get; set; } = "";
    [Required] public string Stamp { get; set; } = "";
    [Required] public string Selection { get; set; } = "";
    [Required] public string Input { get; set; } = "";
    [Required] public string OutputDirectory { get; set; } = "";
    [Required] public string WorkingDirectory { get; set; } = "";
    public string CompilerSymbolFile { get; set; } = "";
    public ITaskItem[] Arguments { get; set; } = Array.Empty<ITaskItem>();
    public ITaskItem[] References { get; set; } = Array.Empty<ITaskItem>();
    public ITaskItem[] ImplementationInputs { get; set; } = Array.Empty<ITaskItem>();
    [Output] public string[] Dependencies { get; private set; } = Array.Empty<string>();
    [Output] public string Assembly { get; private set; } = "";
    [Output] public string Symbols { get; private set; } = "";
    [Output] public string CompilerSymbols { get; private set; } = "";

    internal static string? Attribute(AssemblyDefinition image,string name) => image.CustomAttributes
        .Where(value => value.AttributeType.FullName == "System.Reflection." + name + "Attribute")
        .Select(value => (string)value.ConstructorArguments[0].Value).FirstOrDefault();

    public bool Execute()
    {
        try
        {
            var arguments = Arguments.Select(value => value.ItemSpec).ToArray();
            if (arguments.Length == 0 && File.Exists(InputFile)) arguments = XElement.Load(InputFile).Elements("argument").Select(value => value.Value).ToArray();
            if (arguments.Length == 0) throw new InvalidOperationException("The original product compiler arguments were not delivered.");
            var inputs = new FSharpCompilerInputs(arguments,WorkingDirectory);
            using var image = AssemblyDefinition.ReadAssembly(Input);
            var embedded = image.MainModule.HasDebugHeader && image.MainModule.GetDebugHeader().Entries.Any(value => value.Directory.Type == ImageDebugType.EmbeddedPortablePdb);
            var compilerSymbols = CompilerSymbolFile.Length != 0 ? CompilerSymbolFile : Path.ChangeExtension(Input,".pdb");
            CompilerSymbols = HelperAssembly.HasCompilerSymbols(image.MainModule) && !embedded && File.Exists(compilerSymbols) ? compilerSymbols : "";
            var key = inputs.Option("keyfile") ?? Attribute(image,"AssemblyKeyFile");
            Dependencies = inputs.Dependencies.Concat(key == null ? Array.Empty<string>() : new[] { inputs.Resolve(key) }).Distinct(StringComparer.Ordinal).ToArray();
            HelperAssembly.Publish(InputFile,new XElement("inputs",new XAttribute("input",Input),new XAttribute("directory",WorkingDirectory),new XAttribute("compilerSymbols",CompilerSymbols),
                new XAttribute("output",OutputDirectory),arguments.Select(value => new XElement("argument",value)),
                Dependencies.Select(value => new XElement("dependency",value)),References.Select(value => new XElement("reference",value.ItemSpec)),
                ImplementationInputs.Select(value => new XElement("implementation",value.ItemSpec))));
            if (!File.Exists(Stamp)) return true;
            if (File.Exists(Selection))
            {
                var state = XElement.Load(Selection);
                Assembly = FSharpBuildState.Value(state,"assembly"); Symbols = FSharpBuildState.Value(state,"symbols");
                if (File.Exists(Assembly) && (Symbols.Length == 0 || File.Exists(Symbols)) &&
                    (CompilerSymbols.Length == 0 || File.Exists(CompilerSymbols))) return true;
            }
            File.Delete(Stamp);
            return true;
        }
        catch (Exception error) { return FSharpBuildState.Error(BuildEngine,"SOLO005",Input,error); }
    }
}

public sealed class CompleteProductOperations : ITask
{
    public IBuildEngine BuildEngine { get; set; } = null!;
    public ITaskHost HostObject { get; set; } = null!;
    [Required] public string Input { get; set; } = "";
    [Required] public string InputFile { get; set; } = "";
    [Required] public string OutputDirectory { get; set; } = "";
    [Required] public string Selection { get; set; } = "";
    public ITaskItem[] References { get; set; } = Array.Empty<ITaskItem>();
    [Output] public string Assembly { get; private set; } = "";
    [Output] public string Symbols { get; private set; } = "";
    [Output] public string CompilerSymbols { get; private set; } = "";

    public bool Execute()
    {
        try
        {
            var state = XElement.Load(InputFile);
            var inputs = new FSharpCompilerInputs(state.Elements("argument").Select(value => value.Value).ToArray(),FSharpBuildState.Value(state,"directory"));
            using var resolver = new DefaultAssemblyResolver();
            foreach (var reference in References) resolver.AddSearchDirectory(Path.GetDirectoryName(Path.GetFullPath(reference.ItemSpec))!);
            using var image = AssemblyDefinition.ReadAssembly(Input,new ReaderParameters { AssemblyResolver = resolver });
            var compilerSymbols = FSharpBuildState.Value(state,"compilerSymbols");
            using var originalSymbols = compilerSymbols.Length == 0 ? null : File.OpenRead(compilerSymbols);
            if (HelperAssembly.HasCompilerSymbols(image.MainModule))
            {
                var provider = new DefaultSymbolReaderProvider(false);
                var reader = originalSymbols == null ? provider.GetSymbolReader(image.MainModule,Input)
                    : provider.GetSymbolReader(image.MainModule,originalSymbols);
                if (reader != null)
                {
                    try { image.MainModule.ReadSymbols(reader); }
                    catch { reader.Dispose(); throw; }
                }
            }
            ProductOperations.Complete(image);
            var sidecar = image.MainModule.HasSymbols && image.MainModule.SymbolReader is not EmbeddedPortablePdbReader;
            CompilerSymbols = sidecar ? compilerSymbols : "";
            var output = Path.Combine(OutputDirectory,Path.GetFileName(Input));
            Directory.CreateDirectory(OutputDirectory);
            using var bytes = new MemoryStream();
            using var symbols = image.MainModule.SymbolReader is PortablePdbReader ? new MemoryStream() : null;
            var writer = new WriterParameters
            {
                WriteSymbols = image.MainModule.HasSymbols,
                SymbolWriterProvider = image.MainModule.HasSymbols
                    ? new HelperAssembly.OutputSymbolWriterProvider(image.MainModule.SymbolReader.GetWriterProvider(),output) : null,
                SymbolStream = symbols
            };
            var delay = image.CustomAttributes.Any(value => value.AttributeType.FullName == "System.Reflection.AssemblyDelaySignAttribute" && (bool)value.ConstructorArguments[0].Value);
            if (image.Name.HasPublicKey && (image.MainModule.Attributes & ModuleAttributes.StrongNameSigned) != 0 &&
                !inputs.Switch("publicsign") && !inputs.Switch("delaysign") && !delay)
            {
                var key = inputs.Option("keyfile") ?? ProductOperationInputs.Attribute(image,"AssemblyKeyFile")
                    ?? throw new InvalidOperationException("The original fully signed product has no effective signing key input.");
                var privateKey = File.ReadAllBytes(inputs.Resolve(key));
                if (!HelperAssembly.SigningPublicKey(privateKey).SequenceEqual(image.Name.PublicKey))
                    throw new InvalidOperationException("The product signing key differs from the original compiler identity.");
                writer.StrongNameKeyBlob = privateKey;
            }
            image.Write(bytes,writer);
            if (symbols != null) HelperAssembly.Publish(Path.ChangeExtension(output,".pdb"),symbols.ToArray());
            HelperAssembly.Publish(output,bytes.ToArray());
            if (!sidecar && File.Exists(Path.ChangeExtension(output,".pdb"))) File.Delete(Path.ChangeExtension(output,".pdb"));
            Assembly = output; Symbols = sidecar ? Path.ChangeExtension(output,".pdb") : "";
            HelperAssembly.Publish(Selection,new XElement("selection",new XAttribute("assembly",Assembly),
                new XAttribute("symbols",Symbols),new XAttribute("compilerSymbols",CompilerSymbols)));
            return true;
        }
        catch (Exception error) { return FSharpBuildState.Error(BuildEngine,"SOLO005",Input,error); }
    }
}
