using System;
using System.IO;
using System.Linq;
using Microsoft.Build.Framework;
using Mono.Cecil;
using Mono.Cecil.Cil;

namespace SoloDatabase.Metadata.Tools;

/// Completes assembly-level access after normal provider linking; compiler output remains pristine.
public sealed class CompleteFSharpAssembly : ITask
{
    private sealed class CompletionSymbolWriterProvider : ISymbolWriterProvider
    {
        private readonly ISymbolWriterProvider provider;
        private readonly string output;
        public CompletionSymbolWriterProvider(ISymbolWriterProvider provider, string output)
        { this.provider = provider; this.output = output; }
        public ISymbolWriter GetSymbolWriter(ModuleDefinition module, string fileName) =>
            provider.GetSymbolWriter(module, output);
        public ISymbolWriter GetSymbolWriter(ModuleDefinition module, Stream stream) =>
            provider.GetSymbolWriter(module, stream);
    }

    public IBuildEngine BuildEngine { get; set; } = null!;
    public ITaskHost HostObject { get; set; } = null!;
    [Required] public string Input { get; set; } = "";
    [Required] public string Context { get; set; } = "";
    [Required] public string OutputDirectory { get; set; } = "";
    public ITaskItem[] References { get; set; } = Array.Empty<ITaskItem>();
    [Output] public string Assembly { get; private set; } = "";
    [Output] public string Symbols { get; private set; } = "";
    [Output] public string CompilerSymbols { get; private set; } = "";
    [Output] public bool Changed { get; private set; }

    public bool Execute()
    {
        Assembly = Symbols = CompilerSymbols = "";
        Changed = false;
        try
        {
            using var context = AssemblyDefinition.ReadAssembly(Context);
            var facts = context.MainModule.GetType("SoloDatabase.Tooling.ProjectContext");
            string Literal(string name) => (string)facts.Fields.Single(field => field.Name == name).Constant;
            var mode = Literal("Mode");
            if (mode != "execute" && mode != "inactive")
                throw new InvalidOperationException("FSharp executable completion requires current executable compiler inputs.");
            if (mode == "inactive")
            {
                Assembly = Input;
                return true;
            }
            using var resolver = new DefaultAssemblyResolver();
            resolver.AddSearchDirectory(Path.GetDirectoryName(Path.GetFullPath(Input))!);
            foreach (var reference in References)
                resolver.AddSearchDirectory(Path.GetDirectoryName(Path.GetFullPath(reference.ItemSpec))!);
            using var template = AssemblyDefinition.ReadAssembly(Literal("Template"));
            var product = resolver.Resolve(template.MainModule.AssemblyReferences.Single(reference => reference.Name == "SoloDB"));
            const string accessType = "System.Runtime.CompilerServices.IgnoresAccessChecksToAttribute";
            var targets = template.CustomAttributes.Where(attribute => attribute.AttributeType.FullName == accessType)
                .Select(attribute => (string)attribute.ConstructorArguments[0].Value).Distinct(StringComparer.Ordinal).ToArray();
            bool readSymbols;
            string[] missing;
            using (var header = AssemblyDefinition.ReadAssembly(Input, new ReaderParameters { AssemblyResolver = resolver }))
            {
                var existing = header.CustomAttributes.Where(attribute => attribute.AttributeType.FullName == accessType)
                    .Select(attribute => (string)attribute.ConstructorArguments[0].Value);
                missing = targets.Except(existing, StringComparer.Ordinal).ToArray();
                if (missing.Length == 0 && !GeneratedSchemaIndexes.HasUncompleted(header.MainModule,product) && !GeneratedSchemaIndexes.HasUncompleted(template.MainModule,product))
                {
                    Assembly = Input;
                    return true;
                }
                readSymbols = HelperAssembly.HasCompilerSymbols(header.MainModule);
            }
            using var consumer = AssemblyDefinition.ReadAssembly(Input, new ReaderParameters
            {
                ReadingMode = ReadingMode.Immediate,
                AssemblyResolver = resolver,
                ReadSymbols = readSymbols,
                SymbolReaderProvider = readSymbols ? new DefaultSymbolReaderProvider(false) : null
            });
            var completedTargets = new System.Collections.Generic.HashSet<string>(targets,StringComparer.Ordinal);
            GeneratedSchemaIndexes.Complete(consumer,product,template,name => completedTargets.Add(name));
            missing = completedTargets.Except(consumer.CustomAttributes.Where(attribute => attribute.AttributeType.FullName == accessType)
                .Select(attribute => (string)attribute.ConstructorArguments[0].Value),StringComparer.Ordinal).ToArray();
            var sidecarSymbols = consumer.MainModule.HasSymbols && consumer.MainModule.SymbolReader is not EmbeddedPortablePdbReader;
            CompilerSymbols = sidecarSymbols ? Path.ChangeExtension(Input, ".pdb") : "";
            var definition = consumer.MainModule.GetType(accessType)
                ?? throw new InvalidOperationException("The current consumer has no access attribute definition.");
            var constructor = definition.Methods.Single(method => method.IsConstructor && !method.IsStatic && !method.IsPrivate &&
                method.Parameters.Count == 1 && method.Parameters[0].ParameterType.FullName == "System.String");
            HelperAssembly.AllowAccess(consumer.MainModule, missing,
                definition.BaseType.Scope, constructor);
            Directory.CreateDirectory(OutputDirectory);
            var output = Path.Combine(OutputDirectory, Path.GetFileName(Input));
            using var image = new MemoryStream();
            // Native PDB providers require their original filename operation.
            using var symbols = consumer.MainModule.SymbolReader is PortablePdbReader ? new MemoryStream() : null;
            var writer = new WriterParameters
            {
                WriteSymbols = consumer.MainModule.HasSymbols,
                SymbolWriterProvider = consumer.MainModule.HasSymbols
                    ? new CompletionSymbolWriterProvider(consumer.MainModule.SymbolReader.GetWriterProvider(), output) : null,
                SymbolStream = symbols
            };
            var fullSigningKeyFile = Literal("FullSigningKeyFile");
            if (fullSigningKeyFile.Length != 0) writer.StrongNameKeyBlob = File.ReadAllBytes(fullSigningKeyFile);
            consumer.Write(image, writer);
            if (symbols != null) Publish(Path.ChangeExtension(output, ".pdb"), symbols.ToArray());
            Publish(output, image.ToArray());
            Assembly = output;
            Symbols = sidecarSymbols ? Path.ChangeExtension(output, ".pdb") : "";
            Changed = true;
            return true;
        }
        catch (Exception error)
        {
            BuildEngine.LogErrorEvent(new BuildErrorEventArgs("SoloDB", "SOLO004", Input, 0, 0, 0, 0,
                error.ToString(), null, nameof(CompleteFSharpAssembly)));
            return false;
        }
    }

    private static void Publish(string path, byte[] content)
    {
        if (!File.Exists(path) || !File.ReadAllBytes(path).SequenceEqual(content)) File.WriteAllBytes(path, content);
    }
}
