using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Xml.Linq;
using Microsoft.Build.Framework;
using Mono.Cecil;
using Mono.Cecil.Cil;

namespace SoloDatabase.Metadata.Tools;

/// Completes assembly-level access after normal provider linking; compiler output remains pristine.
public sealed class CompleteFSharpAssembly : ITask
{
    public IBuildEngine BuildEngine { get; set; } = null!;
    public ITaskHost HostObject { get; set; } = null!;
    [Required] public string Input { get; set; } = "";
    [Required] public string Context { get; set; } = "";
    [Required] public string OutputDirectory { get; set; } = "";
    public ITaskItem[] References { get; set; } = Array.Empty<ITaskItem>();
    public string Selection { get; set; } = "";
    public bool ProfileBuild { get; set; }
    [Output] public string Assembly { get; private set; } = "";
    [Output] public string Symbols { get; private set; } = "";
    [Output] public string CompilerSymbols { get; private set; } = "";
    [Output] public bool Changed { get; private set; }

    public bool Execute()
    {
        Assembly = Symbols = CompilerSymbols = "";
        Changed = false;
        var watch = ProfileBuild ? Stopwatch.StartNew() : null;
        void Timing(string name, double seconds) =>
            BuildEngine.LogMessageEvent(new BuildMessageEventArgs("SoloDB build timing: completion-" + name + " " +
                seconds.ToString("R", CultureInfo.InvariantCulture) + " seconds", null,
                nameof(CompleteFSharpAssembly), MessageImportance.Normal));
        void Phase(string name)
        {
            if (watch == null) return;
            Timing(name, watch.Elapsed.TotalSeconds);
            watch.Restart();
        }
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
                Phase("inputs");
                return Success();
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
            using var consumer = AssemblyDefinition.ReadAssembly(Input, new ReaderParameters { AssemblyResolver = resolver });
            var existing = consumer.CustomAttributes.Where(attribute => attribute.AttributeType.FullName == accessType)
                .Select(attribute => (string)attribute.ConstructorArguments[0].Value);
            var missing = targets.Except(existing, StringComparer.Ordinal).ToArray();
            var consumerCandidates = new GeneratedSchemaIndexes.Candidates(consumer.MainModule);
            var templateCandidates = new GeneratedSchemaIndexes.Candidates(template.MainModule);
            if (missing.Length == 0 && !GeneratedSchemaIndexes.HasUncompleted(consumer.MainModule,product,consumerCandidates) &&
                !GeneratedSchemaIndexes.HasUncompleted(template.MainModule,product,templateCandidates))
            {
                Assembly = Input;
                Phase("inputs");
                return Success();
            }
            Phase("inputs");
            if (HelperAssembly.HasCompilerSymbols(consumer.MainModule))
            {
                var reader = new DefaultSymbolReaderProvider(false).GetSymbolReader(consumer.MainModule, Input);
                if (reader != null)
                {
                    try { consumer.MainModule.ReadSymbols(reader); }
                    catch { reader.Dispose(); throw; }
                }
            }
            Phase("symbols");
            var completedTargets = new System.Collections.Generic.HashSet<string>(targets,StringComparer.Ordinal);
            GeneratedSchemaIndexes.Complete(consumer,product,template,name => completedTargets.Add(name),
                ProfileBuild ? (name,seconds) => Timing("schema-" + name,seconds) : null,consumerCandidates,templateCandidates);
            Phase("schema");
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
            Phase("access");
            Directory.CreateDirectory(OutputDirectory);
            var output = Path.Combine(OutputDirectory, Path.GetFileName(Input));
            using var image = new MemoryStream();
            // Native PDB providers require their original filename operation.
            using var symbols = consumer.MainModule.SymbolReader is PortablePdbReader ? new MemoryStream() : null;
            var writer = new WriterParameters
            {
                WriteSymbols = consumer.MainModule.HasSymbols,
                SymbolWriterProvider = consumer.MainModule.HasSymbols
                    ? new HelperAssembly.OutputSymbolWriterProvider(consumer.MainModule.SymbolReader.GetWriterProvider(), output) : null,
                SymbolStream = symbols
            };
            var fullSigningKeyFile = Literal("FullSigningKeyFile");
            if (fullSigningKeyFile.Length != 0) writer.StrongNameKeyBlob = File.ReadAllBytes(fullSigningKeyFile);
            consumer.Write(image, writer);
            Phase("serialize");
            if (symbols != null) HelperAssembly.Publish(Path.ChangeExtension(output, ".pdb"), symbols.ToArray());
            HelperAssembly.Publish(output, image.ToArray());
            Assembly = output;
            Symbols = sidecarSymbols ? Path.ChangeExtension(output, ".pdb") : "";
            Changed = true;
            return Success();
        }
        catch (Exception error)
        {
            BuildEngine.LogErrorEvent(new BuildErrorEventArgs("SoloDB", "SOLO004", Input, 0, 0, 0, 0,
                error.ToString(), null, nameof(CompleteFSharpAssembly)));
            return false;
        }

        bool Success()
        {
            if (Selection.Length != 0)
            {
                if (!Changed)
                {
                    var obsolete = Path.Combine(OutputDirectory, Path.GetFileName(Input));
                    // A fresh inactive consumer has never created this owned
                    // completion directory. Original compiler output stays intact.
                    if (Path.GetFullPath(obsolete) != Path.GetFullPath(Input) && Directory.Exists(OutputDirectory))
                    {
                        File.Delete(obsolete);
                        File.Delete(Path.ChangeExtension(obsolete, ".pdb"));
                    }
                }
                HelperAssembly.Publish(Selection, new XElement("selection", new XAttribute("assembly", Assembly),
                    new XAttribute("symbols", Symbols), new XAttribute("compilerSymbols", CompilerSymbols),
                    new XAttribute("changed", Changed)));
            }
            Phase("publish");
            return true;
        }
    }

}
