using System;
using System.IO;
using System.Linq;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using System.Xml.Linq;
using Microsoft.Build.Framework;

namespace SoloDatabase.Metadata.Tools;

public sealed class HelperImplementationInputs : ITask
{
    public IBuildEngine BuildEngine { get; set; } = null!;
    public ITaskHost HostObject { get; set; } = null!;
    [Required] public string InputFile { get; set; } = "";
    [Required] public string Stamp { get; set; } = "";
    [Required] public string Selection { get; set; } = "";
    [Required] public string Consumer { get; set; } = "";
    [Required] public string Output { get; set; } = "";
    [Required] public string ConsumerOutputDirectory { get; set; } = "";
    [Required] public string Identity { get; set; } = "";
    public string? KeyFile { get; set; }
    public bool PublicSign { get; set; }
    public bool DelaySign { get; set; }
    public bool ReadSelectionOnly { get; set; }
    public ITaskItem[] References { get; set; } = Array.Empty<ITaskItem>();
    public ITaskItem[] CompilerReferences { get; set; } = Array.Empty<ITaskItem>();
    public ITaskItem[] ImplementationInputs { get; set; } = Array.Empty<ITaskItem>();
    [Output] public bool Active { get; private set; }
    [Output] public string Assembly { get; private set; } = "";
    [Output] public string Symbols { get; private set; } = "";
    [Output] public string CompilerSymbols { get; private set; } = "";
    [Output] public bool Changed { get; private set; }

    public bool Execute()
    {
        Active = Changed = false;
        Assembly = Consumer;
        Symbols = CompilerSymbols = "";
        try
        {
            if (ReadSelectionOnly) { ReadSelection(); return true; }
            Active = HasActivation(Consumer);
            if (!Active)
            {
                GeneratedConsumerOutput.Inactive(Consumer,ConsumerOutputDirectory,Output,Selection,Array.Empty<string>());
                if (File.Exists(InputFile)) File.Delete(InputFile);
                if (File.Exists(Stamp)) File.Delete(Stamp);
                return true;
            }
            HelperAssembly.Publish(InputFile, new XElement("inputs", new XAttribute("consumer", Consumer),
                new XAttribute("output", Output), new XAttribute("identity", Identity), new XAttribute("key", KeyFile ?? ""),
                new XAttribute("publicSign", PublicSign), new XAttribute("delaySign", DelaySign),
                References.Select(reference => new XElement("reference", reference.ItemSpec)),
                CompilerReferences.Select(reference => new XElement("compilerReference", reference.ItemSpec)),
                ImplementationInputs.Select(input => new XElement("implementation", input.ItemSpec))));
            if (!File.Exists(Stamp)) return true;
            if (File.Exists(Selection) && File.Exists(Output) &&
                File.Exists(Path.Combine(ConsumerOutputDirectory,Path.GetFileName(Consumer))))
            {
                ReadSelection();
                if (Changed && File.Exists(Assembly) && (Symbols.Length == 0 || File.Exists(Symbols)) &&
                    (CompilerSymbols.Length == 0 || File.Exists(CompilerSymbols))) return true;
            }
            File.Delete(Stamp);
            return true;
        }
        catch (Exception error)
        {
            BuildEngine.LogErrorEvent(new BuildErrorEventArgs("SoloDB", "SOLO002", Consumer, 0, 0, 0, 0,
                error.ToString(), null, nameof(HelperImplementationInputs)));
            return false;
        }
    }

    private void ReadSelection()
    {
        var state = XElement.Load(Selection);
        Assembly = GeneratedConsumerOutput.Value(state,"assembly");
        Symbols = GeneratedConsumerOutput.Value(state,"symbols");
        CompilerSymbols = GeneratedConsumerOutput.Value(state,"compilerSymbols");
        Changed = (bool)state.Attribute("changed")!;
    }

    private static bool HasActivation(string input)
    {
        using var stream = File.OpenRead(input);
        using var image = new PEReader(stream);
        var reader = image.GetMetadataReader();
        foreach (var handle in reader.GetAssemblyDefinition().GetCustomAttributes())
        {
            var constructor = reader.GetCustomAttribute(handle).Constructor;
            var owner = constructor.Kind == HandleKind.MethodDefinition
                ? reader.GetMethodDefinition((MethodDefinitionHandle)constructor).GetDeclaringType()
                : reader.GetMemberReference((MemberReferenceHandle)constructor).Parent;
            StringHandle name, space;
            if (owner.Kind == HandleKind.TypeDefinition)
            {
                var type = reader.GetTypeDefinition((TypeDefinitionHandle)owner);
                name = type.Name; space = type.Namespace;
            }
            else if (owner.Kind == HandleKind.TypeReference)
            {
                var type = reader.GetTypeReference((TypeReferenceHandle)owner);
                name = type.Name; space = type.Namespace;
            }
            else continue;
            if (reader.StringComparer.Equals(name,"MetadataActivationAttribute") &&
                reader.StringComparer.Equals(space,"SoloDatabase.Generated")) return true;
        }
        return false;
    }
}
