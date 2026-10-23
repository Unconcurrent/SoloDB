using System;
using System.IO;
using System.Linq;
using System.Xml.Linq;
using Microsoft.Build.Framework;
using Mono.Cecil;

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
    [Required] public string Identity { get; set; } = "";
    public string? KeyFile { get; set; }
    public bool PublicSign { get; set; }
    public bool DelaySign { get; set; }
    public ITaskItem[] References { get; set; } = Array.Empty<ITaskItem>();
    public ITaskItem[] CompilerReferences { get; set; } = Array.Empty<ITaskItem>();
    public ITaskItem[] ImplementationInputs { get; set; } = Array.Empty<ITaskItem>();
    [Output] public bool Active { get; private set; }

    public bool Execute()
    {
        Active = false;
        try
        {
            using var consumer = AssemblyDefinition.ReadAssembly(Consumer);
            Active = consumer.CustomAttributes.Any(attribute => attribute.AttributeType.FullName == "SoloDatabase.Generated.MetadataActivationAttribute");
            HelperAssembly.Publish(InputFile, new XElement("inputs", new XAttribute("consumer", Consumer),
                new XAttribute("output", Output), new XAttribute("identity", Identity), new XAttribute("key", KeyFile ?? ""),
                new XAttribute("publicSign", PublicSign), new XAttribute("delaySign", DelaySign),
                References.Select(reference => new XElement("reference", reference.ItemSpec)),
                CompilerReferences.Select(reference => new XElement("compilerReference", reference.ItemSpec)),
                ImplementationInputs.Select(input => new XElement("implementation", input.ItemSpec))));
            if (!Active)
            {
                if (File.Exists(Output)) File.Delete(Output);
                HelperAssembly.Publish(Selection, new XElement("selection", new XAttribute("active", false)));
                return true;
            }
            if (!File.Exists(Stamp)) return true;
            if (File.Exists(Selection))
            {
                if ((bool)XElement.Load(Selection).Attribute("active")! && File.Exists(Output)) return true;
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
}
