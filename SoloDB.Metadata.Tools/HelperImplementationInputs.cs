using System;
using System.IO;
using System.Linq;
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
    [Required] public string Identity { get; set; } = "";
    public string? KeyFile { get; set; }
    public ITaskItem[] References { get; set; } = Array.Empty<ITaskItem>();
    public ITaskItem[] ImplementationInputs { get; set; } = Array.Empty<ITaskItem>();
    [Output] public bool Active { get; private set; }

    public bool Execute()
    {
        Active = false;
        try
        {
            HelperAssembly.Publish(InputFile, new XElement("inputs", new XAttribute("consumer", Consumer),
                new XAttribute("output", Output), new XAttribute("identity", Identity), new XAttribute("key", KeyFile ?? ""),
                References.Select(reference => new XElement("reference", reference.ItemSpec)),
                ImplementationInputs.Select(input => new XElement("implementation", input.ItemSpec))));
            if (!File.Exists(Stamp)) return true;
            if (File.Exists(Selection))
            {
                Active = (bool)XElement.Load(Selection).Attribute("active")!;
                if (!Active || File.Exists(Output)) return true;
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
