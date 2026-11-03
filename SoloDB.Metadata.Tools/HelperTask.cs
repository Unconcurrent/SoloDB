using System;
using System.Xml.Linq;
using Microsoft.Build.Framework;

namespace SoloDatabase.Metadata.Tools;

public sealed class GenerateHelper : ITask
{
    public IBuildEngine BuildEngine { get; set; } = null!;
    public ITaskHost HostObject { get; set; } = null!;
    [Required] public string Output { get; set; } = "";
    [Required] public string Identity { get; set; } = "";
    [Output] public bool Active { get; private set; }
    [Required] public string Consumer { get; set; } = "";
    public string? KeyFile { get; set; }
    public bool PublicSign { get; set; }
    public bool DelaySign { get; set; }
    public ITaskItem[] References { get; set; } = Array.Empty<ITaskItem>();
    public ITaskItem[]? CompilerReferences { get; set; }
    [Required] public string ConsumerOutputDirectory { get; set; } = "";
    public string Selection { get; set; } = "";

    public bool Execute()
    {
        try
        {
            Active = HelperAssembly.WriteImplementation(Consumer, Output, Identity, KeyFile, References, PublicSign, DelaySign, CompilerReferences, Selection, ConsumerOutputDirectory);
            return true;
        }
        catch (Exception error)
        {
            BuildEngine.LogErrorEvent(new BuildErrorEventArgs("SoloDB", "SOLO002", Consumer, 0, 0, 0, 0,
                error.ToString(), null, nameof(GenerateHelper)));
            return false;
        }
    }
}
