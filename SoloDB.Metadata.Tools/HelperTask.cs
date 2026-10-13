using System;
using Microsoft.Build.Framework;

namespace SoloDatabase.Metadata.Tools;

public sealed class GenerateHelper : ITask
{
    public IBuildEngine BuildEngine { get; set; } = null!;
    public ITaskHost HostObject { get; set; } = null!;
    [Required] public string Output { get; set; } = "";
    [Required] public string Identity { get; set; } = "";
    [Output] public bool Active { get; private set; }
    public string? Consumer { get; set; }
    public string? KeyFile { get; set; }
    public ITaskItem[] References { get; set; } = Array.Empty<ITaskItem>();

    public bool Execute()
    {
        try
        {
            if (string.IsNullOrEmpty(Consumer)) HelperAssembly.WriteContract(Output, Identity, KeyFile, References);
            else Active = HelperAssembly.WriteImplementation(Consumer!, Output, Identity, KeyFile, References);
            return true;
        }
        catch (Exception error)
        {
            BuildEngine.LogErrorEvent(new BuildErrorEventArgs("SoloDB", "SOLO002", Consumer ?? Output, 0, 0, 0, 0,
                error.ToString(), null, nameof(GenerateHelper)));
            return false;
        }
    }
}
