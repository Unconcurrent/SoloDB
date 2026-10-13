using System;
using System.IO;
using System.Linq;
using Microsoft.Build.Framework;
using Mono.Cecil;

namespace SoloDatabase.Metadata.Tools;

public sealed class CaptureProjectContext : ITask
{
    public IBuildEngine BuildEngine { get; set; } = null!;
    public ITaskHost HostObject { get; set; } = null!;
    [Required] public string Output { get; set; } = "";
    [Required] public string Project { get; set; } = "";
    [Required] public string Mode { get; set; } = "";
    [Required] public ITaskItem[] Arguments { get; set; } = Array.Empty<ITaskItem>();

    public bool Execute()
    {
        try
        {
            CompilerContext.Write(Output, Project, Mode, Arguments.Select(argument => argument.ItemSpec).ToArray());
            return true;
        }
        catch (Exception error)
        {
            BuildEngine.LogErrorEvent(new BuildErrorEventArgs("SoloDB", "SOLO003", Project, 0, 0, 0, 0,
                error.ToString(), null, nameof(CaptureProjectContext)));
            return false;
        }
    }
}
