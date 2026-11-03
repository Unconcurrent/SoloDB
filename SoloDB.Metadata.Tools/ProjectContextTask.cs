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
    public string Input { get; set; } = "";
    public string ConsumerOutputDirectory { get; set; } = "";
    public string HelperOutput { get; set; } = "";
    public string Selection { get; set; } = "";
    public ITaskItem[] InactiveOutputs { get; set; } = Array.Empty<ITaskItem>();
    [Output] public bool GenerationRequired { get; private set; } = true;
    [Required] public ITaskItem[] Arguments { get; set; } = Array.Empty<ITaskItem>();

    public bool Execute()
    {
        try
        {
            GenerationRequired = Input.Length == 0 || FSharpBuildState.HasRegistration(Input);
            if (!GenerationRequired)
            {
                GeneratedConsumerOutput.Inactive(Input,ConsumerOutputDirectory,HelperOutput,Selection,Array.Empty<string>());
                if (File.Exists(Output)) File.Delete(Output);
                foreach (var file in InactiveOutputs)
                    if (File.Exists(file.ItemSpec)) File.Delete(file.ItemSpec);
                return true;
            }
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
