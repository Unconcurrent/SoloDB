using System;
using System.Reflection;
using Microsoft.Build.Framework;

namespace SoloDatabase.Metadata.Tools;

public sealed class PrepareFSharpProject : ITask
{
    public IBuildEngine BuildEngine { get; set; } = null!;
    public ITaskHost HostObject { get; set; } = null!;
    [Required] public string Provider { get; set; } = "";
    [Required] public string Context { get; set; } = "";
    [Required] public string AssemblyName { get; set; } = "";
    [Required] public string AttributeSource { get; set; } = "";
    [Required] public string Template { get; set; } = "";

    public bool Execute()
    {
        try
        {
            // FCS and its providers share the normal design-time loading context.
            // MSBuild's task loading context must not split their FSharp.Core identities.
            var generator = Assembly.LoadFrom(Provider).GetType("SoloDatabase.Metadata.FSharp.ProjectGeneration", true)!;
            var write = generator.GetMethod("Write", BindingFlags.Public | BindingFlags.Static,
                null, new[] { typeof(string), typeof(string), typeof(string), typeof(string), typeof(Action<string,int,int,string>) }, null)
                ?? throw new InvalidOperationException("The bundled FSharp generation entry point is absent.");
            var report = new Action<string,int,int,string>((file,line,column,message) =>
                BuildEngine.LogWarningEvent(new BuildWarningEventArgs("SoloDB", "SOLO003", file, line, column, line, column,
                    message, null, nameof(PrepareFSharpProject))));
            write.Invoke(null, new object[] { Context, AssemblyName, AttributeSource, Template, report });
            return true;
        }
        catch (Exception error)
        {
            if (error is TargetInvocationException invocation && invocation.InnerException != null) error = invocation.InnerException;
            BuildEngine.LogErrorEvent(new BuildErrorEventArgs("SoloDB", "SOLO003", Context, 0, 0, 0, 0,
                error.ToString(), null, nameof(PrepareFSharpProject)));
            return false;
        }
    }
}
