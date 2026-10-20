using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using Microsoft.Build.Framework;

namespace SoloDatabase.Metadata.Tools;

public sealed class PrepareFSharpProject : ICancelableTask
{
    private readonly object gate = new();
    private Process? child;
    private bool canceled;
    public IBuildEngine BuildEngine { get; set; } = null!;
    public ITaskHost HostObject { get; set; } = null!;
    [Required] public string Provider { get; set; } = "";
    [Required] public string Context { get; set; } = "";
    public string Project { get; set; } = "";
    public ITaskItem[] Arguments { get; set; } = Array.Empty<ITaskItem>();
    [Required] public string AssemblyName { get; set; } = "";
    [Required] public string AttributeSource { get; set; } = "";
    [Required] public string Template { get; set; } = "";
    [Required] public string DotNetHost { get; set; } = "";
    [Output] public string ActiveTemplate { get; private set; } = "";

    public void Cancel()
    {
        lock (gate)
        {
            canceled = true;
            if (child != null)
            {
                try { if (!child.HasExited) child.Kill(); }
                catch (InvalidOperationException) { /* The owned child has already exited. */ }
            }
        }
    }

    public bool Execute()
    {
        ActiveTemplate = "";
        var directory = Path.Combine(Path.GetDirectoryName(Context)!, Guid.NewGuid().ToString("N"));
        var request = Path.Combine(directory, "project.xml");
        var response = request + ".result";
        try
        {
            Directory.CreateDirectory(directory);
            if ((Project.Length == 0) != (Arguments.Length == 0))
                throw new InvalidOperationException("Current FSharp project and compiler arguments must be supplied together.");
            var input = new XElement("project",
                new XAttribute("context", Context), new XAttribute("assembly", AssemblyName),
                new XAttribute("attributes", AttributeSource), new XAttribute("template", Template));
            if (Project.Length != 0)
            {
                input.Add(new XAttribute("project", Project));
                var arguments = new XElement("arguments");
                foreach (var argument in Arguments) arguments.Add(new XElement("argument", argument.ItemSpec));
                input.Add(arguments);
            }
            using (var xml = XmlWriter.Create(request, new XmlWriterSettings { NewLineHandling = NewLineHandling.Entitize }))
                new XDocument(input).Save(xml);
            var worker = Path.Combine(Path.GetDirectoryName(Provider)!, "analysis", "SoloDB.Metadata.FSharp.Analysis.dll");
            using var process = new Process
            {
                StartInfo = new ProcessStartInfo(DotNetHost, Quote(worker) + " " + Quote(request))
                {
                    UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true,
                    CreateNoWindow = true
                }
            };
            lock (gate)
            {
                if (canceled) return false;
                process.Start();
                child = process;
            }
            try
            {
                var output = process.StandardOutput.ReadToEndAsync();
                var errors = process.StandardError.ReadToEndAsync();
                process.WaitForExit();
                var text = output.GetAwaiter().GetResult() + errors.GetAwaiter().GetResult();
                lock (gate) { if (canceled) return false; }
                if (text.Length != 0)
                    BuildEngine.LogMessageEvent(new BuildMessageEventArgs(text, null, nameof(PrepareFSharpProject), MessageImportance.Low));
                if (!File.Exists(response))
                    throw new InvalidOperationException("FSharp project analysis did not return diagnostics. Exit code: " +
                        process.ExitCode + Environment.NewLine + text);
                var result = XDocument.Load(response).Root!;
                var failed = false;
                foreach (var diagnostic in result.Elements("diagnostic"))
                {
                    var file = (string)diagnostic.Attribute("file")!;
                    var line = int.Parse((string)diagnostic.Attribute("line")!, CultureInfo.InvariantCulture);
                    var column = int.Parse((string)diagnostic.Attribute("column")!, CultureInfo.InvariantCulture);
                    var code = (string)diagnostic.Attribute("code")!;
                    if ((string?)diagnostic.Attribute("severity") == "warning")
                        BuildEngine.LogWarningEvent(new BuildWarningEventArgs("SoloDB", code, file, line, column,
                            line, column, diagnostic.Value, null, nameof(PrepareFSharpProject)));
                    else
                    {
                        failed = true;
                        BuildEngine.LogErrorEvent(new BuildErrorEventArgs("SoloDB", code, file, line, column,
                            line, column, diagnostic.Value, null, nameof(PrepareFSharpProject)));
                    }
                }
                if (process.ExitCode != 0 || (bool?)result.Attribute("success") != true)
                {
                    if (!failed) throw new InvalidOperationException("FSharp project analysis failed. Exit code: " +
                        process.ExitCode + Environment.NewLine + text);
                    return false;
                }
                if (!failed) ActiveTemplate = (string?)result.Attribute("template") ?? "";
                return !failed;
            }
            finally
            {
                lock (gate)
                {
                    try { if (!process.HasExited) process.Kill(); }
                    catch (InvalidOperationException) { /* The owned child has already exited. */ }
                    child = null;
                }
                process.WaitForExit();
            }
        }
        catch (Exception error)
        {
            BuildEngine.LogErrorEvent(new BuildErrorEventArgs("SoloDB", "SOLO003", Context, 0, 0, 0, 0,
                error.ToString(), null, nameof(PrepareFSharpProject)));
            return false;
        }
        finally
        {
            if (Directory.Exists(directory)) Directory.Delete(directory, true);
        }
    }

    // ProcessStartInfo.Arguments uses the same quoted-argument grammar on supported hosts.
    private static string Quote(string value)
    {
        var result = new StringBuilder().Append('"');
        var slashes = 0;
        foreach (var character in value)
        {
            if (character == '\\') { slashes++; continue; }
            result.Append('\\', character == '"' ? slashes * 2 + 1 : slashes);
            result.Append(character);
            slashes = 0;
        }
        return result.Append('\\', slashes * 2).Append('"').ToString();
    }
}
