using System;
using System.Collections;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using Microsoft.Build.Framework;

namespace SoloDatabase.Metadata.Tools;

[RunInMTA]
public sealed class PrepareFSharpProject : ICancelableTask
{
    private readonly object gate = new();
    private Process? child;
    private string? compilerInput;
    private bool canceled;
    public IBuildEngine BuildEngine { get; set; } = null!;
    public ITaskHost HostObject { get; set; } = null!;
    [Required] public string Provider { get; set; } = "";
    [Required] public string Context { get; set; } = "";
    public string Project { get; set; } = "";
    public string Input { get; set; } = "";
    public string OutputDirectory { get; set; } = "";
    public string HelperOutput { get; set; } = "";
    public string Selection { get; set; } = "";
    public bool ProfileBuild { get; set; }
    public string[] CompilerTargets { get; set; } = Array.Empty<string>();
    public bool Prepare { get; set; } = true;
    public ITaskItem[] AcquisitionInputs { get; set; } = Array.Empty<ITaskItem>();
    public ITaskItem[] Arguments { get; set; } = Array.Empty<ITaskItem>();
    [Required] public string AssemblyName { get; set; } = "";
    [Required] public string AttributeSource { get; set; } = "";
    [Required] public string Template { get; set; } = "";
    [Required] public string DotNetHost { get; set; } = "";
    [Output] public string ActiveTemplate { get; private set; } = "";
    [Output] public bool Prepared { get; private set; }
    [Output] public ITaskItem[] CompilerArguments { get; private set; } = Array.Empty<ITaskItem>();

    private bool Compile()
    {
        lock (gate) { if (canceled) return false; }
        if (BuildEngine is not IBuildEngine3 engine)
            throw new InvalidOperationException("Current-project compilation requires the standard MSBuild task engine.");
        // Match CallTarget's current-project request, including requested outputs.
        var result = engine.BuildProjectFilesInParallel(new string[] { null! }, CompilerTargets,
            new IDictionary[] { null! }, null!, new string[] { null! }, true);
        if (result.TargetOutputsPerProject.Count != 0 &&
            result.TargetOutputsPerProject[0].TryGetValue("CoreCompile",out var arguments)) CompilerArguments = arguments;
        if (result.TargetOutputsPerProject.Count != 0 &&
            result.TargetOutputsPerProject[0].TryGetValue("SoloDBFSharpCompiledOutput",out var inputs) && inputs.Length == 1)
            compilerInput = inputs[0].ItemSpec;
        lock (gate) { return !canceled && result.Result; }
    }

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
        Prepared = false;
        CompilerArguments = Array.Empty<ITaskItem>();
        compilerInput = null;
        var directory = Path.Combine(Path.GetDirectoryName(Context)!, Guid.NewGuid().ToString("N"));
        var request = Path.Combine(directory, "project.xml");
        var response = request + ".result";
        try
        {
            var retained = CompilerTargets.Length != 0;
            Directory.CreateDirectory(directory);
            if ((Project.Length == 0) != (Arguments.Length == 0))
                throw new InvalidOperationException("Current FSharp project and compiler arguments must be supplied together.");
            var input = new XElement("project",
                new XAttribute("context", Context), new XAttribute("assembly", AssemblyName),
                new XAttribute("attributes", AttributeSource), new XAttribute("template", Template));
            if (Input.Length != 0)
            {
                input.Add(new XAttribute("input",Input),new XAttribute("outputDirectory",OutputDirectory),
                    new XAttribute("helper",HelperOutput),new XAttribute("selection",Selection));
                if (Project.Length == 0)
                {
                    using var context = Mono.Cecil.AssemblyDefinition.ReadAssembly(Context);
                    var facts = context.MainModule.GetType("SoloDatabase.Tooling.ProjectContext");
                    string Literal(string name) => (string)System.Linq.Enumerable.Single(facts.Fields,field => field.Name == name).Constant;
                    Project = Literal("Project");
                    Arguments = System.Linq.Enumerable.ToArray(System.Linq.Enumerable.Select(Literal("Arguments").Split('\0'),
                        value => (ITaskItem)new ProjectArgument(value)));
                }
            }
            if (retained && !Prepare) return Compile();
            if (ProfileBuild) input.Add(new XAttribute("timings", "true"));
            if (retained)
            {
                input.Add(new XAttribute("awaitCompiler", "true"));
                var files = new XElement("acquisitionInputs");
                foreach (var file in AcquisitionInputs)
                {
                    var path = file.ItemSpec;
                    if (!Path.IsPathRooted(path)) path = Path.Combine(Path.GetDirectoryName(Project)!,path);
                    files.Add(new XElement("file",Path.GetFullPath(path)));
                }
                input.Add(files);
            }
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
                    WorkingDirectory = Project.Length == 0 ? Environment.CurrentDirectory : Path.GetDirectoryName(Path.GetFullPath(Project))!,
                    UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true,
                    RedirectStandardInput = retained,
                    CreateNoWindow = true
                }
            };
            lock (gate)
            {
                if (canceled) return false;
                process.Start();
                child = process;
            }
            Task<string>? output = null, errors = null;
            try
            {
                output = process.StandardOutput.ReadToEndAsync();
                errors = process.StandardError.ReadToEndAsync();
                if (retained)
                {
                    if (!Compile()) return false;
                    try
                    {
                        if (!process.HasExited)
                        {
                            var actual = CompilerArguments.Length == 0 ? Arguments : CompilerArguments;
                            string FullPath(string path) => Path.GetFullPath(Path.IsPathRooted(path) ? path :
                                Path.Combine(Path.GetDirectoryName(Project)!,path));
                            new XElement("compiler",new XAttribute("captured",CompilerArguments.Length != 0),
                                new XAttribute("sameOutput",compilerInput != null && FullPath(compilerInput) == FullPath(Input)),
                                actual.Select(argument => new XElement("argument",argument.ItemSpec)))
                                .Save(process.StandardInput);
                            process.StandardInput.Close();
                        }
                    }
                    catch (IOException) when (process.HasExited) { }
                    catch (InvalidOperationException) when (process.HasExited) { }
                }
                process.WaitForExit();
                var text = output.GetAwaiter().GetResult() + errors.GetAwaiter().GetResult();
                lock (gate) { if (canceled) return false; }
                if (text.Length != 0)
                    BuildEngine.LogMessageEvent(new BuildMessageEventArgs(text, null, nameof(PrepareFSharpProject), MessageImportance.Low));
                if (!File.Exists(response))
                    throw new InvalidOperationException("FSharp project analysis did not return diagnostics. Exit code: " +
                        process.ExitCode + Environment.NewLine + text);
                var result = XDocument.Load(response).Root!;
                if (ProfileBuild)
                    foreach (var phase in result.Elements("phase"))
                        BuildEngine.LogMessageEvent(new BuildMessageEventArgs("SoloDB build timing: " +
                            (string?)phase.Attribute("name") + " " + (string?)phase.Attribute("seconds") + " seconds",
                            null, nameof(PrepareFSharpProject), MessageImportance.Normal));
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
                if (!failed)
                {
                    ActiveTemplate = (string?)result.Attribute("template") ?? "";
                    Prepared = (bool?)result.Attribute("deferred") != true;
                }
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
                if (output != null) output.GetAwaiter().GetResult();
                if (errors != null) errors.GetAwaiter().GetResult();
            }
        }
        catch (Exception error)
        {
            lock (gate) { if (canceled) return false; }
            BuildEngine.LogErrorEvent(new BuildErrorEventArgs("SoloDB", "SOLO003", Context, 0, 0, 0, 0,
                error.ToString(), null, nameof(PrepareFSharpProject)));
            return false;
        }
        finally
        {
            if (Directory.Exists(directory)) Directory.Delete(directory, true);
        }
    }

    private sealed class ProjectArgument : ITaskItem
    {
        internal ProjectArgument(string value) { ItemSpec = value; }
        public string ItemSpec { get; set; }
        public int MetadataCount => 0;
        public System.Collections.ICollection MetadataNames => Array.Empty<string>();
        public string GetMetadata(string name) => "";
        public void SetMetadata(string name,string value) => throw new NotSupportedException();
        public void RemoveMetadata(string name) => throw new NotSupportedException();
        public System.Collections.IDictionary CloneCustomMetadata() => new System.Collections.Hashtable();
        public void CopyMetadataTo(ITaskItem target) { }
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
