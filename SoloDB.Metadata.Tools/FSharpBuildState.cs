using System;
using System.IO;
using System.Linq;
using System.Xml.Linq;
using Microsoft.Build.Framework;
using Mono.Cecil;

namespace SoloDatabase.Metadata.Tools;

internal static class FSharpBuildState
{
    internal static string Value(XElement state, string name) => (string?)state.Attribute(name)
        ?? throw new InvalidDataException("The FSharp build selection has no " + name + " fact.");

    internal static (string Mode, string Template, string Key, string[] Dependencies) Context(string path)
    {
        using var context = AssemblyDefinition.ReadAssembly(path);
        var facts = context.MainModule.GetType("SoloDatabase.Tooling.ProjectContext");
        string Literal(string name) => (string)facts.Fields.Single(field => field.Name == name).Constant;
        var mode = Literal("Mode");
        if (mode != "execute" && mode != "inactive" && mode != "check")
            throw new InvalidOperationException("FSharp executable selection requires current executable compiler inputs.");
        var dependencies = facts.Fields.FirstOrDefault(field => field.Name == "Dependencies")?.Constant as string;
        return (mode, Literal("Template"), Literal("FullSigningKeyFile"),
            string.IsNullOrEmpty(dependencies) ? Array.Empty<string>() : dependencies!.Split('\0'));
    }

    internal static bool Error(IBuildEngine engine, string code, string file, Exception error)
    {
        engine.LogErrorEvent(new BuildErrorEventArgs("SoloDB", code, file, 0, 0, 0, 0,
            error.ToString(), null, nameof(FSharpBuildState)));
        return false;
    }
}

public sealed class FSharpPreparationInputs : ITask
{
    public IBuildEngine BuildEngine { get; set; } = null!;
    public ITaskHost HostObject { get; set; } = null!;
    [Required] public string InputFile { get; set; } = "";
    [Required] public string Stamp { get; set; } = "";
    [Required] public string Context { get; set; } = "";
    [Required] public string Project { get; set; } = "";
    [Required] public string AssemblyName { get; set; } = "";
    [Required] public string AttributeSource { get; set; } = "";
    [Required] public string Template { get; set; } = "";
    [Required] public string Provider { get; set; } = "";
    [Required] public string DotNetHost { get; set; } = "";
    [Required] public ITaskItem[] Arguments { get; set; } = Array.Empty<ITaskItem>();
    public ITaskItem[] ImplementationInputs { get; set; } = Array.Empty<ITaskItem>();
    [Output] public string ActiveTemplate { get; private set; } = "";
    [Output] public string[] Dependencies { get; private set; } = Array.Empty<string>();

    public bool Execute()
    {
        ActiveTemplate = "";
        Dependencies = Array.Empty<string>();
        try
        {
            HelperAssembly.Publish(InputFile, new XElement("inputs",
                new XAttribute("context", Context), new XAttribute("project", Project),
                new XAttribute("assembly", AssemblyName), new XAttribute("attributes", AttributeSource),
                new XAttribute("template", Template), new XAttribute("provider", Provider), new XAttribute("host", DotNetHost),
                ImplementationInputs.Select(input => new XElement("implementation", input.ItemSpec)),
                Arguments.Where(argument => !CompilerContext.IsContextArgument(argument.ItemSpec))
                    .Select(argument => new XElement("argument", argument.ItemSpec))));
            if (!File.Exists(Stamp)) return true;
            if (File.Exists(Context) && File.Exists(AttributeSource))
            {
                var selection = FSharpBuildState.Context(Context);
                Dependencies = selection.Dependencies;
                if (selection.Mode == "inactive") return true;
                if (selection.Template == Template && File.Exists(Template))
                {
                    ActiveTemplate = Template;
                    return true;
                }
            }
            File.Delete(Stamp);
            return true;
        }
        catch (Exception error) { return FSharpBuildState.Error(BuildEngine, "SOLO003", Project, error); }
    }
}

public sealed class FSharpCompletionInputs : ITask
{
    public IBuildEngine BuildEngine { get; set; } = null!;
    public ITaskHost HostObject { get; set; } = null!;
    [Required] public string InputFile { get; set; } = "";
    [Required] public string Stamp { get; set; } = "";
    [Required] public string Selection { get; set; } = "";
    [Required] public string Input { get; set; } = "";
    [Required] public string Context { get; set; } = "";
    [Required] public string OutputDirectory { get; set; } = "";
    public ITaskItem[] References { get; set; } = Array.Empty<ITaskItem>();
    public ITaskItem[] ImplementationInputs { get; set; } = Array.Empty<ITaskItem>();
    public ITaskItem[] AdditionalInputs { get; set; } = Array.Empty<ITaskItem>();
    public bool ReadSelectionOnly { get; set; }
    [Output] public bool NeedsWork { get; private set; } = true;
    [Output] public string Helper { get; private set; } = "";
    [Output] public string Template { get; private set; } = "";
    [Output] public string SigningKey { get; private set; } = "";
    [Output] public string Assembly { get; private set; } = "";
    [Output] public string Symbols { get; private set; } = "";
    [Output] public string CompilerSymbols { get; private set; } = "";
    [Output] public bool Changed { get; private set; }
    [Output] public string[] Dependencies { get; private set; } = Array.Empty<string>();

    public bool Execute()
    {
        Helper = Template = SigningKey = Assembly = Symbols = CompilerSymbols = "";
        Changed = false;
        NeedsWork = true;
        Dependencies = Array.Empty<string>();
        try
        {
            var context = FSharpBuildState.Context(Context);
            Template = context.Template;
            SigningKey = context.Key;
            Dependencies = context.Dependencies;
            if (!ReadSelectionOnly) HelperAssembly.Publish(InputFile, new XElement("inputs", new XAttribute("input", Input),
                new XAttribute("context", Context), new XAttribute("output", OutputDirectory),
                ImplementationInputs.Select(input => new XElement("implementation", input.ItemSpec)),
                References.Select(reference => new XElement("reference", reference.ItemSpec))));
            if (File.Exists(Selection))
            {
                var state = XElement.Load(Selection);
                Assembly = FSharpBuildState.Value(state, "assembly");
                Symbols = FSharpBuildState.Value(state, "symbols");
                CompilerSymbols = FSharpBuildState.Value(state, "compilerSymbols");
                Changed = (bool)state.Attribute("changed")!;
                Helper = (string?)state.Attribute("helper") ?? "";
                Dependencies = Dependencies.Concat(state.Elements("dependency").Select(value => value.Value)).Distinct().ToArray();
                if (File.Exists(Assembly) && (Helper.Length == 0 || File.Exists(Helper)) && (Symbols.Length == 0 || File.Exists(Symbols)) &&
                    (CompilerSymbols.Length == 0 || File.Exists(CompilerSymbols)))
                {
                    if (File.Exists(Stamp))
                    {
                        var stamp = File.GetLastWriteTimeUtc(Stamp);
                        NeedsWork = new[] { InputFile, Input, Context, Template, SigningKey }
                            .Concat(References.Select(item => item.ItemSpec))
                            .Concat(ImplementationInputs.Select(item => item.ItemSpec))
                            .Concat(AdditionalInputs.Select(item => item.ItemSpec)).Concat(Dependencies)
                            .Where(path => path.Length != 0)
                            .Any(path => !File.Exists(path) || File.GetLastWriteTimeUtc(path) > stamp);
                    }
                    return true;
                }
            }
            File.Delete(Stamp);
            return true;
        }
        catch (Exception error) { return FSharpBuildState.Error(BuildEngine, "SOLO004", Input, error); }
    }
}
