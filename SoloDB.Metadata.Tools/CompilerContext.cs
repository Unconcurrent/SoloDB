using System;
using System.IO;
using System.Linq;
using Mono.Cecil;

namespace SoloDatabase.Metadata.Tools;

public static class CompilerContext
{
    public static bool IsContextArgument(string argument)
    {
        var prefix = argument.StartsWith("-r:", StringComparison.Ordinal) ? 3 :
            argument.StartsWith("--reference:", StringComparison.Ordinal) ? 12 : 0;
        return prefix != 0 && Path.GetFileName(argument.Substring(prefix).Trim('"')) == "SoloDB.ProjectContext.dll";
    }

    public static void Write(string output, string project, string mode, string[] arguments, string template = "")
        => Write(output, project, mode, arguments, template, "");

    internal static void Write(string output, string project, string mode, string[] arguments, string template, string fullSigningKeyFile)
        => Write(output, project, mode, arguments, template, fullSigningKeyFile, Array.Empty<string>());

    internal static void Write(string output, string project, string mode, string[] arguments, string template, string fullSigningKeyFile, string[] dependencies)
    {
        if (mode != "check" && mode != "execute" && mode != "analysis" && mode != "inactive")
            throw new ArgumentException("The compiler context mode must be check, execute, analysis or inactive.", nameof(mode));
        if (arguments.Length == 0) throw new InvalidOperationException("The evaluated FSharp compiler arguments are empty.");
        using var assembly = HelperAssembly.Create("SoloDB.ProjectContext");
        var module = assembly.MainModule;
        var context = new TypeDefinition("SoloDatabase.Tooling", "ProjectContext",
            TypeAttributes.Public | TypeAttributes.Abstract | TypeAttributes.Sealed,
            new TypeReference("System", "Object", module, module.AssemblyReferences[0]));
        module.Types.Add(context);
        Add("Project", Path.GetFullPath(project));
        Add("Mode", mode);
        Add("Template", template);
        Add("Format", "NulSeparated");
        Add("Arguments", string.Join("\0", arguments.Where(argument => !IsContextArgument(argument))));
        Add("FullSigningKeyFile", fullSigningKeyFile);
        Add("Dependencies", string.Join("\0", dependencies));
        HelperAssembly.Write(assembly, output, null);

        void Add(string name, string value) => context.Fields.Add(new FieldDefinition(name,
            FieldAttributes.Public | FieldAttributes.Static | FieldAttributes.Literal | FieldAttributes.HasDefault,
            module.TypeSystem.String) { Constant = value });
    }
}
