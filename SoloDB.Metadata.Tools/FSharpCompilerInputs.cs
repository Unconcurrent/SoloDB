using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace SoloDatabase.Metadata.Tools;

// Cold compiler option acquisition shared by checked and physical assembly facts.
internal sealed class FSharpCompilerInputs
{
    internal string Directory { get; }
    internal string[] Arguments { get; }
    internal string[] NormalizedArguments { get; }
    internal string[] Dependencies { get; }

    internal FSharpCompilerInputs(string[] arguments, string directory)
    {
        Directory = directory;
        var dependencies = new List<string>();
        IEnumerable<string> Expand(IEnumerable<string> values)
        {
            foreach (var value in values)
                if (value.StartsWith("@", StringComparison.Ordinal))
                {
                    var file = Resolve(value.TrimStart('@'));
                    dependencies.Add(file);
                    foreach (var nested in Expand(File.ReadLines(file)
                        .Where(line => !string.IsNullOrWhiteSpace(line) && !line.StartsWith("#", StringComparison.Ordinal))
                        .Select(line => line.Trim()))) yield return nested;
                }
                else yield return value;
        }
        Arguments = Expand(arguments).ToArray();
        Dependencies = dependencies.ToArray();
        NormalizedArguments = Arguments.Select(value => value.StartsWith("--", StringComparison.Ordinal) ? value.Substring(2)
            : value.StartsWith("/", StringComparison.Ordinal) && !value.Substring(1).Contains('/') ? value.Substring(1) : value).ToArray();
    }

    internal string Resolve(string value) => Path.GetFullPath(Path.IsPathRooted(value) ? value : Path.Combine(Directory,value));
    internal string? Option(string name)
    {
        for (var index = NormalizedArguments.Length - 1; index >= 0; index--)
        {
            var value = NormalizedArguments[index];
            if (value.StartsWith(name + ":", StringComparison.Ordinal)) return value.Substring(name.Length + 1).Trim('"');
        }
        return null;
    }
    internal bool Switch(string name)
    {
        for (var index = NormalizedArguments.Length - 1; index >= 0; index--)
        {
            var value = NormalizedArguments[index];
            if (value == name || value == name + "+") return true;
            if (value == name + "-") return false;
        }
        return false;
    }
}
