using System;
using System.IO;
using System.Linq;
using System.Xml.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;

namespace SoloDatabase.Metadata.Tools;

internal static class GeneratedConsumerOutput
{
    internal static void ReadSymbols(AssemblyDefinition consumer,string input)
    {
        if (!HelperAssembly.HasCompilerSymbols(consumer.MainModule)) return;
        var reader = new DefaultSymbolReaderProvider(false).GetSymbolReader(consumer.MainModule,input);
        if (reader == null) return;
        try { consumer.MainModule.ReadSymbols(reader); }
        catch { reader.Dispose(); throw; }
    }

    internal static void Inactive(string input,string outputDirectory,string helper,string selection,string[] dependencies)
    {
        var obsolete = Path.Combine(outputDirectory,Path.GetFileName(input));
        if (Path.GetFullPath(obsolete) != Path.GetFullPath(input))
        {
            if (File.Exists(obsolete)) File.Delete(obsolete);
            if (File.Exists(Path.ChangeExtension(obsolete,".pdb"))) File.Delete(Path.ChangeExtension(obsolete,".pdb"));
        }
        if (File.Exists(helper)) File.Delete(helper);
        if (File.Exists(selection)) File.Delete(selection);
    }

    internal static void PublishConsumer(AssemblyDefinition consumer,string input,string outputDirectory,string selection,
        string key,string helper,string[] dependencies)
    {
        var output = Write(consumer,input,outputDirectory,key);
        Select(selection,output.Assembly,output.Symbols,output.CompilerSymbols,true,helper,dependencies);
    }

    internal static (string Assembly,string Symbols,string CompilerSymbols) Write(AssemblyDefinition consumer,string input,
        string outputDirectory,string key,Action? serialized = null)
    {
        Directory.CreateDirectory(outputDirectory);
        var output = Path.Combine(outputDirectory,Path.GetFileName(input));
        var sidecar = consumer.MainModule.HasSymbols && consumer.MainModule.SymbolReader is not EmbeddedPortablePdbReader;
        using var image = new MemoryStream();
        using var symbols = consumer.MainModule.SymbolReader is PortablePdbReader ? new MemoryStream() : null;
        var writer = new WriterParameters
        {
            WriteSymbols = consumer.MainModule.HasSymbols,
            SymbolWriterProvider = consumer.MainModule.HasSymbols
                ? new HelperAssembly.OutputSymbolWriterProvider(consumer.MainModule.SymbolReader.GetWriterProvider(),output) : null,
            SymbolStream = symbols
        };
        if (key.Length != 0) writer.StrongNameKeyBlob = File.ReadAllBytes(key);
        consumer.Write(image,writer);
        serialized?.Invoke();
        if (symbols != null) HelperAssembly.Publish(Path.ChangeExtension(output,".pdb"),symbols.ToArray());
        HelperAssembly.Publish(output,image.ToArray());
        return (output,sidecar ? Path.ChangeExtension(output,".pdb") : "",sidecar ? Path.ChangeExtension(input,".pdb") : "");
    }

    internal static string Value(XElement state,string name) => (string?)state.Attribute(name)
        ?? throw new InvalidDataException("The generated output selection has no " + name + " fact.");

    private static void Select(string selection,string assembly,string symbols,string compilerSymbols,bool changed,string helper,string[] dependencies)
        => HelperAssembly.Publish(selection,new XElement("selection",new XAttribute("assembly",assembly),
            new XAttribute("symbols",symbols),new XAttribute("compilerSymbols",compilerSymbols),new XAttribute("changed",changed),
            new XAttribute("helper",helper),dependencies.Select(path => new XElement("dependency",path))));

}
