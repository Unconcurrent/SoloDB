namespace SoloDatabase.Metadata.FSharp

open System
open System.IO
open System.Globalization
open System.Xml
open System.Xml.Linq

module internal Program =
    exception private CompilerInputsChanged

    [<EntryPoint>]
    let main arguments =
        let requestPath =
            match arguments with
            | [| path |] -> path
            | _ -> invalidArg (nameof arguments) "FSharp project analysis requires its current project request."
        let diagnostics = XElement(XName.Get "diagnostics")
        let mutable contextPath = requestPath
        let add severity file line column message =
            diagnostics.Add(XElement(XName.Get "diagnostic",
                XAttribute(XName.Get "severity", severity), XAttribute(XName.Get "code", "SOLO003"),
                XAttribute(XName.Get "file", file), XAttribute(XName.Get "line", line),
                XAttribute(XName.Get "column", column), message))
        let success =
            try
                let request = XDocument.Load(requestPath).Root
                let field name = request.Attribute(XName.Get name).Value
                contextPath <- field "context"
                let workspace = Path.Combine(Path.GetDirectoryName(requestPath), "analysis")
                let report = Action<string,int,int,string>(fun file line column message -> add "warning" file line column message)
                let timing =
                    if request.Attribute(XName.Get "timings") = null || request.Attribute(XName.Get "timings").Value <> "true" then null else
                    Action<string,float>(fun phase seconds ->
                        diagnostics.Add(XElement(XName.Get "phase", XAttribute(XName.Get "name", phase),
                            XAttribute(XName.Get "seconds", seconds.ToString("R", CultureInfo.InvariantCulture)))))
                let retained = request.Attribute(XName.Get "awaitCompiler") <> null && request.Attribute(XName.Get "awaitCompiler").Value = "true"
                let stamps = System.Collections.Generic.Dictionary<string,struct(bool * int64 * DateTime)>(StringComparer.Ordinal)
                let stamp path =
                    let file = FileInfo(path)
                    if file.Exists then struct(true,file.Length,file.LastWriteTimeUtc) else struct(false,0L,DateTime.MinValue)
                let observe =
                    if not retained then null else
                    Action<string>(fun path -> if not(stamps.ContainsKey(path)) then stamps.Add(path,stamp path))
                if retained then
                    for file in request.Element(XName.Get "acquisitionInputs").Elements(XName.Get "file") do observe.Invoke(file.Value)
                let template =
                    match request.Attribute(XName.Get "project") with
                    | null -> ProjectGeneration.Write(contextPath, field "assembly", field "attributes", field "template", workspace, report, timing)
                    | project ->
                        let arguments = request.Element(XName.Get "arguments").Elements(XName.Get "argument") |> Seq.map(fun item -> item.Value) |> Seq.toArray
                        if request.Attribute(XName.Get "input") = null then
                            ProjectGeneration.Write(contextPath, project.Value, arguments, field "assembly", field "attributes", field "template", workspace, report, timing)
                        else
                            let mutable joined = false
                            let waitForCompiler =
                                if not retained then null else
                                Action(fun () ->
                                    if not joined then
                                        let compiler = XElement.Parse(Console.In.ReadToEnd())
                                        let actual = compiler.Elements(XName.Get "argument") |> Seq.map(fun item -> item.Value) |> Seq.toArray
                                        joined <- true
                                        let original values = values |> Array.filter(SoloDatabase.Metadata.Tools.CompilerContext.IsContextArgument >> not)
                                        if compiler.Attribute(XName.Get "captured").Value <> "true" || compiler.Attribute(XName.Get "sameOutput").Value <> "true" ||
                                           original actual <> original arguments || stamps |> Seq.exists(fun entry ->
                                            let struct(exists,_,_) = entry.Value
                                            not exists || entry.Value <> stamp entry.Key) then
                                            raise CompilerInputsChanged)
                            let complete () =
                                ProjectGeneration.Complete(contextPath, project.Value, arguments, field "assembly", field "attributes", field "template", workspace, report, timing,
                                    field "input", field "outputDirectory", field "helper", field "selection", waitForCompiler, observe)
                            try
                                try complete ()
                                with _ ->
                                    // A producer may create a source only inside CoreCompile.
                                    // Do not publish an early acquisition error before that join.
                                    if not(isNull waitForCompiler) && not joined then
                                        waitForCompiler.Invoke()
                                        raise CompilerInputsChanged
                                    reraise()
                            with CompilerInputsChanged ->
                                for diagnostic in diagnostics.Elements(XName.Get "diagnostic") |> Seq.toArray do diagnostic.Remove()
                                for phase in diagnostics.Elements(XName.Get "phase") do
                                    phase.SetAttributeValue(XName.Get "name","discarded-" + phase.Attribute(XName.Get "name").Value)
                                diagnostics.Add(XAttribute(XName.Get "deferred",true))
                                ""
                diagnostics.Add(XAttribute(XName.Get "template", template))
                true
            with error ->
                add "error" contextPath 0 0 (error.ToString())
                false
        diagnostics.Add(XAttribute(XName.Get "success", success))
        use output = XmlWriter.Create(requestPath + ".result", XmlWriterSettings(NewLineHandling = NewLineHandling.Entitize))
        XDocument(diagnostics).Save(output)
        if success then 0 else 1
