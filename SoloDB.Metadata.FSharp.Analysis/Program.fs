namespace SoloDatabase.Metadata.FSharp

open System
open System.IO
open System.Globalization
open System.Xml
open System.Xml.Linq

module internal Program =
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
                let template =
                    match request.Attribute(XName.Get "project") with
                    | null -> ProjectGeneration.Write(contextPath, field "assembly", field "attributes", field "template", workspace, report, timing)
                    | project ->
                        let arguments = request.Element(XName.Get "arguments").Elements(XName.Get "argument") |> Seq.map(fun item -> item.Value) |> Seq.toArray
                        if request.Attribute(XName.Get "input") = null then
                            ProjectGeneration.Write(contextPath, project.Value, arguments, field "assembly", field "attributes", field "template", workspace, report, timing)
                        else
                            ProjectGeneration.Complete(contextPath, project.Value, arguments, field "assembly", field "attributes", field "template", workspace, report, timing,
                                field "input", field "outputDirectory", field "helper", field "selection")
                diagnostics.Add(XAttribute(XName.Get "template", template))
                true
            with error ->
                add "error" contextPath 0 0 (error.ToString())
                false
        diagnostics.Add(XAttribute(XName.Get "success", success))
        use output = XmlWriter.Create(requestPath + ".result", XmlWriterSettings(NewLineHandling = NewLineHandling.Entitize))
        XDocument(diagnostics).Save(output)
        if success then 0 else 1
