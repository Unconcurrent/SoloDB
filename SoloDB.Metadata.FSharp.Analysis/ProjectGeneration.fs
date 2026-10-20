namespace SoloDatabase.Metadata.FSharp

open System
open System.IO
open FSharp.Compiler.CodeAnalysis
open FSharp.Compiler.Diagnostics
open FSharp.Compiler.Syntax
open FSharp.Compiler.Symbols
open FSharp.Compiler.Text
open Mono.Cecil
open SoloDatabase.Metadata.Tools

/// Produces the executable template and its final compiler inputs together.
type ProjectGeneration private () =
    static member Write(contextPath: string, assemblyName: string, attributeSource: string, templatePath: string, workspace: string, report: Action<string,int,int,string>) =
        try
            Directory.CreateDirectory(workspace) |> ignore
            let project, arguments =
                use context = AssemblyDefinition.ReadAssembly(contextPath)
                let typ = context.MainModule.GetType("SoloDatabase.Tooling.ProjectContext")
                let literal name = typ.Fields |> Seq.find(fun field -> field.Name = name) |> fun field -> field.Constant :?> string
                literal "Project", (literal "Arguments").Split([|'\000'|], StringSplitOptions.None)
            let analysis = Path.Combine(workspace, "SoloDB.ProjectContext.dll")
            CompilerContext.Write(analysis, project, "analysis", arguments)
            let checker = FSharpChecker.Create(keepAssemblyContents = true)
            let options = checker.GetProjectOptionsFromCommandLineArgs(project, Array.append arguments [|"-r:" + analysis|])
            let parsing, diagnostics = checker.GetParsingOptionsFromProjectOptions(options)
            if parsing.SourceFiles.Length = 0 || diagnostics |> List.exists(fun diagnostic -> diagnostic.Severity = FSharpDiagnosticSeverity.Error) then
                invalidOp "The current FSharp compiler inputs do not describe a valid source project."
            let parsedInputs = System.Collections.Generic.Dictionary<string,ParsedInput>(StringComparer.Ordinal)
            for source in parsing.SourceFiles do
                let source = Path.GetFullPath(if Path.IsPathRooted(source) then source else Path.Combine(Path.GetDirectoryName(project),source))
                let parsed = checker.ParseFile(source, SourceText.ofString(File.ReadAllText(source)), parsing) |> Async.RunSynchronously
                parsedInputs.Add(source, parsed.ParseTree)
            let candidate input =
                ParsedInput.fold (fun found _ node ->
                    found ||
                    match node with
                    | SyntaxNode.SynExpr(SynExpr.LongIdent(longDotId = name))
                    | SyntaxNode.SynExpr(SynExpr.DotGet(longDotId = name)) ->
                        name.LongIdent |> List.tryLast |> Option.exists(fun ident -> ident.idText = "register")
                    | SyntaxNode.SynExpr(SynExpr.Ident ident) -> ident.idText = "register"
                    | _ -> false) false input
            let checkedProject =
                if not(parsedInputs.Values |> Seq.exists candidate) then None else
                let result = checker.ParseAndCheckProject(options) |> Async.RunSynchronously
                if result.HasCriticalErrors || result.Diagnostics |> Array.exists(fun diagnostic -> diagnostic.Severity = FSharpDiagnosticSeverity.Error) then
                    invalidOp ("SoloDB generation could not check the current source project:\n" + (result.Diagnostics |> Array.map string |> String.concat "\n"))
                let uses = result.GetAllUsesOfAllSymbols()
                let providerIdentity = result.ProjectContext.GetReferencedAssemblies()
                                       |> Seq.find(fun assembly -> assembly.SimpleName = "SoloDB.Metadata.FSharp")
                                       |> fun assembly -> "SoloDatabase.SoloDBAOT, " + assembly.QualifiedName
                let file (path: string) = Path.GetFullPath(if Path.IsPathRooted(path) then path else Path.Combine(Path.GetDirectoryName(project),path))
                let contains (outer: range) (inner: range) =
                    file outer.FileName = file inner.FileName && (outer.StartLine,outer.StartColumn) <= (inner.StartLine,inner.StartColumn) &&
                    (inner.EndLine,inner.EndColumn) <= (outer.EndLine,outer.EndColumn)
                let aliases = ResizeArray<FSharpEntity>()
                for input in parsedInputs.Values do
                    ParsedInput.fold (fun () _ node ->
                        match node with
                        | SyntaxNode.SynTypeDefn(SynTypeDefn(typeRepr = SynTypeDefnRepr.Simple(simpleRepr = SynTypeDefnSimpleRepr.TypeAbbrev _); range = binding)) ->
                            let owned = uses |> Array.exists(fun useSite ->
                                contains binding useSite.Range &&
                                match useSite.Symbol with
                                | :? FSharpEntity as entity -> entity.IsProvided && entity.QualifiedName = providerIdentity
                                | _ -> false)
                            if owned then
                                for useSite in uses do
                                    if useSite.IsFromDefinition && contains binding useSite.Range then
                                        match useSite.Symbol with
                                        | :? FSharpEntity as entity when entity.IsProvidedAndGenerated -> aliases.Add entity
                                        | _ -> ()
                        | _ -> ()) () input
                let active = uses |> Array.exists(fun useSite ->
                    not useSite.IsFromDefinition &&
                    match useSite.Symbol with
                    | :? FSharpMemberOrFunctionOrValue as method when method.CompiledName = "register" ->
                        method.DeclaringEntity |> Option.exists(fun entity -> aliases |> Seq.exists(entity.Equals))
                    | _ -> false)
                if active then Some result else None
            match checkedProject with
            | None ->
                File.Delete(templatePath)
                Directory.CreateDirectory(Path.GetDirectoryName(attributeSource)) |> ignore
                File.WriteAllText(attributeSource, "namespace SoloDatabase.Generated\ndo ()\n")
                CompilerContext.Write(contextPath, project, "inactive", arguments)
            | Some result ->
                let referencePaths =
                    arguments |> Array.choose(fun argument ->
                        let prefix = if argument.StartsWith("-r:", StringComparison.Ordinal) then 3 elif argument.StartsWith("--reference:", StringComparison.Ordinal) then 12 else 0
                        if prefix = 0 then None else Some(argument.Substring(prefix).Trim('"')))
                let productPath =
                    referencePaths |> Array.filter(fun path -> Path.GetFileName(path) = "SoloDB.dll")
                    |> function [|path|] -> path | _ -> invalidOp "SoloDB generation requires exactly one current runtime reference."
                use product = AssemblyDefinition.ReadAssembly(productPath)
                use references = ModuleDefinition.CreateModule("SoloDB.CompilerReferences", ModuleKind.Dll)
                let queries, queryCalls, queryWitnesses, queryLambdas, inferredRoots, providerOperations, operationAccess, argumentSources = QueryDiscovery.collect result references parsedInputs (Path.GetDirectoryName(project)) (fun location methodName ->
                    report.Invoke(location.FileName,location.StartLine,location.StartColumn + 1,
                        sprintf "Cannot infer closed types for SoloDB use '%s'. Supply explicit SoloDBAOT model roots and a closed query signature for any runtime-selected use." methodName))
                let models = ModelDiscovery.collect result references inferredRoots parsedInputs (Path.GetDirectoryName(project))
                let queryWitnesses, queryCalls =
                    if queryWitnesses.Length = 0 then queryWitnesses, queryCalls else
                    let core = result.ProjectContext.GetReferencedAssemblies() |> Seq.find(fun assembly -> assembly.SimpleName = "FSharp.Core")
                    let path = core.FileName |> Option.defaultWith(fun () -> invalidOp "The current FSharp.Core expression-construction input has no assembly path.")
                    let struct(witnesses, calls) = QueryConstructionWitnesses.Read(path, references)
                    Array.append queryWitnesses witnesses, Array.append queryCalls calls
                let hasAttribute = ModelDiscovery.hasAccessAttribute result
                let access = Array.append [|product.Name.Name|] (models |> Array.collect(fun model -> model.AccessAssemblies |> Seq.toArray)) |> Array.distinct |> Array.sort
                let source = Text.StringBuilder()
                if access.Length > 0 && not hasAttribute then
                    source.AppendLine("namespace System.Runtime.CompilerServices\nopen System\n[<Sealed; AttributeUsage(AttributeTargets.Assembly, AllowMultiple = true)>]\ntype internal IgnoresAccessChecksToAttribute(_target: string) =\n    inherit Attribute()\n") |> ignore
                source.AppendLine("namespace SoloDatabase.Generated\n") |> ignore
                source.AppendLine("do ()") |> ignore
                let template = Path.Combine(workspace, "Template.dll")
                let attribute = if access.Length = 0 then null else ModelDiscovery.accessAttribute result references assemblyName
                GeneratedJsonAssembly.Write(template, "SoloDB.Generated.FSharp", "Template", product, models, attribute, queries, queryCalls, queryWitnesses, queryLambdas, providerOperations, referencePaths, operationAccess, argumentSources)
                Directory.CreateDirectory(Path.GetDirectoryName(attributeSource)) |> ignore
                File.Copy(template, templatePath, true)
                File.WriteAllText(attributeSource, source.ToString())
                // Context publication is last. A failed producer never advertises an old template.
                CompilerContext.Write(contextPath, project, "execute", arguments, Path.GetFullPath(templatePath))
        finally
            if Directory.Exists(workspace) then Directory.Delete(workspace, true)
