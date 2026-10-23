namespace SoloDatabase.Metadata.FSharp

open System
open System.IO
open System.Diagnostics
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
        ProjectGeneration.Write(contextPath, assemblyName, attributeSource, templatePath, workspace, report, null)

    static member internal Write(contextPath: string, assemblyName: string, attributeSource: string, templatePath: string, workspace: string, report: Action<string,int,int,string>, timing: Action<string,float>) =
        let project, arguments =
            use context = AssemblyDefinition.ReadAssembly(contextPath)
            let typ = context.MainModule.GetType("SoloDatabase.Tooling.ProjectContext")
            let literal name = typ.Fields |> Seq.find(fun field -> field.Name = name) |> fun field -> field.Constant :?> string
            literal "Project", (literal "Arguments").Split([|'\000'|], StringSplitOptions.None)
        ProjectGeneration.Write(contextPath, project, arguments, assemblyName, attributeSource, templatePath, workspace, report, timing)

    static member Write(contextPath: string, project: string, arguments: string[], assemblyName: string, attributeSource: string, templatePath: string, workspace: string, report: Action<string,int,int,string>) =
        ProjectGeneration.Write(contextPath, project, arguments, assemblyName, attributeSource, templatePath, workspace, report, null)

    static member internal Write(contextPath: string, project: string, arguments: string[], assemblyName: string, attributeSource: string, templatePath: string, workspace: string, report: Action<string,int,int,string>, timing: Action<string,float>) =
        ProjectGeneration.Generate(contextPath,project,arguments,assemblyName,attributeSource,templatePath,workspace,report,timing,"","","","",null,null)

    static member internal Complete(contextPath,project,arguments,assemblyName,attributeSource,templatePath,workspace,report,timing,input,outputDirectory,helperPath,selection) =
        ProjectGeneration.Complete(contextPath,project,arguments,assemblyName,attributeSource,templatePath,workspace,report,timing,input,outputDirectory,helperPath,selection,null,null)

    static member internal Complete(contextPath,project,arguments,assemblyName,attributeSource,templatePath,workspace,report,timing,input,outputDirectory,helperPath,selection,waitForCompiler: Action,observe: Action<string>) =
        ProjectGeneration.Generate(contextPath,project,arguments,assemblyName,attributeSource,templatePath,workspace,report,timing,input,outputDirectory,helperPath,selection,waitForCompiler,observe)

    static member private Generate(contextPath: string, project: string, arguments: string[], assemblyName: string, attributeSource: string,
        templatePath: string, workspace: string, report: Action<string,int,int,string>, timing: Action<string,float>,
        input: string, outputDirectory: string, helperPath: string, selection: string, waitForCompiler: Action,observe: Action<string>) =
        let measure name work =
            if isNull timing then work() else
            let watch = Stopwatch.StartNew()
            try work()
            finally timing.Invoke(name, watch.Elapsed.TotalSeconds)
        try
            Directory.CreateDirectory(workspace) |> ignore
            let arguments = arguments |> Array.filter(CompilerContext.IsContextArgument >> not)
            let inputs = CurrentAssembly.inputsWith observe arguments (Path.GetDirectoryName(project))
            let dependencies = ResizeArray<string>(inputs.Dependencies)
            let writeSource content =
                Directory.CreateDirectory(Path.GetDirectoryName(attributeSource)) |> ignore
                if not(File.Exists(attributeSource)) || File.ReadAllText(attributeSource) <> content then
                    File.WriteAllText(attributeSource, content)
            let analysis = Path.Combine(workspace, "SoloDB.ProjectContext.dll")
            CompilerContext.Write(analysis, project, "analysis", arguments)
            let checker = FSharpChecker.Create(keepAssemblyContents = true, parallelReferenceResolution = true)
            let options = checker.GetProjectOptionsFromCommandLineArgs(project, Array.append arguments [|"-r:" + analysis|])
            let parsing, diagnostics = checker.GetParsingOptionsFromProjectOptions(options)
            if parsing.SourceFiles.Length = 0 || diagnostics |> List.exists(fun diagnostic -> diagnostic.Severity = FSharpDiagnosticSeverity.Error) then
                invalidOp "The current FSharp compiler inputs do not describe a valid source project."
            let parsedInputs = System.Collections.Generic.Dictionary<string,ParsedInput>(StringComparer.Ordinal)
            measure "parse" (fun () ->
                for source in parsing.SourceFiles do
                    let source = Path.GetFullPath(if Path.IsPathRooted(source) then source else Path.Combine(Path.GetDirectoryName(project),source))
                    dependencies.Add(source)
                    if not(isNull observe) then observe.Invoke(source)
                    let parsed = checker.ParseFile(source, SourceText.ofString(File.ReadAllText(source)), parsing) |> Async.RunSynchronously
                    parsedInputs.Add(source, parsed.ParseTree))
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
                let result = measure "check" (fun () -> checker.ParseAndCheckProject(options) |> Async.RunSynchronously)
                if result.HasCriticalErrors || result.Diagnostics |> Array.exists(fun diagnostic -> diagnostic.Severity = FSharpDiagnosticSeverity.Error) then
                    invalidOp ("SoloDB generation could not check the current source project:\n" + (result.Diagnostics |> Array.map string |> String.concat "\n"))
                let uses = measure "symbol-uses" (fun () -> result.GetAllUsesOfAllSymbols())
                for assembly in result.ProjectContext.GetReferencedAssemblies() do
                    match assembly.FileName with
                    // Provider-generated assemblies are in-memory products, even
                    // when FCS exposes a nominal filename for them.
                    | Some file when not assembly.IsProviderGenerated && Path.GetFullPath(file) <> Path.GetFullPath(analysis) -> dependencies.Add(Path.GetFullPath(file))
                    | _ -> ()
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
                if active then
                    let rec enclosing (entity: FSharpEntity) =
                        let parent =
                            match entity.DeclaringEntity with
                            | Some parent -> enclosing parent
                            | None when entity.Namespace.IsNone -> []
                            | None -> invalidOp "The checked generated alias has no original enclosing compiler identity."
                        parent @ [entity.CompiledName]
                    let paths =
                        aliases |> Seq.map(fun alias ->
                            let parent =
                                match alias.DeclaringEntity with
                                | Some parent -> enclosing parent
                                | None when alias.Namespace.IsNone -> []
                                | None -> invalidOp "The checked generated alias has no original enclosing compiler identity."
                            Array.ofList(parent @ [alias.LogicalName]))
                        |> Seq.distinct |> Seq.toArray
                    let rec compiledName (entity: FSharpEntity) =
                        match entity.DeclaringEntity with
                        | Some parent when not parent.IsNamespace -> compiledName parent + "/" + entity.CompiledName
                        | _ -> (entity.Namespace |> Option.map(fun name -> name + ".") |> Option.defaultValue "") + entity.CompiledName
                    Some(result,uses,paths,aliases |> Seq.map compiledName |> Seq.distinct |> Seq.toArray)
                else None
            match checkedProject with
            | None ->
                if not(isNull waitForCompiler) then waitForCompiler.Invoke()
                if input.Length = 0 then
                    File.Delete(templatePath)
                    writeSource "namespace SoloDatabase.Generated\ndo ()\n"
                    CompilerContext.Write(contextPath, project, "inactive", arguments, "", "", dependencies.ToArray() |> Array.distinct)
                else
                    FSharpConsumerOutput.Inactive(input,outputDirectory,helperPath,selection,dependencies.ToArray() |> Array.distinct)
                    if File.Exists(templatePath) then File.Delete(templatePath)
                    if File.Exists(attributeSource) then File.Delete(attributeSource)
                ""
            | Some(result,uses,aliases,aliasNames) ->
                let current = measure "assembly-facts" (fun () -> CurrentAssembly.acquireWith observe result inputs (Path.GetDirectoryName(project)) assemblyName)
                let referencePaths =
                    result.ProjectContext.GetReferencedAssemblies()
                    |> Seq.choose(fun assembly ->
                        match assembly.FileName with
                        | Some file when not assembly.IsProviderGenerated && Path.GetFullPath(file) <> Path.GetFullPath(analysis) -> Some(Path.GetFullPath(file))
                        | _ -> None)
                    |> Seq.distinct |> Seq.toArray
                let productPath =
                    referencePaths |> Array.filter(fun path -> Path.GetFileName(path) = "SoloDB.dll")
                    |> function [|path|] -> path | _ -> invalidOp "SoloDB generation requires exactly one current runtime reference."
                use product = AssemblyDefinition.ReadAssembly(productPath)
                use references = ModuleDefinition.CreateModule("SoloDB.CompilerReferences", ModuleKind.Dll)
                let queries, queryCalls, queryWitnesses, queryLambdas, inferredRoots, providerOperations, operationAccess, argumentSources = measure "query-discovery" (fun () -> QueryDiscovery.collect result uses references current.Identity parsedInputs (Path.GetDirectoryName(project)) (fun location methodName ->
                    report.Invoke(location.FileName,location.StartLine,location.StartColumn + 1,
                        sprintf "Cannot infer closed types for SoloDB use '%s'. Supply explicit SoloDBAOT model roots and a closed query signature for any runtime-selected use." methodName)))
                let models = measure "model-discovery" (fun () -> ModelDiscovery.collect result references current.Identity inferredRoots parsedInputs (Path.GetDirectoryName(project)))
                let queryWitnesses, queryCalls =
                    if queryWitnesses.Length = 0 then queryWitnesses, queryCalls else
                    let core = result.ProjectContext.GetReferencedAssemblies() |> Seq.find(fun assembly -> assembly.SimpleName = "FSharp.Core")
                    let path = core.FileName |> Option.defaultWith(fun () -> invalidOp "The current FSharp.Core expression-construction input has no assembly path.")
                    let struct(witnesses, calls) = QueryConstructionWitnesses.Read(path, references)
                    Array.append queryWitnesses witnesses, Array.append queryCalls calls
                let hasAttribute = ModelDiscovery.hasAccessAttribute result
                if not(isNull waitForCompiler) then waitForCompiler.Invoke()
                if input.Length = 0 then
                    let access = Array.append [|product.Name.Name|] (models |> Array.collect(fun model -> model.AccessAssemblies |> Seq.toArray)) |> Array.distinct |> Array.sort
                    let source = Text.StringBuilder()
                    if access.Length > 0 && not hasAttribute then
                        source.AppendLine("namespace System.Runtime.CompilerServices\nopen System\n[<Sealed; AttributeUsage(AttributeTargets.Assembly, AllowMultiple = true)>]\ntype internal IgnoresAccessChecksToAttribute(_target: string) =\n    inherit Attribute()\n") |> ignore
                    source.AppendLine("namespace SoloDatabase.Generated\n") |> ignore
                    source.AppendLine("do ()") |> ignore
                    let attribute = if access.Length = 0 then null else ModelDiscovery.accessAttribute references current.Identity
                    // The shared owner survives static linking beside original types.
                    let occupied = System.Collections.Generic.HashSet<string>(StringComparer.Ordinal)
                    let rec originalNames declarations =
                        for declaration in declarations do
                            match declaration with
                            | FSharpImplementationFileDeclaration.Entity(entity, children) ->
                                if entity.Namespace = Some "SoloDatabase.Generated" &&
                                   (entity.DeclaringEntity |> Option.forall(fun parent -> parent.IsNamespace)) then
                                    occupied.Add(entity.CompiledName) |> ignore
                                originalNames children
                            | _ -> ()
                    for file in result.AssemblyContents.ImplementationFiles do originalNames file.Declarations
                    let mutable name = "Template"
                    let mutable ordinal = 0
                    while occupied.Contains(name) do
                        ordinal <- ordinal + 1
                        name <- "Template" + string ordinal
                    measure "emit" (fun () -> GeneratedJsonAssembly.Write(templatePath, "SoloDB.Generated.FSharp." + current.Identity.Name, name, product, models, attribute, queries, queryCalls, queryWitnesses, queryLambdas, providerOperations, referencePaths, operationAccess, argumentSources, aliases))
                    writeSource (source.ToString())
                    // Publish the provider context only after all current outputs succeed.
                    CompilerContext.Write(contextPath, project, "execute", arguments, Path.GetFullPath(templatePath), current.FullSigningKeyFile,
                        Array.append (dependencies.ToArray()) current.Dependencies |> Array.distinct)
                    Path.GetFullPath(templatePath)
                else
                    measure "emit" (fun () -> GeneratedJsonAssembly.WriteCompanion(helperPath,input,outputDirectory,selection,
                        product,models,queries,queryCalls,queryWitnesses,queryLambdas,providerOperations,referencePaths,
                        operationAccess,argumentSources,aliases,aliasNames,current.Identity,current.FullSigningKeyFile,
                        Array.append (dependencies.ToArray()) current.Dependencies |> Array.distinct))
                    if File.Exists(templatePath) then File.Delete(templatePath)
                    if File.Exists(attributeSource) then File.Delete(attributeSource)
                    helperPath
        finally
            if Directory.Exists(workspace) then Directory.Delete(workspace, true)
