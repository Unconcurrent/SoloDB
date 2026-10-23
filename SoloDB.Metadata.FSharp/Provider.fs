namespace SoloDatabase.Metadata.FSharp

open System
open System.IO
open System.Reflection
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Core.CompilerServices
open ProviderImplementation.ProvidedTypes
open SoloDatabase.Metadata.Tools

[<TypeProvider>]
type ModelsProvider(config: TypeProviderConfig) =
    let helper = new TypeProviderForNamespaces(config)
    let root = ProvidedTypeDefinition(Assembly.GetExecutingAssembly(), "SoloDatabase", "SoloDBAOT",
                                     Some(helper.TargetContext.ConvertSourceTypeToTarget typeof<obj>), isErased = false)
    let invalidated = Event<EventHandler, EventArgs>()
    let context = lazy (
        let contextPath =
            match config.ReferencedAssemblies |> Array.filter(fun path -> Path.GetFileName(path) = "SoloDB.ProjectContext.dll") with
            | [| path |] -> path
            | _ -> invalidOp "SoloDB generation requires exactly one current compiler context. Import the SoloDB package build targets."
        let context = helper.TargetContext.ReadRelatedAssembly(contextPath).GetType("SoloDatabase.Tooling.ProjectContext")
        if context.GetField("Format").GetRawConstantValue() :?> string <> "NulSeparated" then
            invalidOp "The SoloDB compiler context format is not supported."
        context)
    let literal name = context.Value.GetField(name).GetRawConstantValue() :?> string
    let executable = lazy (
        let template = literal "Template"
        if String.IsNullOrEmpty(template) || not(File.Exists(template)) then
            invalidOp "SoloDB executable generation requires the completed current project template."
        let assembly = helper.RegisterGeneratedTargetAssembly(template)
        assembly.GetTypes()
        |> Array.choose(fun typ ->
            match typ.GetField("OriginalPath", BindingFlags.NonPublic ||| BindingFlags.Static) with
            | null -> None
            | field -> Some(field.GetRawConstantValue() :?> string,typ))
        |> dict)
    let generate (typePath: string[]) =
        let mode = literal "Mode"
        let name = typePath[typePath.Length - 1]
        let identity = "SoloDB.Generated.FSharp." + String.Join(".", typePath)
        match mode with
        | "execute" ->
            match executable.Value.TryGetValue(String.Join("\000",typePath)) with
            | true, typ -> typ
            | _ -> invalidOp ("The current template has no generated alias for the original compiler path: " + String.Join(".",typePath))
        | "check" | "analysis" | "inactive" ->
            let workspace = Path.Combine(config.TemporaryFolder, "SoloDB.Generated", Guid.NewGuid().ToString("N"))
            Directory.CreateDirectory(workspace) |> ignore
            try
                let output = Path.Combine(workspace, "SoloDB.Generated.FSharp.dll")
                GeneratedJsonAssembly.Surface(output, identity, name, typePath)
                // Registration retains assembly bytes and a fully read metadata reader.
                helper.RegisterGeneratedTargetAssembly(output).GetType("SoloDatabase.Generated." + name)
            finally
                Directory.Delete(workspace, true)
        | _ -> invalidOp ("Unknown SoloDB compiler context mode: " + mode)
    interface ITypeProvider with
        member _.GetNamespaces() =
            [| { new IProvidedNamespace with
                    member _.NamespaceName = "SoloDatabase"
                    member _.GetNestedNamespaces() = [||]
                    member _.GetTypes() = [|root :> Type|]
                    member _.ResolveTypeName(name) = if name = "SoloDBAOT" then root :> Type else null } |]
        member _.GetStaticParameters typ =
            if typ.Name = "SoloDBAOT" then
                [| ProvidedStaticParameter("Mode", helper.TargetContext.ConvertSourceTypeToTarget typeof<int>) :> ParameterInfo |]
            else [||]
        member _.ApplyStaticArguments(_, path, arguments) =
            if unbox<int> arguments[0] <> 0 then invalidArg "Mode" "Project-wide SoloDB generation uses static argument zero."
            try generate path
            with error -> raise (InvalidOperationException("SoloDB compiler generation failed: " + error.ToString(), error))
        member _.GetInvokerExpression(methodBase, arguments) =
            match methodBase with
            | :? MethodInfo as method -> Expr.Call(method, List.ofArray arguments)
            | _ -> invalidOp "The generated registration surface contains only static methods."
        [<CLIEvent>]
        member _.Invalidate = invalidated.Publish
        member _.GetGeneratedAssemblyContents assembly = (helper :> ITypeProvider).GetGeneratedAssemblyContents(assembly)
        member _.Dispose() = (helper :> IDisposable).Dispose()

[<assembly: TypeProviderAssembly>]
do ()
