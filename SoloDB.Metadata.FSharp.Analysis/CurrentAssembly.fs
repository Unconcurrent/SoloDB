namespace SoloDatabase.Metadata.FSharp

open System
open System.IO
open FSharp.Compiler.CodeAnalysis
open FSharp.Compiler.Symbols
open Mono.Cecil
open SoloDatabase.Metadata.Tools

/// Acquires the current source identity before a compiler image exists.
module internal CurrentAssembly =
    type Facts =
        { Identity: AssemblyNameReference
          FullSigningKeyFile: string
          Dependencies: string[] }

    type Inputs = FSharpCompilerInputs

    let inputs (arguments: string[]) projectDirectory = FSharpCompilerInputs(arguments,projectDirectory)

    let acquire (project: FSharpCheckProjectResults) (inputs: Inputs) projectDirectory assemblyName =
        let path (value: string) = Path.GetFullPath(if Path.IsPathRooted(value) then value else Path.Combine(projectDirectory,value))
        let dependencies = ResizeArray<string>(inputs.Dependencies)
        let arguments = inputs.NormalizedArguments
        let attribute name =
            project.AssemblySignature.Attributes
            |> Seq.tryFind(fun attribute -> attribute.AttributeType.FullName = "System.Reflection." + name + "Attribute")
            |> Option.map(fun attribute -> snd attribute.ConstructorArguments[0])
        let textAttribute name = attribute name |> Option.map unbox<string>
        let lastOption choose =
            let mutable selected = None
            let mutable index = arguments.Length - 1
            while selected.IsNone && index >= 0 do
                selected <- choose arguments[index]
                index <- index - 1
            selected
        let optionValue name (argument: string) =
            if argument.StartsWith(name + ":", StringComparison.Ordinal) then Some(argument.Substring(name.Length + 1).Trim('"')) else None
        let option name = inputs.Option(name) |> Option.ofObj
        let switch name = inputs.Switch(name)
        let version =
            textAttribute "AssemblyVersion"
            |> Option.orElseWith(fun () ->
                lastOption(fun argument ->
                    optionValue "version" argument
                    |> Option.orElseWith(fun () ->
                        optionValue "versionfile" argument |> Option.map(fun file ->
                            let file = path file
                            dependencies.Add(file)
                            use reader = File.OpenText(file)
                            reader.ReadLine()))))
            |> Option.map(fun value ->
                let version = Version(value.TrimStart('v'))
                let part value = if value < 0 then 0 else int(uint16 value)
                Version(part version.Major, part version.Minor, part version.Build, part version.Revision))
            |> Option.defaultValue(Version(0,0,0,0))
        let identity = AssemblyNameReference(assemblyName, version)
        textAttribute "AssemblyCulture" |> Option.iter(fun culture -> identity.Culture <- culture)
        attribute "AssemblyFlags" |> Option.iter(fun flags -> identity.IsRetargetable <- Convert.ToInt32(flags) &&& 0x100 <> 0)
        let container = option "keycontainer" |> Option.orElseWith(fun () -> textAttribute "AssemblyKeyName")
        if container.IsSome then
            raise (PlatformNotSupportedException("The current FSharp compiler does not support key-container signing on this platform."))
        let keyFile = option "keyfile" |> Option.orElseWith(fun () -> textAttribute "AssemblyKeyFile")
        let delaySign = switch "delaysign" || (attribute "AssemblyDelaySign" |> Option.exists unbox<bool>)
        let publicSign = switch "publicsign"
        let keyFile =
            keyFile |> Option.map path
        match keyFile with
        | None -> ()
        | Some path ->
            dependencies.Add(path)
            let bytes = File.ReadAllBytes(path)
            identity.PublicKey <-
                if publicSign || delaySign then bytes else
                HelperAssembly.SigningPublicKey(bytes)
        { Identity = identity
          FullSigningKeyFile = if publicSign || delaySign then "" else defaultArg keyFile ""
          Dependencies = dependencies.ToArray() }

    let reference (identity: AssemblyNameReference) (assembly: FSharpAssembly) =
        if not(String.IsNullOrEmpty assembly.QualifiedName) then AssemblyNameReference.Parse(assembly.QualifiedName)
        elif assembly.SimpleName = identity.Name then identity
        else invalidOp "A referenced FSharp assembly has no complete compiler identity."
