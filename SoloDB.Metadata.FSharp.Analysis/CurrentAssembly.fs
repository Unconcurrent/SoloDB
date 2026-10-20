namespace SoloDatabase.Metadata.FSharp

open System
open System.IO
open System.Security.Cryptography
open FSharp.Compiler.CodeAnalysis
open FSharp.Compiler.Symbols
open Mono.Cecil

/// Acquires the current source identity before a compiler image exists.
module internal CurrentAssembly =
    type Facts =
        { Identity: AssemblyNameReference
          FullSigningKeyFile: string }

    let acquire (project: FSharpCheckProjectResults) (arguments: string[]) projectDirectory assemblyName =
        let path (value: string) = Path.GetFullPath(if Path.IsPathRooted(value) then value else Path.Combine(projectDirectory,value))
        // FSharp response files contain one option per nonempty, noncomment line,
        // expanded in place. Read them at this cold compiler-input boundary only.
        let rec expanded (arguments: seq<string>) = seq {
            for argument in arguments do
                if argument.StartsWith("@", StringComparison.Ordinal) then
                    yield! File.ReadLines(path (argument.TrimStart('@')))
                           |> Seq.choose(fun line -> if String.IsNullOrWhiteSpace(line) || line.StartsWith("#", StringComparison.Ordinal) then None else Some(line.Trim()))
                           |> expanded
                else yield argument
        }
        let arguments =
            expanded arguments
            |> Seq.map(fun argument ->
                if argument.StartsWith("--", StringComparison.Ordinal) then argument.Substring(2)
                elif argument.StartsWith("/", StringComparison.Ordinal) && not(argument.Substring(1).Contains('/')) then argument.Substring(1)
                else argument)
            |> Seq.toArray
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
        let option name = lastOption (optionValue name)
        let switch name =
            lastOption(fun argument ->
                match argument with
                | value when value = name || value = name + "+" -> Some true
                | value when value = name + "-" -> Some false
                | _ -> None)
            |> Option.defaultValue false
        let version =
            textAttribute "AssemblyVersion"
            |> Option.orElseWith(fun () ->
                lastOption(fun argument ->
                    optionValue "version" argument
                    |> Option.orElseWith(fun () ->
                        optionValue "versionfile" argument |> Option.map(fun file ->
                            use reader = File.OpenText(path file)
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
            let bytes = File.ReadAllBytes(path)
            identity.PublicKey <-
                if publicSign || delaySign then bytes else
                use rsa = new RSACryptoServiceProvider()
                rsa.ImportCspBlob(bytes)
                if rsa.PublicOnly then raise (CryptographicException("Full signing requires a private key pair."))
                let publicKey = rsa.ExportCspBlob(false)
                // CLR public keys frame the CAPI public blob with RSA-sign and SHA1 identifiers.
                use buffer = new MemoryStream(12 + publicKey.Length)
                use writer = new BinaryWriter(buffer)
                writer.Write(0x2400)
                writer.Write(0x8004)
                writer.Write(publicKey.Length)
                writer.Write(publicKey,0,4)
                writer.Write(0x2400)
                writer.Write(publicKey,8,publicKey.Length - 8)
                buffer.ToArray()
        { Identity = identity
          FullSigningKeyFile = if publicSign || delaySign then "" else defaultArg keyFile "" }

    let reference (identity: AssemblyNameReference) (assembly: FSharpAssembly) =
        if not(String.IsNullOrEmpty assembly.QualifiedName) then AssemblyNameReference.Parse(assembly.QualifiedName)
        elif assembly.SimpleName = identity.Name then identity
        else invalidOp "A referenced FSharp assembly has no complete compiler identity."
