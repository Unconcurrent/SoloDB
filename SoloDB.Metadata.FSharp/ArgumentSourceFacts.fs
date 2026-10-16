namespace SoloDatabase.Metadata.FSharp

open System
open System.Collections.Generic
open System.IO
open FSharp.Compiler.Symbols
open Mono.Cecil
open SoloDatabase.Metadata.CodecGeneration
open SoloDatabase.Metadata.Generation
open SoloDatabase.Metadata.Tools

/// Raw CLR property facts have no JSON naming, construction or child-model duties.
module internal ArgumentSourceFacts =
    let memberFact name typ getter : CodecMember<TypeReference,MethodReference,FieldReference> =
        { Name=name; Type=typ; Field=None; Getter=Some getter; Setter=None; Static=false
          Readable=true; Writable=false; Ignored=false; Many=false }

    let anonymous (mapper: CompilerReferences.References) reference details valueType (arguments: FSharpType[]) =
        let values = arguments |> Array.map reference
        let actual = mapper.Anonymous details valueType values
        let root = match actual with :? GenericInstanceType as closed -> closed.ElementType | _ -> actual
        let members = details.SortedFieldNames |> Array.mapi(fun index name ->
            memberFact name values[index] (MethodReference("get_" + name,root.GenericParameters[index],actual,HasThis=true)))
        let result = GeneratedArgumentSource(actual,members)
        result.AccessAssemblies.Add(details.Assembly.SimpleName) |> ignore
        result

    let create (moduleDefinition: ModuleDefinition) (mapper: CompilerReferences.References) (assemblies: seq<FSharpAssembly>) =
        let resolver = new DefaultAssemblyResolver()
        for assembly in assemblies do
            assembly.FileName |> Option.iter(fun path -> resolver.AddSearchDirectory(Path.GetDirectoryName path))
        let inputs = Dictionary<string,AssemblyDefinition>(StringComparer.Ordinal)
        let imported path =
            match inputs.TryGetValue path with
            | true, value -> value
            | _ ->
                let value = AssemblyDefinition.ReadAssembly(path,ReaderParameters(AssemblyResolver=resolver))
                inputs.Add(path,value)
                value
        let rec closed (typ: FSharpType) = not typ.IsGenericParameter && (typ.GenericArguments |> Seq.forall closed)
        let rec read reference (typ: FSharpType) =
            let typ = typ.StripAbbreviations()
            if not(closed typ) || typ.IsFunctionType || not(typ.HasTypeDefinition || typ.IsAnonRecordType || typ.IsTupleType) then None
            elif typ.HasTypeDefinition && typ.TypeDefinition.TryFullName = Some "System.Object" then None
            elif typ.HasTypeDefinition && typ.TypeDefinition.TryFullName = Some "System.Nullable`1" then read reference typ.GenericArguments[0]
            elif typ.IsAnonRecordType then Some(anonymous mapper reference typ.AnonRecordTypeDetails (CompilerReferences.isValueType typ) (Seq.toArray typ.GenericArguments))
            elif typ.HasTypeDefinition && typ.TypeDefinition.IsArrayType then
                let original = SourceTypeIdentity.Original(reference typ.BaseType.Value,resolver)
                let properties = OriginalArgumentSources.Read(moduleDefinition,original)
                let result = GeneratedArgumentSource(reference typ,properties.Members)
                for name in properties.AccessAssemblies do result.AccessAssemblies.Add name |> ignore
                Some result
            elif typ.IsTupleType then
                let original = SourceTypeIdentity.Original(reference typ,resolver)
                Option.ofObj(OriginalArgumentSources.Read(moduleDefinition,original))
            elif typ.HasTypeDefinition && (typ.TypeDefinition.IsInterface || typ.TypeDefinition.IsAbstractClass) then None
            elif typ.HasTypeDefinition && typ.TypeDefinition.Assembly.FileName.IsSome && not(String.IsNullOrEmpty typ.TypeDefinition.Assembly.QualifiedName) then
                let actual: TypeReference = reference typ
                let root = match actual with :? GenericInstanceType as closed -> closed.ElementType | _ -> actual
                let assembly = imported typ.TypeDefinition.Assembly.FileName.Value
                if assembly.Name.FullName <> typ.TypeDefinition.Assembly.QualifiedName then invalidOp "The raw argument input does not match its compiler identity."
                let definition = SourceTypeIdentity.Definition(assembly,root.FullName)
                let original =
                    match actual with
                    | :? GenericInstanceType as instance ->
                        let value = GenericInstanceType(definition)
                        for argument in instance.GenericArguments do value.GenericArguments.Add argument
                        value :> TypeReference
                    | _ -> definition :> TypeReference
                Option.ofObj(OriginalArgumentSources.Read(moduleDefinition,original))
            elif typ.HasTypeDefinition then
                let actual = reference typ
                let access = HashSet<string>(StringComparer.Ordinal)
                let direct = seq { yield typ; yield! typ.AllInterfaces } |> Seq.exists(fun item ->
                    item.HasTypeDefinition && TypeSupport.DirectSqlArguments(defaultArg item.TypeDefinition.TryFullName "",
                        item.GenericArguments |> Seq.map(fun typ -> (reference typ).FullName) |> Seq.toArray))
                if direct then None else
                let members = CompilerPropertyFacts.read moduleDefinition mapper.Reference reference resolver access
                                  (fun memberValue _ _ -> GeneratedProperty(memberValue)) (fun _ -> Nullable()) false typ
                              |> Array.map _.Member
                              |> Array.filter(fun memberValue -> TypeSupport.SqlArgumentProperty(true,memberValue.Getter.IsSome,
                                  memberValue.Static,GeneratedRowFields.IndexParameters(memberValue).Length <> 0))
                let result = GeneratedArgumentSource(actual,members)
                for name in access do result.AccessAssemblies.Add name |> ignore
                Some result
            else None
        let dispose () =
            for assembly in inputs.Values do assembly.Dispose()
            resolver.Dispose()
        read, dispose
