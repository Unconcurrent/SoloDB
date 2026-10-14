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
                let members = ResizeArray<CodecMember<TypeReference,MethodReference,FieldReference>>()
                let access = HashSet<string>(StringComparer.Ordinal)
                let names = HashSet<string>(StringComparer.Ordinal)
                let rec owners (current: FSharpType) = seq {
                    yield current
                    match current.BaseType with
                    | Some parent when parent.HasTypeDefinition && parent.TypeDefinition.TryFullName <> Some "System.Object" -> yield! owners parent
                    | _ -> () }
                let direct = typ.AllInterfaces |> Seq.exists(fun item ->
                    item.HasTypeDefinition && (item.TypeDefinition.TryFullName = Some "System.Collections.IDictionary" ||
                        item.TypeDefinition.TryFullName = Some "System.Collections.Generic.IDictionary`2" &&
                        item.GenericArguments[0].TypeDefinition.TryFullName = Some "System.String" && item.GenericArguments[1].TypeDefinition.TryFullName = Some "System.Object"))
                if direct then None else
                for owning in owners typ do
                    let entity = owning.TypeDefinition
                    let declaring = reference owning
                    let definition = match declaring with :? GenericInstanceType as closed -> closed.ElementType | _ -> declaring
                    let substitutions = Seq.zip entity.GenericParameters owning.GenericArguments |> Seq.toList
                    let signature = mapper.Reference(fun parameter ->
                        let index = entity.GenericParameters |> Seq.findIndex(fun candidate -> candidate.Equals parameter)
                        definition.GenericParameters[index] :> TypeReference)
                    if not entity.Accessibility.IsPublic then access.Add(entity.Assembly.SimpleName) |> ignore
                    let add name (value: FSharpType) getter =
                        let typ = reference (value.Instantiate substitutions)
                        if names.Add(name + "|" + GeneratedJsonDefinitions.Identity typ) then members.Add(memberFact name typ getter)
                    if entity.IsFSharpRecord then
                        for field in entity.FSharpFields do
                            if field.Accessibility.IsPublic then
                                add field.Name field.FieldType (MethodReference("get_" + field.Name,signature field.FieldType,declaring,HasThis=true))
                    for property in entity.MembersFunctionsAndValues |> Seq.filter _.IsProperty |> Seq.sortBy(fun property -> property.DeclarationLocation.StartLine,property.DeclarationLocation.StartColumn) do
                        let getter = if property.HasGetterMethod then Some property.GetterMethod else None
                        let indexed = getter |> Option.exists(fun method -> method.CurriedParameterGroups |> Seq.collect id |> Seq.exists(fun parameter ->
                            let typ = parameter.Type.StripAbbreviations()
                            not typ.HasTypeDefinition || typ.TypeDefinition.TryFullName <> Some "Microsoft.FSharp.Core.Unit"))
                        if TypeSupport.SqlArgumentProperty(property.Accessibility.IsPublic,getter.IsSome,not property.IsInstanceMember,indexed) then
                            let method = getter.Value
                            if not method.Accessibility.IsPublic then access.Add(entity.Assembly.SimpleName) |> ignore
                            add property.DisplayName method.ReturnParameter.Type (MethodReference(method.CompiledName,signature method.ReturnParameter.Type,declaring,HasThis=true))
                let result = GeneratedArgumentSource(actual,members.ToArray())
                for name in access do result.AccessAssemblies.Add name |> ignore
                Some result
            else None
        let dispose () =
            for assembly in inputs.Values do assembly.Dispose()
            resolver.Dispose()
        read, dispose
