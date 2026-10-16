namespace SoloDatabase.Metadata.FSharp

open System
open System.Collections.Generic
open FSharp.Compiler.Symbols
open Mono.Cecil
open SoloDatabase.Metadata.CodecGeneration
open SoloDatabase.Metadata.Generation
open SoloDatabase.Metadata.Tools

/// Original public CLR declarations, before serialization, capture or mutation roles.
module internal CompilerPropertyFacts =
    let many (typ: FSharpType) =
        let typ = typ.StripAbbreviations()
        seq { yield typ; yield! typ.AllInterfaces }
        |> Seq.exists(fun candidate ->
            let candidate = candidate.StripAbbreviations()
            candidate.HasTypeDefinition && (candidate.TypeDefinition.TryFullName |> Option.exists(fun name ->
                TypeSupport.ManyContract(candidate.TypeDefinition.Assembly.SimpleName,name))))

    let rec polymorphic (typ: FSharpType) =
        typ.TypeDefinition.Attributes |> Seq.exists(fun attribute -> attribute.AttributeType.FullName = "SoloDatabase.Attributes.PolymorphicAttribute")
        || (typ.BaseType |> Option.exists polymorphic)

    let private virtualSlot referenceWith reference (owner: FSharpType) (method: FSharpMemberOrFunctionOrValue) =
        let declaringType: TypeReference = reference owner
        let definition = match declaringType with :? GenericInstanceType as closed -> closed.ElementType | _ -> declaringType
        let substitutions = Seq.zip owner.TypeDefinition.GenericParameters owner.GenericArguments |> Seq.toList
        let signature typ = referenceWith (fun parameter ->
            let index = owner.TypeDefinition.GenericParameters |> Seq.findIndex(fun candidate -> candidate.Equals parameter)
            definition.GenericParameters[index] :> TypeReference) typ
        let slot = method.ImplementedAbstractSignatures |> Seq.tryFind(fun slot -> not slot.DeclaringType.TypeDefinition.IsInterface)
        match slot with
        | Some slot ->
            let declaring = slot.DeclaringType.Instantiate(substitutions).StripAbbreviations()
            let actual = reference declaring
            let definition = match actual with :? GenericInstanceType as closed -> closed.ElementType | _ -> actual
            let parameterType = referenceWith (fun parameter ->
                let index = slot.DeclaringTypeGenericParameters |> Seq.findIndex(fun candidate -> candidate.Equals parameter)
                definition.GenericParameters[index] :> TypeReference)
            let result = if method.IsPropertySetterMethod then actual.Module.TypeSystem.Void else parameterType slot.AbstractReturnType
            GeneratedRowFields.SlotIdentity(actual,slot.Name,result,slot.AbstractArguments |> Seq.collect id |> Seq.map(fun parameter -> parameterType parameter.Type) |> Seq.toArray)
        | None when method.IsDispatchSlot ->
            let result = if method.IsPropertySetterMethod then declaringType.Module.TypeSystem.Void else signature method.ReturnParameter.Type
            GeneratedRowFields.SlotIdentity(declaringType,method.CompiledName,result,method.CurriedParameterGroups |> Seq.collect id |> Seq.map(fun parameter -> signature parameter.Type) |> Seq.toArray)
        | None -> null

    let private attributes slotIdentity (owner: FSharpType) (property: FSharpMemberOrFunctionOrValue) =
        let rec inherited (owner: FSharpType) (property: FSharpMemberOrFunctionOrValue) =
            match property.Attributes |> Seq.tryFind(fun attribute -> attribute.AttributeType.FullName = "SoloDatabase.Attributes.SoloId") with
            | Some attribute -> Some attribute
            | None ->
                let accessor = if property.HasGetterMethod then property.GetterMethod else property.SetterMethod
                if not accessor.IsOverrideOrExplicitInterfaceImplementation then None else
                let substitutions (owner: FSharpType) = Seq.zip owner.TypeDefinition.GenericParameters owner.GenericArguments |> Seq.toList
                let originalSlot = slotIdentity owner accessor
                let expected = accessor.CurriedParameterGroups |> Seq.collect id |> Seq.map(fun parameter -> parameter.Type.Instantiate(substitutions owner)) |> Seq.toArray
                let rec nearest (parent: FSharpType option) =
                    match parent with
                    | None -> None
                    | Some parent ->
                        let parent = parent.StripAbbreviations()
                        let candidate = parent.TypeDefinition.MembersFunctionsAndValues |> Seq.tryFind(fun candidate ->
                            if not candidate.IsProperty || (property.HasGetterMethod && not candidate.HasGetterMethod) ||
                               (not property.HasGetterMethod && not candidate.HasSetterMethod) then false else
                            let method = if property.HasGetterMethod then candidate.GetterMethod else candidate.SetterMethod
                            let parameters = method.CurriedParameterGroups |> Seq.collect id |> Seq.map(fun parameter -> parameter.Type.Instantiate(substitutions parent)) |> Seq.toArray
                            if method.CompiledName <> accessor.CompiledName || parameters.Length <> expected.Length ||
                               not(Array.forall2 (fun (left: FSharpType) (right: FSharpType) -> left.Equals right) parameters expected) then false else
                            slotIdentity parent method = originalSlot)
                        match candidate with
                        | Some candidate ->
                            let valueType (owner: FSharpType) (property: FSharpMemberOrFunctionOrValue) =
                                let value = if property.HasGetterMethod then property.GetterMethod.ReturnParameter.Type else (property.SetterMethod.CurriedParameterGroups |> Seq.collect id |> Seq.last).Type
                                value.Instantiate(substitutions owner)
                            if (valueType owner property).Equals(valueType parent candidate) then inherited parent candidate else None
                        | None -> nearest parent.BaseType
                nearest owner.BaseType
        let own = property.Attributes |> Seq.toArray
        if own |> Array.exists(fun attribute -> attribute.AttributeType.FullName = "SoloDatabase.Attributes.SoloId") then own :> seq<_>
        else seq { yield! own; match inherited owner property with Some attribute -> yield attribute | None -> () }

    let read (moduleDefinition: ModuleDefinition) referenceWith reference (resolver: IAssemblyResolver)
             (access: HashSet<string>) (declaration: CodecMember<TypeReference,MethodReference,FieldReference> -> seq<FSharpAttribute> -> seq<FSharpAttribute> -> GeneratedProperty)
             (importedMany: TypeReference -> Nullable<bool>) identities (typ: FSharpType) =
        let result = ResizeArray<GeneratedProperty>()
        let rec owners (owner: FSharpType) inherited =
            let owner = owner.StripAbbreviations()
            let entity = owner.TypeDefinition
            let declaringType: TypeReference = reference owner
            if not(String.IsNullOrEmpty(entity.Assembly.QualifiedName)) then
                let original = SourceTypeIdentity.Original(declaringType,resolver)
                result.AddRange(GeneratedRowFields.Properties(moduleDefinition,original,Action<string>(fun assembly -> access.Add assembly |> ignore),
                    inherited,Func<TypeReference,Nullable<bool>>(importedMany),identities))
            else
                let definition = match declaringType with :? GenericInstanceType as closed -> closed.ElementType | _ -> declaringType
                let substitutions = Seq.zip entity.GenericParameters owner.GenericArguments |> Seq.toList
                let signature typ = referenceWith (fun parameter ->
                    let index = entity.GenericParameters |> Seq.findIndex(fun candidate -> candidate.Equals parameter)
                    definition.GenericParameters[index] :> TypeReference) typ
                let operation (method: FSharpMemberOrFunctionOrValue) =
                    let instance = method.IsInstanceMemberInCompiledCode
                    let result = if method.IsPropertySetterMethod then moduleDefinition.TypeSystem.Void else signature method.ReturnParameter.Type
                    let call = MethodReference(method.CompiledName,result,declaringType,HasThis = instance)
                    if method.IsInstanceMember && not instance then call.Parameters.Add(ParameterDefinition(signature(entity.AsType())))
                    for parameter in method.CurriedParameterGroups |> Seq.collect id do
                        call.Parameters.Add(ParameterDefinition(signature parameter.Type))
                    call
                if entity.IsFSharpRecord then
                    let mutableRecord = entity.Attributes |> Seq.exists(fun attribute -> attribute.AttributeType.FullName = "Microsoft.FSharp.Core.CLIMutableAttribute")
                    for field in entity.FSharpFields do
                        if field.Accessibility.IsPublic then
                            let typ = reference(field.FieldType.Instantiate substitutions)
                            let getter = MethodReference("get_" + field.Name,signature field.FieldType,declaringType,HasThis = true)
                            let setter =
                                if not(field.IsMutable || mutableRecord) then None else
                                let method = MethodReference("set_" + field.Name,moduleDefinition.TypeSystem.Void,declaringType,HasThis = true)
                                method.Parameters.Add(ParameterDefinition(signature field.FieldType))
                                Some method
                            result.Add(declaration
                                { Name = field.Name; Type = typ; Field = None; Getter = Some getter; Setter = setter
                                  Static = false; Readable = true; Writable = setter.IsSome; Ignored = false
                                  Many = identities && many (field.FieldType.Instantiate substitutions) } field.PropertyAttributes field.PropertyAttributes)
                for property in entity.MembersFunctionsAndValues |> Seq.filter(fun property ->
                    property.IsProperty && property.Accessibility.IsPublic && (not inherited || property.IsInstanceMemberInCompiledCode) &&
                    not(entity.IsFSharpRecord && (entity.FSharpFields |> Seq.exists(fun field -> field.Name = property.DisplayName))))
                    |> Seq.sortBy(fun property -> property.DeclarationLocation.StartLine,property.DeclarationLocation.StartColumn) do
                    let getter = if property.HasGetterMethod && not(inherited && property.GetterMethod.Accessibility.IsPrivate) then Some(operation property.GetterMethod) else None
                    let setter = if property.HasSetterMethod && not(inherited && property.SetterMethod.Accessibility.IsPrivate) then Some(operation property.SetterMethod) else None
                    let valueType = if property.HasGetterMethod then property.GetterMethod.ReturnParameter.Type else (property.SetterMethod.CurriedParameterGroups |> Seq.collect id |> Seq.last).Type
                    if (property.HasGetterMethod && not property.GetterMethod.Accessibility.IsPublic) ||
                       (property.HasSetterMethod && not property.SetterMethod.Accessibility.IsPublic) || not entity.Accessibility.IsPublic then
                        access.Add(entity.Assembly.SimpleName) |> ignore
                    let memberFacts = {
                        Name = property.DisplayName; Type = reference(valueType.Instantiate substitutions); Field = None
                        Getter = getter; Setter = setter; Static = not property.IsInstanceMemberInCompiledCode
                        Readable = getter.IsSome; Writable = setter.IsSome; Ignored = false
                        Many = identities && property.IsInstanceMemberInCompiledCode && getter.IsSome && setter.IsSome && many (valueType.Instantiate substitutions) }
                    let selected = declaration memberFacts (if identities then property.Attributes :> seq<_> else Seq.empty) (if identities then attributes (virtualSlot referenceWith reference) owner property else Seq.empty)
                    result.Add(GeneratedProperty(selected.Member,selected.Identity,
                        property.HasGetterMethod && property.GetterMethod.Accessibility.IsPrivate,
                        property.HasSetterMethod && property.SetterMethod.Accessibility.IsPrivate,
                        virtualSlot referenceWith reference owner (if property.HasGetterMethod then property.GetterMethod else property.SetterMethod),selected.Attributes,selected.DeferredSchema))
                owner.BaseType |> Option.iter(fun parent -> owners parent true)
        owners typ false
        GeneratedRowFields.Merge(result.ToArray())
