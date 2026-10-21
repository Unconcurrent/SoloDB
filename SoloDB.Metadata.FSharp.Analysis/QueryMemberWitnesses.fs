namespace SoloDatabase.Metadata.FSharp

open System
open System.Collections.Generic
open Mono.Cecil
open SoloDatabase.Metadata.Tools
open FSharp.Compiler.Symbols

/// Source member witnesses share exact signatures and once-only metadata acquisition.
module internal QueryMemberWitnesses =
    type OriginalMembers(moduleDefinition: ModuleDefinition, currentIdentity: AssemblyNameReference,
                         referenceWith: (FSharpGenericParameter -> TypeReference) -> FSharpType -> TypeReference,
                         witnesses: IDictionary<string, GeneratedQueryWitness>) =
        let seen = HashSet<string>(StringComparer.Ordinal)
        let images = Dictionary<string,AssemblyDefinition>(StringComparer.Ordinal)
        let returns = Dictionary<FSharpMemberOrFunctionOrValue,bool>()
        let image (entity: FSharpEntity) =
            match entity.Assembly.FileName with
            | None when String.IsNullOrEmpty(entity.Assembly.QualifiedName) && entity.Assembly.SimpleName = currentIdentity.Name -> None
            | None -> invalidOp ("The original callable image is unavailable: " + entity.FullName)
            | Some path ->
                let input =
                    match images.TryGetValue path with
                    | true,input -> input
                    | _ ->
                        let input = AssemblyDefinition.ReadAssembly(path)
                        images.Add(path,input)
                        input
                if input.Name.FullName <> entity.Assembly.QualifiedName then
                    invalidOp ("The callable input differs from its checked identity: " + entity.FullName)
                Some input

        let originalDeclaration (selected: FSharpMemberOrFunctionOrValue) =
            let candidates = selected.DeclaringEntity.Value.MembersFunctionsAndValues |> Seq.filter(fun method ->
                (method.IsMethod || method.IsFunction || method.IsConstructor) &&
                method.IsConstructor = selected.IsConstructor && method.XmlDocSig = selected.XmlDocSig) |> Seq.toArray
            let candidates = if candidates.Length <= 1 then candidates else
                             candidates |> Array.filter(fun method -> method.ReturnParameter.Type.Equals selected.ReturnParameter.Type)
            match candidates with
            | [|method|] -> method
            | _ -> invalidOp ("The original checked callable signature is not unique: " + selected.FullName + " requested " + selected.XmlDocSig)

        let originalSignature (original: FSharpMemberOrFunctionOrValue) (declaring: TypeReference) (target: MethodReference) =
            let parameters = if original.IsConstructor then [||] else original.GenericParameters |> Seq.toArray
            let definition = declaring.GetElementType()
            referenceWith(fun parameter ->
                match parameters |> Seq.tryFindIndex(fun candidate -> candidate.Equals parameter) with
                | Some index -> target.GenericParameters[index] :> TypeReference
                | None ->
                    let index = original.DeclaringEntity.Value.GenericParameters |> Seq.findIndex(fun candidate -> candidate.Equals parameter)
                    definition.GenericParameters[index] :> TypeReference)

        member private this.SlotReturnsVoid(reference: FSharpType -> TypeReference, slot: FSharpAbstractSignature) =
            let declaring = reference slot.DeclaringType
            let definition = declaring.GetElementType()
            let signature = MethodDefinition(slot.Name,MethodAttributes.Public,moduleDefinition.TypeSystem.Void)
            for parameter in slot.MethodGenericParameters do signature.GenericParameters.Add(GenericParameter(parameter.Name,signature))
            let parameterType = referenceWith(fun parameter ->
                match slot.MethodGenericParameters |> Seq.tryFindIndex(fun candidate -> candidate.Equals parameter) with
                | Some index -> signature.GenericParameters[index] :> TypeReference
                | None ->
                    let index = slot.DeclaringTypeGenericParameters |> Seq.findIndex(fun candidate -> candidate.Equals parameter)
                    definition.GenericParameters[index] :> TypeReference)
            for parameter in slot.AbstractArguments |> Seq.collect id do
                signature.Parameters.Add(ParameterDefinition(parameterType parameter.Type))
            let expected = QueryConstructionWitnesses.MethodSignature signature
            let entity = slot.DeclaringType.TypeDefinition
            match image entity with
            | Some input ->
                let original = SourceTypeIdentity.Definition(input,definition.FullName).Methods
                               |> Seq.filter(fun method -> method.HasThis && QueryConstructionWitnesses.MethodSignature method = expected)
                               |> Seq.exactlyOne
                original.ReturnType.MetadataType = MetadataType.Void
            | None ->
                let prefix = entity.XmlDocSig.Substring(2) + "."
                let original = entity.MembersFunctionsAndValues |> Seq.filter(fun method ->
                    let documentation = method.XmlDocSig
                    documentation.Length > 2 && documentation.Substring(2).StartsWith(prefix,StringComparison.Ordinal) &&
                    (let actual = documentation.Substring(2 + prefix.Length)
                     actual = expected || actual + "()" = expected)) |> Seq.exactlyOne
                this.SourceReturnsVoid(reference,original)

        member private this.SourceReturnsVoid(reference: FSharpType -> TypeReference, memberValue: FSharpMemberOrFunctionOrValue) =
            match returns.TryGetValue memberValue with
            | true,value -> value
            | _ ->
                let result = memberValue.ReturnParameter.Type.StripAbbreviations()
                let value =
                    if not result.HasTypeDefinition || result.TypeDefinition.TryFullName <> Some "Microsoft.FSharp.Core.Unit" then false else
                    if memberValue.IsPropertyGetterMethod then false
                    elif memberValue.IsOverrideOrExplicitInterfaceImplementation then
                        let slots = memberValue.ImplementedAbstractSignatures |> Seq.toArray
                        if slots.Length = 0 then invalidOp ("The source callable has no original override slot: " + memberValue.FullName)
                        let values = slots |> Array.map(fun slot -> this.SlotReturnsVoid(reference,slot))
                        if values |> Array.exists((<>) values[0]) then invalidOp ("The source callable has incompatible physical return slots: " + memberValue.FullName)
                        values[0]
                    else true
                returns.Add(memberValue,value)
                value

        member this.Template(reference: FSharpType -> TypeReference, selected: FSharpMemberOrFunctionOrValue, declaring: TypeReference) =
            let entity = selected.DeclaringEntity.Value
            let prefix = entity.XmlDocSig.Substring(2) + "."
            let documentation = selected.XmlDocSig.Substring(2)
            if not(documentation.StartsWith(prefix,StringComparison.Ordinal)) then
                invalidOp ("The original callable has no declaration signature: " + selected.FullName)
            let expected = documentation.Substring(prefix.Length).Replace('#','.')
            let returnPosition = expected.IndexOf('~')
            let declarationSignature method =
                if returnPosition < 0 then QueryConstructionWitnesses.MethodSignature method
                else QueryConstructionWitnesses.ReturnQualifiedSignature method
            let same signature = signature = expected || signature = expected + "()"
            let original = originalDeclaration selected
            let target = MethodReference((if original.IsConstructor then ".ctor" else original.CompiledName),
                moduleDefinition.TypeSystem.Void,declaring,HasThis = (original.IsConstructor || original.IsInstanceMemberInCompiledCode))
            let methodParameters = if original.IsConstructor then [||] else original.GenericParameters |> Seq.toArray
            for parameter in methodParameters do target.GenericParameters.Add(GenericParameter(parameter.Name,target))
            let definition = declaring.GetElementType()
            let signature = originalSignature original declaring target
            let parameters = original.CurriedParameterGroups |> Seq.collect id |> Seq.toArray
            for parameter in parameters do target.Parameters.Add(ParameterDefinition(signature parameter.Type))
            let physical = MethodDefinition(target.Name,MethodAttributes.Public,target.ReturnType)
            if not original.IsConstructor then physical.ReturnType <- signature original.ReturnParameter.Type
            for parameter in target.GenericParameters do physical.GenericParameters.Add(GenericParameter(parameter.Name,physical))
            for parameter in target.Parameters do physical.Parameters.Add(ParameterDefinition(parameter.ParameterType))
            match image entity with
            | Some input ->
                let methods = SourceTypeIdentity.Definition(input,definition.FullName).Methods
                // Documentation can include inline witnesses absent from the normal CLR entrypoint.
                let expectedParameters = QueryConstructionWitnesses.MethodSignature physical +
                                         (if returnPosition < 0 then "" else expected.Substring(returnPosition))
                let matching = methods |> Seq.filter(fun method -> declarationSignature method = expectedParameters) |> Seq.toArray
                let matching = if matching.Length <= 1 then matching else
                               let expectedReturn = QueryConstructionWitnesses.ReturnQualifiedSignature physical
                               matching |> Array.filter(fun method -> QueryConstructionWitnesses.ReturnQualifiedSignature method = expectedReturn)
                let matching = if matching.Length <> 0 then matching else methods |> Seq.filter(fun method -> same (declarationSignature method)) |> Seq.toArray
                let selected =
                    match matching with
                    | [|method|] -> method
                    | _ -> invalidOp ("The original callable signature is not unique: " + selected.FullName + " requested " + expectedParameters +
                                      "; declarations " + String.Join(";",methods |> Seq.filter(fun method -> method.Name = selected.CompiledName || method.IsConstructor && selected.IsConstructor) |> Seq.map QueryConstructionWitnesses.MethodSignature))
                let target = moduleDefinition.ImportReference selected
                target.DeclaringType <- declaring
                target
            | None ->
                if not original.IsConstructor && not(this.SourceReturnsVoid(reference,original)) then
                    target.ReturnType <- signature original.ReturnParameter.Type
                physical.ReturnType <- target.ReturnType
                let matchesSource () =
                    same (declarationSignature physical) ||
                    (match original.GetWitnessPassingInfo() with
                     | None -> false
                     | Some(_,parameters) ->
                        let witnessed = MethodDefinition(physical.Name,physical.Attributes,physical.ReturnType)
                        for parameter in physical.GenericParameters do witnessed.GenericParameters.Add(GenericParameter(parameter.Name,witnessed))
                        for parameter in parameters do witnessed.Parameters.Add(ParameterDefinition(signature parameter.Type))
                        for parameter in physical.Parameters do witnessed.Parameters.Add(ParameterDefinition(parameter.ParameterType))
                        same (declarationSignature witnessed))
                if not(matchesSource()) then
                    target.Parameters.Clear()
                    physical.Parameters.Clear()
                    for group in original.CurriedParameterGroups do
                        let standaloneUnit = group.Count = 1 &&
                                             (let typ = group[0].Type.StripAbbreviations()
                                              typ.HasTypeDefinition && typ.TypeDefinition.TryFullName = Some "Microsoft.FSharp.Core.Unit")
                        if not standaloneUnit then
                          for parameter in group do
                            let typ = signature parameter.Type
                            target.Parameters.Add(ParameterDefinition typ)
                            physical.Parameters.Add(ParameterDefinition typ)
                    if not(matchesSource()) then
                        invalidOp ("The checked callable parameter representation differs from its declaration: " + original.FullName)
                target

        member _.Call(target: MethodReference, generics: TypeReference[]) =
            let method =
                if generics.Length = 0 then target else
                let closed = GenericInstanceMethod(target)
                for argument in generics do closed.GenericArguments.Add(argument)
                closed :> MethodReference
            let close typ = moduleDefinition.ImportReference(OriginalArgumentSources.Close(typ,target.DeclaringType,generics))
            let result = if target.Name = ".ctor" then target.DeclaringType else close target.ReturnType
            let arguments = target.Parameters |> Seq.map(fun parameter -> close parameter.ParameterType) |> Seq.toArray
            GeneratedQueryCall(method,result,arguments)

        member this.Add(reference: FSharpType -> TypeReference, memberValue: FSharpMemberOrFunctionOrValue) =
            if memberValue.DeclaringEntity.IsSome &&
               (memberValue.IsProperty || memberValue.IsMethod || memberValue.IsFunction || memberValue.IsConstructor) then
                let memberValue, _ = CompilerReferences.compiledMember memberValue
                let entity = memberValue.DeclaringEntity.Value
                let prefix = entity.XmlDocSig.Substring(2) + "."
                let documentation = memberValue.XmlDocSig
                if documentation.Length > 2 && documentation.Substring(2).StartsWith(prefix, StringComparison.Ordinal) then
                    let declaring = reference memberValue.ApparentEnclosingType
                    let signature = documentation.Substring(2 + prefix.Length)
                    let key = declaring.FullName + "," + declaring.Scope.ToString() + "|" + signature
                    if seen.Add key then
                        if memberValue.IsFunction then
                            let original = originalDeclaration memberValue
                            let target = this.Template(reference,memberValue,declaring)
                            let physical name =
                                let method = MethodDefinition(name,MethodAttributes.Public,target.ReturnType)
                                for parameter in target.GenericParameters do method.GenericParameters.Add(GenericParameter(parameter.Name,method))
                                for parameter in target.Parameters do method.Parameters.Add(ParameterDefinition(parameter.ParameterType))
                                method
                            let preserve (method: MethodDefinition) =
                                method.Name <- method.Name.Replace('.','#')
                                let signature = QueryConstructionWitnesses.MethodSignature method
                                let key = declaring.FullName + "," + declaring.Scope.ToString() + "|" + signature
                                witnesses[key] <- GeneratedQueryWitness(signature,declaring)
                            preserve (physical target.Name)
                            match original.GetWitnessPassingInfo() with
                            | None -> ()
                            | Some(name,parameters) ->
                                let witnessed = physical name
                                witnessed.Parameters.Clear()
                                let signature = originalSignature original declaring witnessed
                                for parameter in parameters do witnessed.Parameters.Add(ParameterDefinition(signature parameter.Type))
                                for parameter in target.Parameters do witnessed.Parameters.Add(ParameterDefinition(parameter.ParameterType))
                                preserve witnessed
                        else witnesses[key] <- GeneratedQueryWitness(signature, declaring)

        interface IDisposable with
            member _.Dispose() = for input in images.Values do input.Dispose()
