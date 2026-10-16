namespace SoloDatabase.Metadata.FSharp

open System
open System.Collections.Generic
open FSharp.Compiler.Symbols
open Mono.Cecil
open SoloDatabase.Metadata.Generation
open SoloDatabase.Metadata.CodecGeneration
open SoloDatabase.Metadata.Tools

/// Case reading and ordered CLR representation are separate compiler facts.
module internal UnionDiscovery =
    let collect (moduleDefinition: ModuleDefinition) (typ: FSharpType) (actual: TypeReference)
                (memberType: FSharpType -> TypeReference) (signature: FSharpType -> TypeReference) (originalProperties: GeneratedProperty[])
                (declaration: CodecMember<TypeReference,MethodReference,FieldReference> -> seq<FSharpAttribute> -> seq<FSharpAttribute> -> GeneratedProperty) =
        let hasNoArguments (memberValue: FSharpMemberOrFunctionOrValue) =
            memberValue.CurriedParameterGroups |> Seq.collect id |> Seq.forall(fun argument ->
                let actual = argument.Type.StripAbbreviations()
                actual.HasTypeDefinition && actual.TypeDefinition.TryFullName = Some "Microsoft.FSharp.Core.Unit")
        let entity = typ.TypeDefinition
        let cases = entity.UnionCases |> Seq.toArray
        let attribute name = entity.Attributes |> Seq.tryFind(fun item -> item.AttributeType.FullName = name)
        let helpers =
            match attribute "Microsoft.FSharp.Core.DefaultAugmentationAttribute" with
            | Some item -> unbox<bool>(snd item.ConstructorArguments[0])
            | None -> true
        let nullFlag =
            match attribute "Microsoft.FSharp.Core.CompilationRepresentationAttribute" with
            | Some item -> Convert.ToInt32(snd item.ConstructorArguments[0]) &&& int Microsoft.FSharp.Core.CompilationRepresentationFlags.UseNullAsTrueValue <> 0
            | None -> false
        let emptyCases = cases |> Array.sumBy(fun item -> if item.Fields.Count = 0 then 1 else 0)
        let payloadCases = cases.Length - emptyCases
        let nullBacked = not entity.IsValueType && nullFlag && cases.Length <= 3 && cases.Length >= 2 && emptyCases = 1 && payloadCases > 0
        let runtimeTypes = not entity.IsValueType && cases.Length >= 2 && cases.Length <= 3 && payloadCases > 0 && not nullBacked
        let nested = not entity.IsValueType && (runtimeTypes || payloadCases > 0 && cases.Length > 1 && (not nullBacked || payloadCases > 1))
        let abstractRoot = runtimeTypes || nested && emptyCases = 0
        let publicRepresentation = entity.RepresentationAccessibility.IsPublic
        let definition = match actual with :? GenericInstanceType as closed -> closed.ElementType | _ -> actual
        let returnSignature =
            if definition.GenericParameters.Count = 0 then definition
            else
                let self = GenericInstanceType(definition)
                for parameter in definition.GenericParameters do self.GenericArguments.Add(parameter)
                self :> TypeReference
        let caseFacts =
            cases |> Array.mapi(fun tag item ->
                let factory = MethodReference((if item.Fields.Count = 0 then "get_" else "New") + item.CompiledName, returnSignature, actual)
                let fieldTypes = item.Fields |> Seq.map(fun field -> memberType field.FieldType) |> Seq.toArray
                for field in item.Fields do factory.Parameters.Add(ParameterDefinition(signature field.FieldType))
                GeneratedUnionCase(item.Name,tag,(if helpers && publicRepresentation then factory else null),item.Fields |> Seq.map _.Name |> Seq.toArray,fieldTypes))
        let rootFacts = ResizeArray<Member>()
        let rootMembers = ResizeArray<GeneratedMember>()
        let property (facts: ResizeArray<Member>) (members: ResizeArray<GeneratedMember>) (owner: TypeReference) name (valueType: TypeReference) result ignored =
            facts.Add(Member(name,valueType.FullName,false,true,false,ignored))
            members.Add(GeneratedMember(valueType,MethodReference("get_" + name,result,owner,HasThis=true),null,null))
        if publicRepresentation && not nullBacked then
            property rootFacts rootMembers actual "Tag" moduleDefinition.TypeSystem.Int32 moduleDefinition.TypeSystem.Int32 false
            if helpers && cases.Length > 1 then
                for item in cases do property rootFacts rootMembers actual ("Is" + item.CompiledName) moduleDefinition.TypeSystem.Boolean moduleDefinition.TypeSystem.Boolean false
        if publicRepresentation && not nested then
            let names = HashSet<string>(StringComparer.Ordinal)
            for item in cases do
                for field in item.Fields do
                    if names.Add field.Name then
                        let ignored = field.PropertyAttributes |> Seq.exists(fun item -> item.AttributeType.FullName = "System.Runtime.Serialization.IgnoreDataMemberAttribute")
                        property rootFacts rootMembers actual field.Name (memberType field.FieldType) (signature field.FieldType) ignored
        for item in entity.MembersFunctionsAndValues do
            if item.IsProperty && item.IsInstanceMemberInCompiledCode && item.HasGetterMethod && item.Accessibility.IsPublic
               && not(rootFacts |> Seq.exists(fun fact -> fact.Name = item.DisplayName))
               && hasNoArguments item then
                let ignored = item.Attributes |> Seq.exists(fun attribute -> attribute.AttributeType.FullName = "System.Runtime.Serialization.IgnoreDataMemberAttribute")
                property rootFacts rootMembers actual item.DisplayName (memberType item.FullType) (signature item.FullType) ignored
        let root = Model(actual.FullName,entity.FullName,TypeShape.Object,entity.IsValueType,not nested,false,null,rootFacts)
        root.AbstractType <- abstractRoot
        let polymorphic = CompilerPropertyFacts.polymorphic typ
        root.RequiresTypeInformation <- abstractRoot || polymorphic
        let generated = GeneratedModel(root,actual,null,rootMembers.ToArray())
        let declarations (facts: ResizeArray<Member>) (members: ResizeArray<GeneratedMember>) attributes =
            members |> Seq.mapi(fun index memberValue ->
                let memberFacts = {
                    Name = facts[index].Name; Type = memberValue.Type; Field = None
                    Getter = Option.ofObj memberValue.Getter; Setter = Option.ofObj memberValue.Setter
                    Static = false; Readable = not(isNull memberValue.Getter); Writable = not(isNull memberValue.Setter)
                    Ignored = false; Many = false }
                declaration memberFacts (attributes index) (attributes index)) |> Seq.toArray
        let rootAttributes index =
            if nested then Seq.empty else
            cases |> Seq.collect _.Fields |> Seq.tryFind(fun field -> field.Name = rootFacts[index].Name)
            |> Option.map(fun field -> field.PropertyAttributes :> seq<_>) |> Option.defaultValue Seq.empty
        let rootDeclarations = declarations rootFacts rootMembers rootAttributes
        generated.OriginalProperties <- GeneratedRowFields.Merge(originalProperties,rootDeclarations)
        if not entity.IsValueType && (runtimeTypes || nullBacked || cases.Length = 1 && emptyCases = 1) then
            generated.CloneConstructor <- MethodReference(".ctor",moduleDefinition.TypeSystem.Void,actual,HasThis = true)
            generated.AccessAssemblies.Add(entity.Assembly.SimpleName) |> ignore
        generated.UnionCases <- caseFacts
        let visited = HashSet<string>(StringComparer.Ordinal)
        let rec hostedReferences (candidate: FSharpType) =
            let candidate = candidate.StripAbbreviations()
            if candidate.IsGenericParameter then false
            elif candidate.IsTupleType || candidate.IsAnonRecordType then candidate.GenericArguments |> Seq.exists hostedReferences
            else
            let owner = candidate.TypeDefinition
            match owner.TryFullName with
            | Some "SoloDatabase.DBRef`1" | Some "SoloDatabase.DBRef`2" | Some "SoloDatabase.DBRefMany`1" | Some "SoloDatabase.DBRefMany`2"
            | Some "SoloDatabase.FileRef" | Some "SoloDatabase.DirectoryRef" | Some "SoloDatabase.FileRefMany" | Some "SoloDatabase.DirectoryRefMany" -> true
            | Some ("System.String" | "System.Decimal" | "System.Guid" | "System.DateTime" | "System.DateTimeOffset" | "System.TimeSpan") -> false
            | _ when owner.IsEnum -> false
            | _ ->
                let reference = memberType candidate
                if not(visited.Add(reference.FullName + "," + reference.Scope.Name)) then false
                elif owner.IsArrayType then candidate.GenericArguments |> Seq.exists hostedReferences
                elif candidate.GenericArguments |> Seq.exists hostedReferences then true
                else
                    let substitutions = Seq.zip owner.GenericParameters candidate.GenericArguments |> Seq.toList
                    if owner.IsFSharpUnion then
                        owner.UnionCases |> Seq.exists(fun item -> item.Fields |> Seq.exists(fun field -> hostedReferences(field.FieldType.Instantiate(substitutions))))
                    elif owner.IsFSharpRecord then
                        owner.FSharpFields |> Seq.exists(fun field -> hostedReferences(field.FieldType.Instantiate(substitutions)))
                    else
                        owner.MembersFunctionsAndValues |> Seq.exists(fun property ->
                            property.IsProperty && property.IsInstanceMember && property.HasGetterMethod && property.Accessibility.IsPublic
                            && hasNoArguments property
                            && hostedReferences(property.FullType.Instantiate(substitutions)))
                        || (match candidate.BaseType with Some parent -> hostedReferences parent | None -> false)
        let hosted = hostedReferences typ
        generated.UnionHostedReferences <- hosted
        if not entity.Accessibility.IsPublic then generated.AccessAssemblies.Add(entity.Assembly.SimpleName) |> ignore
        let models = ResizeArray<GeneratedModel>()
        models.Add generated
        if nested then
            for item in cases |> Array.filter(fun item -> item.Fields.Count > 0 || runtimeTypes) do
                let caseDefinition = TypeReference("",(if item.Fields.Count = 0 then "_" else "") + item.CompiledName,moduleDefinition,definition.Scope,DeclaringType=definition)
                for parameter in definition.GenericParameters do caseDefinition.GenericParameters.Add(GenericParameter(parameter.Name,caseDefinition))
                let rec caseSignature (value: TypeReference) =
                    match value with
                    | :? GenericParameter as parameter when parameter.Type = GenericParameterType.Type ->
                        caseDefinition.GenericParameters[parameter.Position] :> TypeReference
                    | :? GenericInstanceType as generic ->
                        let closed = GenericInstanceType(generic.ElementType)
                        for argument in generic.GenericArguments do closed.GenericArguments.Add(caseSignature argument)
                        closed :> TypeReference
                    | :? ArrayType as array -> ArrayType(caseSignature array.ElementType,array.Rank) :> TypeReference
                    | _ -> value
                let caseType =
                    match actual with
                    | :? GenericInstanceType as closed ->
                        let value = GenericInstanceType(caseDefinition)
                        for argument in closed.GenericArguments do value.GenericArguments.Add(argument)
                        value :> TypeReference
                    | _ -> caseDefinition
                let facts = ResizeArray<Member>()
                let members = ResizeArray<GeneratedMember>()
                for field in item.Fields do
                    let fieldSignature = caseSignature(signature field.FieldType)
                    if publicRepresentation then
                        property facts members caseType field.Name (memberType field.FieldType) fieldSignature
                            (field.PropertyAttributes |> Seq.exists(fun attribute -> attribute.AttributeType.FullName = "System.Runtime.Serialization.IgnoreDataMemberAttribute"))
                let localDeclarations = if publicRepresentation then declarations facts members (fun index -> item.Fields[index].PropertyAttributes) else [||]
                for fact in rootFacts do facts.Add fact
                for memberValue in rootMembers do members.Add memberValue
                let model = Model(caseType.FullName,entity.FullName,TypeShape.Object,false,false,false,null,facts)
                let caseModel = GeneratedModel(model,caseType,null,members.ToArray())
                caseModel.OriginalProperties <- GeneratedRowFields.Merge(
                    localDeclarations,
                    GeneratedRowFields.Inherited(generated.OriginalProperties))
                if item.Fields.Count = 0 then
                    caseModel.CloneConstructor <- MethodReference(".ctor",moduleDefinition.TypeSystem.Void,caseType,HasThis = true)
                    caseModel.AccessAssemblies.Add(entity.Assembly.SimpleName) |> ignore
                model.RequiresTypeInformation <- polymorphic
                caseModel.UnionCases <- caseFacts
                caseModel.UnionRoot <- actual
                caseModel.UnionHostedReferences <- hosted
                if (helpers && item.Fields.Count = 0) || not entity.Accessibility.IsPublic || not publicRepresentation then caseModel.AccessAssemblies.Add(entity.Assembly.SimpleName) |> ignore
                models.Add caseModel
        if String.IsNullOrEmpty(entity.Assembly.QualifiedName) then models.ToArray()
        else
            match entity.Assembly.FileName with
            | Some path ->
                use resolver = new DefaultAssemblyResolver()
                resolver.AddSearchDirectory(System.IO.Path.GetDirectoryName(path))
                resolver.AddSearchDirectory(System.IO.Path.GetDirectoryName(typeof<Microsoft.FSharp.Core.CompilationMappingAttribute>.Assembly.Location))
                use input = AssemblyDefinition.ReadAssembly(path, ReaderParameters(AssemblyResolver = resolver))
                if input.Name.FullName <> entity.Assembly.QualifiedName then invalidOp ("The union input does not match its compiler identity: " + entity.FullName)
                let original = input.MainModule.GetType(definition.FullName)
                let source =
                    match actual with
                    | :? GenericInstanceType as closed ->
                        let source = GenericInstanceType(original)
                        for argument in closed.GenericArguments do source.GenericArguments.Add(argument)
                        source :> TypeReference
                    | _ -> original
                GeneratedUnionMetadata.Collect(moduleDefinition, source, hosted)
            | None -> invalidOp ("The compiler union input has no metadata path: " + entity.FullName)
