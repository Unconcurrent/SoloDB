namespace SoloDatabase.Metadata.FSharp

open System
open System.Collections.Generic
open System.IO
open FSharp.Compiler.Syntax
open FSharp.Compiler.Text
open FSharp.Compiler.CodeAnalysis
open FSharp.Compiler.Symbols
open FSharp.Compiler.Symbols.FSharpExprPatterns
open Mono.Cecil
open SoloDatabase.Metadata.CodecGeneration
open SoloDatabase.Metadata.Generation
open SoloDatabase.Metadata.Tools

module internal ModelDiscovery =
    let private hasAttribute name (attributes: seq<FSharpAttribute>) =
        attributes |> Seq.exists (fun attribute -> attribute.AttributeType.FullName = name)

    let private sealedEntity (entity: FSharpEntity) =
        entity.IsValueType || entity.IsFSharpRecord || hasAttribute "Microsoft.FSharp.Core.SealedAttribute" entity.Attributes

    let rec private entities (items: seq<FSharpEntity>) = seq {
        for entity in items do
            yield entity
            yield! entities entity.NestedEntities
    }

    let hasAccessAttribute (project: FSharpCheckProjectResults) =
        match entities project.AssemblySignature.Entities
              |> Seq.tryFind(fun entity -> entity.TryFullName = Some "System.Runtime.CompilerServices.IgnoresAccessChecksToAttribute") with
        | None -> false
        | Some entity ->
            let compatible =
                entity.BaseType |> Option.exists(fun typ -> typ.TypeDefinition.TryFullName = Some "System.Attribute")
                && (entity.MembersFunctionsAndValues |> Seq.exists(fun memberValue ->
                    memberValue.IsConstructor && not memberValue.Accessibility.IsPrivate
                    && (memberValue.CurriedParameterGroups |> Seq.collect id |> Seq.map(fun argument -> argument.Type.StripAbbreviations().TypeDefinition.TryFullName) |> Seq.toList) = [Some "System.String"]))
            if not compatible then invalidOp "The existing IgnoresAccessChecksToAttribute must inherit Attribute and expose an accessible constructor accepting one string."
            true

    let accessAttribute moduleDefinition (currentIdentity: AssemblyNameReference) =
        TypeReference("System.Runtime.CompilerServices","IgnoresAccessChecksToAttribute",moduleDefinition,
            currentIdentity)

    let collect (project: FSharpCheckProjectResults) (moduleDefinition: ModuleDefinition) currentIdentity (inferredRoots: FSharpType[])
                (parsedInputs: IReadOnlyDictionary<string,ParsedInput>) projectDirectory =
        // FCS reports effective accessibility inside private types. CLR constructors retain
        // their own declared visibility, unlike the properties emitted for those types.
        let constructorVisibility = ResizeArray<range * bool>()
        let isPublic = function None | Some(SynAccess.Public _) -> true | _ -> false
        for input in parsedInputs.Values do
            ParsedInput.fold (fun () _ node ->
                match node with
                | SyntaxNode.SynTypeDefn(SynTypeDefn(typeInfo = SynComponentInfo(range = location);
                    implicitConstructor = Some(SynMemberDefn.ImplicitCtor(accessibility = access)))) ->
                    constructorVisibility.Add(location,isPublic access)
                | SyntaxNode.SynMemberDefn(SynMemberDefn.Member(SynBinding(accessibility = access;
                    valData = SynValData(memberFlags = Some flags); range = location), _)) when flags.MemberKind = SynMemberKind.Constructor ->
                    constructorVisibility.Add(location,isPublic access)
                | _ -> ()) () input
        let path (file: string) = Path.GetFullPath(if Path.IsPathRooted(file) then file else Path.Combine(projectDirectory,file))
        let publicConstructor (memberValue: FSharpMemberOrFunctionOrValue) =
            memberValue.IsConstructor &&
            ((memberValue :> FSharpSymbol).DeclarationLocation |> Option.bind(fun declared ->
                constructorVisibility |> Seq.tryPick(fun (location,isPublic) ->
                    if path location.FileName = path declared.FileName &&
                       (location.StartLine,location.StartColumn) <= (declared.StartLine,declared.StartColumn) &&
                       (declared.EndLine,declared.EndColumn) <= (location.EndLine,location.EndColumn) then Some isPublic else None))
             |> Option.defaultValue memberValue.Accessibility.IsPublic)
        let referenceWith = (CompilerReferences.create moduleDefinition currentIdentity (project.ProjectContext.GetReferencedAssemblies() |> Seq.find(fun assembly -> assembly.SimpleName = "FSharp.Core") |> _.QualifiedName)).Reference
        let sources = ResizeArray<GeneratedModel * FSharpType>()
        let reference typ = referenceWith (fun _ -> invalidOp "An open model type requires its actual closed use-site arguments.") typ
        let completeReferenceFacts (facts: ResizeArray<Member>) (members: ResizeArray<GeneratedMember>) =
            for index = 0 to facts.Count - 1 do
                let fact = facts[index]
                let referenceType = members[index].Type
                let definition = match referenceType with :? GenericInstanceType as generic -> generic.ElementType | _ -> referenceType
                let shape = TypeSupport.ReferenceShape definition.FullName
                fact.Reference <- not fact.Field && shape <> TypeShape.Object
                fact.ManyReference <- TypeSupport.ReferenceMany shape
        let directGetters = HashSet<string>(StringComparer.Ordinal)
        let rec inspect declarations =
            for declaration in declarations do
                match declaration with
                | FSharpImplementationFileDeclaration.Entity(_, children) -> inspect children
                | FSharpImplementationFileDeclaration.MemberOrFunctionOrValue(memberValue, _, body) when
                    memberValue.IsPropertyGetterMethod && memberValue.CompiledName = "get_Id" ->
                    match body with
                    | FSharpFieldGet(Some(Value receiver), _, field) when
                        memberValue.IsPropertyGetterMethod && receiver.IsMemberThisValue && not field.IsStatic ->
                        directGetters.Add(memberValue.FullName) |> ignore
                    | _ -> ()
                | _ -> ()
        for file in project.AssemblyContents.ImplementationFiles do inspect file.Declarations
        let queue = Queue<FSharpType>()
        for entity in entities project.AssemblySignature.Entities do
            if hasAttribute "SoloDatabase.SoloDBAOTAttribute" entity.Attributes then queue.Enqueue(entity.AsType())
        for root in inferredRoots do queue.Enqueue root
        let visited = HashSet<string>(StringComparer.Ordinal)
        let models = ResizeArray<GeneratedModel>()
        let referenced = project.ProjectContext.GetReferencedAssemblies() |> Seq.toArray
        let named (name: string) =
            let path = name.Split('.') |> Array.toList
            referenced |> Seq.choose(fun assembly -> assembly.Contents.FindEntityByPath(path))
        let sourceAssemblies = project.AssemblySignature.Entities |> Seq.map(fun entity -> entity.Assembly.SimpleName) |> Set.ofSeq
        use metadataResolver = new DefaultAssemblyResolver()
        for directory in referenced |> Seq.choose _.FileName |> Seq.map Path.GetDirectoryName |> Seq.distinct do
            metadataResolver.AddSearchDirectory(directory)
        let images = Dictionary<string,AssemblyDefinition>(StringComparer.Ordinal)
        use ownedImages = { new IDisposable with member _.Dispose() = for image in images.Values do image.Dispose() }
        let image path =
            match images.TryGetValue path with
            | true, input -> input
            | _ ->
                let input = AssemblyDefinition.ReadAssembly(path,ReaderParameters(AssemblyResolver=metadataResolver))
                images.Add(path,input)
                input
        let arrayTemplates = Dictionary<int,FSharpType>()
        let arrayTemplate rank =
            match arrayTemplates.TryGetValue rank with
            | true, value -> value
            | _ ->
                let moduleName = if rank = 1 then "ArrayModule" else "Array" + string rank + "DModule"
                let template =
                    referenced
                    |> Array.tryPick(fun assembly ->
                        assembly.Contents.FindEntityByPath(["Microsoft";"FSharp";"Collections";moduleName])
                        |> Option.bind(fun entity ->
                            entity.MembersFunctionsAndValues
                            |> Seq.tryPick(fun memberValue ->
                                if memberValue.CompiledName <> "ZeroCreate" then None else
                                let result = memberValue.ReturnParameter.Type.StripAbbreviations()
                                if result.HasTypeDefinition && result.TypeDefinition.IsArrayType && result.TypeDefinition.ArrayRank = rank then Some result
                                else None)))
                    |> Option.defaultWith(fun () -> invalidOp ("The compiler has no array type identity for rank " + string rank + "."))
                arrayTemplates.Add(rank,template)
                template
        let rec importedType (value: TypeReference) : FSharpType =
            match value with
            | :? ArrayType as array ->
                let template = arrayTemplate array.Rank
                template.Instantiate([template.GenericArguments[0].GenericParameter,importedType array.ElementType])
            | _ ->
                let definition, arguments =
                    match value with
                    | :? GenericInstanceType as closed -> closed.ElementType, closed.GenericArguments |> Seq.map importedType |> Seq.toArray
                    | _ -> value, [||]
                let path = definition.FullName.Replace('/', '.').Split('.') |> Array.toList
                let candidates =
                    Array.append
                        (project.AssemblySignature.FindEntityByPath(path) |> Option.toArray)
                        (referenced |> Array.choose(fun assembly -> assembly.Contents.FindEntityByPath(path)))
                let entity =
                    match candidates |> Array.filter(fun entity -> entity.Assembly.SimpleName = definition.Scope.Name) with
                    | [|entity|] -> entity
                    | _ ->
                        match candidates with
                        | [|entity|] when definition.Scope.Name = "netstandard" || definition.Scope.Name = "mscorlib" || definition.Scope.Name = "System.Runtime" -> entity
                        | _ -> invalidOp ("The compiler has no unique field type identity: " + definition.FullName)
                let source = entity.AsType()
                if arguments.Length = 0 then source
                else source.Instantiate(Seq.zip entity.GenericParameters arguments |> Seq.toList)
        let rec enqueueImported (value: TypeReference) =
            match value with
            | :? ArrayType as array ->
                enqueueImported array.ElementType
                if visited.Add(value.FullName + "," + value.Scope.Name) then
                    let shape = (TypeSupport.Select { Definition = value.FullName; ArrayRank = array.Rank; ElementDefinition = array.ElementType.FullName
                                                      Enum = false; Tuple = false; Dictionary = false; ReadOnlyDictionary = false; NonGenericDictionary = false; Enumerable = false; Grouping = false }).Shape
                    let model = Model(value.FullName,value.FullName,shape,false,true,false,array.ElementType.FullName,[||])
                    model.NonGenericEnumerable <- array.Rank <> 1
                    model.SupportsValuesWrapper <- TypeSupport.SupportsValuesWrapper array.ElementType.FullName
                    models.Add(GeneratedModel(model,value,null,[||],SequenceElement = array.ElementType,OriginalProperties = [||]))
            | _ -> queue.Enqueue(importedType value)
        while queue.Count <> 0 do
            let typ = queue.Dequeue().StripAbbreviations()
            if typ.IsAnonRecordType then
                let actual = reference typ
                if visited.Add(actual.FullName + "," + actual.Scope.Name) then
                    let names = typ.AnonRecordTypeDetails.SortedFieldNames
                    let definition = match actual with :? GenericInstanceType as closed -> closed.ElementType | _ -> actual
                    let facts = ResizeArray<Member>()
                    let members = ResizeArray<GeneratedMember>()
                    let constructor = MethodReference(".ctor",moduleDefinition.TypeSystem.Void,actual,HasThis = true)
                    for index = 0 to names.Length - 1 do
                        let child = typ.GenericArguments[index]
                        queue.Enqueue(child)
                        let value = reference child
                        let parameter = definition.GenericParameters[index]
                        constructor.Parameters.Add(ParameterDefinition(parameter))
                        facts.Add(Member(names[index],value.FullName,false,true,false,false))
                        members.Add(GeneratedMember(value,MethodReference("get_" + names[index],parameter,actual,HasThis = true),null,null))
                    completeReferenceFacts facts members
                    let model = Model(actual.FullName,actual.FullName,TypeShape.Object,CompilerReferences.isValueType typ,true,names.Length = 0,null,facts)
                    model.ConstructFromMembers <- true
                    model.RecordType <- true
                    model.StandardComparison <- true
                    model.RecordFields <- names
                    let generated = GeneratedModel(model,actual,constructor,members.ToArray())
                    generated.OriginalProperties <-
                        if String.IsNullOrEmpty typ.AnonRecordTypeDetails.Assembly.QualifiedName then
                            members |> Seq.mapi(fun index memberValue ->
                                GeneratedProperty({ Name = names[index]; Type = memberValue.Type; Field = None; Getter = Some memberValue.Getter; Setter = None;
                                  Static = false; Readable = true; Writable = false; Ignored = false; Many = false },deferredSchema = true)) |> Seq.toArray
                        else GeneratedRowFields.Properties(moduleDefinition,SourceTypeIdentity.Original(actual,metadataResolver),Action<string>(fun assembly -> generated.AccessAssemblies.Add assembly |> ignore))
                    models.Add(generated)
                    sources.Add(generated,typ)
            elif typ.IsTupleType then
                for argument in typ.GenericArguments do queue.Enqueue argument
                let rec addTuple (actual: TypeReference) =
                    if visited.Add(actual.FullName + "," + actual.Scope.Name) then
                        let closed = actual :?> GenericInstanceType
                        let facts = ResizeArray<Member>()
                        let members = ResizeArray<GeneratedMember>()
                        let constructor = MethodReference(".ctor", moduleDefinition.TypeSystem.Void, actual, HasThis = true)
                        for index = 0 to closed.GenericArguments.Count - 1 do
                            let name = if index = 7 then "Rest" else "Item" + string (index + 1)
                            let valueType = closed.GenericArguments[index]
                            let parameter = closed.ElementType.GenericParameters[index]
                            constructor.Parameters.Add(ParameterDefinition(parameter))
                            facts.Add(Member(name, valueType.FullName, typ.IsStructTupleType, true, false, false))
                            if typ.IsStructTupleType then
                                members.Add(GeneratedMember(valueType, null, null, FieldReference(name, parameter, actual)))
                            else
                                members.Add(GeneratedMember(valueType, MethodReference("get_" + name, parameter, actual, HasThis = true), null, null))
                            if index = 7 then addTuple valueType
                        let model = Model(actual.FullName, actual.FullName, TypeShape.Tuple, typ.IsStructTupleType, true, false, null, facts)
                        let generated = GeneratedModel(model, actual, constructor, members.ToArray())
                        generated.OriginalProperties <- GeneratedRowFields.Properties(moduleDefinition,SourceTypeIdentity.Original(actual,metadataResolver),Action<string>(fun assembly -> generated.AccessAssemblies.Add assembly |> ignore))
                        models.Add generated
                addTuple (reference typ)
            else
                let entity = typ.TypeDefinition
                let actual = reference typ
                let fullName = defaultArg entity.TryFullName actual.FullName
                if visited.Add(actual.FullName + "," + actual.Scope.Name) then
                    let contract name =
                        seq { yield typ; yield! typ.AllInterfaces }
                        |> Seq.map _.StripAbbreviations()
                        |> Seq.tryFind(fun candidate -> candidate.HasTypeDefinition && candidate.TypeDefinition.TryFullName = Some name)
                    let dictionaryContract = contract "System.Collections.Generic.IDictionary`2"
                    let readOnlyContract = contract "System.Collections.Generic.IReadOnlyDictionary`2"
                    let sequenceContract = contract "System.Collections.Generic.IEnumerable`1"
                    let nonGenericDictionary = (contract "System.Collections.IDictionary").IsSome
                    let elementName = if entity.IsArrayType then (reference (typ.GenericArguments[0].StripAbbreviations())).FullName else ""
                    let decision = TypeSupport.Select {
                        Definition = fullName
                        ArrayRank = (if entity.IsArrayType then entity.ArrayRank else 0)
                        ElementDefinition = elementName
                        Enum = entity.IsEnum; Tuple = false
                        Dictionary = dictionaryContract.IsSome; ReadOnlyDictionary = readOnlyContract.IsSome
                        NonGenericDictionary = nonGenericDictionary; Enumerable = sequenceContract.IsSome; Grouping = (contract "System.Linq.IGrouping`2").IsSome }
                    let shape = decision.Shape
                    if decision.GenericChildren || entity.IsArrayType then
                        for argument in typ.GenericArguments do queue.Enqueue argument
                    for child in decision.NamedChildren do
                        let target = named child |> Seq.find(fun entity -> entity.TryFullName = Some child)
                        queue.Enqueue(target.AsType())
                    for child in TypeSupport.OptionalChildren shape do
                        match named child |> Seq.tryFind(fun entity -> entity.TryFullName = Some child) with
                        | Some target -> queue.Enqueue(target.AsType())
                        | None -> ()
                    let collectionArguments =
                        if decision.Contract = "" then [||]
                        else (contract decision.Contract).Value.GenericArguments |> Seq.toArray
                    for argument in collectionArguments do queue.Enqueue argument
                    let constructorParameters (memberValue: FSharpMemberOrFunctionOrValue) =
                        memberValue.CurriedParameterGroups |> Seq.collect id
                        |> Seq.filter(fun argument -> let value = argument.Type.StripAbbreviations() in not(value.HasTypeDefinition && value.TypeDefinition.TryFullName = Some "Microsoft.FSharp.Core.Unit"))
                        |> Seq.toArray
                    let publicConstructors = entity.MembersFunctionsAndValues |> Seq.filter(fun memberValue -> publicConstructor memberValue) |> Seq.toArray
                    let populateCollection = TypeSupport.PopulateCollection(shape,typ.GenericArguments.Count,(contract "System.Collections.IList").IsSome,
                                             (publicConstructors |> Array.exists(fun memberValue -> (constructorParameters memberValue).Length = 0)))
                    let mutable indexedRead, indexedWrite = null, null
                    let facts = ResizeArray<Member>()
                    let members = ResizeArray<GeneratedMember>()
                    let access = HashSet<string>(StringComparer.Ordinal)
                    let requireAccess (accessibility: FSharpAccessibility) =
                        if accessibility.IsPrivate || (not accessibility.IsPublic && not(String.IsNullOrEmpty(entity.Assembly.QualifiedName))) then
                            access.Add(entity.Assembly.SimpleName) |> ignore
                    if shape = TypeShape.Object then requireAccess entity.Accessibility
                    let mutableRecord = hasAttribute "Microsoft.FSharp.Core.CLIMutableAttribute" entity.Attributes
                    let substitutions = Seq.zip entity.GenericParameters typ.GenericArguments |> Seq.toList
                    let memberType (memberType: FSharpType) =
                        let closed = memberType.Instantiate(substitutions)
                        queue.Enqueue(closed)
                        reference closed
                    let definition = match actual with :? GenericInstanceType as closed -> closed.ElementType | _ -> actual
                    let signature memberType =
                        referenceWith (fun parameter ->
                            let index = entity.GenericParameters |> Seq.findIndex(fun candidate -> candidate.Equals(parameter))
                            definition.GenericParameters[index] :> TypeReference) memberType
                    let referencePolicyFacts (fact: Member) (attributes: seq<FSharpAttribute>) =
                        for attribute in attributes do
                            match attribute.AttributeType.FullName with
                            | "SoloDatabase.Attributes.SoloRefAttribute" ->
                                fact.HasReferencePolicy <- true
                                for _, name, _, value in attribute.NamedArguments do
                                    match name with
                                    | "OnDelete" -> fact.OnDelete <- Convert.ToInt32(value)
                                    | "OnOwnerDelete" -> fact.OnOwnerDelete <- Convert.ToInt32(value)
                                    | "Unique" -> fact.Unique <- unbox<bool> value
                                    | "OrderBy" -> fact.OrderBy <- Convert.ToInt32(value)
                                    | _ -> ()
                            | _ -> ()
                    let originalType =
                        if not entity.IsArrayType && not(String.IsNullOrEmpty(entity.Assembly.QualifiedName)) then
                            Some(SourceTypeIdentity.Original(actual,metadataResolver))
                        else None
                    let declaration memberValue (attributes: seq<FSharpAttribute>) (identityAttributes: seq<FSharpAttribute>) =
                        let identity =
                            identityAttributes |> Seq.tryFind(fun attribute -> attribute.AttributeType.FullName = "SoloDatabase.Attributes.SoloId")
                            |> Option.map(fun attribute ->
                                match snd attribute.ConstructorArguments[0] with
                                | :? FSharpType as generatorType ->
                                    let generatorType = generatorType.StripAbbreviations()
                                    let actual = reference generatorType
                                    let constructor =
                                        if String.IsNullOrEmpty(generatorType.TypeDefinition.Assembly.QualifiedName) then
                                            if generatorType.TypeDefinition.MembersFunctionsAndValues |> Seq.exists(fun memberValue -> publicConstructor memberValue && (constructorParameters memberValue).Length = 0) then
                                                MethodReference(".ctor",moduleDefinition.TypeSystem.Void,actual,HasThis = true)
                                            else null
                                        else
                                            let original = SourceTypeIdentity.Original(actual,metadataResolver)
                                            match original.Resolve().Methods |> Seq.tryFind(fun method -> method.IsConstructor && not method.IsStatic && method.IsPublic && method.Parameters.Count = 0) with
                                            | Some constructor ->
                                                let call = moduleDefinition.ImportReference constructor
                                                call.DeclaringType <- actual
                                                call
                                            | None -> null
                                    let family = generatorType.AllInterfaces |> Seq.exists(fun contract ->
                                        let contract = contract.StripAbbreviations()
                                        contract.HasTypeDefinition && (contract.TypeDefinition.TryFullName |> Option.exists(fun name -> TypeSupport.IdentityGeneratorContract(name,contract.TypeDefinition.IsArrayType))))
                                    access.Add(generatorType.TypeDefinition.Assembly.SimpleName) |> ignore
                                    GeneratedIdGenerator(actual,constructor,family)
                                | null -> GeneratedIdGenerator(null,null,false)
                                | _ -> invalidOp "The compiler did not supply the original identity generator type.") |> Option.toObj
                        for attribute in attributes do access.Add attribute.AttributeType.Assembly.SimpleName |> ignore
                        GeneratedProperty(memberValue,identity,deferredSchema = true)
                    let originalProperties =
                        if entity.IsArrayType then [||] else
                        CompilerPropertyFacts.read moduleDefinition referenceWith reference metadataResolver access declaration
                            (fun value ->
                                if sourceAssemblies.Contains value.Scope.Name then Nullable(CompilerPropertyFacts.many (importedType value))
                                else Nullable()) true typ
                    if (shape = TypeShape.Object || shape = TypeShape.BsonDocument) && not entity.IsArrayType then
                        if entity.IsFSharpUnion then
                            ()
                        elif entity.IsFSharpRecord then
                            for field in entity.FSharpFields do
                                let valueType = memberType field.FieldType
                                facts.Add(Member(field.Name, valueType.FullName, false, true, (field.IsMutable || mutableRecord),
                                                 hasAttribute "System.Runtime.Serialization.IgnoreDataMemberAttribute" field.PropertyAttributes))
                                let setter =
                                    if field.IsMutable || mutableRecord then
                                        let method = MethodReference("set_" + field.Name, moduleDefinition.TypeSystem.Void, actual, HasThis = true)
                                        method.Parameters.Add(ParameterDefinition(valueType))
                                        method
                                    else null
                                members.Add(GeneratedMember(valueType, MethodReference("get_" + field.Name, signature field.FieldType, actual, HasThis = true), setter, null))
                                referencePolicyFacts facts.[facts.Count - 1] field.PropertyAttributes
                        else
                            if String.IsNullOrEmpty(entity.Assembly.QualifiedName) then
                                for field in entity.FSharpFields do
                                    if field.Accessibility.IsPublic && (entity.IsValueType || not field.IsStatic) then
                                        let valueType = memberType field.FieldType
                                        facts.Add(Member(field.Name, valueType.FullName, true, true, entity.IsValueType && field.IsMutable, false))
                                        members.Add(GeneratedMember(valueType, null, null, FieldReference(field.Name, signature field.FieldType, actual), StaticField = field.IsStatic, QueryWritable = field.IsMutable))
                            else
                                let path = entity.Assembly.FileName |> Option.defaultWith(fun () -> invalidOp ("The compiler field input has no metadata path: " + fullName))
                                let input = image path
                                if input.Name.FullName <> entity.Assembly.QualifiedName then invalidOp ("The model input does not match its compiler identity: " + fullName)
                                let source = input.MainModule.GetType(definition.FullName)
                                if isNull source then invalidOp ("The model input has no original definition: " + definition.FullName)
                                let rec closedFieldType (value: TypeReference) =
                                    match value with
                                    | :? GenericParameter as parameter -> reference typ.GenericArguments[parameter.Position]
                                    | :? ArrayType as array -> ArrayType(closedFieldType array.ElementType,array.Rank) :> TypeReference
                                    | :? GenericInstanceType as closed ->
                                        let result = GenericInstanceType(moduleDefinition.ImportReference(closed.ElementType))
                                        for argument in closed.GenericArguments do result.GenericArguments.Add(closedFieldType argument)
                                        result :> TypeReference
                                    | _ -> moduleDefinition.ImportReference(value)
                                for field in source.Fields do
                                    if field.IsPublic && (entity.IsValueType || not field.IsStatic) then
                                        let valueType =
                                            match entity.FSharpFields |> Seq.tryFind(fun original -> original.Name = field.Name) with
                                            | Some original -> memberType original.FieldType
                                            | None ->
                                                let value = closedFieldType field.FieldType
                                                enqueueImported value
                                                value
                                        facts.Add(Member(field.Name,valueType.FullName,true,true,entity.IsValueType && not field.IsInitOnly && not field.IsLiteral,false))
                                        let original = moduleDefinition.ImportReference(field)
                                        original.DeclaringType <- actual
                                        members.Add(GeneratedMember(valueType,null,null,original,StaticField = field.IsStatic, QueryWritable = (not field.IsInitOnly && not field.IsLiteral)))
                        let rec propertyOwners (current: FSharpType) = seq {
                            yield current
                            match current.BaseType with
                            | Some parent when not entity.IsValueType ->
                                let parent = parent.StripAbbreviations()
                                if parent.TypeDefinition.TryFullName <> Some "System.Object" then
                                    queue.Enqueue parent
                                    yield! propertyOwners parent
                            | _ -> ()
                        }
                        let names = HashSet<string>(StringComparer.Ordinal)
                        for owningType in propertyOwners typ do
                            let declaringEntity = owningType.TypeDefinition
                            let declaringType = reference owningType
                            let declaringDefinition = match declaringType with :? GenericInstanceType as closed -> closed.ElementType | _ -> declaringType
                            let declaringSubstitutions = Seq.zip declaringEntity.GenericParameters owningType.GenericArguments |> Seq.toList
                            let declaringMemberType (memberType: FSharpType) =
                                let closed = memberType.Instantiate(declaringSubstitutions)
                                queue.Enqueue(closed)
                                reference closed
                            let declaringSignature memberType =
                                referenceWith (fun parameter ->
                                    let index = declaringEntity.GenericParameters |> Seq.findIndex(fun candidate -> candidate.Equals(parameter))
                                    declaringDefinition.GenericParameters[index] :> TypeReference) memberType
                            let propertyKey (property: FSharpMemberOrFunctionOrValue) =
                                let valueType =
                                    if property.HasGetterMethod then property.GetterMethod.ReturnParameter.Type
                                    else property.SetterMethod.CurriedParameterGroups |> Seq.collect id |> Seq.last |> _.Type
                                struct(property.DisplayName,(declaringSignature valueType).FullName)
                            let imported = Dictionary<struct(string * string),MethodReference * MethodReference * bool>()
                            let properties =
                                let properties = declaringEntity.MembersFunctionsAndValues |> Seq.filter(fun memberValue -> memberValue.IsProperty && not (declaringEntity.IsFSharpRecord && (declaringEntity.FSharpFields |> Seq.exists(fun field -> field.Name = memberValue.DisplayName))))
                                if String.IsNullOrEmpty(declaringEntity.Assembly.QualifiedName) then
                                    properties |> Seq.sortBy(fun memberValue -> memberValue.DeclarationLocation.StartLine, memberValue.DeclarationLocation.StartColumn)
                                else
                                    match declaringEntity.Assembly.FileName with
                                    | Some path ->
                                        let input = image path
                                        if input.Name.FullName <> declaringEntity.Assembly.QualifiedName then invalidOp ("The model input does not match its compiler identity: " + fullName)
                                        let inputDefinition = input.MainModule.GetType(declaringDefinition.FullName)
                                        if isNull inputDefinition then invalidOp ("The model input has no original definition: " + declaringDefinition.FullName)
                                        let positions = Dictionary<struct(string * string),int>()
                                        for property in inputDefinition.Properties do
                                            if property.Parameters.Count > 0 then
                                                let getter = if isNull property.GetMethod then property.SetMethod else property.GetMethod
                                                let signature =
                                                    if isNull getter then property.Name
                                                    else
                                                        let format (typ: TypeReference) =
                                                            if typ.IsPrimitive || typ.FullName = "System.Void" then typ.Name else typ.FullName.Replace('/', '+')
                                                        format getter.ReturnType + " " + getter.Name + "(" + (getter.Parameters |> Seq.map(fun parameter -> format parameter.ParameterType) |> String.concat ", ") + ")"
                                                if not(isNull property.SetMethod) && property.SetMethod.IsPublic && isNull indexedRead then indexedRead <- signature
                                                if not(isNull property.GetMethod) && property.GetMethod.IsPublic && not(property.GetMethod.IsStatic) && isNull indexedWrite &&
                                                   not(property.CustomAttributes |> Seq.exists(fun attribute -> attribute.AttributeType.FullName = "System.Runtime.Serialization.IgnoreDataMemberAttribute")) then indexedWrite <- signature
                                            let key = struct(property.Name,property.PropertyType.FullName)
                                            if not(positions.ContainsKey key) then positions.Add(key,property.MetadataToken.ToInt32())
                                            // Copy complete signatures while the exact imported input is owned.
                                            let accessor (method: MethodDefinition) =
                                                if isNull method then null
                                                else
                                                    let reference = moduleDefinition.ImportReference(method)
                                                    reference.DeclaringType <- declaringType
                                                    reference
                                            let requiresAccess =
                                                (not(isNull property.GetMethod) && not property.GetMethod.IsPublic)
                                                || (not(isNull property.SetMethod) && not property.SetMethod.IsPublic)
                                            if not(imported.ContainsKey key) then imported.Add(key,(accessor property.GetMethod, accessor property.SetMethod, requiresAccess))
                                        properties |> Seq.sortBy(fun property -> positions[propertyKey property]) |> Seq.toArray :> seq<_>
                                    | None -> invalidOp ("The compiler model input has no metadata path: " + fullName)
                            for property in properties do
                                if property.Accessibility.IsPublic && not property.IsInstanceMemberInCompiledCode && property.HasGetterMethod && not property.HasSetterMethod &&
                                   (property.GetterMethod.CurriedParameterGroups |> Seq.collect id |> Seq.forall(fun parameter ->
                                       parameter.Type.StripAbbreviations().HasTypeDefinition && parameter.Type.StripAbbreviations().TypeDefinition.TryFullName = Some "Microsoft.FSharp.Core.Unit")) then
                                    let valueType = declaringMemberType property.GetterMethod.ReturnParameter.Type
                                    if names.Add(property.DisplayName + ":" + valueType.FullName + ":") then
                                        facts.Add(Member(property.DisplayName,valueType.FullName,false,false,false,false))
                                        members.Add(GeneratedMember(valueType,null,null,null))
                                        referencePolicyFacts facts.[facts.Count - 1] property.Attributes
                                match imported.TryGetValue(propertyKey property) with
                                | true,(getter,setter,_) when (not(isNull getter) && getter.Parameters.Count > 0) || (not(isNull setter) && setter.Parameters.Count > 1) ->
                                    let valueType =
                                        if not(isNull getter) then declaringMemberType property.GetterMethod.ReturnParameter.Type
                                        else property.SetterMethod.CurriedParameterGroups |> Seq.collect id |> Seq.last |> fun parameter -> declaringMemberType parameter.Type
                                    if names.Add(property.DisplayName + ":" + valueType.FullName + ":indexer") then
                                        facts.Add(Member(property.DisplayName,valueType.FullName,false,false,false,false))
                                        members.Add(GeneratedMember(valueType,null,null,null))
                                        referencePolicyFacts facts.[facts.Count - 1] property.Attributes
                                | _ -> ()
                                if String.IsNullOrEmpty(declaringEntity.Assembly.QualifiedName) && property.Accessibility.IsPublic && property.IsInstanceMemberInCompiledCode then
                                    let accessor = if property.HasGetterMethod then property.GetterMethod else property.SetterMethod
                                    let arguments = accessor.CurriedParameterGroups |> Seq.collect id |> Seq.filter(fun argument ->
                                        let actual = argument.Type.Instantiate(declaringSubstitutions).StripAbbreviations()
                                        not actual.HasTypeDefinition || actual.TypeDefinition.TryFullName <> Some "Microsoft.FSharp.Core.Unit") |> Seq.toArray
                                    let parameters = if property.HasGetterMethod then arguments else arguments |> Array.take (max 0 (arguments.Length - 1))
                                    if parameters.Length > 0 then
                                        let format (typ: TypeReference) =
                                            match typ.FullName with
                                            | "System.Boolean" | "System.Byte" | "System.SByte" | "System.Char" | "System.Int16" | "System.UInt16"
                                            | "System.Int32" | "System.UInt32" | "System.Int64" | "System.UInt64" | "System.Single" | "System.Double"
                                            | "System.IntPtr" | "System.UIntPtr" | "System.Void" -> typ.Name
                                            | _ -> typ.FullName.Replace('/', '+')
                                        let result = if property.HasGetterMethod then declaringMemberType accessor.ReturnParameter.Type else moduleDefinition.TypeSystem.Void
                                        let signature = format result + " " + accessor.CompiledName + "(" +
                                                        (arguments |> Array.map(fun argument -> format (declaringMemberType argument.Type)) |> String.concat ", ") + ")"
                                        if property.HasSetterMethod && isNull indexedRead then indexedRead <- signature
                                        if property.HasGetterMethod && isNull indexedWrite && not(hasAttribute "System.Runtime.Serialization.IgnoreDataMemberAttribute" property.Attributes) then indexedWrite <- signature
                                        let valueType = if property.HasGetterMethod then result else declaringMemberType (Array.last arguments).Type
                                        let key = property.DisplayName + ":" + valueType.FullName + ":" + (parameters |> Array.map(fun argument -> (declaringMemberType argument.Type).FullName) |> String.concat ",")
                                        if names.Add key then
                                            facts.Add(Member(property.DisplayName,valueType.FullName,false,false,false,false))
                                            members.Add(GeneratedMember(valueType,null,null,null))
                                            referencePolicyFacts facts.[facts.Count - 1] property.Attributes
                                if property.IsProperty && property.Accessibility.IsPublic && property.IsInstanceMemberInCompiledCode
                                   && (property.CurriedParameterGroups |> Seq.collect id
                                       |> Seq.forall(fun argument -> argument.Type.StripAbbreviations().HasTypeDefinition && argument.Type.StripAbbreviations().TypeDefinition.TryFullName = Some "Microsoft.FSharp.Core.Unit"))
                                   && names.Add(property.DisplayName + ":" + (reference (property.FullType.Instantiate(declaringSubstitutions))).FullName) then
                                    let valueType = declaringMemberType property.FullType
                                    let effective = originalProperties |> Array.tryFind(fun declaration ->
                                        let memberValue = declaration.Member
                                        memberValue.Name = property.DisplayName && memberValue.Type.FullName = valueType.FullName &&
                                        (match memberValue.Getter,memberValue.Setter with
                                         | Some method,_ | _,Some method -> GeneratedJsonDefinitions.SameType(method.DeclaringType,declaringType)
                                         | _ -> false) && GeneratedRowFields.IndexParameters(memberValue).Length = 0)
                                    let getter = effective |> Option.bind(fun declaration -> declaration.Member.Getter) |> Option.toObj
                                    let setter = effective |> Option.bind(fun declaration -> declaration.Member.Setter) |> Option.toObj
                                    facts.Add(Member(property.DisplayName, valueType.FullName, false, getter <> null, setter <> null,
                                                     hasAttribute "System.Runtime.Serialization.IgnoreDataMemberAttribute" property.Attributes))
                                    members.Add(GeneratedMember(valueType, getter, setter, null))
                                    referencePolicyFacts facts.[facts.Count - 1] property.Attributes
                    completeReferenceFacts facts members
                    let mutable constructFromMembers = entity.IsFSharpRecord
                    let constructor =
                        if shape = TypeShape.BsonDocument then
                            let constructor = MethodReference(".ctor",moduleDefinition.TypeSystem.Void,actual,HasThis = true)
                            let json = TypeReference("SoloDatabase.JsonSerializator","JsonValue",moduleDefinition,actual.Scope)
                            constructor.Parameters.Add(ParameterDefinition(json))
                            constructor
                        elif shape = TypeShape.CustomDictionary || shape = TypeShape.NonGenericDictionary || shape = TypeShape.CustomSequence then
                            publicConstructors |> Array.tryFind(fun memberValue ->
                                let parameters = constructorParameters memberValue
                                if shape <> TypeShape.CustomSequence || populateCollection then parameters.Length = 0
                                else parameters.Length = 1 && (reference(parameters[0].Type.Instantiate(substitutions))).FullName = "System.Collections.Generic.IEnumerable`1<" + (reference collectionArguments[0]).FullName + ">")
                            |> Option.map(fun memberValue ->
                                let constructor = MethodReference(".ctor",moduleDefinition.TypeSystem.Void,actual,HasThis = true)
                                for parameter in constructorParameters memberValue do constructor.Parameters.Add(ParameterDefinition(signature parameter.Type))
                                constructor) |> Option.toObj
                        elif shape <> TypeShape.Object || entity.IsArrayType || entity.IsAbstractClass || entity.IsInterface then null
                        elif entity.IsFSharpRecord then
                            let constructor = MethodReference(".ctor", moduleDefinition.TypeSystem.Void, actual, HasThis = true)
                            for field in entity.FSharpFields do constructor.Parameters.Add(ParameterDefinition(signature field.FieldType))
                            constructor
                        else
                            let properties = Seq.zip facts members |> Seq.filter(fun (fact,_) -> not fact.Field) |> Seq.map(fun (_,memberValue) -> memberValue.Type.FullName) |> Seq.toArray
                            let parameters (memberValue: FSharpMemberOrFunctionOrValue) =
                                memberValue.CurriedParameterGroups |> Seq.collect id
                                |> Seq.filter(fun argument -> let value = argument.Type.StripAbbreviations() in not(value.HasTypeDefinition && value.TypeDefinition.TryFullName = Some "Microsoft.FSharp.Core.Unit"))
                                |> Seq.toArray
                            let constructors = publicConstructors
                            let selected =
                                constructors |> Array.tryFind(fun memberValue -> parameters memberValue |> Array.map(fun parameter -> reference(parameter.Type.Instantiate(substitutions)) |> _.FullName) = properties)
                            let selected =
                                match selected with
                                | Some memberValue -> constructFromMembers <- true; Some memberValue
                                | None -> constructors |> Array.tryFind(fun memberValue -> (parameters memberValue).Length = 0)
                            selected |> Option.map(fun memberValue ->
                                let constructor = MethodReference(".ctor",moduleDefinition.TypeSystem.Void,actual,HasThis = true)
                                for parameter in parameters memberValue do constructor.Parameters.Add(ParameterDefinition(signature parameter.Type))
                                constructor) |> Option.toObj
                    let sealedType = sealedEntity entity
                    let model = Model(actual.FullName, (if entity.IsArrayType then actual.FullName else fullName), shape,
                                      entity.IsValueType || entity.IsEnum, sealedType, constructor <> null && constructor.Parameters.Count = 0, null, facts)
                    model.IndexedRead <- indexedRead
                    model.IndexedWrite <- indexedWrite
                    model.PopulateCollection <- populateCollection
                    model.Formattable <- (contract "System.IFormattable").IsSome
                    model.DictionaryReadOnly <-
                        let selected = if decision.Contract = "" then typ.GenericArguments |> Seq.toArray else collectionArguments
                        selected.Length = 2 &&
                        (seq { yield typ; yield! typ.AllInterfaces }
                         |> Seq.map _.StripAbbreviations()
                         |> Seq.exists(fun readOnly ->
                             readOnly.HasTypeDefinition && readOnly.TypeDefinition.TryFullName = Some "System.Collections.Generic.IReadOnlyDictionary`2" &&
                             (let readonlyArguments = readOnly.GenericArguments |> Seq.toArray
                              readonlyArguments.Length = 2 &&
                              Array.forall2 (fun (left: FSharpType) (right: FSharpType) -> left.StripAbbreviations().Equals(right.StripAbbreviations())) selected readonlyArguments)))
                    model.NonGenericEnumerable <- shape = TypeShape.Object && (entity.IsArrayType || (contract "System.Collections.IEnumerable").IsSome)
                    model.AbstractType <- entity.IsInterface || entity.IsAbstractClass
                    model.RequiresTypeInformation <- model.AbstractType || CompilerPropertyFacts.polymorphic typ
                    model.ConstructFromMembers <- constructFromMembers
                    model.RecordType <- entity.IsFSharpRecord
                    model.CustomComparison <- Nullable(hasAttribute "Microsoft.FSharp.Core.CustomComparisonAttribute" entity.Attributes)
                    model.StandardComparison <- entity.IsFSharpRecord &&
                        not(hasAttribute "Microsoft.FSharp.Core.CustomComparisonAttribute" entity.Attributes) &&
                        not(hasAttribute "Microsoft.FSharp.Core.NoComparisonAttribute" entity.Attributes)
                    if entity.IsFSharpRecord then model.RecordFields <- entity.FSharpFields |> Seq.map _.Name |> Seq.toArray
                    model.IdentityTrivialGetter <-
                        if entity.IsFSharpRecord then
                            entity.FSharpFields |> Seq.exists(fun field ->
                                field.Name = "Id" && (field.IsMutable || mutableRecord) && field.FieldType.TypeDefinition.TryFullName = Some "System.Int64" &&
                                TypeSupport.TrivialIdentityReader(false,false,sealedType,true))
                        else
                            entity.MembersFunctionsAndValues |> Seq.exists(fun property ->
                                property.IsProperty && property.DisplayName = "Id" && property.HasGetterMethod && property.HasSetterMethod
                                && TypeSupport.TrivialIdentityReader(
                                    property.GetterMethod.IsDispatchSlot || property.GetterMethod.IsOverrideOrExplicitInterfaceImplementation,
                                    false,(property.DeclaringEntity |> Option.exists sealedEntity),directGetters.Contains(property.GetterMethod.FullName)))
                    if shape = TypeShape.Array then
                        model.SupportsValuesWrapper <- TypeSupport.SupportsValuesWrapper elementName
                    let generated = GeneratedModel(model, actual, constructor, members.ToArray())
                    generated.OriginalProperties <- originalProperties
                    let defaultConstructor =
                        if model.AbstractType then null else
                        match originalType with
                        | Some original -> GeneratedRowFields.DefaultConstructor(moduleDefinition,original)
                        | None ->
                            let declared = entity.MembersFunctionsAndValues |> Seq.exists(fun memberValue ->
                                memberValue.IsConstructor && (constructorParameters memberValue).Length = 0 &&
                                ((memberValue :> FSharpSymbol).DeclarationLocation |> Option.exists(fun declared ->
                                    constructorVisibility |> Seq.exists(fun (location,_) ->
                                        path location.FileName = path declared.FileName &&
                                        (location.StartLine,location.StartColumn) <= (declared.StartLine,declared.StartColumn) &&
                                        (declared.EndLine,declared.EndColumn) <= (location.EndLine,location.EndColumn)))))
                            if not entity.IsArrayType && (declared || (mutableRecord && not model.ValueType)) then
                                MethodReference(".ctor",moduleDefinition.TypeSystem.Void,actual,HasThis = true)
                            else null
                    if shape = TypeShape.Object then
                        generated.CloneConstructor <- defaultConstructor
                        if not(isNull defaultConstructor) then
                            match originalType with
                            | Some original -> access.Add(original.Resolve().Module.Assembly.Name.Name) |> ignore
                            | None -> access.Add(entity.Assembly.SimpleName) |> ignore
                    if shape = TypeShape.Object && TypeSupport.SqlRowFieldFallback(entity.IsValueType,entity.IsFSharpRecord,(generated.OriginalProperties |> Array.sumBy(fun property -> if property.Member.Setter.IsSome then 1 else 0))) then
                        if not(String.IsNullOrEmpty(entity.Assembly.QualifiedName)) then
                            generated.SqlFields <- GeneratedRowFields.Read(moduleDefinition,originalType.Value)
                            for field in generated.SqlFields do enqueueImported field.Type
                        else
                            generated.SqlFields <- entity.FSharpFields |> Seq.filter(fun field ->
                                not field.IsStatic && not field.Accessibility.IsPublic && not field.Accessibility.IsPrivate)
                                |> Seq.map(fun field ->
                                    let valueType = memberType field.FieldType
                                    let fieldReference = FieldReference(field.Name,signature field.FieldType,actual)
                                    { Name = field.Name; Type = valueType; Field = Some fieldReference
                                      Getter = None; Setter = None; Static = false; Readable = true
                                      Writable = field.IsMutable; Ignored = false; Many = false }) |> Seq.toArray
                    if model.ValueType then
                        generated.DefaultConstructor <- defaultConstructor
                    generated.CollectionArguments <- collectionArguments |> Array.map reference
                    if entity.IsArrayType then generated.SequenceElement <- reference typ.GenericArguments[0]
                    else
                        seq { yield typ; yield! typ.AllInterfaces }
                        |> Seq.map _.StripAbbreviations()
                        |> Seq.tryFind(fun candidate -> candidate.HasTypeDefinition && candidate.TypeDefinition.TryFullName = Some "System.Collections.Generic.IEnumerable`1")
                        |> Option.iter(fun contract -> generated.SequenceElement <- reference contract.GenericArguments[0])
                    for assembly in access do generated.AccessAssemblies.Add(assembly) |> ignore
                    if shape = TypeShape.Enum then
                        let width =
                            match entity.FSharpFields |> Seq.tryPick(fun field -> field.LiteralValue) with
                            | Some (:? sbyte) -> moduleDefinition.TypeSystem.SByte
                            | Some (:? byte) -> moduleDefinition.TypeSystem.Byte
                            | Some (:? int16) -> moduleDefinition.TypeSystem.Int16
                            | Some (:? uint16) -> moduleDefinition.TypeSystem.UInt16
                            | Some (:? int32) -> moduleDefinition.TypeSystem.Int32
                            | Some (:? uint32) -> moduleDefinition.TypeSystem.UInt32
                            | Some (:? int64) -> moduleDefinition.TypeSystem.Int64
                            | Some (:? uint64) -> moduleDefinition.TypeSystem.UInt64
                            | _ ->
                                match entity.Assembly.FileName with
                                | Some path ->
                                    // Only imported compiler inputs are read here; current-project models never need a prior output.
                                    let input = image path
                                    if input.Name.FullName <> entity.Assembly.QualifiedName then
                                        invalidOp ("The enum input does not match its compiler identity: " + fullName)
                                    let definition = input.MainModule.GetType(actual.FullName)
                                    if isNull definition || not definition.IsEnum then invalidOp ("The enum input has no original definition: " + fullName)
                                    let backing = definition.Fields |> Seq.find(fun field -> field.Name = "value__")
                                    match backing.FieldType.MetadataType with
                                    | MetadataType.SByte -> moduleDefinition.TypeSystem.SByte
                                    | MetadataType.Byte -> moduleDefinition.TypeSystem.Byte
                                    | MetadataType.Int16 -> moduleDefinition.TypeSystem.Int16
                                    | MetadataType.UInt16 -> moduleDefinition.TypeSystem.UInt16
                                    | MetadataType.Int32 -> moduleDefinition.TypeSystem.Int32
                                    | MetadataType.UInt32 -> moduleDefinition.TypeSystem.UInt32
                                    | MetadataType.Int64 -> moduleDefinition.TypeSystem.Int64
                                    | MetadataType.UInt64 -> moduleDefinition.TypeSystem.UInt64
                                    | _ -> invalidOp ("The enum input has an invalid underlying type: " + fullName)
                                | None -> invalidOp ("The compiler did not supply an enum width: " + fullName)
                        generated.ElementType <- width
                    if shape = TypeShape.Object && entity.IsFSharpUnion then
                        for unionModel in UnionDiscovery.collect moduleDefinition typ actual memberType signature generated.OriginalProperties declaration (CompilerPropertyFacts.signatureIdentity metadataResolver reference typ) do models.Add(unionModel)
                    else
                        models.Add(generated)
                        sources.Add(generated,typ)
        let variances = Dictionary<FSharpEntity,int[]>()
        let variance (typ: FSharpType) =
            let entity = typ.TypeDefinition
            match variances.TryGetValue entity with
            | true, values -> values
            | _ ->
                let values =
                    if entity.IsFSharp || not(entity.IsInterface || entity.IsDelegate) then Array.zeroCreate entity.GenericParameters.Count
                    else
                        match entity.Assembly.FileName with
                        | Some path ->
                            let input = image path
                            if input.Name.FullName <> entity.Assembly.QualifiedName then invalidOp "Generic variance input differs from the compiler identity."
                            let actual = reference typ
                            let definition = match actual with :? GenericInstanceType as instance -> instance.ElementType | _ -> actual
                            let original = SourceTypeIdentity.Definition(input,definition.FullName)
                            if original.GenericParameters.Count <> entity.GenericParameters.Count then
                                invalidOp ("The original generic variance arity differs from its compiler identity: " + definition.FullName)
                            original.GenericParameters |> Seq.map(fun parameter ->
                                if parameter.IsCovariant then 1 elif parameter.IsContravariant then -1 else 0) |> Seq.toArray
                        | None -> invalidOp ("The compiler generic variance input has no original metadata path: " + entity.BasicQualifiedName)
                variances.Add(entity,values)
                values
        let parentTypes = Dictionary<FSharpType,FSharpType[]>()
        let parents (typ: FSharpType) =
            match parentTypes.TryGetValue typ with
            | true, values -> values
            | _ ->
                let values =
                    if typ.HasTypeDefinition || typ.IsAnonRecordType then
                        Array.append (typ.BaseType |> Option.toArray) (typ.AllInterfaces |> Seq.toArray)
                    else [||]
                parentTypes.Add(typ,values)
                values
        let assignment: CodecTypeAssignment<FSharpType> =
            { Equal = fun (a: FSharpType) b -> a.Equals b
              Parents = parents
              Arguments = fun typ -> typ.GenericArguments |> Seq.toArray
              Definition = fun typ -> if typ.HasTypeDefinition then typ.TypeDefinition.AsType() else typ
              Variance = variance
              ValueType = CompilerReferences.isValueType
              ArrayRank = fun typ -> if typ.HasTypeDefinition && typ.TypeDefinition.IsArrayType then typ.TypeDefinition.ArrayRank else 0
              Object = fun typ -> typ.HasTypeDefinition && typ.TypeDefinition.TryFullName = Some "System.Object" }
        let assignments = Dictionary<struct(FSharpType * FSharpType),bool>()
        let rec accepts source target =
            let key = struct(source,target)
            match assignments.TryGetValue key with
            | true, value -> value
            | _ ->
                let value = CodecTypeAssignment.evaluate accepts assignment source target
                assignments.Add(key,value)
                value
        for model,source in sources do
            let assignable = ResizeArray<TypeReference>()
            let unknownInterfaces = ResizeArray<TypeReference>()
            for target,destination in sources do
                if not target.Facts.ValueType && not target.Facts.SealedType && target.Type.FullName <> "System.Object" then
                    if accepts source destination then
                        assignable.Add(target.Type)
                    elif source.IsAnonRecordType && destination.HasTypeDefinition && destination.TypeDefinition.IsInterface then
                        unknownInterfaces.Add(target.Type)
            model.AssignableTo <- assignable.ToArray()
            model.UnknownInterfaces <- unknownInterfaces.ToArray()
        models.ToArray()
