namespace SoloDatabase

open System
open System.Globalization
open SoloDatabase.JsonSerializator
open SoloDatabase.Metadata.Generation
open SoloDatabase.Metadata.CodecGeneration

/// Immutable collection descriptions share the existing per-type fact owner.
[<AbstractClass; Sealed>]
type internal TypeDescriptions private () =
    static let gate = obj()
    static member RegisterFacts<'T>
        (shape: int, abstractType: bool, requiresTypeInformation: bool, record: bool, nonGenericSequence: bool, arguments: Type[],
         names: string[], types: Type[], many: bool[], templates: ModelTypeTemplate[], caseNames: string[], tags: int[],
         caseFields: string[][], caseTypes: Type[][], caseTemplates: ModelTypeTemplate[][], unionRoot: Type, enumNames: string[], enumValues: string[],
         underlying: Type, flags: bool, physicalId: bool, customId: string, alternatives: Type[]) =
        let target = typeof<'T>
        let definition = if target.IsGenericType then target.GetGenericTypeDefinition().FullName elif target.IsArray then "System.Array" else target.FullName
        let decision = TypeSupport.ForDefinition (enum<TypeShape> shape) definition
        let arguments =
            if DBRefTypeHelpers.isFileRefType target || DBRefTypeHelpers.isFileRefManyType target then
                [|DBRefTypeHelpers.referenceTargetType target|]
            else arguments
        let facts = {
            Definition = definition; Scope = CodecDescription.assemblyScope target; FullName = target.FullName; Identity = target.AssemblyQualifiedName
            GenericDefinition = if target.IsGenericType && decision.Shape = TypeShape.Object then Some(target.GetGenericTypeDefinition()) else None
            GenericArguments = if target.IsGenericType then target.GetGenericArguments() else [||]
            Templates = Array.map2(fun name template -> struct(name,template)) names templates
            CaseTemplates = if isNull unionRoot then caseTemplates else [||]
            UnionRoot = Option.ofObj unionRoot
            Abstract = abstractType; Sealed = target.IsSealed || target.IsValueType; RequiresTypeInformation = requiresTypeInformation; NonGenericSequence = nonGenericSequence
            Record = record; Decision = decision; Arguments = arguments
            Members = Array.init names.Length (fun index -> struct(names[index],types[index],many[index]))
            Cases = if not(isNull unionRoot) then [||] else Array.init caseNames.Length (fun index -> struct(caseNames[index],tags[index],Array.map2(fun name child -> struct(name,child)) caseFields[index] caseTypes[index]))
            Enum = Array.map2(fun name value -> struct(name,value)) enumNames enumValues
            EnumUnderlying = Option.ofObj underlying; Flags = flags; PhysicalId = physicalId; CustomId = Option.ofObj customId
            Alternatives = alternatives
            ByteSequence = typeof<Collections.Generic.IEnumerable<byte>>.IsAssignableFrom target }
        lock gate (fun () ->
            TypeMetadataCache.Register(typeof<'T>,fun metadata ->
                let alternatives =
                    match MetadataValue.tryValue metadata.TypeDescriptionFacts with
                    | ValueSome previous -> Array.append previous.Alternatives facts.Alternatives |> Array.distinct
                    | ValueNone -> facts.Alternatives
                { metadata with TypeDescriptionFacts = MetadataValue.generated { facts with Alternatives = alternatives }; TypeDescription = ValueNone }))

    static member private Facts(known: Type[], target: Type): CodecDescriptionFacts<Type> =
        let dynamic = MetadataRuntime.supportsDynamicCode()
        let metadata = TypeMetadataCache.Get target
        match MetadataValue.tryValue metadata.TypeDescriptionFacts with
        | ValueSome facts ->
            if not(target.IsSealed || target.IsValueType) && facts.Decision.Shape <> TypeShape.DynamicObject then
                let alternatives =
                    Array.append facts.Alternatives (known |> Array.filter(fun candidate -> candidate <> target && target.IsAssignableFrom candidate))
                    |> Array.distinct
                    |> Array.sortWith(fun a b -> String.CompareOrdinal(a.FullName,b.FullName))
                { facts with Alternatives = alternatives }
            else facts
        | ValueNone ->
            let definition = if target.IsGenericType then target.GetGenericTypeDefinition().FullName elif target.IsArray then "System.Array" else target.FullName
            let shape =
                if dynamic then JsonValue.SerializedValueFacts target else
                let original = TypeSupport.RuntimeFacts target
                let intrinsic = TypeSupport.Select { original with Enum = false; Dictionary = false; ReadOnlyDictionary = false
                                                                   NonGenericDictionary = false; Enumerable = false; Grouping = false }
                if intrinsic.Shape = TypeShape.Object then JsonValue.SerializedValueFacts target
                else (JsonCodecShape.ForValueType target).Shape
            let decision =
                if dynamic then TypeSupport.Runtime target
                else
                    TypeSupport.Select {
                        Definition = definition; ArrayRank = if target.IsArray then target.GetArrayRank() else 0
                        ElementDefinition = if target.IsArray then target.GetElementType().FullName else ""
                        Enum = target.IsEnum
                        Tuple = match shape with JsonSerializationShape.ArrayMembers _ | JsonSerializationShape.DeclaredArrayMembers _ -> true | _ -> false
                        Dictionary = match shape with JsonSerializationShape.DictionaryValues _ -> true | _ -> false
                        ReadOnlyDictionary = false; NonGenericDictionary = false
                        Enumerable = match shape with JsonSerializationShape.SequenceValues _ -> true | _ -> false
                        Grouping = match shape with JsonSerializationShape.GroupingValues _ -> true | _ -> false }
            let nonGenericSequence = dynamic && decision.Shape = TypeShape.Object && typeof<Collections.IEnumerable>.IsAssignableFrom target
            let requiresTypeInformation =
                match MetadataValue.tryValue metadata.TypeFacts with
                | ValueSome facts -> facts.RequiresTypeInformation
                | _ when dynamic -> TypeMetadata.requiresTypeInformation target
                | _ -> target.IsAbstract
            let model =
                if dynamic && decision.Shape = TypeShape.Object && not nonGenericSequence then Some(JsonModelDiscovery.Model target)
                else None
            let arguments =
                match shape with
                | _ when DBRefTypeHelpers.isFileRefType target || DBRefTypeHelpers.isFileRefManyType target -> [|DBRefTypeHelpers.referenceTargetType target|]
                | JsonSerializationShape.ArrayMembers fields -> fields |> Array.map _.ValueType
                | JsonSerializationShape.DeclaredArrayMembers fields -> fields
                | _ when dynamic && decision.Contract <> "" ->
                    let original = if target.IsGenericType then target.GetGenericTypeDefinition() else target
                    let contract =
                        if original.FullName = decision.Contract then target
                        else target.GetInterface(decision.Contract.Split('.') |> Array.last)
                    TypeMetadata.GenericTypeArgCache.Get contract
                | _ when target.IsArray -> [|target.GetElementType()|]
                | _ ->
                    match MetadataValue.tryValue metadata.GenericArguments with
                    | ValueSome arguments -> arguments
                    | ValueNone when dynamic -> TypeMetadata.GenericTypeArgCache.Get target
                    | _ -> [||]
            let members =
                match model with
                | Some model ->
                    model.Members |> Array.choose(fun memberValue ->
                        if memberValue.Many && memberValue.Readable && not memberValue.Ignored then Some(struct(memberValue.Name,memberValue.Type,true)) else None)
                    |> Array.append (CodecEmitter.writtenMembers model |> Array.map(fun index ->
                        let memberValue = model.Members[index]
                        struct(memberValue.Name,memberValue.Type,false)))
                | None ->
                    match shape with
                    | JsonSerializationShape.ObjectMembers members -> members |> Array.map(fun memberValue -> struct(memberValue.StoredName,memberValue.ValueType,false))
                    | JsonSerializationShape.DeclaredMembers members -> members |> Array.map(fun struct(name,child) -> struct(name,child,false))
                    | _ -> [||]
            let cases =
                match model with
                | Some model -> model.Cases |> Array.map(fun case -> struct(case.Name,case.Tag,case.Fields |> Array.map(fun (name,child) -> struct(name,child))))
                | None ->
                    match MetadataValue.tryValue metadata.UnionStorage with
                    | ValueSome struct(_,tags,names,types) ->
                        tags |> Array.mapi(fun index tag -> struct("",tag,Array.map2(fun name child -> struct(name,child)) names[index] types[index]))
                    | _ -> [||]
            let unionRoot =
                if dynamic && (model |> Option.exists _.Union) then
                    let original = Microsoft.FSharp.Reflection.FSharpType.GetUnionCases target |> Array.tryHead
                    original |> Option.bind(fun case -> if case.DeclaringType = target then None else Some case.DeclaringType)
                else None
            let cases = if unionRoot.IsSome then [||] else cases
            let templates =
                match model with
                | Some model when target.IsGenericType ->
                    let selected =
                        Array.append
                            (CodecEmitter.writtenMembers model |> Array.map(fun index -> model.Members[index]))
                            (model.Members |> Array.filter(fun memberValue -> memberValue.Many && memberValue.Readable && not memberValue.Ignored))
                    let original = JsonModelDiscovery.Model(target.GetGenericTypeDefinition())
                    let signatures = Collections.Generic.Dictionary<struct(Reflection.Module * int),Type>()
                    let identity (memberValue: CodecMember<Type,Reflection.MethodBase,Reflection.FieldInfo>) =
                        match memberValue.Field,memberValue.Getter with
                        | Some field,_ -> Some(struct(field.Module,field.MetadataToken))
                        | _,Some getter -> Some(struct(getter.Module,getter.MetadataToken))
                        | _ -> None
                    for memberValue in original.Members do
                        match identity memberValue with
                        | Some key -> signatures[key] <- memberValue.Type
                        | None -> ()
                    selected |> Array.map(fun memberValue ->
                        let template = identity memberValue |> Option.map(fun key -> ModelMetadata.TypeTemplate signatures[key]) |> Option.defaultValue UnavailableType
                        struct(memberValue.Name,template))
                | _ -> [||]
            let caseTemplates =
                if dynamic && target.IsGenericType && cases.Length > 0 then
                    let originals = Microsoft.FSharp.Reflection.FSharpType.GetUnionCases(target.GetGenericTypeDefinition())
                    cases |> Array.map(fun struct(_,tag,_) ->
                        (originals |> Array.find(fun case -> case.Tag = tag)).GetFields() |> Array.map(fun field -> ModelMetadata.TypeTemplate field.PropertyType))
                else [||]
            let enumValues =
                if target.IsEnum && dynamic then
                    target.GetFields(System.Reflection.BindingFlags.Public ||| System.Reflection.BindingFlags.Static)
                    |> Array.map(fun field -> struct(field.Name,Convert.ToString(field.GetRawConstantValue(),CultureInfo.InvariantCulture)))
                else [||]
            let enumUnderlying =
                if target.IsEnum then
                    match MetadataValue.tryValue metadata.EnumUnderlying with
                    | ValueSome underlying -> Some underlying
                    | ValueNone when dynamic -> Some(TypeMetadata.enumUnderlying target)
                    | _ -> None
                else None
            let physical =
                match MetadataValue.tryValue metadata.PhysicalIdentity with
                | ValueSome facts -> facts.HasWritableId
                | ValueNone ->
                    model |> Option.exists(fun model -> model.Members |> Array.exists(fun memberValue ->
                        memberValue.Field.IsNone && memberValue.Name = "Id" && memberValue.Type = typeof<int64> && memberValue.Readable && memberValue.Writable))
            let customId =
                match MetadataValue.tryValue metadata.CustomIdentity with
                | ValueSome members -> members |> Array.tryHead |> Option.map _.Name
                | _ when dynamic && decision.Shape = TypeShape.Object -> CustomIdMetadata.GetMembers target |> Array.tryHead |> Option.map _.Name
                | _ -> None
            { Definition = definition; Scope = CodecDescription.assemblyScope target; FullName = target.FullName; Identity = target.AssemblyQualifiedName
              GenericDefinition = if target.IsGenericType && decision.Shape = TypeShape.Object then Some(target.GetGenericTypeDefinition()) else None
              GenericArguments = if target.IsGenericType then target.GetGenericArguments() else [||]
              Templates = templates; CaseTemplates = caseTemplates; UnionRoot = unionRoot
              RequiresTypeInformation = requiresTypeInformation; NonGenericSequence = nonGenericSequence
              Abstract = target.IsAbstract; Sealed = target.IsSealed || target.IsValueType; Record = if dynamic then Microsoft.FSharp.Reflection.FSharpType.IsRecord target else match MetadataValue.tryValue metadata.RecordFields with ValueSome(ValueSome _) -> true | _ -> false
              Decision = decision; Arguments = arguments; Members = members; Cases = cases
              Enum = enumValues; EnumUnderlying = enumUnderlying; Flags = dynamic && target.IsEnum && target.IsDefined(typeof<FlagsAttribute>,false)
              PhysicalId = physical; CustomId = customId
              Alternatives = if target.IsSealed || target.IsValueType || decision.Shape = TypeShape.DynamicObject then [||] else known |> Array.filter(fun candidate -> candidate <> target && target.IsAssignableFrom candidate)
              ByteSequence = typeof<Collections.Generic.IEnumerable<byte>>.IsAssignableFrom target }

    static member Get(target: Type) =
        match MetadataValue.tryValue (TypeMetadataCache.Get(target).TypeDescription) with
        | ValueSome description -> description
        | ValueNone ->
            lock gate (fun () ->
                TypeMetadataCache.Register(target,fun metadata -> { metadata with DescriptionKnown = true })
                let rec acquire() =
                    match MetadataValue.tryValue (TypeMetadataCache.Get(target).TypeDescription) with
                    | ValueSome description -> description
                    | ValueNone ->
                        let known = TypeMetadataCache.DescribedTypes() |> Array.sortWith(fun a b -> String.CompareOrdinal(a.FullName,b.FullName))
                        let facts = Collections.Generic.Dictionary<Type,CodecDescriptionFacts<Type>>()
                        let bases = Collections.Generic.HashSet<Type>()
                        let acquireFacts = Func<Type,CodecDescriptionFacts<Type>>(fun child ->
                            match facts.TryGetValue child with
                            | true,value -> value
                            | _ ->
                                let value = TypeDescriptions.Facts(known,child)
                                facts.Add(child,value)
                                if not(child.IsSealed || child.IsValueType) && value.Decision.Shape <> TypeShape.DynamicObject then bases.Add child |> ignore
                                value)
                        let description = CodecDescription.describe acquireFacts target
                        if TypeMetadataCache.StoreDescription(target,known,facts.Keys |> Seq.toArray,bases |> Seq.toArray,description) then description
                        else acquire()
                acquire())
