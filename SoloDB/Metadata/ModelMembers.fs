namespace SoloDatabase

open System
open System.Collections.Generic
open System.Reflection
open System.Reflection.Emit
open SoloDatabase.Metadata.CodecGeneration
open SoloDatabase.Metadata.Generation

/// Declared shape facts shared by translation and generated registration.
[<AbstractClass; Sealed>]
type internal ModelMetadata private () =
    static member TemplateUnavailable() = UnavailableType
    static member TemplateParameter(position: int) = FormalType position
    static member TemplateNamed(owner: Type, arguments: ModelTypeTemplate[]) = NamedType(owner,arguments)
    static member TemplateElement(rank: int, element: ModelTypeTemplate) = ElementType(rank,element)

    static member TypeTemplate(target: Type) =
        let rec original (value: Type) =
            if value.IsGenericParameter then FormalType value.GenericParameterPosition
            elif value.HasElementType then
                ElementType((if value.IsArray then value.GetArrayRank() else 0),original(value.GetElementType()))
            elif value.IsGenericType then
                NamedType(value.GetGenericTypeDefinition(),value.GetGenericArguments() |> Array.map original)
            else NamedType(value,[||])
        original target

    static member private PublishMembers(owner: Type, names: string[], types: Type[], fields: bool[], templates: ModelTypeTemplate[] option) =
        if names.Length <> types.Length || names.Length <> fields.Length then invalidArg "types" "Member facts have inconsistent lengths."
        let facts = Array.init names.Length (fun index -> struct(names[index],types[index],fields[index]))
        let originals = templates |> Option.map(fun values ->
            if facts.Length <> values.Length then invalidArg "templates" "Original member signatures require the same declared member facts."
            let result = Dictionary<int,MetadataValue<ModelTypeTemplate>>(values.Length)
            for index = 0 to values.Length - 1 do result.Add(index,{ Generated = true; Value = values[index] })
            result)
        TypeMetadataCache.Register(owner,fun metadata ->
            { metadata with MemberTypes = MetadataValue.generated facts; MemberTemplates = originals })
        if originals.IsSome then
            let definition = if owner.IsGenericType then owner.GetGenericTypeDefinition() else owner
            TypeMetadataCache.Register(definition,fun metadata ->
                if metadata.DefinitionRepresentative.IsSome then metadata
                else { metadata with DefinitionRepresentative = ValueSome owner })

    static member RegisterMembers(owner: Type, names: string[], types: Type[], fields: bool[]) =
        ModelMetadata.PublishMembers(owner,names,types,fields,None)

    /// Publishes original signatures and their selected member inventory together.
    static member RegisterOriginalMembers(owner: Type, names: string[], types: Type[], fields: bool[], templates: ModelTypeTemplate[]) =
        ModelMetadata.PublishMembers(owner,names,types,fields,Some templates)

    static member DefinitionOwner(definition: Type) =
        match TypeMetadataCache.Read(definition,fun metadata -> metadata.DefinitionRepresentative) with
        | ValueSome owner -> ValueSome owner
        | ValueNone when MetadataRuntime.supportsDynamicCode() -> ValueSome definition
        | _ -> ValueNone

    static member RegisterSequence(owner: Type, element: Type) =
        TypeMetadataCache.Register(owner,fun metadata -> { metadata with SequenceElement = MetadataValue.generated element })

    static member RegisterComparer<'T>() =
        TypeMetadataCache.Register(typeof<'T>,fun metadata -> { metadata with EqualityComparer = MetadataValue.generated (box EqualityComparer<'T>.Default) })

    static member private Members(owner: Type) =
        TypeMetadataCache.Discover(owner,(fun metadata -> metadata.MemberTypes),
            (fun value metadata -> { metadata with MemberTypes = ValueSome value }),Func<struct(string * Type * bool)[]>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "declared members" owner
                [| for property in owner.GetProperties(BindingFlags.Public ||| BindingFlags.Instance) do
                       yield struct(property.Name,property.PropertyType,false)
                   for memberField in owner.GetFields(BindingFlags.Public ||| BindingFlags.Instance) do
                       yield struct(memberField.Name,memberField.FieldType,true) |]))

    static member MemberType(owner: Type, name: string, field: bool) =
        let members = ModelMetadata.Members owner
        members |> Array.tryPick(fun struct(memberName,valueType,isField) -> if memberName = name && isField = field then Some valueType else None)

    static member MemberTemplate(owner: Type, name: string, field: bool) =
        ModelMetadata.Members owner |> ignore
        let selected = TypeMetadataCache.Read(owner,fun metadata ->
            match MetadataValue.tryValue metadata.MemberTypes with
            | ValueNone -> ValueNone
            | ValueSome members ->
                match members |> Array.tryFindIndex(fun struct(memberName,_,isField) -> memberName = name && isField = field) with
                | None -> ValueNone
                | Some index -> ValueSome(members,index,MetadataMap.tryFind index metadata.MemberTemplates |> ValueOption.map _.Value))
        match selected with
        | ValueNone -> ValueNone
        | ValueSome(members,index,known) ->
            let sameInventory metadata =
                match MetadataValue.tryValue metadata.MemberTypes with
                | ValueSome current -> Object.ReferenceEquals(current,members)
                | _ -> false
            match known with
            | ValueSome UnavailableType -> ValueNone
            | ValueSome template -> ValueSome template
            | ValueNone when not(MetadataRuntime.supportsDynamicCode()) -> ValueNone
            | ValueNone ->
                let template = TypeMetadataCache.Discover(owner,
                    (fun metadata -> if sameInventory metadata then MetadataMap.tryFind index metadata.MemberTemplates else ValueNone),
                    (fun value metadata -> if sameInventory metadata then { metadata with MemberTemplates = MetadataMap.set index value metadata.MemberTemplates } else metadata),Func<ModelTypeTemplate>(fun () ->
                        let definition = if owner.IsGenericType then owner.GetGenericTypeDefinition() else owner
                        let target =
                            if field then definition.GetField(name,BindingFlags.Public ||| BindingFlags.Instance).FieldType
                            else definition.GetProperty(name,BindingFlags.Public ||| BindingFlags.Instance).PropertyType
                        ModelMetadata.TypeTemplate target))
                ValueSome template

    static member internal RegisterStringIndexer(owner: Type, valueType: Type, read: Func<obj,string,obj>, ambiguous: bool, missingGetter: bool, restrictedMethod: string) =
        let operation = { Value = if isNull read then ValueNone else ValueSome(struct(valueType,read))
                          Ambiguous = ambiguous; MissingGetter = missingGetter; RestrictedMethod = restrictedMethod }
        TypeMetadataCache.Register(owner,fun metadata -> { metadata with StringIndexer = MetadataValue.generated operation })

    static member StringIndexer(owner: Type) =
        let operation = TypeMetadataCache.Discover(owner,(fun metadata -> metadata.StringIndexer),
            (fun operation metadata -> { metadata with StringIndexer = ValueSome operation }),Func<StringIndexerOperations>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "string indexer" owner
                let properties = owner.GetProperties()
                let index = TypeSupport.NamedProperty("Item",false,properties,Func<PropertyInfo,string>(fun property -> property.Name),
                    Func<PropertyInfo,bool>(fun property -> (if property.CanRead then property.GetGetMethod(true) else property.GetSetMethod(true)).IsStatic))
                let empty = { Value = ValueNone; Ambiguous = index = -2; MissingGetter = false; RestrictedMethod = null }
                if index < 0 then empty else
                let property = properties[index]
                if not(TypeSupport.StringIndexer(property.GetIndexParameters() |> Array.map(fun parameter -> parameter.ParameterType.FullName))) then empty else
                if not property.CanRead then { empty with MissingGetter = true } else
                let call = QueryMethodDiscovery.create (property.GetGetMethod(true))
                { empty with Value = ValueSome(struct(property.PropertyType,Func<obj,string,obj>(fun value key -> call.Invoke(value,[|box key|])))) }))
        TypeSupport.RequireNamedProperty operation.Ambiguous
        if operation.MissingGetter then raise(NullReferenceException())
        TypeSupport.RequireQueryCall(operation.RestrictedMethod,not(isNull operation.RestrictedMethod))
        operation.Value

    /// Binds a declared member without constructing reflective expression syntax.
    static member NamedMember(owner: Type, name: string) =
        let direct = TypeMetadataCache.Read(owner,fun metadata ->
            let mutable property = ValueNone
            let mutable fieldValue = ValueNone
            match metadata.QueryMembers with
            | Some operations ->
                for KeyValue(struct(memberName,field,valueType),read) in operations do
                    if memberName = name then
                        if field then
                            if fieldValue.IsSome then raise(AmbiguousMatchException())
                            fieldValue <- ValueSome(struct(valueType,read.Value))
                        else
                            if property.IsSome then raise(AmbiguousMatchException())
                            property <- ValueSome(struct(valueType,read.Value))
            | None -> ()
            if property.IsSome then property else fieldValue)
        match direct with
        | ValueSome operation -> Some operation
        | ValueNone ->
        let selected =
            match ModelMetadata.MemberType(owner,name,false) with
            | Some valueType -> Some(false,valueType)
            | None -> ModelMetadata.MemberType(owner,name,true) |> Option.map(fun valueType -> true,valueType)
        selected |> Option.map(fun (field,valueType) ->
            let read = TypeMetadataCache.Discover(owner,
                (fun metadata -> MetadataMap.tryFind (struct(name,field,valueType)) metadata.QueryMembers),
                (fun operation metadata -> { metadata with QueryMembers = MetadataMap.set (struct(name,field,valueType)) operation metadata.QueryMembers }),
                Func<Func<obj,obj>>(fun () ->
                    let model = JsonModelDiscovery.Model owner
                    let members = model.Members |> Array.filter(fun memberValue -> memberValue.Name = name && memberValue.Field.IsSome = field)
                    if members.Length > 1 then raise(AmbiguousMatchException())
                    let memberValue = members |> Array.exactlyOne
                    let method = DynamicMethod("ReadModelMember",typeof<obj>,[|typeof<obj>|],owner.Module,true)
                    CodecEmitter.boxedMemberGetter (RuntimeCodecIL(method.GetILGenerator())) owner owner.IsValueType memberValue valueType.IsValueType
                    method.CreateDelegate(typeof<Func<obj,obj>>) :?> Func<obj,obj>))
            struct(valueType,read))

    static member private Comparer<'T>() = box EqualityComparer<'T>.Default
    static member DefaultComparer(owner: Type) =
        TypeMetadataCache.Discover(owner,(fun metadata -> metadata.EqualityComparer),
            (fun value metadata -> { metadata with EqualityComparer = ValueSome value }),Func<obj>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "default comparer" owner
                typeof<ModelMetadata>.GetMethod("Comparer",BindingFlags.NonPublic ||| BindingFlags.Static).MakeGenericMethod(owner).Invoke(null,null)))

    static member SequenceElement(owner: Type) =
        if owner.IsArray then owner.GetElementType() else
        TypeMetadataCache.Discover(owner,(fun metadata -> metadata.SequenceElement),
            (fun value metadata -> { metadata with SequenceElement = ValueSome value }),Func<Type>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "sequence element" owner
                let enumerable (candidate: Type) = candidate.IsGenericType && typedefof<IEnumerable<_>>.Equals(candidate.GetGenericTypeDefinition())
                if enumerable owner then owner.GetGenericArguments()[0]
                else match owner.GetInterfaces() |> Array.tryFind enumerable with
                     | Some contract -> contract.GetGenericArguments()[0]
                     | None -> raise(NotSupportedException("Could not resolve sequence element type."))))

    static member PropertyReader(owner: Type, name: string) =
        let selected = TypeMetadataCache.Read(owner,fun metadata ->
            let mutable selected = ValueNone
            match metadata.QueryMembers with
            | None -> ()
            | Some readers ->
                for KeyValue(struct(memberName,field,_),operation) in readers do
                    if memberName = name && not field then
                        if selected.IsSome then raise(AmbiguousMatchException())
                        selected <- ValueSome operation
            selected)
        match selected with
        | ValueSome operation -> ValueSome operation.Value
        | ValueNone when TypeMetadataCache.Read(owner,fun metadata -> metadata.MemberTypes)
                         |> ValueOption.exists(fun facts -> facts.Generated) -> ValueNone
        | ValueNone ->
            TypeMetadataCache.Discover(owner,(fun metadata -> MetadataMap.tryFind name metadata.NamedGetters),
                (fun operation metadata -> { metadata with NamedGetters = MetadataMap.set name operation metadata.NamedGetters }),
                Func<Func<obj,obj> voption>(fun () ->
                    if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "property value" owner
                    let property = owner.GetProperty(name,BindingFlags.Public ||| BindingFlags.Instance)
                    if isNull property || not property.CanRead then ValueNone
                    elif property.GetIndexParameters().Length <> 0 then
                        raise(TargetParameterCountException())
                    else ValueSome(QueryValueMetadata.Reader(property,owner))))

    static member ReadProperty(read: Func<obj,obj>, value: obj) =
        try read.Invoke(value)
        with error -> raise(TargetInvocationException(error))

    static member ReadProperty(property: PropertyInfo, value: obj) =
        let getter = property.GetGetMethod(true)
        if isNull getter then invalidArg "property" "Property Get method was not found."
        if property.GetIndexParameters().Length <> 0 then raise(TargetParameterCountException())
        if not getter.IsStatic then
            if isNull value then raise(TargetException("Non-static method requires a target."))
            if not(property.DeclaringType.IsInstanceOfType(value)) then raise(TargetException("Object does not match target type."))
        ModelMetadata.ReadProperty(QueryValueMetadata.Reader(property,property.DeclaringType),value)
