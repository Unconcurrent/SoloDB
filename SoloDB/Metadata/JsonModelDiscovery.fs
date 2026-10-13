namespace SoloDatabase

open System
open System.Reflection
open System.Reflection.Emit
open System.Runtime.Serialization
open Microsoft.FSharp.Reflection
open SoloDatabase.Metadata.CodecGeneration

/// Ordinary serialized-model facts and factories shared by existing metadata consumers.
[<AbstractClass; Sealed>]
type internal JsonModelDiscovery private () =
    static member private Discover(target: Type): CodecModel<Type,MethodBase,FieldInfo> =
        let fields = target.GetFields()
        let allProperties = target.GetProperties()
        let properties = allProperties
        let indexed predicate =
            allProperties |> Array.tryFind(fun property -> property.GetIndexParameters().Length > 0 && predicate property)
            |> Option.map(fun property ->
                let getter = property.GetGetMethod(true)
                (if isNull getter then property.GetSetMethod(true) else getter).ToString())
        let members = [|
            for field in fields do
                yield { Name = field.Name; Type = field.FieldType; Field = Some field; Getter = None; Setter = None
                        Static = field.IsStatic; Readable = true; Writable = target.IsValueType && not field.IsInitOnly && not field.IsLiteral; Ignored = false; Many = false }
            for property in properties do
                let getter = property.GetGetMethod(true)
                let setter = property.GetSetMethod(true)
                yield { Name = property.Name; Type = property.PropertyType; Field = None
                        Getter = Option.ofObj getter |> Option.map (fun method -> method :> MethodBase)
                        Setter = Option.ofObj setter |> Option.map (fun method -> method :> MethodBase)
                        Static = (not (isNull getter) && getter.IsStatic) || (not (isNull setter) && setter.IsStatic)
                        Readable = property.CanRead && not (isNull getter) && not getter.IsStatic
                        Writable = property.CanWrite
                        Ignored = not (isNull (property.GetCustomAttribute<IgnoreDataMemberAttribute>(true)))
                        Many = DBRefTypeHelpers.isManyReferenceType property.PropertyType }
        |]
        let record = FSharpType.IsRecord target
        let constructorProperties = if record then FSharpType.GetRecordFields target else properties
        let constructorTypes = constructorProperties |> Array.map _.PropertyType
        let memberConstructor = target.GetConstructors() |> Array.tryFind (fun constructor ->
            let parameterTypes = constructor.GetParameters() |> Array.map _.ParameterType
            parameterTypes.Length = constructorTypes.Length &&
            Array.forall2 (fun (parameterType: Type) propertyType ->
                if isNull parameterType then isNull propertyType else parameterType.Equals(propertyType)) parameterTypes constructorTypes)
        let constructorMembers = constructorProperties |> Array.map (fun property ->
            fields.Length + (properties |> Array.findIndex (fun candidate -> candidate = property)))
        let constructFromMembers = record || memberConstructor.IsSome
        let constructor =
            if constructFromMembers then memberConstructor
            else target.GetConstructors() |> Array.tryFind (fun constructor -> constructor.GetParameters().Length = 0)
        let union = FSharpType.IsUnion target
        let cases =
            if not union then [||]
            else
                let factories = target.GetMethods(BindingFlags.Public ||| BindingFlags.Static) |> Array.choose (fun method ->
                    method.GetCustomAttributes<CompilationMappingAttribute>()
                    |> Seq.tryFind (fun attribute -> attribute.SourceConstructFlags &&& SourceConstructFlags.UnionCase <> enum 0)
                    |> Option.map (fun attribute -> method,attribute.SequenceNumber))
                FSharpType.GetUnionCases target |> Array.map (fun case ->
                    { Name = case.Name; Tag = case.Tag
                      Factory = factories |> Array.tryFind (fun (_,tag) -> tag = case.Tag) |> Option.map (fun (method,_) -> method :> MethodBase)
                      Fields = case.GetFields() |> Array.map (fun field -> field.Name,field.PropertyType) })
        { Type = target; Name = target.Name; RuntimeName = target.FullName; ValueType = target.IsValueType
          Sealed = target.IsSealed; Abstract = target.IsAbstract
          Constructor = constructor |> Option.map (fun constructor -> constructor :> MethodBase)
          ConstructFromMembers = constructFromMembers; ConstructorMembers = constructorMembers
          Members = members; Union = union; Cases = cases; CastUnionResult = union && not target.IsValueType
          IndexedRead = indexed(fun property -> property.CanWrite)
          IndexedWrite = indexed(fun property -> property.CanRead && not(isNull(property.GetGetMethod())) && not(property.GetGetMethod().IsStatic) && isNull(property.GetCustomAttribute<IgnoreDataMemberAttribute>(true))) }

    static member Model(target: Type) =
        TypeMetadataCache.Discover(target,(fun metadata -> metadata.ObjectModel),
            (fun model metadata -> { metadata with ObjectModel = ValueSome model }),Func<CodecModel<Type,MethodBase,FieldInfo>>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "serialized model facts" target
                JsonModelDiscovery.Discover target))

    static member PublishConstruction(model: CodecModel<Type,MethodBase,FieldInfo>) =
        match model.Constructor with
        | Some (:? ConstructorInfo as constructor) ->
            let arguments = constructor.GetParameters() |> Array.map _.ParameterType
            let key = Array.toList arguments
            TypeMetadataCache.Discover(model.Type,
                (fun metadata -> MetadataMap.tryFind key metadata.QueryConstructors),
                (fun operation metadata -> { metadata with QueryConstructors = MetadataMap.set key operation metadata.QueryConstructors }),
                Func<Func<obj[],obj>>(fun () ->
                    let method = DynamicMethod("CreateSerializedModel",typeof<obj>,[|typeof<obj[]>|],typeof<JsonModelDiscovery>.Module,true)
                    CodecEmitter.construction (RuntimeCodecIL(method.GetILGenerator())) model.Type model.ValueType (constructor :> MethodBase) arguments
                    method.CreateDelegate(typeof<Func<obj[],obj>>) :?> Func<obj[],obj>)) |> ignore
        | _ -> ()

    static member UnionStorage(model: CodecModel<Type,MethodBase,FieldInfo>) =
        TypeMetadataCache.Discover(model.Type,(fun metadata -> metadata.UnionStorage),
            (fun facts metadata -> { metadata with UnionStorage = ValueSome facts }),Func<_>(fun () ->
                struct(model.Members |> Array.exists(fun memberInfo -> memberInfo.Name = "Tag"),
                       model.Cases |> Array.map _.Tag,
                       model.Cases |> Array.map(fun case -> case.Fields |> Array.map fst),
                       model.Cases |> Array.map(fun case -> case.Fields |> Array.map snd))))
