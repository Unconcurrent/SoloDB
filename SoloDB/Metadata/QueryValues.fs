namespace SoloDatabase

open System

open System.Linq.Expressions
open System.Reflection
open System.Reflection.Emit
open SoloDatabase.Metadata.CodecGeneration

/// Direct operations for values evaluated while constructing or invoking a query.
[<AbstractClass; Sealed>]
type internal QueryValueMetadata private () =
    static member private MakeExpressionLambda<'Delegate>(parameters: Type[]) =
        struct (Array.copy parameters, Func<Expression,ParameterExpression[],LambdaExpression>(fun body variables -> Expression.Lambda<'Delegate>(body,variables) :> LambdaExpression))

    static member private MakeArray<'Element>() =
        Func<obj[],obj>(fun values -> box(Array.map unbox<'Element> values))

    static member private MakeArrayContains<'Element>() =
        Func<obj,obj,bool>(fun array value ->
            let items = array :?> 'Element[]
            if isNull items then false else
            let expected = unbox<'Element> value
            let comparer = System.Collections.Generic.EqualityComparer<'Element>.Default
            let mutable found = false
            let mutable i = 0
            while not found && i < items.Length do
                found <- comparer.Equals(items.[i],expected)
                i <- i + 1
            found)

    static member internal ArrayContains(arrayType: Type) =
        TypeMetadataCache.Discover(arrayType,(fun metadata -> metadata.QueryArrayContains),
            (fun operation metadata -> { metadata with QueryArrayContains = ValueSome operation }),Func<Func<obj,obj,bool>>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "query array membership" arrayType
                typeof<QueryValueMetadata>.GetMethod("MakeArrayContains",BindingFlags.NonPublic ||| BindingFlags.Static)
                    .MakeGenericMethod(arrayType.GetElementType()).Invoke(null,[||]) :?> Func<obj,obj,bool>))

    static member private MakeConversion<'Value>() = Func<obj,obj>(fun value -> box(unbox<'Value> value))

    static member internal RegisterArrayType<'Element>() =
        TypeMetadataCache.Register(typeof<'Element[]>, fun metadata ->
            { metadata with
                QueryArray = MetadataValue.generated (QueryValueMetadata.MakeArray<'Element>())
                QueryArrayContains = MetadataValue.generated (QueryValueMetadata.MakeArrayContains<'Element>()) })

    static member internal Array(arrayType: Type) =
        TypeMetadataCache.Discover(arrayType, (fun metadata -> metadata.QueryArray),
            (fun operation metadata -> { metadata with QueryArray = ValueSome operation }), Func<Func<obj[],obj>>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "query array" arrayType
                typeof<QueryValueMetadata>.GetMethod("MakeArray", BindingFlags.NonPublic ||| BindingFlags.Static)
                    .MakeGenericMethod(arrayType.GetElementType()).Invoke(null,[||]) :?> Func<obj[],obj>))

    static member RegisterMember<'Owner, 'Value>(name: string, field: bool, read: Func<'Owner, 'Value>) =
        let operation = { Generated = true; Value = Func<obj,obj>(fun owner -> box (read.Invoke(unbox<'Owner> owner))) }
        TypeMetadataCache.Register(typeof<'Owner>, fun metadata -> { metadata with QueryMembers = MetadataMap.set (struct(name,field,typeof<'Value>)) operation metadata.QueryMembers })

    /// Registers a direct static read without constructing or casting a receiver.
    static member RegisterStaticMember<'Value>(owner: Type, name: string, field: bool, read: Func<'Value>) =
        let operation = { Generated = true; Value = Func<obj,obj>(fun _ -> box(read.Invoke())) }
        TypeMetadataCache.Register(owner, fun metadata -> { metadata with QueryMembers = MetadataMap.set (struct(name,field,typeof<'Value>)) operation metadata.QueryMembers })

    static member internal RegisterGeneratedConstruction(owner: Type, arguments: Type[], create: Func<obj[],obj>) =
        TypeMetadataCache.Register(owner, fun metadata ->
            { metadata with QueryConstructors = MetadataMap.set (Array.toList arguments) { Generated = true; Value = create } metadata.QueryConstructors
                            DefaultConstructor =
                                if arguments.Length = 0 then MetadataValue.generated (Func<obj>(fun () -> create.Invoke(Array.empty)))
                                else metadata.DefaultConstructor })

    static member internal RegisterDefaultConstruction(owner: Type, create: Func<obj>) =
        TypeMetadataCache.Register(owner, fun metadata -> { metadata with DefaultConstructor = MetadataValue.generated create })

    static member RegisterConstructor<'Value>(arguments: Type array, create: Func<obj array, 'Value>) =
        let operation = { Generated = true; Value = Func<obj[],obj>(fun values -> box(create.Invoke values)) }
        TypeMetadataCache.Register(typeof<'Value>, fun metadata -> { metadata with QueryConstructors = MetadataMap.set (Array.toList arguments) operation metadata.QueryConstructors })

    static member RegisterCall<'Value>(owner: Type, name: string, typeArguments: Type array, arguments: Type array, call: Func<obj, obj array, 'Value>) =
        let operation = { Generated = true; Value = Func<obj,obj[],obj>(fun owner values -> box(call.Invoke(owner,values))) }
        TypeMetadataCache.Register(owner, fun metadata -> { metadata with QueryCalls = MetadataMap.set (struct(name,Array.toList typeArguments,Array.toList arguments)) operation metadata.QueryCalls })

    static member RegisterUnary<'Input, 'Value>(kind: ExpressionType, apply: Func<'Input, 'Value>) =
        let operation = { Generated = true; Value = Func<obj,obj>(fun value -> box(apply.Invoke(unbox<'Input> value))) }
        TypeMetadataCache.Register(typeof<'Input>, fun metadata -> { metadata with QueryUnary = MetadataMap.set (struct(kind,typeof<'Value>)) operation metadata.QueryUnary })

    static member RegisterBinary<'Left, 'Right, 'Value>(kind: ExpressionType, apply: Func<'Left, 'Right, 'Value>) =
        let operation = { Generated = true; Value = Func<obj,obj,obj>(fun left right -> box(apply.Invoke(unbox<'Left> left,unbox<'Right> right))) }
        TypeMetadataCache.Register(typeof<'Left>, fun metadata -> { metadata with QueryBinary = MetadataMap.set (struct(kind,typeof<'Right>,typeof<'Value>)) operation metadata.QueryBinary })

    static member internal RegisterStorage<'Value>() =
        TypeMetadataCache.Register(typeof<'Value>,fun metadata -> { metadata with QueryStorage = MetadataValue.generated (QueryStorage.create<'Value>()) })

    static member private MakeStorage<'Value>() = QueryStorage.create<'Value>()

    static member internal Storage(valueType: Type) =
        TypeMetadataCache.Discover(valueType,(fun metadata -> metadata.QueryStorage),
            (fun operation metadata -> { metadata with QueryStorage = ValueSome operation }),Func<QueryStorageOperations>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "query argument storage" valueType
                typeof<QueryValueMetadata>.GetMethod("MakeStorage",BindingFlags.NonPublic ||| BindingFlags.Static)
                    .MakeGenericMethod(valueType).Invoke(null,[||]) :?> QueryStorageOperations))

    static member internal RegisterGeneratedAddress(owner, name, valueType: Type, address: Delegate) =
        let operation = QueryValueMetadata.Storage(valueType).BindAddress.Invoke(address)
        TypeMetadataCache.Register(owner,fun metadata -> { metadata with QueryAddresses = MetadataMap.set (struct(name,valueType)) { Generated = true; Value = operation } metadata.QueryAddresses })

    static member internal Address(owner: Type, name, valueType: Type, discover: Func<Delegate>) =
        TypeMetadataCache.Discover(owner,(fun metadata -> MetadataMap.tryFind (struct(name,valueType)) metadata.QueryAddresses),
            (fun operation metadata -> { metadata with QueryAddresses = MetadataMap.set (struct(name,valueType)) operation metadata.QueryAddresses }),
            Func<Func<obj,int[],QueryReference>>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "query argument address" owner
                QueryValueMetadata.Storage(valueType).BindAddress.Invoke(discover.Invoke())))

    static member internal FieldAddress(owner: Type, name, valueType: Type) =
        QueryValueMetadata.Address(owner,name,valueType,Func<Delegate>(fun () ->
            let field = owner.GetField(name,BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance ||| BindingFlags.Static)
            if isNull field || not (field.FieldType.Equals(valueType)) then invalidArg "name" "A query argument requires its exact field."
            QueryMethodDiscovery.fieldAddress field))

    static member internal FieldAddress(field: FieldInfo) =
        QueryValueMetadata.Address(field.DeclaringType,field.Name,field.FieldType,Func<Delegate>(fun () -> QueryMethodDiscovery.fieldAddress field))

    static member internal ArrayAddress(arrayType: Type) =
        if arrayType.GetArrayRank() = 1 then QueryValueMetadata.Storage(arrayType.GetElementType()).ArrayAddress else
        QueryValueMetadata.Address(arrayType,"[]",arrayType.GetElementType(),Func<Delegate>(fun () -> QueryMethodDiscovery.arrayAddress arrayType))

    static member internal BoxAddress(valueType: Type) =
        QueryValueMetadata.Address(valueType,"&",valueType,Func<Delegate>(fun () -> QueryMethodDiscovery.boxAddress valueType))

    static member RegisterType<'Value>() =
        if typeof<'Value>.Equals(typeof<string>) then
            QueryValueMetadata.RegisterMember<string,int>("Length",false,Func<string,int>(fun value -> value.Length))
        TypeMetadataCache.Register(typeof<'Value>, fun metadata -> { metadata with QueryDefault = MetadataValue.generated (box Unchecked.defaultof<'Value>); QueryStorage = MetadataValue.generated (QueryStorage.create<'Value>()) })
        let convert = { Generated = true; Value = QueryValueMetadata.MakeConversion<'Value>() }
        TypeMetadataCache.Register(typeof<obj>, fun metadata ->
            { metadata with QueryUnary = metadata.QueryUnary
                                        |> MetadataMap.set (struct(ExpressionType.Convert,typeof<'Value>)) convert
                                        |> MetadataMap.set (struct(ExpressionType.ConvertChecked,typeof<'Value>)) convert })

    static member RegisterLambda(delegateType: Type, create: Func<int,int,Func<obj array, obj>, obj>) =
        TypeMetadataCache.Register(delegateType, fun metadata -> { metadata with QueryLambda = MetadataValue.generated create })

    static member RegisterExpressionLambda<'Delegate>(parameters: Type array) =
        let operation = QueryValueMetadata.MakeExpressionLambda<'Delegate>(parameters)
        TypeMetadataCache.Register(typeof<'Delegate>, fun metadata -> { metadata with ExpressionLambda = MetadataValue.generated operation })

    static member internal ExpressionLambda(delegateType: Type) =
        TypeMetadataCache.Discover(delegateType, (fun metadata -> metadata.ExpressionLambda),
            (fun operation metadata -> { metadata with ExpressionLambda = ValueSome operation }),
            Func<struct (Type[] * Func<Expression,ParameterExpression[],LambdaExpression>)>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "query expression lambda" delegateType
                let parameters = delegateType.GetMethod("Invoke").GetParameters() |> Array.map _.ParameterType
                typeof<QueryValueMetadata>.GetMethod("MakeExpressionLambda", BindingFlags.NonPublic ||| BindingFlags.Static)
                    .MakeGenericMethod(delegateType).Invoke(null,[|box parameters|])
                    :?> struct (Type[] * Func<Expression,ParameterExpression[],LambdaExpression>)))

    static member internal RegisterGeneratedReader(owner, name, field, valueType, read) =
        TypeMetadataCache.Register(owner, fun metadata -> { metadata with QueryMembers = MetadataMap.set (struct(name,field,valueType)) { Generated = true; Value = read } metadata.QueryMembers })
    static member internal RegisterReader(owner, name, field, valueType, read) =
        TypeMetadataCache.Discover(owner,
            (fun metadata -> MetadataMap.tryFind (struct(name,field,valueType)) metadata.QueryMembers),
            (fun operation metadata -> { metadata with QueryMembers = MetadataMap.set (struct(name,field,valueType)) operation metadata.QueryMembers }), Func<Func<obj,obj>>(fun () -> read)) |> ignore
    static member internal Reader(owner: Type, name, field, valueType) =
        match TypeMetadataCache.Read(owner, fun metadata -> MetadataMap.tryFind (struct(name,field,valueType)) metadata.QueryMembers) with
        | ValueSome read -> read.Value
        | _ -> raise (NotSupportedException(sprintf "Query value member %s.%s has no direct operation." owner.FullName name))
    static member internal Reader(memberInfo: MemberInfo, receiverType: Type) =
        let owner = if isNull receiverType then memberInfo.DeclaringType else receiverType
        let valueType =
            match memberInfo with
            | :? PropertyInfo as property -> property.PropertyType
            | :? FieldInfo as field -> field.FieldType
            | _ -> invalidArg "memberInfo" "A query value member must be a property or field."
        let key = struct(memberInfo.Name,memberInfo :? FieldInfo,valueType)
        let selected =
            match TypeMetadataCache.Read(owner, fun metadata -> MetadataMap.tryFind key metadata.QueryMembers) with
            | ValueSome operation -> ValueSome operation.Value
            | ValueNone ->
                TypeMetadataCache.Read(memberInfo.DeclaringType, fun metadata -> MetadataMap.tryFind key metadata.QueryMembers)
                |> MetadataValue.tryValue
        match selected with
        | ValueSome operation -> operation
        | ValueNone ->
            TypeMetadataCache.Discover(owner, (fun metadata -> MetadataMap.tryFind key metadata.QueryMembers),
                (fun operation metadata -> { metadata with QueryMembers = MetadataMap.set key operation metadata.QueryMembers }),
                Func<Func<obj,obj>>(fun () ->
                    match memberInfo with
                    | (:? PropertyInfo | :? FieldInfo) when MetadataRuntime.supportsDynamicCode() ->
                        QueryMethodDiscovery.reader memberInfo
                    | _ -> raise (NotSupportedException(sprintf "Query value member %s.%s has no direct operation." owner.FullName memberInfo.Name))))
    static member internal RegisterGeneratedCase(owner, tag, create, test) =
        TypeMetadataCache.Register(owner,fun metadata -> { metadata with QueryCases = MetadataMap.set tag { Generated=true; Value=struct(create,test) } metadata.QueryCases })
    static member internal UnionCase(owner: Type, tag: int) =
        TypeMetadataCache.Discover(owner,(fun metadata -> MetadataMap.tryFind tag metadata.QueryCases),
            (fun operation metadata -> { metadata with QueryCases=MetadataMap.set tag operation metadata.QueryCases }),
            Func<struct(Func<obj[],obj> * Func<obj,bool>)>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "query union case" owner
                let model = JsonModelDiscovery.Model owner
                let case = model.Cases |> Array.find(fun item -> item.Tag = tag)
                let create =
                    match case.Factory with
                    | Some (:? MethodInfo as method) ->
                        let call = QueryMethodDiscovery.create method
                        Func<obj[],obj>(fun values -> call.Invoke(null,values))
                    | _ -> Func<obj[],obj>(fun _ -> raise(NotSupportedException($"Union case '{owner.FullName}.{case.Name}' has no callable factory.")))
                let flags=BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Static ||| BindingFlags.Instance
                let tagMethod =
                    match owner.GetMethod("GetTag",flags) with
                    | null -> owner.GetProperty("Tag",flags).GetGetMethod(true)
                    | method -> method
                let method = DynamicMethod("QueryUnionCase",typeof<bool>,[|typeof<obj>|],owner.Module,true)
                QueryOperationEmitter.unionTest (RuntimeCodecIL(method.GetILGenerator())) owner owner.IsValueType (tagMethod :> MethodBase) tagMethod.IsStatic tag
                struct(create,method.CreateDelegate(typeof<Func<obj,bool>>) :?> Func<obj,bool>)))
    static member internal Function(functionType: Type, argument: Type, result: Type) =
        let key=struct("Invoke",[],[argument])
        TypeMetadataCache.Discover(functionType,(fun metadata -> MetadataMap.tryFind key metadata.QueryCalls),
            (fun operation metadata -> { metadata with QueryCalls=MetadataMap.set key operation metadata.QueryCalls }),
            Func<Func<obj,obj[],obj>>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "query function application" functionType
                QueryMethodDiscovery.create(functionType.GetMethod("Invoke",[|argument|]))))
    static member internal RegisterGeneratedSetter(owner, name, field, valueType, write) =
        TypeMetadataCache.Register(owner, fun metadata -> { metadata with QuerySetters = MetadataMap.set (struct(name,field,valueType)) { Generated = true; Value = write } metadata.QuerySetters })
    static member internal Setter(memberInfo: MemberInfo) =
        let valueType =
            match memberInfo with
            | :? PropertyInfo as property -> property.PropertyType
            | :? FieldInfo as field -> field.FieldType
            | _ -> invalidArg "memberInfo" "A query assignment requires a field or property."
        let key = struct(memberInfo.Name,memberInfo :? FieldInfo,valueType)
        TypeMetadataCache.Discover(memberInfo.DeclaringType, (fun metadata -> MetadataMap.tryFind key metadata.QuerySetters),
            (fun operation metadata -> { metadata with QuerySetters = MetadataMap.set key operation metadata.QuerySetters }),
            Func<Action<obj,obj>>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "query assignment" memberInfo.DeclaringType
                QueryMethodDiscovery.setter memberInfo))
    static member internal Constructor(valueType: Type, arguments) =
        let current() = TypeMetadataCache.Read(valueType, fun metadata -> MetadataMap.tryFind arguments metadata.QueryConstructors)
        match current() with
        | ValueSome create -> create.Value
        | ValueNone ->
            TypeMetadataCache.Discover(valueType, (fun metadata -> MetadataMap.tryFind arguments metadata.QueryConstructors),
                (fun operation metadata -> { metadata with QueryConstructors = MetadataMap.set arguments operation metadata.QueryConstructors }),
                Func<Func<obj[],obj>>(fun () ->
                    if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "query constructor" valueType
                    let constructor = valueType.GetConstructor(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance,null,Array.ofList arguments,null)
                    if isNull constructor then raise (NotSupportedException(sprintf "Query value constructor %s has no direct operation." valueType.FullName))
                    QueryMethodDiscovery.constructor constructor))
    static member internal Call(owner: Type, name, typeArguments, arguments) =
        match TypeMetadataCache.Read(owner, fun metadata -> MetadataMap.tryFind (struct(name,typeArguments,arguments)) metadata.QueryCalls) with
        | ValueSome call -> call.Value
        | _ -> raise (NotSupportedException(sprintf "Query value method %s.%s has no direct operation." owner.FullName name))
    static member internal Call(methodInfo: MethodInfo) =
        let owner = methodInfo.DeclaringType
        if isNull owner then
            if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "query method" methodInfo.ReturnType
            QueryMethodDiscovery.create methodInfo
        else
            let typeArguments = if methodInfo.IsGenericMethod then methodInfo.GetGenericArguments() |> Array.toList else []
            let arguments = methodInfo.GetParameters() |> Array.map _.ParameterType |> Array.toList
            let key = struct(methodInfo.Name,typeArguments,arguments)
            TypeMetadataCache.Discover(owner, (fun metadata -> MetadataMap.tryFind key metadata.QueryCalls),
                (fun operation metadata -> { metadata with QueryCalls = MetadataMap.set key operation metadata.QueryCalls }),
                Func<Func<obj,obj[],obj>>(fun () ->
                    if not (MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "query method" owner
                    QueryMethodDiscovery.create methodInfo))
    static member internal Unary(kind, input: Type, valueType: Type) =
        TypeMetadataCache.Discover(input, (fun metadata -> MetadataMap.tryFind (struct(kind,valueType)) metadata.QueryUnary),
            (fun operation metadata -> { metadata with QueryUnary = MetadataMap.set (struct(kind,valueType)) operation metadata.QueryUnary }), Func<Func<obj,obj>>(fun () ->
                if typeof<obj>.Equals(input) && (kind = ExpressionType.Convert || kind = ExpressionType.ConvertChecked) && MetadataRuntime.supportsDynamicCode() then
                    typeof<QueryValueMetadata>.GetMethod("MakeConversion", BindingFlags.NonPublic ||| BindingFlags.Static)
                        .MakeGenericMethod(valueType).Invoke(null,[||]) :?> Func<obj,obj>
                elif input.IsEnum && MetadataRuntime.supportsDynamicCode() then
                    let underlying = TypeMetadata.enumUnderlying input
                    let read = QueryMethodDiscovery.enumValue input underlying
                    match QueryScalarOperators.unary kind underlying valueType with
                    | ValueSome apply -> Func<obj,obj>(fun value -> apply.Invoke(read.Invoke value))
                    | ValueNone -> raise(NotSupportedException(sprintf "Query enum conversion from %s to %s has no direct operation." input.FullName valueType.FullName))
                else
                    match QueryScalarOperators.unary kind input valueType with
                    | ValueSome apply -> apply
                    | ValueNone -> raise (NotSupportedException(sprintf "Query value operation %O from %s to %s has no direct operation." kind input.FullName valueType.FullName))))
    static member internal Binary(kind, left: Type, right: Type, valueType) =
        TypeMetadataCache.Discover(left, (fun metadata -> MetadataMap.tryFind (struct(kind,right,valueType)) metadata.QueryBinary),
            (fun operation metadata -> { metadata with QueryBinary = MetadataMap.set (struct(kind,right,valueType)) operation metadata.QueryBinary }), Func<Func<obj,obj,obj>>(fun () ->
                match QueryScalarOperators.binary kind left right valueType with
                | ValueSome apply -> apply
                | ValueNone -> raise (NotSupportedException(sprintf "Query value operation %O on %s and %s has no direct operation." kind left.FullName right.FullName))))
    static member internal Default(valueType: Type) =
        if not valueType.IsValueType then null else
        TypeMetadataCache.Discover(valueType,(fun metadata -> metadata.QueryDefault),
            (fun operation metadata -> { metadata with QueryDefault = ValueSome operation }),Func<obj>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "query value default" valueType
                (QueryMethodDiscovery.defaultConstruction valueType None).Invoke()))
    static member internal Lambda(delegateType: Type) =
        TypeMetadataCache.Discover(delegateType, (fun metadata -> metadata.QueryLambda),
            (fun operation metadata -> { metadata with QueryLambda = ValueSome operation }),
            Func<Func<int,int,Func<obj array,obj>,obj>>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "query value lambda" delegateType
                QueryLambdaDiscovery.create delegateType))
