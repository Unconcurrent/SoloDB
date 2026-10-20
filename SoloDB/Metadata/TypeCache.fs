namespace SoloDatabase

open System
open System.Collections.Generic
open System.Threading
open System.Reflection
open System.Reflection.Emit
open System.Linq
open System.Linq.Expressions
open SoloDatabase.Attributes
open SoloDatabase.Metadata.CodecGeneration

/// Compiler-established type policies over the original closed CLR identity.
[<AllowNullLiteral; Sealed>]
type internal ModelTypeFacts(modelType: Type, sealedType: bool, requiresTypeInformation: bool) =
    do if isNull modelType then nullArg "modelType"
    member _.ModelType = modelType
    member _.IsSealed = sealedType
    member _.RequiresTypeInformation = requiresTypeInformation

[<Struct>]
type internal ReferenceTypeFacts = {
    Target: Type
    Identity: Type
    Many: bool
}

/// The original identity member and direct operations; no runtime member discovery is required.
[<AllowNullLiteral; Sealed>]
type internal CustomIdMember(declaringType: Type, name: string, idType: Type, read: Func<obj,obj>, write: Action<obj,obj>, generator: Func<obj>, isDefault: Func<obj,bool>) =
    let instance = lazy(generator.Invoke())
    member _.DeclaringType = declaringType
    member _.Name = name
    member _.IdType = idType
    member _.Read = read
    member _.Write = write
    member _.Generator = instance.Value
    member _.IsDefault(value) = isDefault.Invoke(value)

/// Immutable policy facts for one declared reference edge.
[<AllowNullLiteral; Sealed>]
type internal RelationPolicy(onDelete: DeletePolicy, onOwnerDelete: DeletePolicy, unique: bool, orderBy: DBRefOrder) =
    member _.OnDelete = onDelete
    member _.OnOwnerDelete = onOwnerDelete
    member _.Unique = unique
    member _.OrderBy = orderBy

/// A reference declaration and its direct operations over the original owner.
[<Sealed>]
type internal RelationMember(name: string, declaringType: Type, referenceType: Type,
                    read: Func<obj, obj>, write: Action<obj, obj>, ignored: bool, policy: RelationPolicy) =
    member _.Name = name
    member _.DeclaringType = declaringType
    member _.ReferenceType = referenceType
    member _.Read = read
    member _.Write = write
    member _.Ignored = ignored
    member _.Policy = policy

/// Ordered declared index facts over the original property.
[<Sealed>]
type internal DeclaredIndex(name: string, valueType: Type, unique: bool, customIdentity: bool) =
    member _.Name = name
    member _.ValueType = valueType
    member _.Unique = unique
    member _.CustomIdentity = customIdentity

/// The existing schema slot owns pending typed acquisition and completed index values.
type internal SchemaIndexSource =
    | PendingIndexes of Func<DeclaredIndex[]>
    | ReadyIndexes of DeclaredIndex[]

[<Sealed>]
type internal SchemaProperty(name: string,valueType: Type,attributes: Func<obj[]>) =
    member _.Name = name
    member _.ValueType = valueType
    member _.Attributes = attributes

/// A layout is independent of one invocation's values and belongs to the argument source.
type internal NativeArgumentLayout(names: string[]) =
    member _.Names = names

/// Mutable public parameter collections use operation errors for duplicate names.
[<Sealed>]
type internal NativeOperationArgumentLayout(names: string[]) =
    inherit NativeArgumentLayout(names)

/// Raw JSON operations retain their typed entry and exact runtime adapter.
type internal JsonRawReadOperations = { Reader: Delegate; ReadRuntime: Delegate; Intrinsic: Delegate }
type internal JsonRawWriteOperations = { Writer: Delegate; WriteRuntime: Delegate }

[<Struct>]
type internal StringIndexerOperations = {
    Value: struct(Type * Func<obj,string,obj>) voption
    Ambiguous: bool
    MissingGetter: bool
    RestrictedMethod: string
}

/// Physical identity preserves typed byref and existing-box receivers.
type internal PhysicalIdTypeOperations = {
    TypedReader: Delegate
    TypedWriter: Delegate
    Reader: Func<obj, int64>
    Writer: Action<obj, int64>
    HasWritableId: bool
    TrivialGetter: bool
    Ambiguous: bool
    MissingGetter: bool
    Indexed: bool
    WriteIndexed: bool
}

[<Struct>]
type internal OwnerCloneOperations = {
    CreateInstance: Func<obj>
    CopyScalars: Action<obj, obj>
    Constructible: bool
    Indexed: bool
}

[<Struct>]
type internal ReferenceFactories = {
    ToOrResolved: Func<int64, obj>
    Loaded: Func<int64, obj, obj>
    ResolvedTyped: Func<int64, obj, obj>
    LoadedTyped: Func<int64, obj, obj, obj>
}

[<Struct>]
type internal ValueOptionAccessors = {
    IsSome: Func<obj, bool>
    GetValue: Func<obj, obj>
}

type internal RelationKind = Single | Many

type internal FileSystemReferenceSpec = {
    Property: RelationMember
    Kind: RelationKind
    TargetType: Type
    TargetTable: string
}

type internal DocumentReferenceSpec = RelationMember * RelationKind * Type * Type voption * DeletePolicy * DeletePolicy * bool * DBRefOrder

[<Struct>]
type internal RelationHydrationShape = { HasAny: bool; HasSingle: bool; HasMany: bool }

type internal ManyRelationEntry = {
    Property: RelationMember
    TargetType: Type
    TrackerGetter: Func<obj,obj>
    TrackerSetter: Action<obj,obj>
    TrackerCtor: Func<obj>
    TargetIdWriter: Action<obj,int64>
}

/// Ordered relation facts; diagnostic arrays copy while ordinary access is indexed.
type internal RelationHydrationDescriptor(singles: RelationMember[], manyRelations: ManyRelationEntry[], shape: RelationHydrationShape) =
    member _.SingleCount = singles.Length
    member _.Single(index) = singles.[index]
    member _.ManyCount = manyRelations.Length
    member _.Many(index) = manyRelations.[index]
    member _.Shape = shape
    member _.SinglePropertyNames = singles |> Array.map _.Name
    member _.ManyPropertyNames = manyRelations |> Array.map _.Property.Name

[<Struct>]
type internal MetadataValue<'T> = { Generated: bool; Value: 'T }

/// Original member signatures retain formal slots independently of closed argument values.
type internal ModelTypeTemplate =
    | UnavailableType
    | FormalType of int
    | NamedType of Type * ModelTypeTemplate[]
    | ElementType of int * ModelTypeTemplate

/// Compiler-supplied delegates remain callable facts until ordinary dynamic preparation needs an operand.
type internal JsonDefaultMethod =
    | EmittedMethod of MethodBase
    | SuppliedBody of Delegate
    member this.Operand =
        match this with
        | EmittedMethod method -> method
        | SuppliedBody body -> body.Method :> MethodBase


/// One concrete record of facts and functions for a closed CLR type.
type internal TypeMetadataRecord = {
    TypeFacts: MetadataValue<ModelTypeFacts> voption
    NullableUnderlying: MetadataValue<Type> voption
    EnumUnderlying: MetadataValue<Type> voption
    MemberTypes: MetadataValue<struct(string * Type * bool)[]> voption
    MemberTemplates: Dictionary<int,MetadataValue<ModelTypeTemplate>> option
    DefinitionRepresentative: Type voption
    StringIndexer: MetadataValue<StringIndexerOperations> voption
    SequenceElement: MetadataValue<Type> voption
    EqualityComparer: MetadataValue<obj> voption
    ObjectModel: MetadataValue<CodecModel<Type,MethodBase,FieldInfo>> voption
    JsonDefaultMethods: Dictionary<struct(Type * Type * string), MetadataValue<JsonDefaultMethod>> option
    JsonUtf8Read: MetadataValue<JsonRawReadOperations> voption
    JsonUtf16Read: MetadataValue<JsonRawReadOperations> voption
    JsonUtf8Write: MetadataValue<JsonRawWriteOperations> voption
    JsonUtf16Write: MetadataValue<JsonRawWriteOperations> voption
    StoredJsonUtf8Read: MetadataValue<JsonRawReadOperations> voption
    StoredJsonUtf16Read: MetadataValue<JsonRawReadOperations> voption
    StoredJsonUtf8Write: MetadataValue<JsonRawWriteOperations> voption
    StoredJsonUtf16Write: MetadataValue<JsonRawWriteOperations> voption
    JsonReader: MetadataValue<Delegate> voption
    JsonWriter: MetadataValue<struct (Delegate * Func<obj>)> voption
    RuntimeJsonRead: MetadataValue<Func<obj,obj>> voption
    RuntimeJsonWrite: MetadataValue<Func<obj,obj>> voption
    PhysicalIdentity: MetadataValue<PhysicalIdTypeOperations> voption
    CustomIdentity: MetadataValue<CustomIdMember[]> voption
    ReferenceType: MetadataValue<ReferenceTypeFacts> voption
    Relations: MetadataValue<RelationMember[]> voption
    Hydration: MetadataValue<struct (RelationMember[] * Lazy<RelationHydrationDescriptor>)> voption
    ReferenceSpecs: MetadataValue<struct (RelationMember[] * struct (DocumentReferenceSpec[] * FileSystemReferenceSpec[]))> voption
    Union: MetadataValue<bool> voption
    UnionKind: MetadataValue<bool> voption
    UnionStorage: MetadataValue<struct(bool * int[] * string[][] * Type[][])> voption
    Indexes: MetadataValue<SchemaIndexSource> voption
    SqlArguments: MetadataValue<struct (NativeArgumentLayout * Action<obj,obj[]>)> voption
    SqlRows: MetadataValue<Delegate> voption
    SqlDefault: MetadataValue<Delegate> voption
    RawNullable: MetadataValue<Delegate> voption
    EnumReader: MetadataValue<Delegate> voption
    OwnerClone: MetadataValue<OwnerCloneOperations> voption
    DefaultConstructor: MetadataValue<Func<obj>> voption
    CollectionAdder: MetadataValue<Action<obj,obj>> voption
    ReferenceFactories: MetadataValue<ReferenceFactories> voption
    ReferenceIdReader: MetadataValue<Func<obj,int64>> voption
    PendingTypedId: MetadataValue<struct (Func<obj,bool> * Func<obj,obj>) voption> voption
    ValueOption: MetadataValue<ValueOptionAccessors voption> voption
    DocumentIdReader: MetadataValue<Delegate> voption
    Int64Reader: MetadataValue<Func<obj,int64>> voption
    Int64Writer: MetadataValue<Action<obj,int64>> voption
    WritableInt64: MetadataValue<bool> voption
    TrivialInt64: MetadataValue<bool> voption
    GenericArguments: MetadataValue<Type[]> voption
    RecordFields: MetadataValue<string[] voption> voption
    StandardComparison: MetadataValue<bool> voption
    ListConstruction: MetadataValue<obj seq -> obj> voption
    TupleDefault: MetadataValue<Delegate> voption
    CascadeIdentity: MetadataValue<Delegate> voption
    QueryStorage: MetadataValue<QueryStorageOperations> voption
    QueryAddresses: Dictionary<struct(string * Type), MetadataValue<Func<obj,int[],QueryReference>>> option
    QueryDefault: MetadataValue<obj> voption
    QueryArray: MetadataValue<Func<obj[],obj>> voption
    QueryArrayContains: MetadataValue<Func<obj,obj,bool>> voption
    QueryLambda: MetadataValue<Func<int,int,Func<obj[],obj>,obj>> voption
    ExpressionLambda: MetadataValue<struct (Type[] * Func<Expression,ParameterExpression[],LambdaExpression>)> voption
    QueryMembers: Dictionary<struct (string * bool * Type), MetadataValue<Func<obj,obj>>> option
    QueryCases: Dictionary<int, MetadataValue<struct (Func<obj[],obj> * Func<obj,bool>)>> option
    QuerySetters: Dictionary<struct (string * bool * Type), MetadataValue<Action<obj,obj>>> option
    QueryConstructors: Dictionary<Type list, MetadataValue<Func<obj[],obj>>> option
    QueryCalls: Dictionary<struct (string * Type list * Type list), MetadataValue<Func<obj,obj[],obj>>> option
    QueryUnary: Dictionary<struct (ExpressionType * Type), MetadataValue<Func<obj,obj>>> option
    QueryBinary: Dictionary<struct (ExpressionType * Type * Type), MetadataValue<Func<obj,obj,obj>>> option
    PropertyGetters: Dictionary<PropertyInfo, MetadataValue<Func<obj,obj>>> option
    PropertySetters: Dictionary<PropertyInfo, MetadataValue<Action<obj,obj>>> option
    NamedGetters: Dictionary<string, MetadataValue<Func<obj,obj> voption>> option
    NamedSetters: Dictionary<string, MetadataValue<Action<obj,obj>>> option
    GenericInterfaces: Dictionary<Type,MetadataValue<bool>> option
    GenericMethodArguments: Dictionary<MethodInfo, MetadataValue<Type[]>> option
    ProviderQueries: Dictionary<Type, MetadataValue<Func<IQueryProvider,Expression,IQueryable>>> option
    ProviderResults: Dictionary<Type, MetadataValue<Func<IQueryProvider,Expression,obj>>> option
    ProviderSequences: Dictionary<Type, MetadataValue<Delegate>> option
    SequenceResults: Dictionary<Type, MetadataValue<Type>> option
    CompiledSequences: Dictionary<struct (Type * Type), MetadataValue<Delegate>> option
    CompiledResults: Dictionary<struct (Type * Type), MetadataValue<Delegate>> option
}

/// A stored-name index points at original identities; type operations remain in the type dictionary.
type internal TypeNameEntry = { Registered: Type[]; Resolved: Type voption }

/// Parallel reads and exclusive publication; discovery runs outside the lock.
[<AbstractClass; Sealed>]
type internal TypeMetadataCache private () =
    static let gate = new ReaderWriterLockSlim()
    static let entries = Dictionary<Type, TypeMetadataRecord>()
    static let names = Dictionary<string, TypeNameEntry>(StringComparer.Ordinal)
    static let empty = {
        TypeFacts = ValueNone; NullableUnderlying = ValueNone; EnumUnderlying = ValueNone
        MemberTypes = ValueNone; MemberTemplates = None; DefinitionRepresentative = ValueNone
        StringIndexer = ValueNone; SequenceElement = ValueNone; EqualityComparer = ValueNone
        ObjectModel = ValueNone; JsonDefaultMethods = None; JsonUtf8Read = ValueNone; JsonUtf16Read = ValueNone; JsonUtf8Write = ValueNone; JsonUtf16Write = ValueNone; PhysicalIdentity = ValueNone
        StoredJsonUtf8Read = ValueNone; StoredJsonUtf16Read = ValueNone; StoredJsonUtf8Write = ValueNone; StoredJsonUtf16Write = ValueNone
        JsonReader = ValueNone; JsonWriter = ValueNone
        RuntimeJsonRead = ValueNone; RuntimeJsonWrite = ValueNone
        CustomIdentity = ValueNone; ReferenceType = ValueNone; Relations = ValueNone
        Hydration = ValueNone; ReferenceSpecs = ValueNone
        Union = ValueNone; UnionKind = ValueNone; UnionStorage = ValueNone; Indexes = ValueNone; SqlArguments = ValueNone
        SqlRows = ValueNone; SqlDefault = ValueNone; RawNullable = ValueNone; EnumReader = ValueNone
        OwnerClone = ValueNone
        DefaultConstructor = ValueNone
        CollectionAdder = ValueNone
        ReferenceFactories = ValueNone
        ReferenceIdReader = ValueNone
        PendingTypedId = ValueNone
        ValueOption = ValueNone
        DocumentIdReader = ValueNone
        Int64Reader = ValueNone
        Int64Writer = ValueNone
        WritableInt64 = ValueNone
        TrivialInt64 = ValueNone
        GenericArguments = ValueNone
        RecordFields = ValueNone
        StandardComparison = ValueNone
        ListConstruction = ValueNone
        TupleDefault = ValueNone
        CascadeIdentity = ValueNone
        QueryStorage = ValueNone
        QueryAddresses = None
        QueryDefault = ValueNone
        QueryArray = ValueNone
        QueryArrayContains = ValueNone
        QueryLambda = ValueNone
        ExpressionLambda = ValueNone
        QueryMembers = None
        QuerySetters = None
        QueryCases = None
        QueryConstructors = None
        QueryCalls = None
        QueryUnary = None
        QueryBinary = None
        PropertyGetters = None
        PropertySetters = None
        NamedGetters = None
        NamedSetters = None
        GenericInterfaces = None
        GenericMethodArguments = None
        ProviderQueries = None
        ProviderResults = None
        ProviderSequences = None
        SequenceResults = None
        CompiledSequences = None
        CompiledResults = None
    }
    static let get modelType =
        match entries.TryGetValue modelType with true, metadata -> metadata | _ -> empty
    static member Get(modelType: Type) =
        gate.EnterReadLock()
        try get modelType
        finally gate.ExitReadLock()
    static member Read(modelType: Type, select: TypeMetadataRecord -> 'T) =
        gate.EnterReadLock()
        try select (get modelType)
        finally gate.ExitReadLock()
    static member Register(modelType: Type, update: TypeMetadataRecord -> TypeMetadataRecord) =
        gate.EnterWriteLock()
        try entries.[modelType] <- update (get modelType)
        finally gate.ExitWriteLock()
    static member KnownJsonWriterBodies(declared: Type, element: Type, contract: Type, operation: string) =
        let known =
            gate.EnterReadLock()
            try
                let key = struct(element,contract,operation)
                [|for KeyValue(target,metadata) in entries do
                      if Metadata.Generation.TypeSupport.KnownWriterCandidate(target.IsAbstract,target.Equals declared) && declared.IsAssignableFrom target then
                          match metadata.JsonDefaultMethods with
                          | Some methods ->
                              match methods.TryGetValue key with
                              | true, method -> yield struct(target,method.Value)
                              | _ -> ()
                          | None -> ()|]
            finally gate.ExitReadLock()
        known |> Array.map(fun struct(target,method) -> struct(target,method.Operand))
    static member RegisterName(modelType: Type) =
        gate.EnterWriteLock()
        try
            let name = modelType.FullName
            let registered =
                match names.TryGetValue name with
                | true, entry when Array.exists (fun registered -> modelType.Equals(registered)) entry.Registered -> entry.Registered
                | true, entry -> Array.append entry.Registered [|modelType|]
                | _ -> [|modelType|]
            names.[name] <- { Registered = registered; Resolved = ValueNone }
        finally gate.ExitWriteLock()
    static member ResolveName(name: string, discover: Func<Type>) =
        let select() =
            match names.TryGetValue name with
            | true, { Registered = [|modelType|] } -> ValueSome modelType
            | true, entry -> entry.Resolved
            | _ -> ValueNone
        gate.EnterReadLock()
        let cached = try select() finally gate.ExitReadLock()
        match cached with
        | ValueSome modelType -> modelType
        | ValueNone ->
            try
                let discovered = discover.Invoke()
                gate.EnterWriteLock()
                try
                    match select() with
                    | ValueSome modelType -> modelType
                    | ValueNone ->
                        let registered = match names.TryGetValue name with true, entry -> entry.Registered | _ -> [||]
                        names.[name] <- { Registered = registered; Resolved = ValueSome discovered }
                        discovered
                finally gate.ExitWriteLock()
            with _ ->
                gate.EnterReadLock()
                let registered =
                    try
                        match names.TryGetValue name with
                        | true, { Registered = [|modelType|] } -> ValueSome modelType
                        | _ -> ValueNone
                    finally gate.ExitReadLock()
                match registered with ValueSome modelType -> modelType | ValueNone -> reraise()
    static member ClearHydration() =
        gate.EnterWriteLock()
        try
            for modelType in entries.Keys |> Seq.toArray do
                entries.[modelType] <- { entries.[modelType] with Hydration = ValueNone }
        finally gate.ExitWriteLock()
    static member ClearReferenceSpecs() =
        gate.EnterWriteLock()
        try
            for modelType in entries.Keys |> Seq.toArray do
                entries.[modelType] <- { entries.[modelType] with ReferenceSpecs = ValueNone }
        finally gate.ExitWriteLock()
    static member Discover(modelType: Type, select: TypeMetadataRecord -> MetadataValue<'T> voption,
                          store: MetadataValue<'T> -> TypeMetadataRecord -> TypeMetadataRecord,
                          create: Func<'T>, prepare: Func<'T,'T voption>) =
        let publish expected discovered generated =
            gate.EnterWriteLock()
            try
                let metadata = get modelType
                match select metadata with
                | ValueSome current when expected |> ValueOption.forall(fun original -> not(obj.ReferenceEquals(original.Value,current.Value))) -> current
                | _ ->
                    let completed = { Generated = generated; Value = discovered }
                    entries.[modelType] <- store completed metadata
                    completed
            finally gate.ExitWriteLock()
        let rec acquire current =
            try
                match current with
                | ValueSome original ->
                    match prepare.Invoke original.Value with
                    | ValueNone -> original.Value
                    | ValueSome completed -> acquire(ValueSome(publish current completed original.Generated))
                | ValueNone ->
                    let discovered = create.Invoke()
                    let completed = prepare.Invoke discovered |> ValueOption.defaultValue discovered
                    acquire(ValueSome(publish ValueNone completed false))
            with _ ->
                match TypeMetadataCache.Read(modelType,select) with
                | ValueSome winner when winner.Generated && (current |> ValueOption.forall(fun original -> not(obj.ReferenceEquals(original.Value,winner.Value)))) ->
                    acquire(ValueSome winner)
                | _ -> reraise()
        acquire(TypeMetadataCache.Read(modelType,select))

    static member Discover(modelType: Type, select: TypeMetadataRecord -> MetadataValue<'T> voption,
                          store: MetadataValue<'T> -> TypeMetadataRecord -> TypeMetadataRecord, create: Func<'T>) =
        match TypeMetadataCache.Read(modelType,select) with
        | ValueSome value -> value.Value
        | ValueNone -> TypeMetadataCache.Discover(modelType,select,store,create,Func<'T,'T voption>(fun _ -> ValueNone))

module internal MetadataValue =
    let tryValue value = match value with ValueSome entry -> ValueSome entry.Value | ValueNone -> ValueNone
    let generated value = ValueSome { Generated = true; Value = value }

/// These helpers are used only while the type cache owns its read or write lock.
module internal MetadataMap =
    let tryFind key (values: Dictionary<'Key,'Value> option) =
        match values with
        | Some values -> match values.TryGetValue key with true, value -> ValueSome value | _ -> ValueNone
        | None -> ValueNone
    let set key value (values: Dictionary<'Key,'Value> option) =
        let values = match values with Some values -> values | None -> Dictionary()
        values.[key] <- value
        Some values

/// Immutable cold bodies share the existing closed-type publication owner.
module internal JsonDefaultMethods =
    let operationWithContract target representation contract role (create: unit -> MethodBase) =
        let key = struct(representation,contract,role)
        let select metadata = MetadataMap.tryFind key metadata.JsonDefaultMethods
        match TypeMetadataCache.Read(target,select) with
        | ValueSome method -> method.Value
        | ValueNone ->
            let method =
                TypeMetadataCache.Discover(target,select,
                    (fun method metadata -> { metadata with JsonDefaultMethods = MetadataMap.set key method metadata.JsonDefaultMethods }),
                    Func<JsonDefaultMethod>(fun () -> EmittedMethod(create())))
            method

    let methodWithContract target representation contract role create =
        (operationWithContract target representation contract role create).Operand

    let callable<'D when 'D :> Delegate> target representation contract role create : 'D =
        match operationWithContract target representation contract role create with
        | SuppliedBody body -> body :?> 'D
        | EmittedMethod method -> (method :?> MethodInfo).CreateDelegate(typeof<'D>) :?> 'D

    let withContract target representation contract role create =
        methodWithContract target representation contract role (fun () -> create() :> MethodBase) :?> DynamicMethod

    let get target representation role create = withContract target representation representation role create

    let publish target representation contract role (body: Delegate) =
        let key = struct(representation,contract,role)
        TypeMetadataCache.Register(target,fun metadata ->
            { metadata with JsonDefaultMethods = MetadataMap.set key { Generated = true; Value = SuppliedBody body } metadata.JsonDefaultMethods })
