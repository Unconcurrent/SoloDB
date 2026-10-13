namespace SoloDatabase

open System
open System.Collections.Generic
open System.Reflection
open System.Threading
open System.Linq.Expressions

module internal TypeMetadata =
    let internal registerFacts (facts: ModelTypeFacts) =
        if isNull facts then nullArg "facts"
        TypeMetadataCache.Register(facts.ModelType, fun metadata ->
            { metadata with TypeFacts = MetadataValue.generated facts
                            NullableUnderlying = match metadata.NullableUnderlying with ValueNone -> MetadataValue.generated null | value -> value })

    let internal registerNullableUnderlying modelType underlying =
        TypeMetadataCache.Register(modelType,fun metadata -> { metadata with NullableUnderlying = MetadataValue.generated underlying })

    let internal registerEnumUnderlying modelType underlying =
        TypeMetadataCache.Register(modelType,fun metadata -> { metadata with EnumUnderlying = MetadataValue.generated underlying })

    let internal nullableUnderlying (modelType: Type) =
        if not modelType.IsGenericType then null else
        TypeMetadataCache.Discover(modelType,(fun metadata -> metadata.NullableUnderlying),
            (fun value metadata -> { metadata with NullableUnderlying = ValueSome value }),
            Func<Type>(fun () -> Nullable.GetUnderlyingType modelType))

    let internal enumUnderlying (modelType: Type) =
        TypeMetadataCache.Discover(modelType,(fun metadata -> metadata.EnumUnderlying),
            (fun value metadata -> { metadata with EnumUnderlying = ValueSome value }),
            Func<Type>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "enum width" modelType
                Enum.GetUnderlyingType modelType))

    let internal enumValue modelType (value: int64) =
        let underlying = enumUnderlying modelType
        let converted = if typeof<uint64>.Equals(underlying) then box(uint64 value)
                        else Convert.ChangeType(value,underlying,Globalization.CultureInfo.InvariantCulture)
        Enum.ToObject(modelType,converted)

    let internal registerArguments (modelType: Type) (arguments: Type[]) =
        TypeMetadataCache.Register(modelType, fun metadata -> { metadata with GenericArguments = MetadataValue.generated (Array.copy arguments) })

    let internal registerRecordFields (modelType: Type) (fields: string[]) =
        let fields = if isNull fields then ValueNone else ValueSome(Array.copy fields)
        TypeMetadataCache.Register(modelType, fun metadata -> { metadata with RecordFields = MetadataValue.generated fields })

    let internal recordFields (modelType: Type) =
        TypeMetadataCache.Discover(modelType, (fun metadata -> metadata.RecordFields),
            (fun value metadata -> { metadata with RecordFields = ValueSome value }), Func<string[] voption>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "record construction" modelType
                if Microsoft.FSharp.Reflection.FSharpType.IsRecord modelType then
                    ValueSome(Microsoft.FSharp.Reflection.FSharpType.GetRecordFields modelType |> Array.map _.Name)
                else ValueNone))

    let internal isSealed (modelType: Type) =
        match MetadataValue.tryValue (TypeMetadataCache.Get(modelType).TypeFacts) with
        | ValueSome facts -> facts.IsSealed
        | _ -> modelType.IsSealed
    let internal typeIdentityKey (t: Type) =
        if Object.ReferenceEquals(t, null) then
            nullArg "t"
        match t.AssemblyQualifiedName with
        | null | "" ->
            if Object.ReferenceEquals(t.FullName, null) then t.Name else t.FullName
        | value -> value
    [<AbstractClass; Sealed>]
    type internal GenericMethodArgCache =
        static member Get(method: MethodInfo) =
            let owner = if isNull method.DeclaringType then typeof<MethodInfo> else method.DeclaringType
            TypeMetadataCache.Discover(owner, (fun metadata -> MetadataMap.tryFind method metadata.GenericMethodArguments),
                (fun operation metadata -> { metadata with GenericMethodArguments = MetadataMap.set method operation metadata.GenericMethodArguments }),
                Func<Type[]>(fun () -> method.GetGenericArguments()))

    [<AbstractClass; Sealed>]
    type internal GenericTypeArgCache =
        static member Get(t: Type) =
            TypeMetadataCache.Discover(t, (fun metadata -> metadata.GenericArguments),
                (fun operation metadata -> { metadata with GenericArguments = ValueSome operation }), Func<Type[]>(fun () -> t.GetGenericArguments()))

    /// For the F# compiler to allow the implicit use of
    /// the .NET Expression we need to use it in a C# style class.
    [<Sealed>][<AbstractClass>] // make it static
    type internal ExpressionHelper =
        static member inline internal get<'a, 'b>(expression: Expression<System.Func<'a, 'b>>) = expression

        static member inline internal min (e1: Expression) (e2: Expression) =
            // Produces an expression tree representing min(e1, e2) using LessThan
            Expression.Condition(
                Expression.LessThan(e1, e2),
                e1,
                e2
            )

        static member inline internal constant<'T> (x: 'T) =
            // Returns a strongly-typed constant expression for value x
            Expression.Constant(x, typeof<'T>)

    let internal registerType (modelType: Type) = TypeMetadataCache.RegisterName modelType

    let internal nameToType (typeName: string) = 
        TypeMetadataCache.ResolveName(typeName, Func<Type>(fun () -> 
                                            match typeName with
                                            | "Double" | "double" -> typeof<double>
                                            | "Single" | "float" -> typeof<float>
                                            | "Byte" | "byte" -> typeof<byte>
                                            | "SByte" | "sbyte" -> typeof<sbyte>
                                            | "Int16" | "short" -> typeof<Int16>
                                            | "UInt16" | "ushort" -> typeof<UInt16>
                                            | "Int32" | "int" -> typeof<int>
                                            | "UInt32" | "uint" -> typeof<uint>
                                            | "Int64" | "long" -> typeof<Int64>
                                            | "UInt64" | "ulong" -> typeof<UInt64>
                                            | "Char" | "char" -> typeof<char>
                                            | "Boolean" | "bool" -> typeof<bool>
                                            | "Object" | "object" -> typeof<obj>
                                            | "String" | "string" -> typeof<string>
                                            | "Decimal" | "decimal" -> typeof<decimal>
                                            | "DateTime" -> typeof<DateTime>
                                            | "Guid" -> typeof<Guid>
                                            | "TimeSpan" -> typeof<TimeSpan>
                                            | "IntPtr" -> typeof<IntPtr>
                                            | "UIntPtr" -> typeof<UIntPtr>
                                            | "Array" -> typeof<Array>
                                            | "Delegate" -> typeof<Delegate>
                                            | "MulticastDelegate" -> typeof<MulticastDelegate>
                                            | "IDisposable" -> typeof<System.IDisposable>
                                            | "Stream" -> typeof<System.IO.Stream>
                                            | "Exception" -> typeof<System.Exception>
                                            | "Thread" -> typeof<System.Threading.Thread>
                                            | typeName ->

                                                if not (MetadataRuntime.supportsDynamicCode()) then
                                                    MetadataRuntime.missing "stored type identity" typeof<Type>
                                                match Type.GetType(typeName) with
                                                | null -> 
                                                    match Type.GetType("System." + typeName) with
                                                    | null ->
                                                        AppDomain.CurrentDomain.GetAssemblies()
                                                            |> Seq.collect (fun a ->
                                                                try
                                                                    a.GetTypes() :> seq<Type>
                                                                with :? System.Reflection.ReflectionTypeLoadException as rtl ->
                                                                    rtl.Types |> Seq.choose (fun t -> if isNull t then None else Some t))
                                                            |> Seq.tryFind (fun t -> t.FullName = typeName)
                                                            |> Option.toObj
                                                    | fastType -> fastType
                                                | fastType -> fastType
                                            ))


    let internal requiresTypeInformation (t: Type) =
        match MetadataValue.tryValue (TypeMetadataCache.Get(t).TypeFacts) with
        | ValueSome facts -> facts.RequiresTypeInformation
        | _ when MetadataRuntime.supportsDynamicCode() ->
            TypeMetadataCache.Discover(t, (fun metadata -> metadata.TypeFacts), (fun value metadata -> { metadata with TypeFacts = ValueSome value }), Func<ModelTypeFacts>(fun () ->
                ModelTypeFacts(t, t.IsSealed, t.IsAbstract || not (Object.ReferenceEquals(t.GetCustomAttribute<Attributes.PolymorphicAttribute>(), null))))).RequiresTypeInformation
        | _ -> MetadataRuntime.missing "polymorphic type" t

    let internal requiresTypeInformationFor<'T> =
        requiresTypeInformation typeof<'T>
