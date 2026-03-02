
/// Compiled-delegate cache for relation engine hot paths.
/// Pattern: reflect once at first access per type, compile LINQ expression, cache in ConcurrentDictionary.
/// Eliminates per-entity GetProperty/GetValue/SetValue/GetMethod/Invoke overhead.
module internal SoloDatabase.RelationsAccessorCache

open System
open System.Reflection
open System.Linq.Expressions
open System.Collections.Concurrent

/// Helper: Expression.Property with explicit PropertyInfo overload (avoids F# overload ambiguity).
let private propExpr (instance: Expression) (prop: PropertyInfo) : MemberExpression =
    Expression.Property(instance, prop)

// ─── Property accessor caches (getter / setter) ─────────────────────────────

let private getterCache = ConcurrentDictionary<struct(Type * string), Func<obj, obj>>()
let private setterCache = ConcurrentDictionary<struct(Type * string), Action<obj, obj>>()

let private buildGetter (t: Type) (n: string) =
    let prop = t.GetProperty(n, BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)
    if isNull prop then
        raise (InvalidOperationException($"Property '{n}' not found on type '{t.FullName}'."))
    let param = Expression.Parameter(typeof<obj>, "instance")
    let cast = Expression.Convert(param, t)
    let read = propExpr cast prop
    let boxed = Expression.Convert(read, typeof<obj>)
    Expression.Lambda<Func<obj, obj>>(boxed, param).Compile()

let private buildSetter (t: Type) (n: string) =
    let prop = t.GetProperty(n, BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)
    if isNull prop then
        raise (InvalidOperationException($"Property '{n}' not found on type '{t.FullName}'."))
    let instanceParam = Expression.Parameter(typeof<obj>, "instance")
    let valueParam = Expression.Parameter(typeof<obj>, "value")
    let castInstance = Expression.Convert(instanceParam, t)
    let castValue = Expression.Convert(valueParam, prop.PropertyType)
    let assign = Expression.Assign(propExpr castInstance prop, castValue)
    Expression.Lambda<Action<obj, obj>>(assign, instanceParam, valueParam).Compile()

let internal compiledGetter (ownerType: Type) (propName: string) =
    getterCache.GetOrAdd(struct(ownerType, propName), Func<struct(Type * string), Func<obj, obj>>(fun struct(t, n) ->
        buildGetter t n
    ))

let internal compiledSetter (ownerType: Type) (propName: string) =
    setterCache.GetOrAdd(struct(ownerType, propName), Func<struct(Type * string), Action<obj, obj>>(fun struct(t, n) ->
        buildSetter t n
    ))

// ─── PropertyInfo-keyed accessors (for descriptors that already hold PropertyInfo) ─────

let private propGetterCache = ConcurrentDictionary<PropertyInfo, Func<obj, obj>>()
let private propSetterCache = ConcurrentDictionary<PropertyInfo, Action<obj, obj>>()

let internal compiledPropGetter (prop: PropertyInfo) =
    propGetterCache.GetOrAdd(prop, fun (p: PropertyInfo) ->
        let param = Expression.Parameter(typeof<obj>, "instance")
        let cast = Expression.Convert(param, p.DeclaringType)
        let read = propExpr cast p
        let boxed = Expression.Convert(read, typeof<obj>)
        Expression.Lambda<Func<obj, obj>>(boxed, param).Compile()
    )

let internal compiledPropSetter (prop: PropertyInfo) =
    propSetterCache.GetOrAdd(prop, fun (p: PropertyInfo) ->
        let instanceParam = Expression.Parameter(typeof<obj>, "instance")
        let valueParam = Expression.Parameter(typeof<obj>, "value")
        let castInstance = Expression.Convert(instanceParam, p.DeclaringType)
        let castValue = Expression.Convert(valueParam, p.PropertyType)
        let assign = Expression.Assign(propExpr castInstance p, castValue)
        Expression.Lambda<Action<obj, obj>>(assign, instanceParam, valueParam).Compile()
    )

// ─── Int64 Id accessor: typed fast path ──────────────────────────────────────

let private int64IdReaderCache = ConcurrentDictionary<Type, Func<obj, int64>>()
let private int64IdWriterCache = ConcurrentDictionary<Type, Action<obj, int64>>()

let internal compiledInt64IdReader (entityType: Type) =
    int64IdReaderCache.GetOrAdd(entityType, fun (t: Type) ->
        let prop = t.GetProperty("Id", BindingFlags.Public ||| BindingFlags.Instance)
        if isNull prop || prop.PropertyType <> typeof<int64> then
            raise (InvalidOperationException($"Type '{t.FullName}' does not have a public int64 Id property."))
        let param = Expression.Parameter(typeof<obj>, "entity")
        let cast = Expression.Convert(param, t)
        let read = propExpr cast prop
        Expression.Lambda<Func<obj, int64>>(read, param).Compile()
    )

let internal compiledInt64IdWriter (entityType: Type) =
    int64IdWriterCache.GetOrAdd(entityType, fun (t: Type) ->
        let prop = t.GetProperty("Id", BindingFlags.Public ||| BindingFlags.Instance)
        if isNull prop || prop.PropertyType <> typeof<int64> || not prop.CanWrite then
            raise (InvalidOperationException($"Type '{t.FullName}' does not have a writable int64 Id property."))
        let instanceParam = Expression.Parameter(typeof<obj>, "entity")
        let valueParam = Expression.Parameter(typeof<int64>, "id")
        let cast = Expression.Convert(instanceParam, t)
        let assign = Expression.Assign(propExpr cast prop, valueParam)
        Expression.Lambda<Action<obj, int64>>(assign, instanceParam, valueParam).Compile()
    )

// ─── DBRef Id reader: reads .Id from a boxed DBRef<T> struct ─────────────────

let private dbRefIdReaderCache = ConcurrentDictionary<Type, Func<obj, int64>>()

let internal compiledDbRefIdReader (dbRefType: Type) =
    dbRefIdReaderCache.GetOrAdd(dbRefType, fun (t: Type) ->
        let idProp = t.GetProperty("Id", BindingFlags.Public ||| BindingFlags.Instance)
        if isNull idProp then
            let param = Expression.Parameter(typeof<obj>, "dbref")
            Expression.Lambda<Func<obj, int64>>(Expression.Constant(0L, typeof<int64>), param).Compile()
        else
            let param = Expression.Parameter(typeof<obj>, "dbref")
            let cast = Expression.Convert(param, t)
            let read = propExpr cast idProp
            let asInt64 : Expression =
                if idProp.PropertyType = typeof<int64> then read :> Expression
                else Expression.Convert(read, typeof<int64>) :> Expression
            Expression.Lambda<Func<obj, int64>>(asInt64, param).Compile()
    )

// ─── DBRef factory delegates ─────────────────────────────────────────────────

[<Struct>]
type internal DbRefFactories = {
    ToOrResolved: Func<int64, obj>
    Loaded: Func<int64, obj, obj>
}

let private dbRefFactoryCache = ConcurrentDictionary<Type, DbRefFactories>()

let internal compiledDbRefFactories (dbRefType: Type) =
    dbRefFactoryCache.GetOrAdd(dbRefType, fun (t: Type) ->
        let toMethod = t.GetMethod("To", BindingFlags.Public ||| BindingFlags.Static, null, [| typeof<int64> |], null)
        let resolvedMethod = t.GetMethod("Resolved", BindingFlags.NonPublic ||| BindingFlags.Public ||| BindingFlags.Static, null, [| typeof<int64> |], null)
        let factoryMethod =
            if not (isNull toMethod) then toMethod
            elif not (isNull resolvedMethod) then resolvedMethod
            else raise (InvalidOperationException($"No To(int64) or Resolved(int64) on '{t.FullName}'."))
        let toParam = Expression.Parameter(typeof<int64>, "id")
        let toCall = Expression.Call(factoryMethod, toParam)
        let toBoxed = Expression.Convert(toCall, typeof<obj>)
        let toDelegate = Expression.Lambda<Func<int64, obj>>(toBoxed, toParam).Compile()

        let loadedMethod =
            t.GetMethods(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Static)
            |> Array.find (fun m -> m.Name = "Loaded" && m.GetParameters().Length = 2)
        let idParam = Expression.Parameter(typeof<int64>, "id")
        let entityParam = Expression.Parameter(typeof<obj>, "entity")
        let entityType = loadedMethod.GetParameters().[1].ParameterType
        let castEntity = Expression.Convert(entityParam, entityType)
        let loadedCall = Expression.Call(loadedMethod, idParam, castEntity)
        let loadedBoxed = Expression.Convert(loadedCall, typeof<obj>)
        let loadedDelegate = Expression.Lambda<Func<int64, obj, obj>>(loadedBoxed, idParam, entityParam).Compile()

        { ToOrResolved = toDelegate; Loaded = loadedDelegate }
    )

// ─── ValueOption introspection ───────────────────────────────────────────────

[<Struct>]
type internal ValueOptionAccessors = {
    IsSome: Func<obj, bool>
    GetValue: Func<obj, obj>
}

let private valueOptionCache = ConcurrentDictionary<Type, ValueOptionAccessors voption>()

let internal compiledValueOptionAccessors (t: Type) =
    valueOptionCache.GetOrAdd(t, fun (t: Type) ->
        let isSomeProp = t.GetProperty("IsSome", BindingFlags.Public ||| BindingFlags.Instance)
        if isNull isSomeProp then
            ValueNone
        else
            let valueProp = t.GetProperty("Value", BindingFlags.Public ||| BindingFlags.Instance)
            if isNull valueProp then
                ValueNone
            else
                let param = Expression.Parameter(typeof<obj>, "opt")
                let cast = Expression.Convert(param, t)
                let isSomeRead = propExpr cast isSomeProp
                let isSomeLambda = Expression.Lambda<Func<obj, bool>>(isSomeRead, param).Compile()

                let param2 = Expression.Parameter(typeof<obj>, "opt")
                let cast2 = Expression.Convert(param2, t)
                let valueRead = propExpr cast2 valueProp
                let valueBoxed = Expression.Convert(valueRead, typeof<obj>)
                let valueLambda = Expression.Lambda<Func<obj, obj>>(valueBoxed, param2).Compile()

                ValueSome { IsSome = isSomeLambda; GetValue = valueLambda }
    )

// ─── Activator replacement: parameterless constructor ────────────────────────

let private ctorCache = ConcurrentDictionary<Type, Func<obj>>()

let internal compiledDefaultCtor (t: Type) =
    ctorCache.GetOrAdd(t, fun (t: Type) ->
        let newExpr = Expression.New(t : Type)
        let boxed = Expression.Convert(newExpr, typeof<obj>)
        Expression.Lambda<Func<obj>>(boxed).Compile()
    )

// ─── Add method delegate for DBRefMany<T> cloning ───────────────────────────

let private addMethodCache = ConcurrentDictionary<Type, Action<obj, obj>>()

let internal compiledAddMethod (collectionType: Type) =
    addMethodCache.GetOrAdd(collectionType, fun (t: Type) ->
        let addMethod =
            t.GetMethods(BindingFlags.Public ||| BindingFlags.Instance)
            |> Array.tryFind (fun m -> m.Name = "Add" && m.GetParameters().Length = 1)
        match addMethod with
        | None ->
            raise (InvalidOperationException($"No Add(T) method found on '{t.FullName}'."))
        | Some m ->
            let instanceParam = Expression.Parameter(typeof<obj>, "collection")
            let itemParam = Expression.Parameter(typeof<obj>, "item")
            let castInstance = Expression.Convert(instanceParam, t)
            let castItem = Expression.Convert(itemParam, m.GetParameters().[0].ParameterType)
            let call = Expression.Call(castInstance, m, castItem)
            Expression.Lambda<Action<obj, obj>>(call, instanceParam, itemParam).Compile()
    )

// ─── Serializer method cache (MakeGenericMethod + Invoke replacement) ────────

let private serializerCache = ConcurrentDictionary<struct(Type * bool), Func<obj, obj>>()

let internal compiledSerializer (targetType: Type) (includeType: bool) (baseMethod: MethodInfo) =
    serializerCache.GetOrAdd(struct(targetType, includeType), Func<struct(Type * bool), Func<obj, obj>>(fun struct(tt, _) ->
        let closedMethod = baseMethod.MakeGenericMethod(tt)
        let param = Expression.Parameter(typeof<obj>, "entity")
        let castParam = Expression.Convert(param, tt)
        let call = Expression.Call(closedMethod, castParam)
        let boxResult = Expression.Convert(call, typeof<obj>)
        Expression.Lambda<Func<obj, obj>>(boxResult, param).Compile()
    ))

// ─── Clone helpers: per-type property copier for cloneOwnerForReplaceMany ────

[<Struct>]
type internal CloneDescriptor = {
    CreateInstance: Func<obj>
    CopyScalars: Action<obj, obj>
    ManyProperties: PropertyInfo array
}

let private cloneDescriptorCache = ConcurrentDictionary<Type, CloneDescriptor>()

let internal compiledCloneDescriptor (ownerType: Type) =
    cloneDescriptorCache.GetOrAdd(ownerType, fun (t: Type) ->
        let ctor = compiledDefaultCtor t

        let props = t.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
        let manyProps = ResizeArray<PropertyInfo>()
        let scalarProps = ResizeArray<PropertyInfo>()

        for prop in props do
            if prop.CanRead && prop.CanWrite then
                if typeof<IDBRefManyInternal>.IsAssignableFrom(prop.PropertyType) then
                    manyProps.Add(prop)
                else
                    scalarProps.Add(prop)

        let srcParam = Expression.Parameter(typeof<obj>, "src")
        let dstParam = Expression.Parameter(typeof<obj>, "dst")
        let castSrc = Expression.Convert(srcParam, t)
        let castDst = Expression.Convert(dstParam, t)

        let assignments =
            [| for p in scalarProps do
                let read = propExpr castSrc p
                let write = Expression.Assign(propExpr castDst p, read)
                write :> Expression |]

        let body : Expression =
            if assignments.Length > 0 then
                Expression.Block(assignments) :> Expression
            else
                Expression.Empty() :> Expression

        let copyDelegate = Expression.Lambda<Action<obj, obj>>(body, srcParam, dstParam).Compile()

        { CreateInstance = ctor; CopyScalars = copyDelegate; ManyProperties = manyProps.ToArray() }
    )
