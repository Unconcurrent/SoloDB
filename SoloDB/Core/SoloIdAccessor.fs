namespace SoloDatabase

open System
open System.Collections.Concurrent
open System.Linq.Expressions
open System.Reflection
open SoloDatabase.Attributes

/// Single source of truth for [<SoloId>]-attribute reflection. Compiled before DBRef.fs and Relations.AccessorCache.fs;
/// consumed by both DBRef.From eager-capture and the Relations hydration / cascade stamping path.
[<AbstractClass; Sealed>]
type internal SoloIdAccessor =
    /// Cache resolves to the SoloId property AND a pre-compiled Expression-tree getter that boxes
    /// the value via Func<obj,obj>. The getter is the hot path used by every relation/JSON/heal
    /// site; PropertyInfo.GetValue reflection is restricted to the cache-miss attribute discovery
    /// step. Mirrors the compiled-delegate pattern in Relations.AccessorCache.fs.
    static let propCache = ConcurrentDictionary<Type, (PropertyInfo * Func<obj, obj>) voption>()

    static let resolve (t: Type) : (PropertyInfo * Func<obj, obj>) voption =
        let props = t.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
        let mutable found = ValueNone
        let mutable i = 0
        while found.IsNone && i < props.Length do
            let p = props.[i]
            let attr = p.GetCustomAttribute<SoloId>(true)
            if not (Utils.isNull attr) then
                let param = Expression.Parameter(typeof<obj>, "entity")
                let cast = Expression.Convert(param, t)
                let read = Expression.Property(cast, p)
                let boxed = Expression.Convert(read, typeof<obj>)
                let getter = Expression.Lambda<Func<obj, obj>>(boxed, param).Compile()
                found <- ValueSome (p, getter)
            i <- i + 1
        found

    static let getOrAddCacheEntry (t: Type) =
        match propCache.TryGetValue t with
        | true, v -> v
        | false, _ ->
            let v = resolve t
            propCache.[t] <- v
            v

    /// Returns the [<SoloId>]-marked property of the given type, if any. Cached.
    static member TryGetProperty (t: Type) : PropertyInfo voption =
        match getOrAddCacheEntry t with
        | ValueSome (p, _) -> ValueSome p
        | ValueNone -> ValueNone

    /// Returns the compiled getter for the [<SoloId>] property of the given type, if any. Cached.
    static member TryGetGetter (t: Type) : Func<obj, obj> voption =
        match getOrAddCacheEntry t with
        | ValueSome (_, g) -> ValueSome g
        | ValueNone -> ValueNone

    /// Reads the [<SoloId>] value from an entity instance, typed as 'TId. ValueNone when:
    ///   - entity is null;
    ///   - the type has no [<SoloId>];
    ///   - the value is null;
    ///   - the value is a string that is null/empty/whitespace (matches IIdGenerator.IsEmpty
    ///     conventions).
    /// Value-type defaults (Guid.Empty, 0L, default-struct) are user-chosen values and pass through
    /// as ValueSome — the heal-sweep + cascade-runs-IIdGenerator guarantees that persisted rows
    /// carry real SoloIds, so post-deserialize values are trusted whatever they are.
    static member TryGetValue<'T, 'TId> (entity: 'T) : 'TId voption =
        if obj.ReferenceEquals(entity, null) then ValueNone
        else
            match SoloIdAccessor.TryGetGetter typeof<'T> with
            | ValueNone -> ValueNone
            | ValueSome g ->
                let raw = g.Invoke(box entity)
                if obj.ReferenceEquals(raw, null) then ValueNone
                else
                    let typed = raw :?> 'TId
                    match box typed with
                    | :? string as s when String.IsNullOrWhiteSpace s -> ValueNone
                    | _ -> ValueSome typed

    /// Reads the [<SoloId>] value from a boxed entity, returning a boxed obj. Used by relation paths
    /// where the typed parameter is not statically known (descriptor-driven hydration, JSON List-2
    /// stamping, lazy-heal sweep). Same semantics as <see cref="TryGetValue"/>: only string null/
    /// whitespace is filtered; value-type defaults pass through as ValueSome.
    static member TryGetBoxedValue (targetType: Type, entity: obj) : obj voption =
        if obj.ReferenceEquals(entity, null) then ValueNone
        else
            match SoloIdAccessor.TryGetGetter targetType with
            | ValueNone -> ValueNone
            | ValueSome g ->
                let raw = g.Invoke entity
                if obj.ReferenceEquals(raw, null) then ValueNone
                else
                    match raw with
                    | :? string as s when String.IsNullOrWhiteSpace s -> ValueNone
                    | _ -> ValueSome raw
