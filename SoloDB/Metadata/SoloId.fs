namespace SoloDatabase

open System
open SoloDatabase.Attributes

/// Single source of truth for [<SoloId>]-attribute reflection. Compiled before DBRef.fs and Relations.AccessorCache.fs;
/// consumed by both DBRef.From eager-capture and the Relations hydration / cascade stamping path.
[<AbstractClass; Sealed>]
type internal SoloIdAccessor =
    /// Acquires the shared declared identity without admitting its generator.
    static member TryGetMember(t: Type) : CustomIdMember voption =
        match CustomIdMetadata.GetMembers t with
        | [||] -> ValueNone
        | members -> ValueSome members.[0]

    static member TryGetGetter(t: Type) : Func<obj,obj> voption =
        match SoloIdAccessor.TryGetMember t with
        | ValueNone -> ValueNone
        | ValueSome memberValue -> ValueSome memberValue.Read

    /// Low-level accessor; does NOT enforce cascade-write or hydration invariants. ValueNone
    /// when entity is null, the type has no [<SoloId>], the value is null, or the value is a
    /// string that is null/empty/whitespace. Value-type defaults (Guid.Empty, 0L, default-
    /// struct) pass through as ValueSome regardless of whether they are user-chosen or
    /// default value — this accessor cannot tell the difference. Callers requiring non-default
    /// value-type SoloIds (cascade post-stamp, hydration paths) must use
    /// `extractSoloIdOrFail` or the equivalent default-literal check; relying on this
    /// accessor's return alone leaves the read-side invariant unenforced.
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
    /// stamping, and cascade processing). Same semantics as <see cref="TryGetValue"/>: only string null/
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
