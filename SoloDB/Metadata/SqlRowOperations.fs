namespace SoloDatabase

open System
open System.Threading

/// The current statement column segment supplied to a typed row factory.
[<Sealed>]
type internal SqlRowShape internal (columns: NativeColumnLayout, first: int, finish: int) =
    /// The number of columns in this result segment.
    member _.Count = finish - first
    /// The absolute ordinal of the first column in the segment.
    member _.Start = first
    /// Returns the absolute ordinal of a named column, or minus one when absent.
    member _.Ordinal(name: string) = columns.Ordinal(name, first, finish)
    /// Validates a selected field only when its column belongs to this layout.
    member this.BindMember(name: string, writable: bool, indexed: bool) =
        let ordinal = this.Ordinal name
        if ordinal >= 0 && indexed then invalidArg "memberInfo" "A query member requires a field or non-indexed property."
        if ordinal >= 0 && not writable then raise(ArgumentException("Expression must be writeable", "left"))
        ordinal
    /// Retains tuple column admission diagnostics for typed factories.
    member _.RequireTupleColumns(tupleName: string, required: int) =
        if required > finish - first then
            raise (InvalidOperationException($"Tuple arity mismatch: tuple type '{tupleName}' requires more than {finish - first} columns from startIndex {first}."))
    /// Retains non-nullable positional tuple element diagnostics.
    static member TupleNull(elementName: string, ordinal: int) =
        InvalidOperationException($"DB null in non-nullable tuple element {ordinal} of type '{elementName}'.")
    /// Returns the name at a segment-relative column position.
    member _.Name(index: int) =
        if index < 0 || index >= finish - first then raise (ArgumentOutOfRangeException("index"))
        columns.Names.[first + index]

/// Completed typed row operations registered before the model's first query.
[<AllowNullLiteral; Sealed>]
type internal SqlRowMapping<'T>(bind: Func<SqlRowShape, Func<SqlRow, 'T>>) =
    do if isNull bind then nullArg "bind"
    member internal _.Binder = bind
    member internal _.Bind(shape) =
        let mapper = bind.Invoke(shape)
        if isNull mapper then raise (InvalidOperationException("The typed row factory returned no mapper."))
        mapper
    static member internal CurrentBinder =
        match MetadataValue.tryValue (TypeMetadataCache.Get(typeof<'T>).SqlRows) with
        | ValueSome operation -> operation :?> Func<SqlRowShape,Func<SqlRow,'T>>
        | ValueNone -> null
    /// Publishes an immutable row factory shared by ordinary and compiled SQL reads.
    static member Register(mapping: SqlRowMapping<'T>) =
        if isNull mapping then nullArg "mapping"
        TypeMetadataCache.Register(typeof<'T>, fun metadata -> { metadata with SqlRows = MetadataValue.generated (mapping.Binder :> Delegate) })

/// Supplies the existing empty-result default through an original typed constructor.
[<AllowNullLiteral; Sealed>]
type internal SqlRowDefault<'T>(create: Func<'T>) =
    do if isNull create then nullArg "create"
    member internal _.Factory = create
    member internal _.Create() = create.Invoke()
    static member internal CreateCurrent() =
        match MetadataValue.tryValue (TypeMetadataCache.Get(typeof<'T>).SqlDefault) with
        | ValueSome operation -> ValueSome ((operation :?> Func<'T>).Invoke())
        | ValueNone -> ValueNone
    static member Register(factory: SqlRowDefault<'T>) =
        if isNull factory then nullArg "factory"
        TypeMetadataCache.Register(typeof<'T>, fun metadata -> { metadata with SqlDefault = MetadataValue.generated (factory.Factory :> Delegate) })
    /// Retains the established constructor-exception wrapper for tuple default leaves.
    static member Construct(factory: Func<'T>) =
        try factory.Invoke()
        with error -> raise (System.Reflection.TargetInvocationException(error))
