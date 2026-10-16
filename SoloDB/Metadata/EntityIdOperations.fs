namespace SoloDatabase

open System
open System.Collections.Concurrent
open System.Threading
open SoloDatabase.Metadata.Generation

/// Writes physical identity while retaining the caller's value-type instance.
type internal EntityIdSetter<'T> = delegate of byref<'T> * int64 -> unit

/// Typed physical identity operations emitted over the original model type.
[<AllowNullLiteral; Sealed>]
type internal EntityIdOperations<'T>
    (read: Func<'T, int64>, write: EntityIdSetter<'T>,
     writeBoxed: Action<obj, int64>, trivialGetter: bool, ambiguous: bool, missingGetter: bool, indexed: bool, writeIndexed: bool, writable: bool) =
    let readBoxed =
        if isNull read then null
        else Func<obj, int64>(fun value -> read.Invoke(unbox<'T> value))
    do
        if isNull write <> isNull writeBoxed then
            invalidArg "writeBoxed" "Typed and boxed identity writers must be supplied together."
    member internal _.Reader = read
    member internal _.BoxedReader = readBoxed
    member internal _.Writer = write
    member internal _.BoxedWriter = writeBoxed
    member internal _.HasWritableId = writable
    member internal _.Ambiguous = ambiguous
    member internal _.MissingGetter = missingGetter
    member internal _.Indexed = indexed
    member internal _.WriteIndexed = writeIndexed
    member internal _.TrivialGetter = trivialGetter && not (isNull read) && not (isNull write)
/// Registers completed physical identity operations before model acquisition.
[<AbstractClass; Sealed>]
type internal EntityIdMetadata private () =
    static member Register<'T>(operations: EntityIdOperations<'T>) =
        if isNull operations then nullArg "operations"
        let functions = {
            TypedReader = operations.Reader :> Delegate
            TypedWriter = operations.Writer :> Delegate
            Reader = operations.BoxedReader
            Writer = operations.BoxedWriter
            HasWritableId = operations.HasWritableId
            TrivialGetter = operations.TrivialGetter
            Ambiguous = operations.Ambiguous; MissingGetter = operations.MissingGetter; Indexed = operations.Indexed; WriteIndexed = operations.WriteIndexed }
        TypeMetadataCache.Register(typeof<'T>, fun metadata -> { metadata with PhysicalIdentity = MetadataValue.generated functions })
    static member internal RequireReader(operations: PhysicalIdTypeOperations) =
        TypeSupport.RequireQueryProperty(false,not operations.MissingGetter,operations.Indexed)
    static member internal RequireWriter(operations: PhysicalIdTypeOperations) =
        TypeSupport.RequireNonIndexedProperty operations.WriteIndexed
    static member internal TryGet(modelType: Type) =
        let operations = MetadataValue.tryValue (TypeMetadataCache.Get(modelType).PhysicalIdentity)
        match operations with
        | ValueSome operations -> TypeSupport.RequireNamedProperty operations.Ambiguous
        | _ -> ()
        operations
