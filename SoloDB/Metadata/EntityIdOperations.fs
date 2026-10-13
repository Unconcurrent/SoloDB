namespace SoloDatabase

open System
open System.Collections.Concurrent
open System.Threading

/// Writes physical identity while retaining the caller's value-type instance.
type internal EntityIdSetter<'T> = delegate of byref<'T> * int64 -> unit

/// Typed physical identity operations emitted over the original model type.
[<AllowNullLiteral; Sealed>]
type internal EntityIdOperations<'T>
    (read: Func<'T, int64>, write: EntityIdSetter<'T>,
     writeBoxed: Action<obj, int64>, trivialGetter: bool) =
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
    member internal _.HasWritableId = not (isNull read) && not (isNull write)
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
            TrivialGetter = operations.TrivialGetter }
        TypeMetadataCache.Register(typeof<'T>, fun metadata -> { metadata with PhysicalIdentity = MetadataValue.generated functions })
    static member internal TryGet(modelType: Type) =
        MetadataValue.tryValue (TypeMetadataCache.Get(modelType).PhysicalIdentity)
