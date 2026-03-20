namespace SoloDatabase

open System
open System.Linq.Expressions
open Microsoft.Data.Sqlite

module internal CollectionInstanceSurface =
    let ensureIndex<'T, 'R>
        (name: string)
        (getConnection: unit -> SqliteConnection)
        (invalidate: unit -> unit)
        (expression: Expression<Func<'T, 'R>>) =
        CollectionSurfaceOps.ensureIndex<'T, 'R> name getConnection invalidate expression

    let ensureUniqueAndIndex<'T, 'R>
        (name: string)
        (getConnection: unit -> SqliteConnection)
        (invalidate: unit -> unit)
        (expression: Expression<Func<'T, 'R>>) =
        CollectionSurfaceOps.ensureUniqueAndIndex<'T, 'R> name getConnection invalidate expression

    let dropIndexIfExists<'T, 'R>
        (name: string)
        (getConnection: unit -> SqliteConnection)
        (invalidate: unit -> unit)
        (expression: Expression<Func<'T, 'R>>) =
        CollectionSurfaceOps.dropIndexIfExists<'T, 'R> name getConnection invalidate expression

    let ensureAddedAttributeIndexes<'T>
        (name: string)
        (getConnection: unit -> SqliteConnection)
        (invalidate: unit -> unit) =
        CollectionSurfaceOps.ensureAddedAttributeIndexes<'T> name getConnection invalidate

    let onInserting<'T> (events: ISoloDBCollectionEvents<'T>) handler =
        CollectionSurfaceOps.onInserting events handler

    let onDeleting<'T> (events: ISoloDBCollectionEvents<'T>) handler =
        CollectionSurfaceOps.onDeleting events handler

    let onUpdating<'T> (events: ISoloDBCollectionEvents<'T>) handler =
        CollectionSurfaceOps.onUpdating events handler

    let onInserted<'T> (events: ISoloDBCollectionEvents<'T>) handler =
        CollectionSurfaceOps.onInserted events handler

    let onDeleted<'T> (events: ISoloDBCollectionEvents<'T>) handler =
        CollectionSurfaceOps.onDeleted events handler

    let onUpdated<'T> (events: ISoloDBCollectionEvents<'T>) handler =
        CollectionSurfaceOps.onUpdated events handler

    let unregisterInserting<'T> (events: ISoloDBCollectionEvents<'T>) (handler: InsertingHandler<'T>) =
        CollectionSurfaceOps.unregisterInserting events handler

    let unregisterDeleting<'T> (events: ISoloDBCollectionEvents<'T>) (handler: DeletingHandler<'T>) =
        CollectionSurfaceOps.unregisterDeleting events handler

    let unregisterUpdating<'T> (events: ISoloDBCollectionEvents<'T>) (handler: UpdatingHandler<'T>) =
        CollectionSurfaceOps.unregisterUpdating events handler

    let unregisterInserted<'T> (events: ISoloDBCollectionEvents<'T>) (handler: InsertedHandler<'T>) =
        CollectionSurfaceOps.unregisterInserted events handler

    let unregisterDeleted<'T> (events: ISoloDBCollectionEvents<'T>) (handler: DeletedHandler<'T>) =
        CollectionSurfaceOps.unregisterDeleted events handler

    let unregisterUpdated<'T> (events: ISoloDBCollectionEvents<'T>) (handler: UpdatedHandler<'T>) =
        CollectionSurfaceOps.unregisterUpdated events handler
