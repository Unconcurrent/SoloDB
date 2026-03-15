namespace SoloDatabase

open Microsoft.Data.Sqlite
open System
open System.Linq.Expressions
open SQLiteTools

module internal CollectionSurfaceOps =
    let ensureIndex<'T, 'R>
        (name: string)
        (getConnection: unit -> SqliteConnection)
        (invalidate: unit -> unit)
        (expression: Expression<Func<'T, 'R>>) =
        use connection = getConnection()
        let result = Helper.ensureIndex name connection expression
        invalidate()
        result

    let ensureUniqueAndIndex<'T, 'R>
        (name: string)
        (getConnection: unit -> SqliteConnection)
        (invalidate: unit -> unit)
        (expression: Expression<Func<'T, 'R>>) =
        use connection = getConnection()
        let result = Helper.ensureUniqueAndIndex name connection expression
        invalidate()
        result

    let dropIndexIfExists<'T, 'R>
        (name: string)
        (getConnection: unit -> SqliteConnection)
        (invalidate: unit -> unit)
        (expression: Expression<Func<'T, 'R>>) =
        if isNull expression then raise (ArgumentNullException(nameof(expression)))
        let indexName, _whereSQL = Helper.getIndexWhereAndName<'T, 'R> name expression
        let indexSQL = $"DROP INDEX IF EXISTS \"{indexName}\""

        use connection = getConnection()
        let result = connection.Execute(indexSQL)
        invalidate()
        result

    let ensureAddedAttributeIndexes<'T>
        (name: string)
        (getConnection: unit -> SqliteConnection)
        (invalidate: unit -> unit) =
        if not (typeof<JsonSerializator.JsonValue>.IsAssignableFrom typeof<'T>) then
            use connection = getConnection()
            Helper.ensureDeclaredIndexesFields<'T> name connection
            invalidate()

    let onInserting<'T> (events: ISoloDBCollectionEvents<'T>) (handler: InsertingHandler<'T>) =
        events.OnInserting(handler)

    let onDeleting<'T> (events: ISoloDBCollectionEvents<'T>) (handler: DeletingHandler<'T>) =
        events.OnDeleting(handler)

    let onUpdating<'T> (events: ISoloDBCollectionEvents<'T>) (handler: UpdatingHandler<'T>) =
        events.OnUpdating(handler)

    let onInserted<'T> (events: ISoloDBCollectionEvents<'T>) (handler: InsertedHandler<'T>) =
        events.OnInserted(handler)

    let onDeleted<'T> (events: ISoloDBCollectionEvents<'T>) (handler: DeletedHandler<'T>) =
        events.OnDeleted(handler)

    let onUpdated<'T> (events: ISoloDBCollectionEvents<'T>) (handler: UpdatedHandler<'T>) =
        events.OnUpdated(handler)

    let unregisterInserting<'T> (events: ISoloDBCollectionEvents<'T>) (handler: InsertingHandler<'T>) =
        events.Unregister(handler)

    let unregisterDeleting<'T> (events: ISoloDBCollectionEvents<'T>) (handler: DeletingHandler<'T>) =
        events.Unregister(handler)

    let unregisterUpdating<'T> (events: ISoloDBCollectionEvents<'T>) (handler: UpdatingHandler<'T>) =
        events.Unregister(handler)

    let unregisterInserted<'T> (events: ISoloDBCollectionEvents<'T>) (handler: InsertedHandler<'T>) =
        events.Unregister(handler)

    let unregisterDeleted<'T> (events: ISoloDBCollectionEvents<'T>) (handler: DeletedHandler<'T>) =
        events.Unregister(handler)

    let unregisterUpdated<'T> (events: ISoloDBCollectionEvents<'T>) (handler: UpdatedHandler<'T>) =
        events.Unregister(handler)
