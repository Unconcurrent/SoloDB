namespace SoloDatabase

open System

/// Non-recursive cache storage helper for EventDbCache.
/// Keeps object/type/name slot management outside the recursive SoloDB.fs type group.
type internal EventDbCacheStore(maxCachedCollections: int) =
    let cachedCollections = Array.zeroCreate<obj> maxCachedCollections
    let cachedCollectionNames = Array.zeroCreate<string> maxCachedCollections
    let cachedCollectionTypes = Array.zeroCreate<Type> maxCachedCollections
    let mutable cachedCount = 0

    member _.TryGetCached<'U>(collectionName: string) =
        let targetType = typeof<'U>
        let mutable found = Unchecked.defaultof<ISoloDBCollection<'U> | null>
        let mutable i = 0
        while isNull found && i < cachedCount do
            let cached = cachedCollections.[i]
            if not (isNull cached) && cachedCollectionNames.[i] = collectionName && cachedCollectionTypes.[i] = targetType then
                found <- cached :?> ISoloDBCollection<'U>
            i <- i + 1
        if isNull found then None else Some found

    member _.InvalidateIfCached(collectionName: string) =
        for i in 0 .. cachedCount - 1 do
            let cached = cachedCollections.[i]
            if not (isNull cached) && cachedCollectionNames.[i] = collectionName then
                cachedCollections.[i] <- null
                cachedCollectionNames.[i] <- null
                cachedCollectionTypes.[i] <- null

    member _.TryStoreCached<'U>(collectionName: string) (collection: ISoloDBCollection<'U>) =
        let targetType = typeof<'U>
        let mutable slot = -1
        let mutable i = 0
        while slot < 0 && i < cachedCount do
            if isNull cachedCollections.[i] then
                slot <- i
            i <- i + 1

        if slot < 0 && cachedCount < maxCachedCollections then
            slot <- cachedCount
            cachedCount <- cachedCount + 1

        if slot >= 0 then
            cachedCollections.[slot] <- collection :> obj
            cachedCollectionNames.[slot] <- collectionName
            cachedCollectionTypes.[slot] <- targetType
            true
        else false

    member _.IsCachedName(collectionName: string) =
        let mutable found = false
        let mutable i = 0
        while not found && i < cachedCount do
            let cached = cachedCollections.[i]
            if not (isNull cached) && cachedCollectionNames.[i] = collectionName then
                found <- true
            i <- i + 1
        found
