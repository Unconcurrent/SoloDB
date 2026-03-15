namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic

// ─── IDBRefManyInternal: non-generic interface for Relations.fs ───────────────

/// <summary>
/// Non-generic interface for accessing DBRefMany change tracking state from Relations.fs
/// without requiring the generic type parameter. Internal to the SoloDB assembly.
/// </summary>
type internal IDBRefManyInternal =
    /// True if this collection was populated by the query pipeline or reset after Insert.
    abstract IsLoaded: bool

    /// True if any mutation (Add/Remove/Clear) was called since construction or last reset.
    abstract HasPendingMutations: bool

    /// True if Clear() was called since construction or last reset.
    abstract WasCleared: bool

    /// Ids present at the last load/reset checkpoint (snapshot before user mutations).
    abstract OriginalIds: IReadOnlyCollection<int64>

    /// Current items as boxed objects. Relations.fs uses HasTypeId to extract Ids.
    abstract GetCurrentItemsBoxed: unit -> obj seq

    /// Reset change tracking after Insert or Update commits.
    /// committedIds: the set of Ids that are now linked in the database.
    abstract ResetTracker: committedIds: int64 seq -> unit

    /// Populate this collection from the query pipeline (batch loading).
    /// items: boxed typed items. ids: their corresponding database Ids.
    abstract SetLoadedBoxed: items: obj seq -> ids: int64 seq -> unit


/// <summary>
/// A typed collection of references to entities of type 'T in another collection.
/// Not stored in JSON. All data lives in a link table with FK RESTRICT.
/// Loaded by default via batched subquery when querying.
/// Implements <see cref="IList{T}"/> with change tracking for diff-on-Update.
/// </summary>
/// <remarks>
/// Mutations (Add, Remove, Clear, indexer) are tracked. When the parent entity is updated
/// via <c>collection.Update(entity)</c>, the engine computes a diff between the original
/// loaded state and the current state, and applies link row insertions/deletions accordingly.
/// Removing an item from the list and calling Update only unlinks it — the entity always survives.
/// </remarks>
type DBRefMany<'T>() =
    let _originalIds = HashSet<int64>()
    let _currentItems = List<'T>()
    let mutable _isLoaded = false
    let mutable _wasCleared = false
    let mutable _hasPendingMutations = false

    // ─── Public properties ────────────────────────────────────────────────────

    /// <summary>Number of items currently in the collection.</summary>
    member _.Count = _currentItems.Count

    /// <summary>True if populated from query or after Insert (TRACKER-RESET).</summary>
    member _.IsLoaded = _isLoaded

    /// <summary>True if any mutation (Add/Remove/Clear) was called since construction or last reset.</summary>
    member _.HasPendingMutations = _hasPendingMutations

    /// <summary>True if Clear() was called since construction or last reset.</summary>
    member _.WasCleared = _wasCleared

    // ─── Internal: for Relations.fs ───────────────────────────────────────────

    /// The set of Ids that were present at the last load/reset checkpoint.
    member internal _.OriginalIds = _originalIds

    /// Direct typed access to current items list. Relations.fs uses this for typed operations.
    member internal _.CurrentItems = _currentItems

    /// Set loaded state from typed items (used by Relations.fs batch loading pipeline).
    member internal _.SetLoaded(items: 'T seq, ids: int64 seq) =
        _currentItems.Clear()
        _currentItems.AddRange items
        _originalIds.Clear()
        for id in ids do
            _originalIds.Add id |> ignore
        _isLoaded <- true
        _wasCleared <- false
        _hasPendingMutations <- false

    /// Reset change tracker after successful commit (Insert or Update).
    member internal _.ResetTrackerTyped(committedIds: int64 seq) =
        _originalIds.Clear()
        for id in committedIds do
            _originalIds.Add id |> ignore
        _isLoaded <- true
        _wasCleared <- false
        _hasPendingMutations <- false

    // ─── IDBRefManyInternal (non-generic interface for Relations.fs) ──────────

    interface IDBRefManyInternal with
        member _.IsLoaded = _isLoaded

        member _.HasPendingMutations = _hasPendingMutations

        member _.WasCleared = _wasCleared

        member _.OriginalIds = _originalIds :> IReadOnlyCollection<int64>

        member _.GetCurrentItemsBoxed() =
            _currentItems |> Seq.cast<obj>

        member _.ResetTracker(committedIds: int64 seq) =
            _originalIds.Clear()
            for id in committedIds do
                _originalIds.Add id |> ignore
            _isLoaded <- true
            _wasCleared <- false
            _hasPendingMutations <- false

        member _.SetLoadedBoxed (items: obj seq) (ids: int64 seq) =
            _currentItems.Clear()
            for item in items do
                _currentItems.Add(item :?> 'T)
            _originalIds.Clear()
            for id in ids do
                _originalIds.Add id |> ignore
            _isLoaded <- true
            _wasCleared <- false
            _hasPendingMutations <- false

    // ─── IList<'T> ───────────────────────────────────────────────────────────

    /// <summary>Adds an item to the collection. Tracked for diff-on-Update.</summary>
    member _.Add(item: 'T) = _currentItems.Add item; _hasPendingMutations <- true

    /// <summary>Removes the first occurrence of an item. Tracked for diff-on-Update.</summary>
    member _.Remove(item: 'T) =
        let removed = _currentItems.Remove item
        if removed then _hasPendingMutations <- true
        removed

    /// <summary>Removes the item at the specified index. Tracked for diff-on-Update.</summary>
    member _.RemoveAt(index: int) = _currentItems.RemoveAt index; _hasPendingMutations <- true

    /// <summary>Removes all items. All original links will be deleted on Update.</summary>
    member _.Clear() = _currentItems.Clear(); _wasCleared <- true; _hasPendingMutations <- true

    /// <summary>Determines whether the collection contains a specific item.</summary>
    member _.Contains(item: 'T) = _currentItems.Contains item

    /// <summary>Returns the index of the first occurrence of an item, or -1 if not found.</summary>
    member _.IndexOf(item: 'T) = _currentItems.IndexOf item

    /// <summary>Inserts an item at the specified index. Tracked for diff-on-Update.</summary>
    member _.Insert(index: int, item: 'T) = _currentItems.Insert(index, item); _hasPendingMutations <- true

    /// <summary>Copies the elements to an array, starting at a particular array index.</summary>
    member _.CopyTo(array: 'T array, arrayIndex: int) = _currentItems.CopyTo(array, arrayIndex)

    /// <summary>Gets or sets the element at the specified index.</summary>
    member _.Item
        with get(index: int) = _currentItems.[index]
        and set (index: int) (value: 'T) =
            if not (EqualityComparer<'T>.Default.Equals(_currentItems.[index], value)) then
                _currentItems.[index] <- value
                _hasPendingMutations <- true

    /// <summary>Returns an enumerator that iterates through the collection.</summary>
    member _.GetEnumerator() : IEnumerator<'T> =
        (_currentItems :> IEnumerable<'T>).GetEnumerator()

    interface IList<'T> with
        member this.Count = this.Count
        member _.IsReadOnly = false
        member this.Item
            with get i = this.[i]
            and set i v = this.[i] <- v
        member this.Add item = this.Add item
        member this.Remove item = this.Remove item
        member this.RemoveAt i = this.RemoveAt i
        member this.Clear() = this.Clear()
        member this.Contains item = this.Contains item
        member this.CopyTo(arr, i) = this.CopyTo(arr, i)
        member this.IndexOf item = this.IndexOf item
        member this.Insert(i, item) = this.Insert(i, item)

    interface IEnumerable<'T> with
        member this.GetEnumerator() : IEnumerator<'T> = this.GetEnumerator()

    interface IEnumerable with
        member _.GetEnumerator() =
            (_currentItems :> IEnumerable).GetEnumerator()

    interface IReadOnlyList<'T> with
        member this.Count = this.Count
        member this.Item with get i = this.[i]

    override _.ToString() =
        if _isLoaded then
            sprintf "DBRefMany<%s> [%d items]" typeof<'T>.Name _currentItems.Count
        else
            sprintf "DBRefMany<%s> [not loaded]" typeof<'T>.Name


/// <summary>
/// A typed collection of references to entities of type <typeparamref name="TTarget"/> using a custom Id type <typeparamref name="TId"/> on the target.
/// Not stored in JSON. All data lives in a link table with FK RESTRICT.
/// Loaded by default via batched subquery when querying.
/// Implements <see cref="IList{T}"/> with change tracking for diff-on-Update.
/// </summary>
/// <remarks>
/// The <typeparamref name="TId"/> parameter is used at schema-validation time to verify that the target type
/// has exactly one <c>[SoloId]</c> property matching the <typeparamref name="TId"/> type.
/// At runtime, behavior is identical to <see cref="DBRefMany{T}"/> — link rows use int64 rowids.
/// </remarks>
type DBRefMany<'TTarget, 'TId>() =
    let _originalIds = HashSet<int64>()
    let _currentItems = List<'TTarget>()
    let mutable _isLoaded = false
    let mutable _wasCleared = false
    let mutable _hasPendingMutations = false

    // ─── Public properties ────────────────────────────────────────────────────

    /// <summary>Number of items currently in the collection.</summary>
    member _.Count = _currentItems.Count

    /// <summary>True if populated from query or after Insert (TRACKER-RESET).</summary>
    member _.IsLoaded = _isLoaded

    /// <summary>True if any mutation (Add/Remove/Clear) was called since construction or last reset.</summary>
    member _.HasPendingMutations = _hasPendingMutations

    /// <summary>True if Clear() was called since construction or last reset.</summary>
    member _.WasCleared = _wasCleared

    // ─── Internal: for Relations.fs ───────────────────────────────────────────

    /// The set of Ids that were present at the last load/reset checkpoint.
    member internal _.OriginalIds = _originalIds

    /// Direct typed access to current items list. Relations.fs uses this for typed operations.
    member internal _.CurrentItems = _currentItems

    /// Set loaded state from typed items (used by Relations.fs batch loading pipeline).
    member internal _.SetLoaded(items: 'TTarget seq, ids: int64 seq) =
        _currentItems.Clear()
        _currentItems.AddRange items
        _originalIds.Clear()
        for id in ids do
            _originalIds.Add id |> ignore
        _isLoaded <- true
        _wasCleared <- false
        _hasPendingMutations <- false

    /// Reset change tracker after successful commit (Insert or Update).
    member internal _.ResetTrackerTyped(committedIds: int64 seq) =
        _originalIds.Clear()
        for id in committedIds do
            _originalIds.Add id |> ignore
        _isLoaded <- true
        _wasCleared <- false
        _hasPendingMutations <- false

    // ─── IDBRefManyInternal (non-generic interface for Relations.fs) ──────────

    interface IDBRefManyInternal with
        member _.IsLoaded = _isLoaded

        member _.HasPendingMutations = _hasPendingMutations

        member _.WasCleared = _wasCleared

        member _.OriginalIds = _originalIds :> IReadOnlyCollection<int64>

        member _.GetCurrentItemsBoxed() =
            _currentItems |> Seq.cast<obj>

        member _.ResetTracker(committedIds: int64 seq) =
            _originalIds.Clear()
            for id in committedIds do
                _originalIds.Add id |> ignore
            _isLoaded <- true
            _wasCleared <- false
            _hasPendingMutations <- false

        member _.SetLoadedBoxed (items: obj seq) (ids: int64 seq) =
            _currentItems.Clear()
            for item in items do
                _currentItems.Add(item :?> 'TTarget)
            _originalIds.Clear()
            for id in ids do
                _originalIds.Add id |> ignore
            _isLoaded <- true
            _wasCleared <- false
            _hasPendingMutations <- false

    // ─── IList<'TTarget> ───────────────────────────────────────────────────────

    /// <summary>Adds an item to the collection. Tracked for diff-on-Update.</summary>
    member _.Add(item: 'TTarget) = _currentItems.Add item; _hasPendingMutations <- true

    /// <summary>Removes the first occurrence of an item. Tracked for diff-on-Update.</summary>
    member _.Remove(item: 'TTarget) =
        let removed = _currentItems.Remove item
        if removed then _hasPendingMutations <- true
        removed

    /// <summary>Removes the item at the specified index. Tracked for diff-on-Update.</summary>
    member _.RemoveAt(index: int) = _currentItems.RemoveAt index; _hasPendingMutations <- true

    /// <summary>Removes all items. All original links will be deleted on Update.</summary>
    member _.Clear() = _currentItems.Clear(); _wasCleared <- true; _hasPendingMutations <- true

    /// <summary>Determines whether the collection contains a specific item.</summary>
    member _.Contains(item: 'TTarget) = _currentItems.Contains item

    /// <summary>Returns the index of the first occurrence of an item, or -1 if not found.</summary>
    member _.IndexOf(item: 'TTarget) = _currentItems.IndexOf item

    /// <summary>Inserts an item at the specified index. Tracked for diff-on-Update.</summary>
    member _.Insert(index: int, item: 'TTarget) = _currentItems.Insert(index, item); _hasPendingMutations <- true

    /// <summary>Copies the elements to an array, starting at a particular array index.</summary>
    member _.CopyTo(array: 'TTarget array, arrayIndex: int) = _currentItems.CopyTo(array, arrayIndex)

    /// <summary>Gets or sets the element at the specified index.</summary>
    member _.Item
        with get(index: int) = _currentItems.[index]
        and set (index: int) (value: 'TTarget) =
            if not (EqualityComparer<'TTarget>.Default.Equals(_currentItems.[index], value)) then
                _currentItems.[index] <- value
                _hasPendingMutations <- true

    /// <summary>Returns an enumerator that iterates through the collection.</summary>
    member _.GetEnumerator() : IEnumerator<'TTarget> =
        (_currentItems :> IEnumerable<'TTarget>).GetEnumerator()

    interface IList<'TTarget> with
        member this.Count = this.Count
        member _.IsReadOnly = false
        member this.Item
            with get i = this.[i]
            and set i v = this.[i] <- v
        member this.Add item = this.Add item
        member this.Remove item = this.Remove item
        member this.RemoveAt i = this.RemoveAt i
        member this.Clear() = this.Clear()
        member this.Contains item = this.Contains item
        member this.CopyTo(arr, i) = this.CopyTo(arr, i)
        member this.IndexOf item = this.IndexOf item
        member this.Insert(i, item) = this.Insert(i, item)

    interface IEnumerable<'TTarget> with
        member this.GetEnumerator() : IEnumerator<'TTarget> = this.GetEnumerator()

    interface IEnumerable with
        member _.GetEnumerator() =
            (_currentItems :> IEnumerable).GetEnumerator()

    interface IReadOnlyList<'TTarget> with
        member this.Count = this.Count
        member this.Item with get i = this.[i]

    override _.ToString() =
        if _isLoaded then
            sprintf "DBRefMany<%s, %s> [%d items]" typeof<'TTarget>.Name typeof<'TId>.Name _currentItems.Count
        else
            sprintf "DBRefMany<%s, %s> [not loaded]" typeof<'TTarget>.Name typeof<'TId>.Name
