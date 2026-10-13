namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open SoloDatabase.Types

[<RequireQualifiedAccess>]
type internal FileSystemRefMutation =
    | Set of int64
    | Add of int64
    | Remove of int64
    | Clear

/// A database-local reference to an existing filesystem header.
type internal IFileSystemReferenceValue =
    abstract Id: int64

type internal IFileSystemRefMany =
    inherit IDBRefManyInternal
    abstract AddedIds: IReadOnlyCollection<int64>
    abstract RemovedIds: IReadOnlyCollection<int64>
    abstract AcceptChanges: loaded: bool -> unit

/// A reference to an existing file. Loaded Value is a header snapshot.
[<Struct; CustomEquality; CustomComparison>]
type FileRef =
    val private id: int64
    val private value: SoloDBFileHeader
    val private loaded: bool
    private new(id, value, loaded) = { id = id; value = value; loaded = loaded }
    member this.Id = this.id
    member this.HasValue = this.id <> 0L
    member this.IsLoaded = this.loaded
    member this.Value =
        if this.id = 0L then raise (InvalidOperationException("FileRef is empty. Check HasValue before accessing Value."))
        if not this.loaded then raise (InvalidOperationException("FileRef.Value is not loaded. Load the reference before accessing Value."))
        this.value
    static member None = Unchecked.defaultof<FileRef>
    static member To(id: int64) =
        if id <= 0L then raise (ArgumentOutOfRangeException(nameof id, id, "A filesystem reference requires a positive header Id."))
        FileRef(id, Unchecked.defaultof<_>, false)
    static member From(header: SoloDBFileHeader) =
        if obj.ReferenceEquals(header, null) then nullArg (nameof header)
        if header.Id <= 0L then raise (ArgumentOutOfRangeException(nameof header, "The referenced header must already exist."))
        FileRef(header.Id, header, true)
    static member internal Unloaded(id: int64) =
        if id = 0L then FileRef.None else FileRef.To(id)
    static member internal Loaded(id: int64, header: SoloDBFileHeader) = FileRef(id, header, true)
    override this.Equals(other: obj) = match other with | :? FileRef as value -> this.id = value.id | _ -> false
    override this.GetHashCode() = this.id.GetHashCode()
    interface IEquatable<FileRef> with
        member this.Equals(other) = this.id = other.id
    interface IComparable<FileRef> with
        member this.CompareTo(other) = compare this.id other.id
    interface IComparable with
        member this.CompareTo(other) =
            match other with
            | :? FileRef as value -> compare this.id value.id
            | _ -> raise (ArgumentException("Cannot compare different reference kinds.", nameof other))
    interface IFileSystemReferenceValue with
        member this.Id = this.id

/// A reference to an existing directory. Loaded Value is a header snapshot.
[<Struct; CustomEquality; CustomComparison>]
type DirectoryRef =
    val private id: int64
    val private value: SoloDBDirectoryHeader
    val private loaded: bool
    private new(id, value, loaded) = { id = id; value = value; loaded = loaded }
    member this.Id = this.id
    member this.HasValue = this.id <> 0L
    member this.IsLoaded = this.loaded
    member this.Value =
        if this.id = 0L then raise (InvalidOperationException("DirectoryRef is empty. Check HasValue before accessing Value."))
        if not this.loaded then raise (InvalidOperationException("DirectoryRef.Value is not loaded. Load the reference before accessing Value."))
        this.value
    static member None = Unchecked.defaultof<DirectoryRef>
    static member To(id: int64) =
        if id <= 0L then raise (ArgumentOutOfRangeException(nameof id, id, "A filesystem reference requires a positive header Id."))
        DirectoryRef(id, Unchecked.defaultof<_>, false)
    static member From(header: SoloDBDirectoryHeader) =
        if obj.ReferenceEquals(header, null) then nullArg (nameof header)
        if header.Id <= 0L then raise (ArgumentOutOfRangeException(nameof header, "The referenced header must already exist."))
        DirectoryRef(header.Id, header, true)
    static member internal Unloaded(id: int64) =
        if id = 0L then DirectoryRef.None else DirectoryRef.To(id)
    static member internal Loaded(id: int64, header: SoloDBDirectoryHeader) = DirectoryRef(id, header, true)
    override this.Equals(other: obj) = match other with | :? DirectoryRef as value -> this.id = value.id | _ -> false
    override this.GetHashCode() = this.id.GetHashCode()
    interface IEquatable<DirectoryRef> with
        member this.Equals(other) = this.id = other.id
    interface IComparable<DirectoryRef> with
        member this.CompareTo(other) = compare this.id other.id
    interface IComparable with
        member this.CompareTo(other) =
            match other with
            | :? DirectoryRef as value -> compare this.id value.id
            | _ -> raise (ArgumentException("Cannot compare different reference kinds.", nameof other))
    interface IFileSystemReferenceValue with
        member this.Id = this.id

/// Tracks explicit mutations without treating an excluded collection as an empty persisted set.
type internal FileSystemRefManyState<'T when 'T :> IFileSystemReferenceValue>() =
    let items = List<'T>()
    let original = HashSet<int64>()
    let added = HashSet<int64>()
    let removed = HashSet<int64>()
    let mutable loaded = false
    let mutable cleared = false
    let mutable changed = false
    member _.Items = items
    member _.IsLoaded = loaded
    member _.HasPendingMutations = changed
    member _.WasCleared = cleared
    member _.OriginalIds = original :> IReadOnlyCollection<int64>
    member _.AddedIds = added :> IReadOnlyCollection<int64>
    member _.RemovedIds = removed :> IReadOnlyCollection<int64>
    member _.Add(item: 'T) =
        if item.Id <= 0L then raise (ArgumentException("An empty reference cannot be a Many member.", nameof item))
        if not (items.Exists(fun value -> value.Id = item.Id)) then items.Add item
        removed.Remove item.Id |> ignore
        added.Add item.Id |> ignore
        changed <- true
    member _.Remove(item: 'T) =
        if item.Id <= 0L then raise (ArgumentException("An empty reference cannot be a Many member.", nameof item))
        let index = items.FindIndex(fun value -> value.Id = item.Id)
        let existed = index >= 0
        if existed then items.RemoveAt index
        added.Remove item.Id |> ignore
        removed.Add item.Id |> ignore
        changed <- true
        existed
    member _.Clear() =
        items.Clear()
        added.Clear()
        removed.Clear()
        cleared <- true
        changed <- true
    member _.Reset(ids: int64 seq) =
        original.Clear()
        for id in ids do original.Add id |> ignore
        loaded <- true
        cleared <- false
        changed <- false
        added.Clear()
        removed.Clear()
    member this.AcceptChanges(isLoaded: bool) =
        this.Reset(items |> Seq.map (fun item -> item.Id))
        loaded <- isLoaded
    member this.SetLoaded(values: obj seq, ids: int64 seq) =
        items.Clear()
        for value in values do items.Add(value :?> 'T)
        this.Reset(ids)

/// A set of file references, loaded by default and tracked for document updates.
type FileRefMany() =
    let state = FileSystemRefManyState<FileRef>()
    new(items: IEnumerable<FileRef>) as this =
        FileRefMany()
        then
            if isNull items then nullArg (nameof items)
            for item in items do this.Add(item)
            (this :> IFileSystemRefMany).AcceptChanges(true)
    member _.Count = state.Items.Count
    member _.IsLoaded = state.IsLoaded
    member _.HasPendingMutations = state.HasPendingMutations
    member _.WasCleared = state.WasCleared
    member _.Add(item: FileRef) = state.Add(item)
    member _.Remove(item: FileRef) = state.Remove(item)
    member _.Clear() = state.Clear()
    member _.Contains(item: FileRef) = state.Items.Contains(item)
    member _.Item with get(index: int) = state.Items.[index]
    member _.GetEnumerator() = (state.Items :> IEnumerable<FileRef>).GetEnumerator()
    interface IEnumerable<FileRef> with
        member this.GetEnumerator() = this.GetEnumerator()
    interface IEnumerable with
        member _.GetEnumerator() = (state.Items :> IEnumerable).GetEnumerator()
    interface IReadOnlyList<FileRef> with
        member this.Count = this.Count
        member this.Item with get(index: int) = this.[index]
    interface IFileSystemRefMany with
        member _.AddedIds = state.AddedIds
        member _.RemovedIds = state.RemovedIds
        member _.AcceptChanges(loaded) = state.AcceptChanges(loaded)
        member _.IsLoaded = state.IsLoaded
        member _.HasPendingMutations = state.HasPendingMutations
        member _.WasCleared = state.WasCleared
        member _.OriginalIds = state.OriginalIds
        member _.GetCurrentItemsBoxed() = state.Items |> Seq.cast<obj>
        member _.ResetTracker(ids) = state.Reset(ids)
        member _.SetLoadedBoxed values ids = state.SetLoaded(values, ids)

/// A set of directory references, loaded by default and tracked for document updates.
type DirectoryRefMany() =
    let state = FileSystemRefManyState<DirectoryRef>()
    new(items: IEnumerable<DirectoryRef>) as this =
        DirectoryRefMany()
        then
            if isNull items then nullArg (nameof items)
            for item in items do this.Add(item)
            (this :> IFileSystemRefMany).AcceptChanges(true)
    member _.Count = state.Items.Count
    member _.IsLoaded = state.IsLoaded
    member _.HasPendingMutations = state.HasPendingMutations
    member _.WasCleared = state.WasCleared
    member _.Add(item: DirectoryRef) = state.Add(item)
    member _.Remove(item: DirectoryRef) = state.Remove(item)
    member _.Clear() = state.Clear()
    member _.Contains(item: DirectoryRef) = state.Items.Contains(item)
    member _.Item with get(index: int) = state.Items.[index]
    member _.GetEnumerator() = (state.Items :> IEnumerable<DirectoryRef>).GetEnumerator()
    interface IEnumerable<DirectoryRef> with
        member this.GetEnumerator() = this.GetEnumerator()
    interface IEnumerable with
        member _.GetEnumerator() = (state.Items :> IEnumerable).GetEnumerator()
    interface IReadOnlyList<DirectoryRef> with
        member this.Count = this.Count
        member this.Item with get(index: int) = this.[index]
    interface IFileSystemRefMany with
        member _.AddedIds = state.AddedIds
        member _.RemovedIds = state.RemovedIds
        member _.AcceptChanges(loaded) = state.AcceptChanges(loaded)
        member _.IsLoaded = state.IsLoaded
        member _.HasPendingMutations = state.HasPendingMutations
        member _.WasCleared = state.WasCleared
        member _.OriginalIds = state.OriginalIds
        member _.GetCurrentItemsBoxed() = state.Items |> Seq.cast<obj>
        member _.ResetTracker(ids) = state.Reset(ids)
        member _.SetLoadedBoxed values ids = state.SetLoaded(values, ids)

