namespace SoloDatabase

module internal FastStringDictionary =

    open System
    open System.Collections
    open System.Collections.Generic
    open System.Runtime.CompilerServices


    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline private hashString (s: string) : int =
        // Store only positive int hashes (use built-in string hash)
        let h = s.GetHashCode()
        h &&& 0x7fffffff

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline private nextPow2 (n: int) =
        let mutable v = 1
        while v < n do v <- v <<< 1
        v

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline private clampMin (x: int) (minv: int) = if x < minv then minv else x

    [<Sealed; AllowNullLiteral>]
    type FastDict<'V>(initialCapacity: int) =

        // hashes[i] meanings:
        //   0  => empty
        //  -1  => tombstone (deleted)
        //  >0  => occupied, storedHash = (hash + 1)
        let mutable hashes : int[] = Array.empty
        let mutable keys   : string[] = Array.empty
        let mutable values : 'V[] = Array.empty

        let mutable count = 0           // number of OCCUPIED slots
        let mutable tombstones = 0      // number of TOMBSTONE slots
        let mutable resizeThreshold = 0 // when (count + 1) exceeds threshold => grow

        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let mask () = hashes.Length - 1

        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let desiredSize (capacity: int) =
            // Keep load factor <= 0.70-ish to keep probe lengths tiny at N<=64.
            // size >= capacity / 0.70  => capacity * 10 / 7
            let want = (capacity * 10) / 7
            nextPow2 (clampMin want 8)

        let init (capacity: int) =
            let size = desiredSize capacity
            let size = max size 16
            hashes <- Array.zeroCreate size
            keys   <- Array.zeroCreate size
            values <- Array.zeroCreate size
            count <- 0
            tombstones <- 0
            // threshold ~ 70% occupancy (ignore tombstones; they trigger rehash separately)
            resizeThreshold <- (size * 7) / 10

        do init (clampMin initialCapacity 0)

        // Reinsert everything into a new table (used for grow or tombstone cleanup)
        member private this.Rehash(newSize: int) =
            let oldHashes = hashes
            let oldKeys   = keys
            let oldValues = values

            hashes <- Array.zeroCreate newSize
            keys   <- Array.zeroCreate newSize
            values <- Array.zeroCreate newSize
            count <- 0
            tombstones <- 0
            resizeThreshold <- (newSize * 7) / 10

            let m = newSize - 1

            for i = 0 to oldHashes.Length - 1 do
                let sh = oldHashes[i]
                if sh > 0 then
                    let k = oldKeys[i]
                    let v = oldValues[i]
                    let mutable idx = sh - 1
                    idx <- idx &&& m
                    // linear probe until empty
                    while hashes[idx] <> 0 do
                        idx <- (idx + 1) &&& m
                    hashes[idx] <- sh
                    keys[idx] <- k
                    values[idx] <- v
                    count <- count + 1

        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        member private this.MaybeRehashForTombstones() =
            // For small tables, tombstones quickly degrade probe length.
            // If tombstones are "noticeable", rebuild in-place (same size).
            if tombstones > (hashes.Length >>> 3) then // > 12.5%
                this.Rehash(hashes.Length)

        member private this.GrowIfNeeded(nextCount: int) =
            if nextCount > resizeThreshold then
                this.Rehash(hashes.Length <<< 1)
            else
                // If inserts keep happening after deletes, clean up occasionally.
                this.MaybeRehashForTombstones()

        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        member private this.KeyEquals(entryKey: string, queryKey: string) =
            // ReferenceEquals is a big win when "same set keys" are reused across instances.
            obj.ReferenceEquals(entryKey, queryKey) || entryKey = queryKey

        /// Finds an existing key index, or returns -1 if not found.
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        member private this.FindIndex(key: string, storedHash: int) : int =
            let m = mask()
            let mutable idx = (storedHash - 1) &&& m
            let mutable h = hashes[idx]
            let mutable res = -1
            let mutable done_ = false

            // Probe until empty (0) or found
            while not done_ do
                if h = 0 then
                    done_ <- true
                elif h = storedHash then
                    let ek = keys[idx]
                    if this.KeyEquals(ek, key) then
                        res <- idx
                        done_ <- true
                    else
                        idx <- (idx + 1) &&& m
                        h <- hashes[idx]
                else
                    idx <- (idx + 1) &&& m
                    h <- hashes[idx]

            res

        /// For insertion: returns (indexToUse, foundExisting).
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        member private this.FindSlotForInsert(key: string, storedHash: int) : struct (int * bool) =
            let m = mask()
            let mutable idx = (storedHash - 1) &&& m
            let mutable firstTomb = -1
            let mutable done_ = false
            let mutable slot = -1
            let mutable exists = false

            while not done_ do
                let h = hashes[idx]
                if h = 0 then
                    // empty => choose first tombstone if any
                    slot <- (if firstTomb >= 0 then firstTomb else idx)
                    exists <- false
                    done_ <- true
                elif h = -1 then
                    // tombstone: remember first and continue probing
                    if firstTomb < 0 then firstTomb <- idx
                    idx <- (idx + 1) &&& m
                elif h = storedHash then
                    // potential match
                    let ek = keys[idx]
                    if this.KeyEquals(ek, key) then
                        slot <- idx
                        exists <- true
                        done_ <- true
                    else
                        idx <- (idx + 1) &&& m
                else
                    idx <- (idx + 1) &&& m

            struct(slot, exists)

        member _.Count = count
        member _.Capacity = hashes.Length

        member this.TryGetValue(key: string, value: byref<'V>) : bool =
            if isNull key then nullArg (nameof key)
            let h = hashString key
            let sh = h + 1
            let idx = this.FindIndex(key, sh)
            if idx >= 0 then
                value <- values[idx]
                true
            else
                value <- Unchecked.defaultof<'V>
                false

        member this.ContainsKey(key: string) : bool =
            if isNull key then nullArg (nameof key)
            let sh = (hashString key) + 1
            this.FindIndex(key, sh) >= 0

        member this.AddOrUpdate(key: string, value: 'V) : unit =
            if isNull key then nullArg (nameof key)
            let sh = (hashString key) + 1
            let struct(slot, exists) = this.FindSlotForInsert(key, sh)
            if exists then
                values[slot] <- value
            else
                // ensure space BEFORE writing (so mask() stable)
                this.GrowIfNeeded(count + 1)
                // recompute after possible grow
                let sh2 = sh
                let struct(slot2, exists2) = this.FindSlotForInsert(key, sh2)
                if exists2 then
                    values[slot2] <- value
                else
                    if hashes[slot2] = -1 then tombstones <- tombstones - 1
                    hashes[slot2] <- sh2
                    keys[slot2] <- key
                    values[slot2] <- value
                    count <- count + 1

        member this.TryAdd(key: string, value: 'V) : bool =
            if isNull key then nullArg (nameof key)
            let sh = (hashString key) + 1

            let struct(slot0, exists0) = this.FindSlotForInsert(key, sh)
            if exists0 then false
            else
                this.GrowIfNeeded(count + 1)
                let struct(slot1, exists1) = this.FindSlotForInsert(key, sh)
                if exists1 then false
                else
                    if hashes[slot1] = -1 then tombstones <- tombstones - 1
                    hashes[slot1] <- sh
                    keys[slot1] <- key
                    values[slot1] <- value
                    count <- count + 1
                    true

        member this.Remove(key: string, removed: byref<'V>) : bool =
            if isNull key then nullArg (nameof key)
            let sh = (hashString key) + 1
            let idx = this.FindIndex(key, sh)
            if idx >= 0 then
                removed <- values[idx]
                // tombstone
                hashes[idx] <- -1
                keys[idx] <- null
                values[idx] <- Unchecked.defaultof<'V>
                count <- count - 1
                tombstones <- tombstones + 1

                this.MaybeRehashForTombstones()
                true
            else
                removed <- Unchecked.defaultof<'V>
                false

        member this.Clear() =
            Array.Clear(hashes, 0, hashes.Length)
            Array.Clear(keys, 0, keys.Length)
            Array.Clear(values, 0, values.Length)
            count <- 0
            tombstones <- 0

        member this.GetEnumerator() : IEnumerator<KeyValuePair<string,'V>> =
            let mutable i = 0
            { new IEnumerator<KeyValuePair<string,'V>> with
                member _.Current : KeyValuePair<string,'V> =
                    let k = keys[i-1]
                    KeyValuePair(k, values[i-1])
                member _.Current : obj =
                    box (KeyValuePair(keys[i-1], values[i-1]))
                member _.MoveNext() =
                    while i < hashes.Length && hashes[i] <= 0 do
                        i <- i + 1
                    if i >= hashes.Length then false
                    else
                        i <- i + 1
                        true
                member _.Reset() = i <- 0
                member _.Dispose() = () }

        interface IEnumerable<KeyValuePair<string,'V>> with
            member this.GetEnumerator() = this.GetEnumerator()
        interface IEnumerable with
            member this.GetEnumerator() =
                (this :> IEnumerable<KeyValuePair<string,'V>>).GetEnumerator() :> IEnumerator

        interface IDictionary<string,'V> with
            member this.Item
                with get (key: string) =
                    let mutable v = Unchecked.defaultof<'V>
                    if this.TryGetValue(key, &v) then v
                    else raise (KeyNotFoundException())
                and set (key: string) (value: 'V) =
                    this.AddOrUpdate(key, value)

            member _.Keys : ICollection<string> =
                // Materialize lazily; for N<=64 this is cheap.
                let arr =
                    let tmp = ResizeArray<string>(count)
                    for i = 0 to hashes.Length - 1 do
                        if hashes[i] > 0 then tmp.Add(keys[i])
                    tmp.ToArray()
                upcast arr

            member _.Values : ICollection<'V> =
                let arr =
                    let tmp = ResizeArray<'V>(count)
                    for i = 0 to hashes.Length - 1 do
                        if hashes[i] > 0 then tmp.Add(values[i])
                    tmp.ToArray()
                upcast arr

            member _.Count = count
            member _.IsReadOnly = false

            member this.Add(key: string, value: 'V) =
                if not (this.TryAdd(key, value)) then
                    raise (ArgumentException("An item with the same key has already been added."))

            member this.ContainsKey(key: string) = this.ContainsKey(key)

            member this.Remove(key: string) =
                let mutable removed = Unchecked.defaultof<'V>
                this.Remove(key, &removed)

            member this.TryGetValue(key: string, value: byref<'V>) =
                this.TryGetValue(key, &value)

            member this.Add(item: KeyValuePair<string,'V>) =
                (this :> IDictionary<string,'V>).Add(item.Key, item.Value)

            member this.Clear() = this.Clear()

            member this.Contains(item: KeyValuePair<string,'V>) =
                let mutable v = Unchecked.defaultof<'V>
                if this.TryGetValue(item.Key, &v) then
                    EqualityComparer<'V>.Default.Equals(v, item.Value)
                else false

            member this.CopyTo(array: KeyValuePair<string,'V>[], arrayIndex: int) =
                if isNull array then nullArg (nameof array)
                if arrayIndex < 0 then invalidArg (nameof arrayIndex) "Must be non-negative."
                if arrayIndex + count > array.Length then invalidArg (nameof array) "Not enough space."
                let mutable j = arrayIndex
                for i = 0 to hashes.Length - 1 do
                    if hashes[i] > 0 then
                        array[j] <- KeyValuePair(keys[i], values[i])
                        j <- j + 1

            member this.Remove(item: KeyValuePair<string,'V>) =
                if (this :> IDictionary<string,'V>).Contains(item) then
                    let mutable removed = Unchecked.defaultof<'V>
                    this.Remove(item.Key, &removed) |> ignore
                    true
                else false
