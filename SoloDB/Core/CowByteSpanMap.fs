namespace SoloDatabase

open System
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Threading

module internal CowByteSpanMap =
    let inline private hashFunction x = Hashing.fnvFast x

    [<Struct>]
    type private CowUtf8SpanMapEntry<'TValue> =
        val Key : byte[]
        val Hash : uint32
        val Value : 'TValue
        new (key: byte[], hash:uint32, value:'TValue) = { Key = key; Hash = hash; Value = value }

    type internal CowByteSpanMapValueFactory<'TValue> = delegate of ReadOnlySpan<byte> -> 'TValue

    [<Sealed>]
    type internal CowByteSpanMap<'TValue> internal () =
        [<Literal>]
        let LinearReadThreshold = 64

        let mutable writeLock = 0
        // Immutable snapshot. Readers only ever see fully-initialized arrays.
        let mutable entries : CowUtf8SpanMapEntry<'TValue>[] = Array.empty

        member _.Count =
            Volatile.Read(&entries).Length

        // ---------------- Reads (never lock) ----------------

        member this.ContainsKey(key: ReadOnlySpan<byte>) : bool =
            let mutable tmp = Unchecked.defaultof<'TValue>
            this.TryGetValue(key, &tmp)

        member _.TryGetValue(key: ReadOnlySpan<byte>, value: byref<'TValue>) : bool =
            let localEntries = Volatile.Read(&entries)
            let n = localEntries.Length

            if n = 0 then
                value <- Unchecked.defaultof<'TValue>
                false
            elif n <= LinearReadThreshold then
                CowByteSpanMap.TryGetLinear(localEntries, key, &value)
            else
                CowByteSpanMap.TryGetBinary(localEntries, key, &value)

        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        static member private TryGetLinear(entries: CowUtf8SpanMapEntry<'TValue>[], key: ReadOnlySpan<byte>, value: byref<'TValue>) : bool =
            let h = hashFunction key
            let mutable i = 0
            let mutable found = false

            while (not found) && i < entries.Length do
                let e = entries[i]
                if e.Hash = h then
                    let k = e.Key
                    if k.Length = key.Length && key.SequenceEqual(ReadOnlySpan<byte>(k)) then
                        value <- e.Value
                        found <- true
                i <- i + 1

            if found then true
            else
                value <- Unchecked.defaultof<'TValue>
                false

        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        static member private TryGetBinary(entries: CowUtf8SpanMapEntry<'TValue>[], key: ReadOnlySpan<byte>, value: byref<'TValue>) : bool =
            let mutable found = false
            let idx = CowByteSpanMap.BinarySearch(entries, key, &found)
            if found then
                value <- entries[idx].Value
                true
            else
                value <- Unchecked.defaultof<'TValue>
                false

        // ---------------- Writes (always lock) ----------------

        member _.TryAdd(key: ReadOnlySpan<byte>, value:'TValue) : bool =
            while Interlocked.CompareExchange(&writeLock, 1, 0) <> 0 do
                if not (Thread.Yield ()) then Thread.SpinWait(1)
            try
                let cur = entries
                let hash = hashFunction key

                let mutable found = false
                let idx = CowByteSpanMap.BinarySearch(cur, key, &found)
                if found then false
                else
                    let newKey = key.ToArray()
                    let next = Array.zeroCreate<CowUtf8SpanMapEntry<'TValue>> (cur.Length + 1)

                    if idx > 0 then
                        Array.Copy(cur, 0, next, 0, idx)

                    next[idx] <- CowUtf8SpanMapEntry<'TValue>(newKey, hash, value)

                    if idx < cur.Length then
                        Array.Copy(cur, idx, next, idx + 1, cur.Length - idx)

                    Volatile.Write(&entries, next)
                    true
            finally
                Volatile.Write(&writeLock, 0)

        member _.AddOrUpdate(key: ReadOnlySpan<byte>, value:'TValue) : unit =
            while Interlocked.CompareExchange(&writeLock, 1, 0) <> 0 do
                if not (Thread.Yield ()) then Thread.SpinWait(1)
            try
                let cur = entries
                let hash = hashFunction key

                let mutable found = false
                let idx = CowByteSpanMap.BinarySearch(cur, key, &found)

                if found then
                    let next = cur.Clone() :?> CowUtf8SpanMapEntry<'TValue>[]
                    let old = cur[idx]
                    next[idx] <- CowUtf8SpanMapEntry<'TValue>(old.Key, old.Hash, value)
                    Volatile.Write(&entries, next)
                else
                    let newKey = key.ToArray()
                    let next = Array.zeroCreate<CowUtf8SpanMapEntry<'TValue>> (cur.Length + 1)

                    if idx > 0 then
                        Array.Copy(cur, 0, next, 0, idx)

                    next[idx] <- CowUtf8SpanMapEntry<'TValue>(newKey, hash, value)

                    if idx < cur.Length then
                        Array.Copy(cur, idx, next, idx + 1, cur.Length - idx)

                    Volatile.Write(&entries, next)
            finally
                Volatile.Write(&writeLock, 0)

        member this.GetOrAdd(key: ReadOnlySpan<byte>, factory: CowByteSpanMapValueFactory<'TValue>) : 'TValue =
            // Fast read path
            let mutable existing = Unchecked.defaultof<'TValue>
            if (this.TryGetValue(key, &existing)) then
                existing
            else
                while Interlocked.CompareExchange(&writeLock, 1, 0) <> 0 do
                    if not (Thread.Yield ()) then Thread.SpinWait(1)
                try
                    // Re-check under lock
                    let cur = entries
                    let mutable found = false
                    let idx = CowByteSpanMap.BinarySearch(cur, key, &found)
                    if found then
                        cur[idx].Value
                    else
                        let created = factory.Invoke(key)
                        let hash = hashFunction key
                        let newKey = key.ToArray()
                        let next = Array.zeroCreate<CowUtf8SpanMapEntry<'TValue>> (cur.Length + 1)

                        if idx > 0 then
                            Array.Copy(cur, 0, next, 0, idx)

                        next[idx] <- CowUtf8SpanMapEntry<'TValue>(newKey, hash, created)

                        if idx < cur.Length then
                            Array.Copy(cur, idx, next, idx + 1, cur.Length - idx)

                        Volatile.Write(&entries, next)
                        created
                finally
                    Volatile.Write(&writeLock, 0)

        member _.TryRemove(key: ReadOnlySpan<byte>, removed: byref<'TValue>) : bool =
            while Interlocked.CompareExchange(&writeLock, 1, 0) <> 0 do
                if not (Thread.Yield ()) then Thread.SpinWait(1)
            try
                let cur = entries
                let mutable found = false
                let idx = CowByteSpanMap.BinarySearch(cur, key, &found)
                if not found then
                    removed <- Unchecked.defaultof<'TValue>
                    false
                else
                    removed <- cur[idx].Value

                    if cur.Length = 1 then
                        Volatile.Write(&entries, Array.empty<CowUtf8SpanMapEntry<'TValue>>)
                        true
                    else
                        let next = Array.zeroCreate<CowUtf8SpanMapEntry<'TValue>> (cur.Length - 1)

                        if idx > 0 then
                            Array.Copy(cur, 0, next, 0, idx)

                        if idx < next.Length then
                            Array.Copy(cur, idx + 1, next, idx, next.Length - idx)

                        Volatile.Write(&entries, next)
                        true
            finally
                Volatile.Write(&writeLock, 0)

        member this.Remove(key: ReadOnlySpan<byte>) : bool =
            let mutable v = Unchecked.defaultof<'TValue>
            this.TryRemove(key, &v)

        // ---------------- Sorted search (lexicographic) ----------------
        static member private BinarySearch(entries: CowUtf8SpanMapEntry<'TValue>[], key: ReadOnlySpan<byte>, found: byref<bool>) : int =
            let mutable lo = 0
            let mutable hi = entries.Length - 1

            // Find the insertion point (first index where entries[i].Key > key)
            while lo <= hi do
                let mid = (lo + hi) >>> 1
                let cmp = key.SequenceCompareTo(ReadOnlySpan<byte>(entries[mid].Key))

                if cmp < 0 then
                    hi <- mid - 1
                else
                    // entries[mid].Key <= key, so the insertion point is to the right
                    lo <- mid + 1

            // lo is the insertion index; if key exists, it is at lo - 1
            found <-
                lo > 0 &&
                key.SequenceEqual(ReadOnlySpan<byte>(entries[lo - 1].Key))

            if found then lo - 1 else lo
