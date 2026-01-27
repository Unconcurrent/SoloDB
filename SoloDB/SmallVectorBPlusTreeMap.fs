namespace SoloDatabase

open Microsoft.FSharp.Core.Option

#nowarn "9"   // NativePtr, stackalloc
#nowarn "51"  // byref / span interop warnings

module internal VectorBPlusTreeMap =

    open System
    open System.Collections
    open System.Collections.Generic
    open System.Runtime.CompilerServices
    open System.Runtime.InteropServices
    open Microsoft.FSharp.NativeInterop

    let private ensureSome = function Some x -> x | None -> raise (NullReferenceException "Object is None, expected to be Some")

    // ----------------------------- hashing / compares -----------------------------

    let inline private hashChars (s: ReadOnlySpan<char>) : uint32 =
        Hashing.xxHash32Chars s 0u

    let inline private hashString (s: string) : uint32 =
        Hashing.xxHash32Chars (s.AsSpan()) 0u

    let inline private ordinalEqualsSpan (a: string) (b: ReadOnlySpan<char>) : bool =
        a.AsSpan().SequenceEqual(b)

    let inline private compareKeySpan (ha:uint32) (ka:string) (hb:uint32) (kb: ReadOnlySpan<char>) : int =
        if ha < hb then -1
        elif ha > hb then 1
        else ka.AsSpan().SequenceCompareTo(kb)

    // ----------------------------- B+Tree nodes (struct-of-arrays) -----------------------------

    [<AbstractClass>]
    type private Node() =
        member val Count = 0 with get,set
        abstract member IsLeaf : bool

    [<Sealed>]
    type private Internal(maxKeys:int) =
        inherit Node()
        // allow 1-key overflow during insertion before splitting
        member val Hashes   : uint32[] = Array.zeroCreate (maxKeys + 1) with get
        member val Keys     : string[] = Array.zeroCreate (maxKeys + 1) with get
        member val Children : (Node | null)[] = Array.zeroCreate (maxKeys + 2) with get
        override _.IsLeaf = false

    [<Sealed>]
    type private Leaf<'TValue>(maxKeys:int) =
        inherit Node()
        member val Hashes : uint32[]  = Array.zeroCreate maxKeys with get
        member val Keys   : string[]  = Array.zeroCreate maxKeys with get
        member val Values : 'TValue[] = Array.zeroCreate maxKeys with get
        member val Next : Leaf<'TValue> option = None with get,set
        member val Prev : Leaf<'TValue> option = None with get,set
        override _.IsLeaf = true

    [<Struct>]
    type private Entry<'TValue> =
        val mutable Hash : uint32
        val mutable Key  : string
        val mutable Value: 'TValue
        new (h,k,v) = { Hash = h; Key = k; Value = v }

    // ----------------------------- tree insert with parent stack -----------------------------

    let inline private insertIntoLeafNoSplit (leaf: Leaf<'TValue>) (idx:int) (h:uint32) (k:string) (v:'TValue) =
        // assumes leaf.Count < maxKeysPerNode
        let hashes = leaf.Hashes
        let keys   = leaf.Keys
        let vals   = leaf.Values
        let mutable j = leaf.Count
        while j > idx do
            hashes[j] <- hashes[j-1]
            keys[j]   <- keys[j-1]
            vals[j]   <- vals[j-1]
            j <- j - 1
        hashes[idx] <- h
        keys[idx]   <- k
        vals[idx]   <- v
        leaf.Count <- leaf.Count + 1


    /// Small-vector -> B+Tree map for string keys (Ordinal semantics).
    /// - Small mode: linear scan over compact Entry[]
    /// - Large mode: B+Tree keyed by (hash(UTF-16 bytes), ordinal key)
    /// Not thread-safe.
    [<Sealed>]
    type internal SmallVectorBPlusTreeMap<'TValue> internal (?smallThreshold:int, ?maxKeysPerNode:int) =
        let smallThreshold = defaultArg smallThreshold 32
        let maxKeysPerNode = defaultArg maxKeysPerNode 32
        do
            if smallThreshold < 1 then invalidArg (nameof smallThreshold) "smallThreshold must be >= 1"
            if maxKeysPerNode < 8 then invalidArg (nameof maxKeysPerNode) "maxKeysPerNode must be >= 8"

        // ----------------------------- B+Tree nodes (struct-of-arrays) -----------------------------

        let mutable root : Node option = None
        let mutable firstLeaf : Leaf<'TValue> option = None
        let mutable lastLeaf  : Leaf<'TValue> option = None

        let mutable count = 0

        // ----------------------------- small vector -----------------------------

        let mutable small : Entry<'TValue>[] = Array.empty
        let mutable smallCount = 0

        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let ensureSmallCapacity (needed:int) =
            if small.Length < needed then
                let mutable cap = if small.Length = 0 then 16 else small.Length * 2
                if cap < needed then cap <- needed
                Array.Resize(&small, cap)

        // ----------------------------- search helpers -----------------------------

        // In internal node: separators store minimal key of child i (i>0) at Keys[i-1].
        // Choose child index by finding first separator > target.
        let findChildIndex (inode: Internal) (h:uint32) (key: ReadOnlySpan<char>) : int =
            let mutable lo = 0
            let mutable hi = inode.Count // keys count
            while lo < hi do
                let mid = lo + ((hi - lo) >>> 1)
                let cmp = compareKeySpan inode.Hashes[mid] inode.Keys[mid] h key
                if cmp <= 0 then lo <- mid + 1
                else hi <- mid
            lo

        let lowerBoundLeafSpan (leaf: Leaf<'TValue>) (h:uint32) (key: ReadOnlySpan<char>) : int =
            let hashes = leaf.Hashes
            let keys   = leaf.Keys
            let n = leaf.Count
            let mutable lo = 0
            let mutable hi = n
            while lo < hi do
                let mid = lo + ((hi - lo) >>> 1)
                let mutable cmp =
                    if hashes[mid] < h then -1
                    elif hashes[mid] > h then 1
                    else keys[mid].AsSpan().SequenceCompareTo(key)
                if cmp < 0 then lo <- mid + 1 else hi <- mid
            lo

        let lowerBoundLeafStr (leaf: Leaf<'TValue>) (h:uint32) (key: string) : int =
            lowerBoundLeafSpan leaf h (key.AsSpan())

        // ----------------------------- tree lookup -----------------------------

        let treeTryGetValue (key: ReadOnlySpan<char>) (value: byref<'TValue>) : bool =
            let h = hashChars key
            let mutable n = ensureSome root
            while not n.IsLeaf do
                let inode = n :?> Internal
                let ci = findChildIndex inode h key
                n <- inode.Children[ci]
            let leaf = n :?> Leaf<'TValue>
            let idx = lowerBoundLeafSpan leaf h key
            if idx < leaf.Count && leaf.Hashes[idx] = h && ordinalEqualsSpan leaf.Keys[idx] key then
                value <- leaf.Values[idx]
                true
            else
                value <- Unchecked.defaultof<'TValue>
                false

        // ----------------------------- tree insert with parent stack -----------------------------

        let promoteNewRoot (left: Node) (right: Node) (sepHash:uint32) (sepKey:string) =
            let nr = Internal(maxKeysPerNode)
            nr.Hashes[0] <- sepHash
            nr.Keys[0]   <- sepKey
            nr.Children[0] <- left
            nr.Children[1] <- right
            nr.Count <- 1
            root <- nr :> Node |> Some

        let insertSeparatorIntoInternal (inode: Internal) (childIndex:int) (sepHash:uint32) (sepKey:string) (rightChild: Node) =
            // separator goes at key index = childIndex, between children[childIndex] and children[childIndex+1]
            let mutable j = inode.Count
            while j > childIndex do
                inode.Hashes[j] <- inode.Hashes[j-1]
                inode.Keys[j]   <- inode.Keys[j-1]
                inode.Children[j+1] <- inode.Children[j]
                j <- j - 1
            inode.Hashes[childIndex] <- sepHash
            inode.Keys[childIndex]   <- sepKey
            inode.Children[childIndex+1] <- rightChild
            inode.Count <- inode.Count + 1

        let splitInternal (inode: Internal) : struct(uint32 * string * Internal) =
            // inode.Count is the number of separator keys (can be maxKeysPerNode+1 here)
            let totalKeys = inode.Count
            // split around the actual key count
            let mid = totalKeys / 2

            let right = Internal(maxKeysPerNode)

            // right gets keys [mid+1 .. totalKeys-1]
            let rightKeyCount = totalKeys - (mid + 1)

            // copy children for right: [mid+1 .. totalKeys] => right.Children[0..rightKeyCount]
            // copy keys for right:     [mid+1 .. totalKeys-1] => right.Keys[0..rightKeyCount-1]
            let mutable i = 0
            while i < rightKeyCount do
                let srcKey = mid + 1 + i
                right.Hashes[i] <- inode.Hashes[srcKey]
                right.Keys[i]   <- inode.Keys[srcKey]
                right.Children[i] <- inode.Children[srcKey]
                i <- i + 1

            right.Children[rightKeyCount] <- inode.Children[totalKeys]  // last child
            right.Count <- rightKeyCount

            let promoHash = inode.Hashes[mid]
            let promoKey  = inode.Keys[mid]

            // shrink left to keys [0..mid-1], children [0..mid]
            inode.Count <- mid

            struct(promoHash, promoKey, right)

        let splitLeafAndInsert (leaf: Leaf<'TValue>) (insertIndex:int) (h:uint32) (k:string) (v:'TValue) : struct(Leaf<'TValue> * uint32 * string) =
            let max = maxKeysPerNode
            let mid = max / 2

            let right = Leaf<'TValue>(max)

            // link
            right.Next <- leaf.Next
            match right.Next with Some next -> next.Prev <- Some right | None -> ()
            right.Prev <- Some leaf
            leaf.Next <- Some right
            if lastLeaf = Some leaf then lastLeaf <- Some right

            // move [mid .. max-1] to right
            let rightCount = max - mid
            let mutable i = 0
            while i < rightCount do
                let src = mid + i
                right.Hashes[i] <- leaf.Hashes[src]
                right.Keys[i]   <- leaf.Keys[src]
                right.Values[i] <- leaf.Values[src]
                i <- i + 1
            right.Count <- rightCount

            // shrink left
            leaf.Count <- mid

            // insert new item into correct side
            if insertIndex <= mid - 1 then
                insertIntoLeafNoSplit leaf insertIndex h k v
            else
                insertIntoLeafNoSplit right (insertIndex - mid) h k v

            // separator = minimal key in right
            struct(right, right.Hashes[0], right.Keys[0])

        let treeInsert (key:string) (value:'TValue) (overwrite:bool) (oldValue: byref<'TValue>) : bool =
            let h = hashString key

            // parent stack
            let parents : Internal[] = Array.zeroCreate 64
            let childIx = NativePtr.stackalloc<int> 64
            let mutable sp = 0

            // descent
            let mutable n = ensureSome root
            while not n.IsLeaf do
                let inode = n :?> Internal
                let ci = findChildIndex inode h (key.AsSpan())
                parents[sp] <- inode
                NativePtr.set childIx sp ci
                sp <- sp + 1
                n <- inode.Children[ci]

            let leaf = n :?> Leaf<'TValue>

            // find position
            let idx = lowerBoundLeafStr leaf h key
            if idx < leaf.Count && leaf.Hashes[idx] = h && leaf.Keys[idx] = key then
                oldValue <- leaf.Values[idx]
                if overwrite then
                    leaf.Values[idx] <- value
                    true
                else
                    false
            else
                oldValue <- Unchecked.defaultof<'TValue>

                // insert or split leaf
                if leaf.Count < maxKeysPerNode then
                    insertIntoLeafNoSplit leaf idx h key value
                    count <- count + 1
                    true
                else
                    let struct(rightLeaf, sepHash, sepKey) = splitLeafAndInsert leaf idx h key value
                    count <- count + 1

                    // propagate split up using stack
                    let mutable promoHash = sepHash
                    let mutable promoKey  = sepKey
                    let mutable rightNode : Node = rightLeaf :> Node
                    let mutable level = sp
                    let mutable done' = false

                    while not done' do
                        if level = 0 then
                            promoteNewRoot (leaf :> Node) rightNode promoHash promoKey
                            // first/last leaf init if needed
                            match firstLeaf with
                            | None -> firstLeaf <- Some leaf
                            | Some _ -> ()
                            match lastLeaf with
                            | None -> lastLeaf <- Some rightLeaf
                            | Some _ -> ()
                            done' <- true
                        else
                            let parent = parents[level - 1]
                            let ci = NativePtr.get childIx (level - 1)
                            insertSeparatorIntoInternal parent ci promoHash promoKey rightNode
                            if parent.Count <= maxKeysPerNode then
                                done' <- true
                            else
                                let struct(ph, pk, rightInternal) = splitInternal parent
                                promoHash <- ph
                                promoKey <- pk
                                rightNode <- rightInternal :> Node
                                // continue up
                                level <- level - 1
                    true

        // ----------------------------- tree delete (rare; works; minimal collapse) -----------------------------

        let removeChildFromInternal (inode: Internal) (childIndex:int) =
            // Remove a child pointer and its associated separator.
            // If childIndex == 0: remove Children[0], and remove Key[0] (minimal key of old child1 becomes minimal of new child1).
            // Else: remove Children[childIndex], and remove Key[childIndex-1].
            let mutable keyRemoveIndex = if childIndex = 0 then 0 else childIndex - 1

            // Shift children left
            let mutable j = childIndex
            while j < inode.Count do
                inode.Children[j] <- inode.Children[j+1]
                j <- j + 1
            inode.Children[inode.Count] <- null

            // Shift keys left from keyRemoveIndex
            j <- keyRemoveIndex
            while j < inode.Count - 1 do
                inode.Hashes[j] <- inode.Hashes[j+1]
                inode.Keys[j]   <- inode.Keys[j+1]
                j <- j + 1

            // Clear last key slot
            inode.Hashes[inode.Count - 1] <- 0u
            inode.Keys[inode.Count - 1] <- null

            inode.Count <- inode.Count - 1

        let treeRemove (key: ReadOnlySpan<char>) (removed: byref<'TValue>) : bool =
            let h = hashChars key

            let parents : Internal[] = Array.zeroCreate 64
            let childIx = NativePtr.stackalloc<int> 64
            let mutable sp = 0

            // descent
            let mutable n = ensureSome root
            while not n.IsLeaf do
                let inode = n :?> Internal
                let ci = findChildIndex inode h key
                parents[sp] <- inode
                NativePtr.set childIx sp ci
                sp <- sp + 1
                n <- inode.Children[ci]

            let leaf = n :?> Leaf<'TValue>
            let idx = lowerBoundLeafSpan leaf h key
            if idx >= leaf.Count || leaf.Hashes[idx] <> h || not (ordinalEqualsSpan leaf.Keys[idx] key) then
                removed <- Unchecked.defaultof<'TValue>
                false
            else
                removed <- leaf.Values[idx]

                // remove entry from leaf (shift left)
                let hashes = leaf.Hashes
                let keys   = leaf.Keys
                let vals   = leaf.Values
                let mutable i = idx
                while i < leaf.Count - 1 do
                    hashes[i] <- hashes[i+1]
                    keys[i]   <- keys[i+1]
                    vals[i]   <- vals[i+1]
                    i <- i + 1
                let last = leaf.Count - 1
                hashes[last] <- 0u
                keys[last]   <- null
                vals[last]   <- Unchecked.defaultof<'TValue>
                leaf.Count <- leaf.Count - 1
                count <- count - 1

                // fix parent separator if we removed the first key and leaf still has entries
                if idx = 0 && leaf.Count > 0 && sp > 0 then
                    let parent = parents.[sp - 1]
                    let ci = NativePtr.get childIx (sp - 1)
                    if ci > 0 then
                        parent.Hashes[ci - 1] <- leaf.Hashes[0]
                        parent.Keys[ci - 1]   <- leaf.Keys[0]

                // if leaf became empty: unlink and remove from parent; collapse empty internal nodes
                if leaf.Count = 0 then
                    // unlink leaf list
                    match leaf.Prev with
                    | Some prev -> prev.Next <- leaf.Next
                    | None -> firstLeaf <- leaf.Next
                    match leaf.Next with
                    | Some next -> next.Prev <- leaf.Prev
                    | None -> lastLeaf <- leaf.Prev
                    leaf.Next <- None
                    leaf.Prev <- None

                    if sp = 0 then
                        // leaf was root
                        root <- None
                        firstLeaf <- None
                        lastLeaf <- None
                    else
                        // remove leaf from its parent
                        let parent = parents[sp - 1]
                        let ci = NativePtr.get childIx (sp - 1)
                        removeChildFromInternal parent ci

                        // collapse upwards if a parent becomes empty (Count=0 => one child)
                        let mutable level = sp - 1
                        let mutable done' = false
                        while not done' && level >= 0 do
                            let inode = parents[level]
                            let isRoot =
                                match root with
                                | Some r -> obj.ReferenceEquals(inode, r)
                                | None -> false

                            if isRoot && inode.Count = 0 then
                                // root internal with single child: collapse
                                let child = inode.Children[0]
                                root <- if isNull child then None else Some child
                                done' <- true
                            elif inode.Count = 0 then
                                // non-root internal with single child: replace it in its parent
                                let p = parents[level - 1]
                                let ci2 = NativePtr.get childIx (level - 1)
                                p.Children[ci2] <- inode.Children[0]
                                // also remove a separator if the structure now has redundant node:
                                // (We keep it minimal: we do not remove extra keys beyond the empty-node removal already done.)
                                level <- level - 1
                            else
                                done' <- true

                true

        // ----------------------------- promotion to tree -----------------------------

        let promoteToTree () =
            let n = smallCount
            if n = 0 then
                root <- None
                firstLeaf <- None
                lastLeaf <- None
            else
                // 1) Sort small[0..n) by (Hash, Key ordinal)
                let span = Span<Entry<'TValue>>(small, 0, n)
                let cmp =
                    Comparison<Entry<'TValue>>(fun a b ->
                        if a.Hash < b.Hash then -1
                        elif a.Hash > b.Hash then 1
                        else a.Key.AsSpan().SequenceCompareTo(b.Key.AsSpan())
                    )
                Sort.heapSort span cmp

                // 2) Pack leaves
                let leafCount = (n + maxKeysPerNode - 1) / maxKeysPerNode
                let leaves = Array.zeroCreate<Leaf<'TValue>> leafCount
                let minH = Array.zeroCreate<uint32> leafCount
                let minK = Array.zeroCreate<string> leafCount

                for li = 0 to leafCount - 1 do
                    let leaf = Leaf(maxKeysPerNode)
                    let start = li * maxKeysPerNode
                    let len = min maxKeysPerNode (n - start)

                    leaf.Count <- len
                    for i = 0 to len - 1 do
                        let e = small[start + i]
                        leaf.Hashes[i] <- e.Hash
                        leaf.Keys[i] <- e.Key
                        leaf.Values[i] <- e.Value

                    leaves[li] <- leaf
                    minH[li] <- leaf.Hashes[0]
                    minK[li] <- leaf.Keys[0]

                // 3) Link leaves
                for i = 0 to leafCount - 1 do
                    let leaf = leaves[i]
                    leaf.Prev <- if i = 0 then None else Some leaves[i - 1]
                    leaf.Next <- if i = leafCount - 1 then None else Some leaves[i + 1]

                firstLeaf <- Some leaves[0]
                lastLeaf <- Some leaves[leafCount - 1]

                // 4) Build internal levels bottom-up.
                // Each Internal holds up to maxKeysPerNode keys, hence up to fanout = maxKeysPerNode+1 children.
                let fanout = maxKeysPerNode + 1

                let mutable curNodes : Node[] = leaves |> Array.map (fun l -> l :> Node)
                let mutable curMinH : uint32[] = minH
                let mutable curMinK : string[] = minK

                while curNodes.Length > 1 do
                    let childN = curNodes.Length
                    let parentCount = (childN + fanout - 1) / fanout

                    let nextNodes = Array.zeroCreate<Node> parentCount
                    let nextMinH = Array.zeroCreate<uint32> parentCount
                    let nextMinK = Array.zeroCreate<string> parentCount

                    for pi = 0 to parentCount - 1 do
                        let start = pi * fanout
                        let ccount = min fanout (childN - start) // number of children in this internal
                        let inode = Internal(maxKeysPerNode)

                        // children[0..ccount-1]
                        for j = 0 to ccount - 1 do
                            inode.Children[j] <- curNodes[start + j]

                        // separators: key[i] is minimal key of children[i+1]
                        // so keys count = ccount - 1
                        for j = 1 to ccount - 1 do
                            inode.Hashes[j - 1] <- curMinH[start + j]
                            inode.Keys[j - 1] <- curMinK[start + j]

                        inode.Count <- ccount - 1

                        nextNodes[pi] <- inode :> Node
                        nextMinH[pi] <- curMinH[start]
                        nextMinK[pi] <- curMinK[start]

                    curNodes <- nextNodes
                    curMinH <- nextMinH
                    curMinK <- nextMinK

                root <- Some curNodes[0]

            // 5) clear small mode storage (do NOT touch 'count' here)
            small <- Array.empty
            smallCount <- 0


        // ----------------------------- public members -----------------------------

        member _.Count = count

        member _.TryGetValue(key: string, value: byref<'TValue>) : bool =
            if isNull key then nullArg (nameof key)
            match root with
            | None ->
                let h = hashString key
                let mutable i = 0
                let mutable found = false
                while (not found) && i < smallCount do
                    let e = small[i]
                    if e.Hash = h && e.Key = key then
                        value <- e.Value
                        found <- true
                    i <- i + 1
                if not found then
                    value <- Unchecked.defaultof<'TValue>
                found
            | Some _ ->
                treeTryGetValue (key.AsSpan()) &value

        member _.TryGetValue(key: ReadOnlySpan<char>, value: byref<'TValue>) : bool =
            match root with
            | None ->
                let h = hashChars key
                let mutable i = 0
                let mutable found = false
                while (not found) && i < smallCount do
                    let e = small[i]
                    if e.Hash = h && ordinalEqualsSpan e.Key key then
                        value <- e.Value
                        found <- true
                    i <- i + 1
                if not found then
                    value <- Unchecked.defaultof<'TValue>
                found
            | Some _ ->
                treeTryGetValue key &value

        member this.ContainsKey(key: string) =
            let mutable tmp = Unchecked.defaultof<'TValue>
            this.TryGetValue(key, &tmp)

        member this.ContainsKey(key: ReadOnlySpan<char>) =
            let mutable tmp = Unchecked.defaultof<'TValue>
            this.TryGetValue(key, &tmp)

        member _.TryAdd(key: string, value:'TValue) : bool =
            if isNull key then nullArg (nameof key)
            match root with
            | None ->
                let h = hashString key
                let mutable i = 0
                let mutable exists = false
                while (not exists) && i < smallCount do
                    let e = small[i]
                    if e.Hash = h && e.Key = key then exists <- true
                    i <- i + 1
                if exists then false
                else
                    ensureSmallCapacity (smallCount + 1)
                    small[smallCount] <- Entry(h, key, value)
                    smallCount <- smallCount + 1
                    count <- count + 1
                    if count > smallThreshold then promoteToTree ()
                    true
            | Some _ ->
                let mutable old = Unchecked.defaultof<'TValue>
                treeInsert key value false &old

        member _.AddOrUpdate(key: string, value:'TValue) : unit =
            if isNull key then nullArg (nameof key)
            match root with
            | None ->
                let h = hashString key
                let mutable i = 0
                let mutable updated = false
                while (not updated) && i < smallCount do
                    if small[i].Hash = h && small[i].Key = key then
                        small[i].Value <- value
                        updated <- true
                    i <- i + 1
                if not updated then
                    ensureSmallCapacity (smallCount + 1)
                    small[smallCount] <- Entry(h, key, value)
                    smallCount <- smallCount + 1
                    count <- count + 1
                    if count > smallThreshold then promoteToTree ()
            | Some _ ->
                let mutable old = Unchecked.defaultof<'TValue>
                ignore (treeInsert key value true &old)

        member _.Remove(key: string, removed: byref<'TValue>) : bool =
            if isNull key then nullArg (nameof key)
            match root with
            | None ->
                let h = hashString key
                let mutable i = 0
                let mutable found = false
                while (not found) && i < smallCount do
                    if small[i].Hash = h && small[i].Key = key then
                        removed <- small[i].Value
                        // swap-remove
                        smallCount <- smallCount - 1
                        if i <> smallCount then small[i] <- small[smallCount]
                        small[smallCount] <- Unchecked.defaultof<Entry<'TValue>>
                        count <- count - 1
                        found <- true
                    i <- i + 1
                if not found then removed <- Unchecked.defaultof<'TValue>
                found
            | Some _ ->
                treeRemove (key.AsSpan()) &removed

        member _.Clear() =
            small <- Array.empty
            smallCount <- 0
            root <- None
            firstLeaf <- None
            lastLeaf <- None
            count <- 0

        // ----------------------------- IDictionary / IReadOnlyDictionary -----------------------------

        interface IReadOnlyDictionary<string,'TValue> with
            member _.Count = count
            member this.Keys =
                seq {
                    match root with
                    | None ->
                        for i = 0 to smallCount - 1 do
                            yield small[i].Key
                    | Some _ ->
                        let mutable l = firstLeaf
                        let mutable done' = false
                        while not done' do
                            match l with
                            | Some leaf ->
                                for i = 0 to leaf.Count - 1 do
                                    yield leaf.Keys[i]
                                l <- leaf.Next
                            | None -> done' <- true
                }
            member this.Values =
                seq {
                    match root with
                    | None ->
                        for i = 0 to smallCount - 1 do
                            yield small[i].Value
                    | Some _ ->
                        let mutable l = firstLeaf
                        let mutable done' = false
                        while not done' do
                            match l with
                            | Some leaf ->
                                for i = 0 to leaf.Count - 1 do
                                    yield leaf.Values[i]
                                l <- leaf.Next
                            | None -> done' <- true
                }
            member this.Item
                with get (key: string) =
                    let mutable v = Unchecked.defaultof<'TValue>
                    if this.TryGetValue(key, &v) then v else raise (KeyNotFoundException())
            member this.ContainsKey(key: string) = this.ContainsKey(key)
            member this.TryGetValue(key: string, value: byref<'TValue>) = this.TryGetValue(key, &value)
        interface IDictionary<string,'TValue> with
            member _.IsReadOnly = false
            member _.Count = count

            member this.Keys =
                let keys = ResizeArray<string>((this :> IReadOnlyDictionary<_,_>).Keys)
                keys :> ICollection<string>

            member this.Values =
                let values = ResizeArray<'TValue>((this :> IReadOnlyDictionary<_,_>).Values)
                values :> ICollection<'TValue>

            member this.Item
                with get (key:string) =
                    let ro = this :> IReadOnlyDictionary<string,'TValue>
                    ro[key]
                and set (key:string) (value:'TValue) =
                    this.AddOrUpdate(key, value)

            member this.Add(key:string, value:'TValue) =
                if not (this.TryAdd(key, value)) then
                    invalidArg (nameof key) "An element with the same key already exists."

            member this.ContainsKey(key:string) = this.ContainsKey(key)

            member this.TryGetValue(key:string, value: byref<'TValue>) = this.TryGetValue(key, &value)

            member this.Remove(key:string) =
                let mutable tmp = Unchecked.defaultof<'TValue>
                this.Remove(key, &tmp)

            member this.Add(item: KeyValuePair<string,'TValue>) =
                (this :> IDictionary<_,_>).Add(item.Key, item.Value)

            member this.Contains(item: KeyValuePair<string,'TValue>) =
                let mutable v = Unchecked.defaultof<'TValue>
                if this.TryGetValue(item.Key, &v) then EqualityComparer<'TValue>.Default.Equals(v, item.Value)
                else false

            member this.CopyTo(arr: KeyValuePair<string,'TValue>[], index:int) =
                if isNull arr then nullArg "arr"
                let mutable i = index
                for kv in (this :> seq<_>) do
                    arr[i] <- kv
                    i <- i + 1

            member this.Remove(item: KeyValuePair<string,'TValue>) =
                if (this :> IDictionary<_,_>).Contains(item) then
                    (this :> IDictionary<_,_>).Remove(item.Key)
                else false

            member this.Clear() = this.Clear()

        interface IEnumerable<KeyValuePair<string,'TValue>> with
            member _.GetEnumerator() : IEnumerator<KeyValuePair<string,'TValue>> =
                (seq {
                    match root with
                    | None ->
                        for i = 0 to smallCount - 1 do
                            let e = small[i]
                            yield KeyValuePair(e.Key, e.Value)
                    | Some _ ->
                        let mutable l = firstLeaf
                        let mutable done' = false
                        while not done' do
                            match l with
                            | Some leaf ->
                                for i = 0 to leaf.Count - 1 do
                                    yield KeyValuePair(leaf.Keys[i], leaf.Values[i])
                                l <- leaf.Next
                            | None -> done' <- true
                }).GetEnumerator()

        interface IEnumerable with
            member this.GetEnumerator() : IEnumerator =
                (this :> IEnumerable<KeyValuePair<string,'TValue>>).GetEnumerator() :> IEnumerator
