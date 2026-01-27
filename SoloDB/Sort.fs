namespace SoloDatabase

open System
open System.Collections.Generic

module private Sort =
    let inline private swap (span: Span<'T>) (i: int) (j: int) =
        let tmp = span[i]
        span[i] <- span[j]
        span[j] <- tmp

    // Sift-down for a max-heap in span[0..count-1]
    let inline private siftDown<'T> (span: Span<'T>) (count: int) (start: int) (cmp: Comparison<'T>) : unit =
        let mutable root = start
        let mutable done_ = false
        while not done_ do
            let child = (root <<< 1) + 1
            if child >= count then
                done_ <- true
            else
                let mutable swapIdx = root

                if cmp.Invoke(span[swapIdx], span[child]) < 0 then
                    swapIdx <- child

                let right = child + 1
                if right < count && cmp.Invoke(span[swapIdx], span[right]) < 0 then
                    swapIdx <- right

                if swapIdx = root then
                    done_ <- true
                else
                    swap span root swapIdx
                    root <- swapIdx

    // In-place heapsort (ascending by cmp)
    let heapSort<'T> (span: Span<'T>) (cmp: Comparison<'T>) : unit =
        let n = span.Length
        if n <= 1 then () else

        // Build max-heap bottom-up
        for start = (n >>> 1) - 1 downto 0 do
            siftDown span n start cmp

        // Extract max to end, shrink heap
        let mutable end_ = n - 1
        while end_ > 0 do
            swap span 0 end_
            siftDown span end_ 0 cmp
            end_ <- end_ - 1


    let inline keyCompare (buf: byref<InternalStack128<KeyValuePair<string, 'J>>>) (i: int) (j: int) : int =
        let ki = (InternalStack128.GetRef(&buf, i)).Key
        let kj = (InternalStack128.GetRef(&buf, j)).Key
        StringComparer.Ordinal.Compare(ki, kj)

    let inline swapAt (buf: byref<InternalStack128<'T>>) (i: int) (j: int) : unit =
        let tmp = InternalStack128.GetRef(&buf, i)          // struct copy
        InternalStack128.GetRef(&buf, i) <- InternalStack128.GetRef(&buf, j)
        InternalStack128.GetRef(&buf, j) <- tmp

    let inline private cmpAt<'T>
        (buf: byref<InternalStack128<'T>>) (cmp: Comparison<'T>) (i: int) (j: int) : int =
        // read values (struct copy) and compare
        let a = InternalStack128.GetRef(&buf, i)
        let b = InternalStack128.GetRef(&buf, j)
        cmp.Invoke(a, b)

    // Sift-down for a max-heap using the provided comparator
    let inline private siftDownStack128<'T>
        (buf: byref<InternalStack128<'T>>) (start: int) (count: int) (cmp: Comparison<'T>) : unit =
        let mutable root = start
        let mutable done_ = false
        while not done_ do
            let child = (root <<< 1) + 1
            if child >= count then
                done_ <- true
            else
                let mutable swapIdx = root

                if cmpAt &buf cmp swapIdx child < 0 then
                    swapIdx <- child

                let right = child + 1
                if right < count && cmpAt &buf cmp swapIdx right < 0 then
                    swapIdx <- right

                if swapIdx = root then
                    done_ <- true
                else
                    swapAt &buf root swapIdx
                    root <- swapIdx

    // In-place heapsort over Stack128 (ascending by cmp)
    let heapSortStack128<'T>
        (buf: byref<InternalStack128<'T>>) (n: int) (cmp: Comparison<'T>) : unit =
        if n <= 1 then () else

        // Build heap (bottom-up)
        let mutable start = (n >>> 1) - 1
        while start >= 0 do
            siftDownStack128 &buf start n cmp
            start <- start - 1

        // Extract max to end, shrink heap
        let mutable end_ = n - 1
        while end_ > 0 do
            swapAt &buf 0 end_
            siftDownStack128 &buf 0 end_ cmp
            end_ <- end_ - 1