namespace SoloDatabase

open System

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
