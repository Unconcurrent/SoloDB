namespace SoloDatabase

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Reflection
open System.Threading
open System.Linq.Expressions

module internal UtilsReflection =
    module internal SeqExt =
        let internal sequentialGroupBy keySelector (sequence: seq<'T>) =
            seq {
                use enumerator = sequence.GetEnumerator()
                if enumerator.MoveNext() then
                    let mutable currentKey = keySelector enumerator.Current
                    let mutable currentList = System.Collections.Generic.List<'T>()
                    let mutable looping = true
                    
                    while looping do
                        let current = enumerator.Current
                        let key = keySelector current
        
                        if key = currentKey then
                            currentList.Add current
                        else
                            yield currentList :> IList<'T>
                            currentList.Clear()
                            currentList.Add current
                            currentKey <- key
        
                        if not (enumerator.MoveNext()) then
                            yield currentList :> IList<'T>
                            looping <- false
            }


    type ReentrantSpinLock() =
        let mutable ownerTid = 0
        let mutable recursion = 0

        member _.IsHeldByCurrentThread =
            Volatile.Read(&ownerTid) = Thread.CurrentThread.ManagedThreadId

        member _.Enter() =
            let tid = Thread.CurrentThread.ManagedThreadId
            if Volatile.Read(&ownerTid) = tid then
                recursion <- recursion + 1
            else
                // acquire
                while Interlocked.CompareExchange(&ownerTid, tid, 0) <> 0 do
                    if not (Thread.Yield()) then Thread.SpinWait(20)
                recursion <- 1

        member _.Exit() =
            let tid = Thread.CurrentThread.ManagedThreadId
            if Volatile.Read(&ownerTid) <> tid then
                invalidOp "Exit from non-owner thread"
            recursion <- recursion - 1
            if recursion = 0 then
                Volatile.Write(&ownerTid, 0)
