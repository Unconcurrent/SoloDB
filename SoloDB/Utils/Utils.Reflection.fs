namespace SoloDatabase

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Reflection
open System.Threading
open System.Linq.Expressions

module internal UtilsReflection =
    let internal typeIdentityKey (t: Type) =
        if Object.ReferenceEquals(t, null) then
            nullArg "t"
        match t.AssemblyQualifiedName with
        | null | "" ->
            if Object.ReferenceEquals(t.FullName, null) then t.Name else t.FullName
        | value -> value
    [<AbstractClass; Sealed>]
    type internal GenericMethodArgCache =
        static member val private cache = ConcurrentDictionary<MethodInfo, Type array>({
                new IEqualityComparer<MethodInfo> with
                    override this.Equals (x: MethodInfo, y: MethodInfo): bool =
                        x.MethodHandle.Value = y.MethodHandle.Value
                    override this.GetHashCode (obj: MethodInfo): int =
                        obj.MethodHandle.Value |> int
        })

        static member Get(method: MethodInfo) =
            let args = GenericMethodArgCache.cache.GetOrAdd(method, (fun m -> m.GetGenericArguments()))
            args

    [<AbstractClass; Sealed>]
    type internal GenericTypeArgCache =
        static member val private cache = ConcurrentDictionary<Type, Type array>({
                new IEqualityComparer<Type> with
                    override this.Equals (x: Type, y: Type): bool =
                        x.TypeHandle.Value = y.TypeHandle.Value
                    override this.GetHashCode (obj: Type): int =
                        obj.TypeHandle.Value |> int
        })

        static member Get(t: Type) =
            let args = GenericTypeArgCache.cache.GetOrAdd(t, (fun m -> m.GetGenericArguments()))
            args

    /// For the F# compiler to allow the implicit use of
    /// the .NET Expression we need to use it in a C# style class.
    [<Sealed>][<AbstractClass>] // make it static
    type internal ExpressionHelper =
        static member inline internal get<'a, 'b>(expression: Expression<System.Func<'a, 'b>>) = expression

        static member inline internal id (x: Type) =
            let parameter = Expression.Parameter x
            Expression.Lambda(parameter, [|parameter|])

        static member inline internal eq (x: Type) (b: obj) =
            let parameter = Expression.Parameter x
            Expression.Lambda(Expression.Equal(parameter, Expression.Constant(b, x)), [|parameter|])

        static member inline internal min (e1: Expression) (e2: Expression) =
            // Produces an expression tree representing min(e1, e2) using LessThan
            Expression.Condition(
                Expression.LessThan(e1, e2),
                e1,
                e2
            )

        static member inline internal constant<'T> (x: 'T) =
            // Returns a strongly-typed constant expression for value x
            Expression.Constant(x, typeof<'T>)

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
