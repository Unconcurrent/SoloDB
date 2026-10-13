namespace SoloDatabase.Data.Sqlite

open System
open System.Collections.Concurrent
open System.Collections.Generic
open SoloDatabase

/// Public standalone acquisition pools native owners, never presentation commands or readers.
[<Sealed; AllowNullLiteral>]
type internal AdoPoolGroup(connectionString: string) =
    let gate = obj()
    let idle = Stack<NativeSqliteConnection>()
    let mutable retired = false
    member _.Acquire(preferred: NativeSqliteConnection) =
        lock gate (fun () ->
            if retired then ValueNone
            elif idle.Count > 0 then ValueSome(idle.Pop())
            else ValueSome(if isNull preferred then new NativeSqliteConnection(connectionString) else preferred))
    member _.Return(connection: NativeSqliteConnection) =
        let keep = lock gate (fun () ->
            if retired || connection.State <> SqliteConnectionState.Open then false
            else idle.Push(connection); true)
        if not keep then connection.Dispose()
    member _.Clear() =
        let pending = lock gate (fun () ->
            retired <- true
            let owners = idle.ToArray()
            idle.Clear()
            owners)
        let mutable failure: exn = null
        for owner in pending do
            try owner.Dispose() with error -> if isNull failure then failure <- error
        if not (isNull failure) then Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw()

module internal AdoConnectionPools =
    let private groups = ConcurrentDictionary<string, AdoPoolGroup>(StringComparer.Ordinal)
    let private eligible (options: SqliteConnectionStringBuilder) =
        options.Pooling && options.Mode <> SqliteOpenMode.Memory
        && options.DataSource <> "" && options.DataSource <> ":memory:"
    let acquire connectionString (preferred: NativeSqliteConnection) =
        let options = SqliteConnectionStringBuilder(connectionString)
        if not (eligible options) then struct (null, if isNull preferred then new NativeSqliteConnection(connectionString) else preferred)
        else
            let rec take () =
                let group = groups.GetOrAdd(connectionString, Func<string,AdoPoolGroup>(fun key -> AdoPoolGroup(key)))
                match group.Acquire(preferred) with
                | ValueSome native -> struct (group, native)
                | ValueNone ->
                    (groups :> ICollection<KeyValuePair<string,AdoPoolGroup>>).Remove(KeyValuePair(connectionString, group)) |> ignore
                    take()
            take()
    let clear (connectionString: string) =
        let mutable group = Unchecked.defaultof<AdoPoolGroup>
        if groups.TryRemove(connectionString, &group) then group.Clear()
    let clearAll () =
        let mutable failure: exn = null
        for pair in groups do
            try clear pair.Key with error -> if isNull failure then failure <- error
        if not (isNull failure) then Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw()
