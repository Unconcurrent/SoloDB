namespace SoloDatabase

open System
open System.Collections.Generic
open System.Data
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open Microsoft.Data.Sqlite
open System.Data.Common
open SQLiteToolsParams
open SQLiteToolsMapper

module internal SQLiteToolsExtensions =
    open SQLiteToolsHandlerFaultState
    
    type internal ICachingDbConnectionOps =
        abstract member CheckNoActiveReader: unit -> unit
        abstract member ReaderActive: bool with get, set
        abstract member Execute: string * obj -> int
        abstract member Query<'T>: string * obj -> seq<'T>
        abstract member QueryFirst<'T>: string * obj -> 'T
        abstract member QueryFirstOrDefault<'T>: string * obj -> 'T
        abstract member Query<'T1, 'T2, 'TReturn>: string * Func<'T1, 'T2, 'TReturn> * obj * string -> seq<'TReturn>
    
    /// <summary>
    /// Provides extension methods for IDbConnection for executing queries.
    /// </summary>
    [<Extension>]
    type IDbConnectionExtensions =
        /// <summary>
        /// Extension method to open a data reader.
        /// </summary>
        [<Extension>]
        static member OpenReader<'R>(this: SqliteConnection, sql: string, outReader: outref<DbDataReader>, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            match box this with
            | :? ICachingDbConnectionOps as c ->
                // Route through CachingDbConnection member to get reader-active guard.
                c.CheckNoActiveReader()
                let command = createCommand this sql parameters
                try command.Prepare()
                with :? ArgumentOutOfRangeException -> command.Prepare()
                let reader = command.ExecuteReader()
                outReader <- reader
                c.ReaderActive <- true
                { new IDisposable with
                    member _.Dispose() =
                        c.ReaderActive <- false
                        reader.Dispose()
                        command.Dispose()
                }
            | _ ->
                let command = createCommand this sql parameters
                try command.Prepare()
                with :? ArgumentOutOfRangeException -> command.Prepare()
                let reader = command.ExecuteReader()
                outReader <- reader
                { new IDisposable with
                    member _.Dispose() =
                        reader.Dispose()
                        command.Dispose()
                }
    
        /// <summary>
        /// Extension method to execute a non-query command.
        /// </summary>
        [<Extension>]
        static member Execute(this: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            try
                match box this with
                | :? ICachingDbConnectionOps as c -> c.Execute(sql, parameters)
                | _ ->
                    use command = createCommand this sql parameters
                    // SQLITE_SCHEMA compensation (same as tryCachedCommand path).
                    try command.Prepare()
                    with :? ArgumentOutOfRangeException -> command.Prepare()
                    let affected = command.ExecuteNonQuery()
                    raiseIfHandlerFaultRecorded this
                    affected
            with ex ->
                tryRecordHandlerFault this ex
                reraise()
    
        /// <summary>
        /// Extension method to execute a query and map the results to a sequence of 'T.
        /// </summary>
        [<Extension>]
        static member Query<'T>(this: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            match box this with
            | :? ICachingDbConnectionOps as c -> c.Query<'T>(sql, parameters)
            | _ ->
                seq {
                    try
                        yield! queryInner<'T> this sql parameters
                    with ex ->
                        tryRecordHandlerFault this ex
                        raise ex
                }
    
        /// <summary>
        /// Extension method to execute a query and return the first result.
        /// </summary>
        [<Extension>]
        static member QueryFirst<'T>(this: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            try
                match box this with
                | :? ICachingDbConnectionOps as c -> c.QueryFirst<'T>(sql, parameters)
                | _ ->
                    let result = queryInner<'T> this sql parameters |> Seq.head
                    raiseIfHandlerFaultRecorded this
                    result
            with ex ->
                tryRecordHandlerFault this ex
                reraise()
    
        /// <summary>
        /// Extension method to execute a query and return the first result, or a default value if the sequence is empty.
        /// </summary>
        [<Extension>]
        static member QueryFirstOrDefault<'T>(this: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            try
                match box this with
                | :? ICachingDbConnectionOps as c -> c.QueryFirstOrDefault<'T>(sql, parameters)
                | _ ->
                    let result =
                        match queryInner<'T> this sql parameters |> Seq.tryHead with
                        | Some x -> x
                        | None -> defaultOf<'T>()
                    raiseIfHandlerFaultRecorded this
                    result
            with ex ->
                tryRecordHandlerFault this ex
                reraise()
    
        /// <summary>
        /// Extension method for executing a multi-mapping query.
        /// </summary>
        [<Extension>]
        static member Query<'T1, 'T2, 'TReturn>(this: SqliteConnection, sql: string, map: Func<'T1, 'T2, 'TReturn>, parameters: obj, splitOn: string) =
            match box this with
            | :? ICachingDbConnectionOps as c -> c.Query<'T1, 'T2, 'TReturn>(sql, map, parameters, splitOn)
            | _ ->
    
            seq {
                use command = createCommand this sql parameters
                use reader = command.ExecuteReader()
    
                let dict = Dictionary<string, int>(reader.FieldCount)
    
                for i in 0..(reader.FieldCount - 1) do
                    dict.Add(reader.GetName(i), i)
    
                let splitIndex = reader.GetOrdinal(splitOn)
    
                while reader.Read() do
                    let t1 = TypeMapper<'T1>.Map reader 0 dict
                    let t2 =
                        if reader.IsDBNull(splitIndex) then Unchecked.defaultof<'T2>
                        else TypeMapper<'T2>.Map reader splitIndex dict
    
                    yield map.Invoke (t1, t2)
            }
