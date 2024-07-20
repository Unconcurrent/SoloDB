namespace SoloDatabase.Types

open System.Linq.Expressions
open System
open System.Threading
open Dapper
open System.Collections.Generic
open System.Runtime.CompilerServices

type internal DisposableMutex =
    val mutex: Mutex

    internal new(name: string) =
        let mutex = new Mutex(false, name)
        mutex.WaitOne() |> ignore

        { mutex = mutex }


    interface IDisposable with
        member this.Dispose() =
            this.mutex.ReleaseMutex()
            this.mutex.Dispose()


[<Struct>]
type SqlId =
    SqlId of int64
    with
    static member (+) (SqlId a, SqlId b) = SqlId (a + b)
    static member (-) (SqlId a, SqlId b) = SqlId (a - b)
    static member (*) (SqlId a, SqlId b) = SqlId (a * b)
    static member (/) (SqlId a, SqlId b) = SqlId (a / b)
    static member (%) (SqlId a, SqlId b) = SqlId (a % b)
    static member (<<<) (SqlId a, shift) = SqlId (a <<< shift)
    static member (>>>) (SqlId a, shift) = SqlId (a >>> shift)
    static member (&&&) (SqlId a, SqlId b) = SqlId (a &&& b)
    static member (|||) (SqlId a, SqlId b) = SqlId (a ||| b)
    static member (^^^) (SqlId a, SqlId b) = SqlId (a ^^^ b)
    static member (~-) (SqlId a) = SqlId (-a)
    static member (~+) (SqlId a) = SqlId (+a)
    static member (~~~) (SqlId a) = SqlId (~~~a)
    static member op_Explicit(SqlId a) = a
    static member op_Implicit(a: int64) = SqlId a
    static member op_Implicit(a: SqlId) = a

    override this.ToString() =
        let (SqlId value) = this
        value.ToString()

    member this.Value =
        let (SqlId value) = this
        value

type internal AccountTypeHandler() =
    inherit SqlMapper.TypeHandler<SqlId>()
    override __.Parse(value) =
        value :?> int64 |> SqlId

    override __.SetValue(p, value) =
        p.DbType <- Data.DbType.Int64
        p.Value <- value.Value

type internal DateTimeMapper() =
    inherit SqlMapper.TypeHandler<DateTimeOffset>()

    override this.Parse(o) =
        DateTimeOffset.FromUnixTimeMilliseconds (o :?> int64)

    override this.SetValue (para, value) =
        para.Value <- value.ToUnixTimeMilliseconds()
        ()


[<CLIMutable>]
type DbObjectRow = {
    Id: SqlId
    ValueJSON: string
}


[<CLIMutable>]
type Metadata = {
    Key: string
    Value: string
}

[<CLIMutable>]
type SoloDBFileHeader = {
    Id: SqlId
    Name: string
    FullPath: string
    DirectoryId: SqlId
    Length: int64
    Created: DateTimeOffset
    Modified: DateTimeOffset
    Hash: byte array
    Metadata: IReadOnlyDictionary<string, string>
}

[<CLIMutable>]
type SoloDBDirectoryHeader = {
    Id: SqlId
    Name: string
    FullPath: string
    ParentId: Nullable<SqlId>
    Created: DateTimeOffset
    Modified: DateTimeOffset
    Metadata: IReadOnlyDictionary<string, string>
}

[<Struct>]
type SoloDBEntryHeader = 
    | File of file: SoloDBFileHeader
    | Directory of directory: SoloDBDirectoryHeader

    member this.Name = 
        match this with
        | File f -> f.Name
        | Directory d -> d.Name

    member this.FullPath = 
        match this with
        | File f -> f.FullPath
        | Directory d -> d.FullPath

    member this.DirectoryId = 
        match this with
        | File f -> f.DirectoryId |> Nullable
        | Directory d -> d.ParentId

    member this.Created = 
        match this with
        | File f -> f.Created
        | Directory d -> d.Created

    member this.Modified = 
        match this with
        | File f -> f.Modified
        | Directory d -> d.Modified

    member this.Metadata = 
        match this with
        | File f -> f.Metadata
        | Directory d -> d.Metadata

[<CLIMutable>]
type internal SoloDBFileChunk = {
    Id: SqlId
    FileId: SqlId
    Number: int64
    Data: byte array
}