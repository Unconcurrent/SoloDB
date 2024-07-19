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
    Id: int64
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
    Metadata: IDictionary<string, string>
}

[<CLIMutable>]
type SoloDBDirectoryHeader = {
    Id: SqlId
    Name: string
    FullPath: string
    ParentId: Nullable<SqlId>
    Created: DateTimeOffset
    Modified: DateTimeOffset
    Metadata: IDictionary<string, string>
}

[<CLIMutable>]
type internal SoloDBFileChunk = {
    Id: SqlId
    FileId: SqlId
    Number: int64
    Data: byte array
}