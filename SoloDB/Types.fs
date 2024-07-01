﻿module SoloDBTypes

open System.Linq.Expressions
open System
open System.Threading
open Dapper
open System.Collections.Generic

type internal DisposableMutex(name: string) =
    let mutex = new Mutex(false, name)
    do mutex.WaitOne() |> ignore

    interface IDisposable with
        member this.Dispose() =
            mutex.ReleaseMutex()
            mutex.Dispose()

type InnerExpr(expr: Expression<System.Func<obj, bool>>) =
    member this.Expression = expr

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
type FileHeader = {
    Id: SqlId
    Name: string
    DirectoryId: SqlId
    Length: int64
    Created: DateTimeOffset
    Modified: DateTimeOffset
    Metadata: IDictionary<string, string>
}

[<CLIMutable>]
type DirectoryHeader = {
    Id: SqlId
    Name: string
    FullPath: string
    ParentId: Nullable<SqlId>
    Created: DateTimeOffset
    Modified: DateTimeOffset
    Metadata: IDictionary<string, string>
}

[<CLIMutable>]
type private FileChunk = {
    Id: SqlId
    Number: int64
    Data: byte array
}