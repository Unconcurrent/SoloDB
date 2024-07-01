module SoloDBTypes

open System.Linq.Expressions
open System
open System.Threading
open Dapper

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

type AccountTypeHandler() =
    inherit SqlMapper.TypeHandler<SqlId>()
    override __.Parse(value) =
        value :?> int64 |> SqlId

    override __.SetValue(p, value) =
        p.DbType <- Data.DbType.Int64
        p.Value <- value.Value

SqlMapper.AddTypeHandler(typeof<SqlId>, AccountTypeHandler())

[<CLIMutable>]
type DbObjectRow = {
    Id: int64
    ValueJSON: string
}

[<CLIMutable>]
type FileHeader = {
    Id: SqlId
    Name: string
    DirectoryId: SqlId
    Length: int64
    Created: DateTimeOffset
    Modified: DateTimeOffset
}

[<CLIMutable>]
type DirectoryHeader = {
    Id: SqlId
    Name: string
    ParentId: Nullable<SqlId>
    Created: DateTimeOffset
    Modified: DateTimeOffset
}

[<CLIMutable>]
type private FileChunk = {
    Id: SqlId
    Number: int64
    Data: byte array
}


(*
    todo:   I am thinking that the metadata better should exists in the 
            FileHeader itself to not hit the database for each file for the headers...
*)

[<CLIMutable>]
type FileMetadata = {
    Id: SqlId
    FileId: SqlId
    Key: string
    Value: string
}

[<CLIMutable>]
type DirectoryMetadata = {
    Id: SqlId
    DirectoryId: SqlId
    Key: string
    Value: string
}
