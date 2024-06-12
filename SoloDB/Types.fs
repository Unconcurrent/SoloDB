module SoloDbTypes

open System.Linq.Expressions
open System
open System.Text.Json.Serialization

type InnerExpr(expr: Expression<System.Func<obj, bool>>) =
    member this.Expression = expr

type SqlId =
    private SqlId of int64
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

[<CLIMutable>]
type DbObjectRow = {
    Id: int64
    ValueJSON: string
}

[<AbstractClass>]
type SoloDBEntry() =
    let mutable id = 0L
    [<JsonIgnore>]
    member this.Id with get() = id and private set value = id <- value
    static member InitId (entry: SoloDBEntry) (value: int64) =
        if entry.Id <> 0 then failwithf "Cannot set id, only init."
        entry.Id <- value