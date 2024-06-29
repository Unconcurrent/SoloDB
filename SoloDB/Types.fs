module SoloDBTypes

open System.Linq.Expressions
open System.Text.Json.Serialization

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

[<CLIMutable>]
type DbObjectRow = {
    Id: int64
    ValueJSON: string
}