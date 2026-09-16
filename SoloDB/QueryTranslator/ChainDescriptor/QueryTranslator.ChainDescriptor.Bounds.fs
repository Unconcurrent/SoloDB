namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorVisitPost

/// Numeric bounds of a query chain: how a Take/Skip argument becomes a value, and how a
/// take/skip pair becomes the LIMIT/OFFSET pair.
///
/// The skip-without-take case emits LIMIT -1, which SQLite reads as unbounded; it is the only
/// way to express OFFSET without LIMIT. Both chain builders previously carried their own copy of
/// this rule, differing only in whether the value was wrapped into a SqlExpr early or late.
module internal ChainBounds =
    let evalNonNegativeInt64Bound (expr: Expression) : int64 =
        let raw = evaluateExpr<obj> expr
        let value = Convert.ToInt64(raw)
        if value < 0L then 0L else value

    /// LIMIT and OFFSET for a take/skip pair, as SQL expressions.
    let buildLimitOffset (takeExpr: Expression option) (skipExpr: Expression option) =
        let takeValue = takeExpr |> Option.map evalNonNegativeInt64Bound
        let skipValue = skipExpr |> Option.map evalNonNegativeInt64Bound
        let limit =
            match takeValue, skipValue with
            | Some n, _ -> Some (SqlExpr.Literal(SqlLiteral.Integer n))
            | None, Some _ -> Some (SqlExpr.Literal(SqlLiteral.Integer -1L))
            | None, None -> None
        let offset = skipValue |> Option.map (fun n -> SqlExpr.Literal(SqlLiteral.Integer n))
        limit, offset
