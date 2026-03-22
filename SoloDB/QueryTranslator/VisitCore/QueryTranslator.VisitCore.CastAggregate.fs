namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Collections.ObjectModel
open System.Linq.Expressions
open System.Reflection
open System.Runtime.InteropServices
open System.Text
open JsonFunctions
open Utils
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorBase
open SqlDu.Engine.C1.Spec

module internal QueryTranslatorVisitCoreCastAggregate =
    let internal castToDu (visitDu: Expression -> QueryBuilder -> SqlExpr) (qb: QueryBuilder) (castToType: Type) (o: Expression) : SqlExpr =
        let sqlType =
            match castToType with
            | OfType float | OfType float32 | OfType decimal -> Some "REAL"
            | OfType bool | OfType int8 | OfType uint8 | OfType int16 | OfType uint16
            | OfType int32 | OfType uint32 | OfType int64 | OfType uint64
            | OfType nativeint | OfType unativeint -> Some "INTEGER"
            | _ -> None
        match sqlType with
        | None -> visitDu o qb
        | Some t -> SqlExpr.Cast(visitDu o {qb with UpdateMode = false}, t)

    let private parseMathFormatDu (format: string) (resolveArg: int -> SqlExpr) : SqlExpr =
        if format = "($1 - $2 * ROUND($1 / $2))" then
            SqlExpr.Binary(resolveArg 1, BinaryOperator.Sub, SqlExpr.Binary(resolveArg 2, BinaryOperator.Mul, SqlExpr.FunctionCall("ROUND", [SqlExpr.Binary(resolveArg 1, BinaryOperator.Div, resolveArg 2)])))
        elif format = "($1 * $2)" then
            SqlExpr.Binary(resolveArg 1, BinaryOperator.Mul, resolveArg 2)
        elif format = "($2 - $3 * ROUND($2 / $3))" then
            SqlExpr.Binary(resolveArg 2, BinaryOperator.Sub, SqlExpr.Binary(resolveArg 3, BinaryOperator.Mul, SqlExpr.FunctionCall("ROUND", [SqlExpr.Binary(resolveArg 2, BinaryOperator.Div, resolveArg 3)])))
        elif format = "($2 * $3)" then
            SqlExpr.Binary(resolveArg 2, BinaryOperator.Mul, resolveArg 3)
        else
            let parenIdx = format.IndexOf('(')
            let funcName = format.Substring(0, parenIdx)
            let argsStr = format.Substring(parenIdx + 1, format.Length - parenIdx - 2)
            let args =
                argsStr.Split(',')
                |> Array.map (fun s ->
                    let s = s.Trim()
                    if s.Length > 0 && s.[0] = '$' then resolveArg (int (s.Substring(1)))
                    else SqlExpr.Literal(SqlLiteral.Integer(int64 s)))
                |> Array.toList
            SqlExpr.FunctionCall(funcName, args)

    let internal visitMathMethodDu (visitDu: Expression -> QueryBuilder -> SqlExpr) (m: MethodCallExpression) (qb: QueryBuilder) : SqlExpr voption =
        match mathFunctionTransformation.TryGetValue((m.Arguments.Count, m.Method.Name)) with
        | false, _ -> ValueNone
        | true, format ->
            let resolveArg (n: int) =
                visitDu (match n with | 0 -> m.Object | d -> m.Arguments.[d - 1]) qb
            ValueSome(parseMathFormatDu format resolveArg)

