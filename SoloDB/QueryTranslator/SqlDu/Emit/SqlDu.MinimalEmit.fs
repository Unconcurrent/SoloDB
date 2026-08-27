namespace SoloDatabase

open SqlDu.Engine.C1.Spec
open SoloDatabase.QueryTranslatorBaseTypes

/// Thin adapter: bridges the canonical emitter (QB-free, returns Emitted records)
/// into the product's QueryBuilder (StringBuilder + parameter dictionary).
/// No independent emission logic — all SQL generation delegates to the canonical emitter.
module internal SqlDuMinimalEmit =

    /// Emit a SqlExpr DU node into a QueryBuilder's StringBuilder and Variables dict.
    /// Uses the canonical emitter with InlineLiterals=true (product behavior).
    /// One expression has one meaning, whatever the destination already contains.
    let rec emitExpr (qb: QueryBuilder) (expr: SqlExpr) : unit =
        let ctx = EmitContext(InlineLiterals = true)
        let result = EmitSelect.emitExpr ctx expr
        qb.StringBuilder.Append(result.Sql) |> ignore
        for (name, value) in result.Parameters do
            qb.Variables.[name] <- value

    and emitSelect (qb: QueryBuilder) (sel: SqlSelect) : unit =
        let ctx = EmitContext(InlineLiterals = true)
        let result = EmitSelect.emitSelect ctx sel
        qb.StringBuilder.Append(result.Sql) |> ignore
        for (name, value) in result.Parameters do
            qb.Variables.[name] <- value
