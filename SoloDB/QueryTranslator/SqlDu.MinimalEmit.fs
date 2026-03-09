namespace SoloDatabase

open SqlDu.Engine.C1.Spec
open SoloDatabase.QueryTranslatorBaseTypes

/// Thin adapter: bridges the canonical C2-style emitter (QB-free, returns Emitted records)
/// into the product's QueryBuilder (StringBuilder + parameter dictionary).
/// No independent emission logic — all SQL generation delegates to the canonical emitter.
module internal SqlDuMinimalEmit =

    /// Emit a SqlExpr DU node into a QueryBuilder's StringBuilder and Variables dict.
    /// Uses the canonical emitter with InlineLiterals=true (product behavior).
    /// Handles one product-specific presentation concern: top-level root JSON extraction
    /// uses json_extract(json(...), '$') for readable output instead of jsonb_extract.
    let rec emitExpr (qb: QueryBuilder) (expr: SqlExpr) : unit =
        let isTopLevel = qb.StringBuilder.Length = 0

        // Special case: top-level root extraction for readable JSON output.
        // When the emitter is called at SB position 0 with a root-path JsonExtractExpr,
        // use json_extract(json(source), '$') instead of the canonical jsonb_extract.
        match expr with
        | SqlExpr.JsonExtractExpr(alias, col, JsonPath []) when isTopLevel ->
            let prefix =
                match alias with
                | Some a -> a + "."
                | None -> ""
            qb.StringBuilder.Append(sprintf "json_extract(json(%s%s), '$')" prefix col) |> ignore
        | _ ->
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
