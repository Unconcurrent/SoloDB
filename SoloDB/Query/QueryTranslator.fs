namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq.Expressions
open System.Text
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBase
open SoloDatabase.QueryTranslatorVisitCore
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.QueryTranslatorVisitDbRef
open SqlDu.Engine.C1.Spec

/// <summary>
/// Contains functions to translate .NET LINQ expression trees into SQLite SQL queries.
/// All entry points route through the DU construction path (visitDu) + DU emission (SqlDuMinimalEmit).
/// </summary>
module internal QueryTranslator =
    let private ensureDbRefHandlersInitialized () =
        let count = QueryTranslatorVisitDbRef.handlerCount
        if count < 3 then
            raise (InvalidOperationException(
                $"DBRef handler registration incomplete: count={count}, expected=3"))


    /// <summary>
    /// Translates a LINQ expression into a SQL string and a dictionary of parameters.
    /// Routes through DU construction (visitDu) and DU emission (SqlDuMinimalEmit).
    /// </summary>
    /// <param name="tableName">The name of the table to query.</param>
    /// <param name="expression">The LINQ expression to translate.</param>
    /// Canonical predicate path: translates a filter expression to SqlExpr DU + variables.
    /// Used by write-path call sites that compose UPDATE/DELETE templates with DU-emitted WHERE.
    /// This is the canonical predicate producer; `translate` is a thin wrapper over this.
    /// Translate a predicate into the supplied parameter dictionary.
    ///
    /// A statement owns one dictionary, and parameter names are allocated in the order the SQL
    /// reads. A predicate translated into a dictionary of its own would start numbering from the
    /// beginning and could not then be merged without either colliding with the assignments or
    /// renaming them, so the caller's dictionary is threaded through rather than combined after.
    let translateWhereExprInto (tableName: string) (expression: Expression) (variables: Dictionary<string, obj>) : SqlExpr =
        ensureDbRefHandlersInitialized()
        let sb = StringBuilder()
        let builder = QueryBuilder.New sb variables false tableName expression -1 ValueNone
        visitDu expression builder

    let translateWhereExpr (tableName: string) (expression: Expression) : SqlExpr * Dictionary<string, obj> =
        let variables = Dictionary<string, obj>()
        let duExpr = translateWhereExprInto tableName expression variables
        duExpr, variables

    /// <returns>A tuple containing the generated SQL string and a dictionary of parameters.</returns>
    /// Returns a SqlExpr DU node for an expression without emitting to any StringBuilder.
    /// Used by the Queryable DU construction path to build SqlSelect trees.
    /// Side effects: allocates parameters in the provided Variables dict; may populate sourceContext.Joins.
    let internal translateToSqlExpr (sourceContext: QueryContext) (tableName: string) (expression: Expression) (variables: Dictionary<string, obj>) : SqlExpr =
        ensureDbRefHandlersInitialized()
        let sb = StringBuilder()
        let builder = QueryBuilder.New sb variables false tableName expression -1 (ValueSome sourceContext)
        visitDu expression builder

    /// Translate an expression in predicate context (WHERE clause).
    /// Min/Max aggregates skip the CASE WHEN sentinel, allowing SQL-native NULL propagation.
    let internal translateToSqlExprForPredicate (sourceContext: QueryContext) (tableName: string) (expression: Expression) (variables: Dictionary<string, obj>) : SqlExpr =
        ensureDbRefHandlersInitialized()
        let sb = StringBuilder()
        let builder = { QueryBuilder.New sb variables false tableName expression -1 (ValueSome sourceContext) with InPredicateContext = true }
        visitDu expression builder

    /// <summary>
    /// Translates an expression and appends the result to an existing StringBuilder.
    /// Routes through DU construction (visitDu) and DU emission (SqlDuMinimalEmit).
    /// </summary>
    let internal translateQueryable (tableName: string) (expression: Expression) (sb: StringBuilder) (variables: Dictionary<string, obj>) =
        ensureDbRefHandlersInitialized()
        let sbStart = sb.Length
        let builder = QueryBuilder.New sb variables false tableName expression -1 ValueNone
        let duExpr = visitDu expression builder
        sb.Length <- sbStart
        SqlDuMinimalEmit.emitExpr builder duExpr
        sb.Append " " |> ignore

    /// <summary>
    /// Translates an expression in "update" mode, generating SQL fragments for jsonb_set arguments.
    /// Routes through DU construction (visitDu with UpdateMode) and DU emission (SqlDuMinimalEmit).
    /// </summary>
    let private updateActionUnsupportedReason = "Error: unsupported update expression."

    let private updateActionUnsupportedFix =
        "Fix: each update expression must be exactly one assignment - a direct property assignment, or Set, Append, Add, Insert, SetAt or RemoveAt. Conditionals and other composite expressions are not supported; choose the value inside the assignment rather than choosing between assignments."

    /// Method names that form an assignment.
    let private updateAssignmentMethods =
        HashSet<string>([ "Set"; "Append"; "Add"; "Insert"; "SetAt"; "RemoveAt" ], StringComparer.Ordinal)

    /// Strip the wrappers a compiler puts around an action body, leaving the expression itself.
    let rec private updateActionCore (expr: Expression) : Expression =
        match expr with
        | :? LambdaExpression as lambda -> updateActionCore lambda.Body
        | :? UnaryExpression as unary when
                unary.NodeType = ExpressionType.Quote || unary.NodeType = ExpressionType.Convert ->
            updateActionCore unary.Operand
        | other -> other

    /// The complete grammar of one update action expression: exactly one assignment.
    ///
    /// Nothing that merely CONTAINS an assignment is admitted. Recording an assignment is a side
    /// effect of visiting, so an assignment sitting in a branch the condition does not select
    /// would still be recorded and then applied unconditionally, writing data the caller never
    /// asked for. Refusing the shape before it is visited is what makes that impossible rather
    /// than unlikely.
    let private isUpdateAssignmentShape (core: Expression) =
        match core with
        | :? MethodCallExpression as call -> updateAssignmentMethods.Contains call.Method.Name
        | :? BinaryExpression as binary -> binary.NodeType = ExpressionType.Assign
        | _ -> false

    /// Refuse anything that is not exactly one assignment, without losing the specific diagnostic
    /// the translator already gives for particular unsupported shapes.
    ///
    /// An unsupported expression is first visited against a throwaway builder. Many shapes have a
    /// precise message worth keeping — indexer assignment on a relation collection, for one — and
    /// that message comes from the visit. Anything the throwaway visit records is discarded with
    /// it, so this cannot contribute an assignment to the real statement. If the visit raises, the
    /// specific error is what the caller sees; if it does not, the shape is refused here.
    let private validateUpdateActionShape (tableName: string) (expression: Expression) : unit =
        let core = updateActionCore expression
        if not (isUpdateAssignmentShape core) then
            // An unsupported method call is given to the visitor first, because several have a
            // precise message worth keeping — indexer assignment on a relation collection, for
            // one. A call has no branches, so nothing about it can be conditionally applied, and
            // whatever this throwaway visit records is discarded with the builder it recorded
            // into. Composite shapes are refused without being visited at all: those are exactly
            // the ones that can carry an assignment down a path the caller never selected.
            match core with
            | :? MethodCallExpression ->
                let scratchBuilder =
                    QueryBuilder.New (StringBuilder()) (Dictionary<string, obj>()) true tableName expression -1 ValueNone
                visitDu expression scratchBuilder |> ignore
            | _ -> ()

            raise (NotSupportedException(
                sprintf "%s\nReason: the expression is %A, not an assignment.\n%s"
                    updateActionUnsupportedReason core.NodeType updateActionUnsupportedFix))

    /// Translate one update action expression into its ordered (path, value) assignments.
    ///
    /// This is the same visit the string corridor performs, stopping before emission so the
    /// caller receives structure instead of text. Parameters are allocated into the supplied
    /// dictionary in visit order, which is what keeps the numbering identical to the corridor
    /// this replaces: assignments first, in source order, then whatever the caller translates
    /// next.
    let internal translateUpdateAssignments
        (tableName: string) (expression: Expression) (variableDict: Dictionary<string, obj>)
        : (SqlExpr * SqlExpr) list =
        ensureDbRefHandlersInitialized()
        match QueryTranslatorVisitPost.tryTranslateUpdateManyRelationTransform expression with
        | ValueSome _ ->
            raise (NotSupportedException updateManyRelationUnsupportedMessage)
        | ValueNone -> ()

        // The visit needs a builder, but nothing is emitted through it; the buffer is a sink that
        // stays empty, so no caller's text can influence the result.
        // The grammar is checked before anything is visited. Recording an assignment is a side
        // effect of visiting, so visiting an expression that merely CONTAINS an assignment — a
        // conditional, a wrapper, a composite — would record it and then apply it
        // unconditionally. Validating first means an unsupported shape is refused before it can
        // leave anything behind.
        validateUpdateActionShape tableName expression

        let sink = StringBuilder()
        let builder = QueryBuilder.New sink variableDict true tableName expression -1 ValueNone
        visitDu expression builder |> ignore

        // One action expression is exactly one assignment. More would mean a shape slipped past
        // the grammar and recorded extra pairs; fewer means it recorded none.
        if builder.UpdateAssignments.Count <> 1 then
            raise (NotSupportedException(
                sprintf "%s\nReason: the expression produced %d assignments; exactly one is expected.\n%s"
                    updateActionUnsupportedReason builder.UpdateAssignments.Count updateActionUnsupportedFix))

        List.ofSeq builder.UpdateAssignments
