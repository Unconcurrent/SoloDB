namespace SoloDatabase

open System
open SoloDatabase.SqlModel
open SoloDatabase.JsonSerializator.JsonEqualitySupport
open SoloDatabase.QueryTranslatorBaseTypes

/// Constructs reference edges from caller-owned source and identity facts.
module internal QueryTranslatorReferenceJoin =
    /// Reference occurrences are identified by their authored path and source scope.
    let tryFindEdge (context: QueryContext) path sourceAlias targetTable referenceValue =
        let normalize = Option.map AliasRewrite.normalizeSourceAlias
        let source = normalize sourceAlias
        context.Joins |> Seq.tryFind (fun edge ->
            edge.PropertyPath = path && normalize edge.OnSourceAlias = source &&
            edge.TargetTable = targetTable && edge.ReferenceValue = referenceValue)

    /// Parse a JoinKind string from the handler mechanism into a DU JoinKind.
    let internal parseJoinKind (kind: string) =
        match kind.Trim().ToUpperInvariant() with
        | s when s.Contains("LEFT") -> JoinKind.Left
        | s when s.Contains("INNER") -> JoinKind.Inner
        | s when s.Contains("CROSS") ->
            raise (InvalidOperationException("CROSS JOIN must use the CrossJoin DU constructor directly."))
        | other ->
            raise (NotSupportedException(sprintf "Error: Unrecognized join kind '%s'.\nFix: Use LEFT, INNER, or CROSS join." other))

    type ReferenceIdentity = StoredId | StoredOrHydratedId

    /// The caller supplies the original stored, hydrated and JSON-kind operands.
    let identityValue representation stored hydrated kind =
        match representation with
        | StoredId -> stored
        | StoredOrHydratedId ->
            SqlExpr.CaseExpr((SqlExpr.Binary(kind(), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.String "array")), hydrated()), [], Some stored)

    /// Stored scalars and hydrated payloads share the original identity token layout.
    let identityFromToken representation token =
        identityValue representation
            (SqlExpr.FunctionCall("jsonb_extract", [token; SqlExpr.Literal(SqlLiteral.String "$")]))
            (fun () -> SqlExpr.FunctionCall("jsonb_extract", [token; SqlExpr.Literal(SqlLiteral.String "$[0]")]))
            (fun () -> SqlExpr.FunctionCall("json_type", [token]))

    /// Pending typed keys retain their marker; persisted snapshots compare by identity.
    let defaultOperand referenceType token =
        let identity = identityFromToken StoredOrHydratedId token
        let identity = SqlExpr.Coalesce(identity, [SqlExpr.Literal(SqlLiteral.Integer 0L)])
        let value =
            if DBRefTypeHelpers.isDBRefTypedType referenceType then
                let pending = SqlExpr.Binary(SqlExpr.FunctionCall("json_type", [token]), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.String "object"))
                SqlExpr.CaseExpr((pending, token), [], Some identity)
            else identity
        value

    /// Each caller supplies its scope and existing reference representation.
    let joinEdgeToClause kind sourceAlias representation (edge: JoinEdge) : JoinShape =
        let token = SqlExpr.map (function
            | SqlExpr.Column(None, name) -> SqlExpr.Column(sourceAlias, name)
            | other -> other) edge.ReferenceValue
        let identity = identityFromToken representation token
        ConditionedJoin(kind, BaseTable(edge.TargetTable, Some edge.TargetAlias),
            SqlExpr.Binary(SqlExpr.Column(Some edge.TargetAlias, "Id"), BinaryOperator.Eq, identity))

    /// Prepared query scopes deliver their discovered edges with the consuming selector.
    let preparedJoinClauses (edges: seq<JoinEdge>) =
        edges |> Seq.map (fun edge ->
            joinEdgeToClause (parseJoinKind edge.JoinKind) edge.OnSourceAlias StoredOrHydratedId edge)
        |> Seq.toList
