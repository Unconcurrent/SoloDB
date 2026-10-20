namespace SoloDatabase

open System
open SoloDatabase.SqlModel

/// Known physical and relation sources remain attached to their selected occurrence.
module internal QueryTranslatorPhysicalSource =
    let internal ownerInput (context: QueryContext) target table identity =
        let ordered values = values |> Seq.sortWith (fun left right -> StringComparer.Ordinal.Compare(left, right)) |> Seq.toArray
        let loading = { Excluded = ordered context.ExcludedPaths; Included = ordered context.IncludedPaths; Whitelist = context.WhitelistMode }
        QuerySerializationInput.ForOwner(target, table, identity, loading)

    /// Branches retain the known sources; the descriptor selects the actual occurrence.
    let internal sharedPhysicalOwner (inputs: QuerySerializationInput array) : QueryPhysicalOwner voption =
        let sources = inputs |> Array.collect (fun input -> input.PhysicalOwner |> ValueOption.map _.Sources |> ValueOption.defaultValue Array.empty)
        let sources = sources |> Array.distinct
        if sources.Length = 0 then ValueNone
        else ValueSome { Sources = sources; Descriptor = SqlExpr.Literal SqlLiteral.Null }

    let internal sharedReferenceOwner (inputs: QuerySerializationInput array) : QueryReferenceOwner voption =
        let sources = inputs |> Array.collect (fun input -> input.ReferenceOwner |> ValueOption.map _.Sources |> ValueOption.defaultValue Array.empty)
        let sources = Array.distinct sources
        if sources.Length = 0 then ValueNone
        else ValueSome { Sources = sources; Descriptor = SqlExpr.Literal SqlLiteral.Null }

    let internal selectsSource (owner: QueryPhysicalOwner) (source: QueryPhysicalSource) =
        let at path = SqlExpr.FunctionCall("jsonb_extract", [owner.Descriptor; SqlExpr.Literal(SqlLiteral.String path)])
        let descriptor = (QuerySerializationInput.ForOwner(source.OwnerType, source.TableName, SqlExpr.Literal SqlLiteral.Null, source.Loading)).PhysicalOwner.Value.Descriptor
        let policy = match descriptor with SqlExpr.JsonArrayExpr [_; _; _; policy] -> policy | _ -> invalidOp "An authored physical source requires its loading policy."
        match owner.Descriptor with
        | SqlExpr.JsonArrayExpr [SqlExpr.Literal(SqlLiteral.String table); SqlExpr.Literal(SqlLiteral.String target); _; original] ->
            SqlExpr.Literal(SqlLiteral.Integer(if table = source.TableName && target = TypeMetadata.typeIdentityKey source.OwnerType && original = policy then 1L else 0L))
        | _ ->
            SqlExpr.Binary(SqlExpr.Binary(SqlExpr.Binary(at "$[0]", BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.String source.TableName)), BinaryOperator.And,
                           SqlExpr.Binary(at "$[1]", BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.String(TypeMetadata.typeIdentityKey source.OwnerType)))), BinaryOperator.And,
                           SqlExpr.Binary(at "$[3]", BinaryOperator.Is, policy))

    let internal referenceOwner (reference: QueryReferenceOwner) : QueryPhysicalOwner =
        let descriptor =
            match reference.Descriptor with
            | SqlExpr.JsonArrayExpr [owner; _] -> owner
            | _ -> SqlExpr.FunctionCall("jsonb_extract", [reference.Descriptor; SqlExpr.Literal(SqlLiteral.String "$[0]")])
        { Sources = reference.Sources |> Array.choose (fun source -> source.Owner |> Utils.valueOptionToOption) |> Array.distinct; Descriptor = descriptor }

    let internal selectsReferenceSource (reference: QueryReferenceOwner) (source: QueryReferenceSource) =
        let property =
            match reference.Descriptor with
            | SqlExpr.JsonArrayExpr [_; property] -> property
            | _ -> SqlExpr.FunctionCall("jsonb_extract", [reference.Descriptor; SqlExpr.Literal(SqlLiteral.String "$[1]")])
        let owner = referenceOwner reference
        let selected =
            match source.Owner with
            | ValueSome source -> selectsSource owner source
            | ValueNone -> SqlExpr.Unary(UnaryOperator.IsNull, owner.Descriptor)
        SqlExpr.Binary(selected, BinaryOperator.And,
                       SqlExpr.Binary(property, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.String source.PropertyName)))
