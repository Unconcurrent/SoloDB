namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorVisitPostJoin
open DBRefTypeHelpers

/// Handles DBRef<T> single-reference navigation in expression trees.
/// Translates o.Ref.Id, o.Ref.HasValue, o.Ref.Value.Name patterns to SQL.
module internal QueryTranslatorVisitDbRefSingleRef =

    let internal handleDBRefExpression (qb: QueryBuilder) (exp: Expression) : bool =
        let publishScalar value = qb.DuHandlerResult.Value <- ValueSome(QueryProduced.typed exp.Type value)
        let referenceIdentity (reference: Expression) =
            let struct(produced, _, _) = QueryTranslatorVisitCoreMemberAccess.referenceInput QueryTranslatorVisitCore.visitProduced reference qb
            QueryProduced.referenceIdentity qb.SourceContext.NextAlias produced
        let selectedTarget asReference resultType (reference: Expression) (produced: QueryProducedValue) project =
            let target = referenceTargetType reference.Type
            let sources = produced.Input.ReferenceOwner.Value.Sources
            let targets = sources |> Array.map (fun source ->
                let table = qb.SourceContext.ResolveQueryRelationTarget(source.Owner |> ValueOption.map _.TableName, source.PropertyName, target)
                let input = QueryTranslatorPhysicalSource.ownerInput qb.SourceContext target table (SqlExpr.Literal SqlLiteral.Null)
                let source = source.Owner |> ValueOption.defaultValue input.PhysicalOwner.Value.Sources[0]
                { source with TableName = table; OwnerType = target })
            QueryProduced.bind qb.SourceContext.NextAlias (fun value input ->
                let token = input.Source |> ValueOption.defaultWith (fun () -> jsonToken value)
                let owner = input.ReferenceOwner.Value
                let branches = sources |> Array.mapi (fun index source ->
                    let alias = qb.SourceContext.NextAlias()
                    let table = targets[index].TableName
                    let targetValue = if isFileRefType reference.Type then fileSystemHeaderValueExpr target alias else groupedRowValue target alias
                    let targetValue =
                        if asReference then
                            let identity = QueryTranslatorReferenceJoin.identityFromToken QueryTranslatorReferenceJoin.StoredOrHydratedId token
                            SqlExpr.JsonArrayExpr [identity; targetValue]
                        else targetValue
                    let navigation = input.Navigation |> Array.map (fun scope -> { scope with Path = if asReference then scope.Path else scope.Path @ ["Value"] })
                    let input =
                        if isFileRefType reference.Type then
                            { QuerySerializationInput.ForType (if asReference then reference.Type else target) with
                                Source = ValueSome targetValue; ReferenceOwner = if asReference then input.ReferenceOwner else ValueNone }
                        else { QuerySerializationInput.ForOwner(target, table, SqlExpr.Column(Some alias, "Id"), targets[index].Loading) with Source = ValueSome targetValue }
                    let input = { input with Navigation = navigation }
                    let edge = { TargetAlias = alias; TargetTable = table; TargetValue = targetValue
                                 ReferenceValue = token; ReferencePath = []; ReferenceOnOwnerRoot = false
                                 JoinKind = "LEFT JOIN"; OnSourceAlias = None; OnPropertyName = ""; PropertyPath = "" }
                    let sourceTable, identity =
                        match QueryTranslatorReferenceJoin.joinEdgeToClause Left None QueryTranslatorReferenceJoin.StoredOrHydratedId edge with
                        | ConditionedJoin(_, source, identity) -> source, identity
                        | _ -> invalidOp "A reference source requires its indexed identity condition."
                    let selected = project (Some alias) (DirectValue(targetValue, input))
                    { Distinct = false; Source = Some sourceTable; Joins = []
                      Projections = Explicit({ Alias = None; Expr = QueryProduced.packet selected }, [])
                      Where = Some(SqlExpr.Binary(QueryTranslatorPhysicalSource.selectsReferenceSource owner source, BinaryOperator.And, identity))
                      GroupBy = []; Having = None; OrderBy = []; Limit = None; Offset = None }, selected.Input)
                let rows = Array.map fst branches
                let output = QueryTranslatorSerialization.mergeInputs resultType (Array.map snd branches)
                let body = if rows.Length = 1 then SingleSelect rows[0] else UnionAllSelect(rows[0], rows[1..] |> Array.toList)
                PackedValue(SqlExpr.ScalarSubquery { Ctes = []; Body = body }, QueryTranslatorSerialization.rebaseSerializationInput output)) produced

        let targetProduced resultType (reference: Expression) project =
            let target = referenceTargetType reference.Type
            let referenceFacts = QueryTranslatorVisitCoreMemberAccess.referenceInput QueryTranslatorVisitCore.visitProduced reference qb
            let struct(referenceInput, _, _) = referenceFacts
            let path = validateReferenceNavigation referenceInput
            let navigation = referenceInput.Input.Navigation |> Array.map (fun scope -> { scope with Path = scope.Path @ ["Value"] })
            let selected =
                match referenceInput.Input.ReferenceOwner with
                | ValueSome owner ->
                    match owner.Descriptor with
                    | SqlExpr.JsonArrayExpr [_; SqlExpr.Literal(SqlLiteral.String _)]
                        when owner.Sources.Length = 1 && (match referenceInput with DirectValue _ -> true | _ -> false) -> false
                    | _ -> true
                | _ -> false
            if selected then selectedTarget false resultType reference referenceInput project
            else
            match reference with
            | :? ParameterExpression when isFileRefType reference.Type ->
                QueryProduced.bind qb.SourceContext.NextAlias (fun value input ->
                    let token = input.Source |> ValueOption.defaultWith (fun () -> jsonToken value)
                    let header = tupleLeafToken token 1
                    let owner = input.PhysicalOwner |> ValueOption.filter (fun owner -> owner.Sources |> Array.forall (fun source -> source.OwnerType.Equals target))
                    let input = { QuerySerializationInput.ForType target with Source = ValueSome header; PhysicalOwner = owner; Navigation = navigation }
                    project None (DirectValue(header, input))) referenceInput
            | _ ->
                let value, source, alias, physicalOwner =
                    let edge = ensurePreparedDBRefJoin qb reference path referenceFacts
                    let alias = edge.TargetAlias
                    if isFileRefType reference.Type then
                        let header = fileSystemHeaderValueExpr target alias
                        header, header, Some alias, ValueNone
                    else
                        let loading = referenceInput.Input.ReferenceOwner |> ValueOption.bind (fun owner -> owner.Sources[0].Owner |> ValueOption.map _.Loading)
                        let owner = (QuerySerializationInput.ForOwner(target, edge.TargetTable, SqlExpr.Column(Some alias, "Id"), ?loading = Utils.valueOptionToOption loading)).PhysicalOwner
                        groupedRowValue target alias,
                        SqlExpr.FunctionCall("jsonb", [SqlExpr.Column(Some alias, "Value")]), Some alias, owner
                let input = { QuerySerializationInput.ForType target with Source = ValueSome source; PhysicalOwner = physicalOwner; Navigation = navigation }
                let input = QueryTranslatorSerialization.scopeSerializationInput alias BaseLayer input
                project alias (DirectValue(value, { input with Carrier = CanonicalToken }))

        let targetValue (reference: Expression) = targetProduced (referenceTargetType reference.Type) reference (fun _ value -> value)
        let targetMember (reference: Expression) memberName resultType =
            targetProduced resultType reference (fun alias target ->
                match alias with
                | Some alias when isFileRefType reference.Type && memberName <> "Metadata" ->
                    QueryProduced.typed resultType (SqlExpr.Column(Some alias, memberName))
                | _ ->
                    QueryTranslatorVisitCoreMemberAccess.accessValue qb.SourceContext.NextAlias false
                        (ClrMember(memberName, resultType)) resultType target
                    |> QueryProduced.bind qb.SourceContext.NextAlias (fun value input ->
                        if isSqlScalarResult resultType then
                            DirectValue(input.Source |> ValueOption.defaultWith (fun () -> jsonToken value), { input with Carrier = CanonicalToken })
                        else DirectValue(value, input)))

        let capturedFilesystemValue =
            match exp with
            | :? MemberExpression as access when
                isFileRefType access.Type || (not (isNull access.Expression) && isFileRefType (unwrapConvert access.Expression).Type) ->
                isFullyConstant exp || (qb.SourceContext.BindQueryValue |> ValueOption.exists (fun binder -> binder.IsValue exp))
            | _ -> false
        match exp with
        | :? MemberExpression as memberAccess when not (isNull memberAccess.Expression) && isFileRefType (unwrapConvert memberAccess.Expression).Type && capturedFilesystemValue -> false
        | :? MemberExpression as reference when isFileRefType reference.Type && not qb.UpdateMode && not capturedFilesystemValue ->
            let referenceFacts = QueryTranslatorVisitCoreMemberAccess.referenceInput QueryTranslatorVisitCore.visitProduced reference qb
            let struct(produced, _, _) = referenceFacts
            if not(referenceSnapshotIncluded produced) then false else
            let dynamic = produced.Input.ReferenceOwner |> ValueOption.exists (fun owner ->
                owner.Sources.Length > 1 ||
                (match owner.Descriptor with
                 | SqlExpr.JsonArrayExpr [_; SqlExpr.Literal(SqlLiteral.String _)] -> false
                 | _ -> true))
            if dynamic then
                validateReferenceNavigation produced |> ignore
                qb.DuHandlerResult.Value <- ValueSome(selectedTarget true reference.Type reference produced (fun _ value -> value))
            else
                let targetType = referenceTargetType reference.Type
                let path = validateReferenceNavigation produced
                let targetAlias = (ensurePreparedDBRefJoin qb reference path referenceFacts).TargetAlias
                let value = QueryProduced.bind qb.SourceContext.NextAlias (fun value input ->
                    let token = input.Source |> ValueOption.defaultWith (fun () -> jsonToken value)
                    let identity = QueryTranslatorReferenceJoin.identityFromToken QueryTranslatorReferenceJoin.StoredOrHydratedId token
                    let payload = SqlExpr.CaseExpr((SqlExpr.Unary(UnaryOperator.IsNull, identity), SqlExpr.Literal(SqlLiteral.Null)), [],
                                                  Some(SqlExpr.JsonArrayExpr [identity; fileSystemHeaderValueExpr targetType targetAlias]))
                    DirectValue(payload, { input with Source = ValueSome payload; Carrier = CanonicalToken })) produced
                qb.DuHandlerResult.Value <- ValueSome value
            true
        | :? MemberExpression as topMe when not (isNull topMe.Expression) ->
            let innerExpr = unwrapConvert topMe.Expression

            // Case 1: Direct member on DBRef<T> — o.Ref.Id, o.Ref.HasValue, o.Ref.TypedId (typed only)
            if isSingleReferenceType innerExpr.Type then
                match topMe.Member.Name with
                | "Id" ->
                    let identity = referenceIdentity innerExpr
                    let identity = if isFileRefType innerExpr.Type then SqlExpr.FunctionCall("coalesce", [identity; SqlExpr.Literal(SqlLiteral.Integer 0L)]) else identity
                    publishScalar identity
                    true
                | "HasValue" ->
                    let identity = referenceIdentity innerExpr
                    publishScalar(SqlExpr.Binary(SqlExpr.Unary(UnaryOperator.IsNotNull, identity), BinaryOperator.And,
                        SqlExpr.Binary(identity, BinaryOperator.Ne, SqlExpr.Literal(SqlLiteral.Integer 0L))))
                    true
                | "Value" ->
                    qb.DuHandlerResult.Value <- ValueSome(targetValue innerExpr)
                    true
                | "TypedId" when isDBRefTypedType innerExpr.Type ->
                    let targetType = referenceTargetType innerExpr.Type
                    match SoloIdAccessor.TryGetMember targetType with
                    | ValueNone ->
                        raise (NotSupportedException(
                            sprintf "Cannot translate DBRef<%s,_>.TypedId: target type has no [<SoloId>] property." targetType.Name))
                    | ValueSome soloIdProp ->
                        qb.DuHandlerResult.Value <- ValueSome(targetMember innerExpr soloIdProp.Name topMe.Type)
                        true
                | "TypedId" ->
                    raise (NotSupportedException(
                        sprintf "DBRef<%s>.TypedId is not available on the single-arg DBRef form. Use .Id for row-id comparisons, or declare the relation as DBRef<%s,'TId> for typed-id queries." innerExpr.Type.Name innerExpr.Type.Name))
                | _ -> false

            // Physical target fields retain their column authority. Other children
            // are consumed by the ordinary paired member producer from Value above.
            else
                match unwrapStructCopy topMe.Expression with
                | :? MemberExpression as boundary when isDBRefValueBoundary boundary ->
                    let reference = unwrapConvert boundary.Expression
                    let filesystem = isFileRefType reference.Type
                    if filesystem && reference :? ParameterExpression then false
                    elif topMe.Member.Name = "Id" && typeof<int64>.Equals(topMe.Type)
                         || filesystem && topMe.Member.Name <> "Metadata" then
                        qb.DuHandlerResult.Value <- ValueSome(targetMember reference topMe.Member.Name topMe.Type)
                        true
                    else false
                | _ -> false
        | _ -> false
