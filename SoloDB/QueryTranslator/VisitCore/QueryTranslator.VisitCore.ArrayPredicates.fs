namespace SoloDatabase

open System
open System.Collections
open System.Linq.Expressions
open Utils
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorReferenceJoin
open SoloDatabase.QueryTranslatorBase
open SoloDatabase.QueryTranslatorSerialization
open SoloDatabase.QuerySerializationSql

/// Array predicates consume the facts returned by their actual collection producer.
module internal QueryTranslatorVisitCoreArrayPredicates =
    let internal contains (isByRefLikeType: Type -> bool) (visitProduced: Expression -> QueryBuilder -> QueryProducedValue) (visitComparisonDu: Expression -> QueryBuilder -> SqlExpr) (qb: QueryBuilder) (array: Expression) (value: Expression) : SqlExpr =
        let rec normalizeContainsSource (expr: Expression) =
            match unwrapConvert expr with
            | :? MethodCallExpression as mc when isByRefLikeType mc.Type && mc.Type.Name = "ReadOnlySpan`1" && mc.Arguments.Count >= 1 ->
                match mc.Method.Name with
                | "AsSpan"
                | "op_Implicit" -> normalizeContainsSource mc.Arguments.[0]
                | _ -> expr
            | :? NewExpression as ne when isByRefLikeType ne.Type && ne.Type.Name = "ReadOnlySpan`1" && ne.Arguments.Count >= 1 ->
                normalizeContainsSource ne.Arguments.[0]
            | other -> other
        let tryIntegerLiteral (value: obj) =
            match value with
            | null -> None
            | :? int8 as v -> Some(SqlExpr.Literal(SqlLiteral.Integer(int64 v)))
            | :? uint8 as v -> Some(SqlExpr.Literal(SqlLiteral.Integer(int64 v)))
            | :? int16 as v -> Some(SqlExpr.Literal(SqlLiteral.Integer(int64 v)))
            | :? uint16 as v -> Some(SqlExpr.Literal(SqlLiteral.Integer(int64 v)))
            | :? int32 as v -> Some(SqlExpr.Literal(SqlLiteral.Integer(int64 v)))
            | :? uint32 as v -> Some(SqlExpr.Literal(SqlLiteral.Integer(int64 v)))
            | :? int64 as v -> Some(SqlExpr.Literal(SqlLiteral.Integer v))
            | :? uint64 as v when v <= uint64 Int64.MaxValue -> Some(SqlExpr.Literal(SqlLiteral.Integer(int64 v)))
            | :? nativeint as v -> Some(SqlExpr.Literal(SqlLiteral.Integer(int64 v)))
            | _ -> None
        let tryConstantIntegerInList (source: Expression) =
            let native = isRootParameter value && Utils.isIntegerBasedType value.Type && (QueryTranslatorNumeric.domain value.Type).IsNone
            let immutableLiteral =
                match source with
                | :? NewArrayExpression as array -> array.Expressions |> Seq.forall (fun element -> element :? ConstantExpression)
                | _ -> qb.SourceContext.BindQueryValue.IsNone
            let predicate comparison literals =
                match literals with
                | [] -> SqlExpr.Binary(SqlExpr.Literal(SqlLiteral.Integer 1L), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L))
                | head :: tail -> SqlExpr.InList(comparison, head, tail)
            if not immutableLiteral && isFullyConstant source && native then
                match qb.SourceContext.BindQueryValue with
                | ValueSome binder when binder.PrepareList.IsSome && ModelMetadata.SequenceElement source.Type = value.Type ->
                    let complete = binder.PrepareList.Value source
                    let comparison = visitComparisonDu value qb
                    let items = complete (fun sampled ->
                        if isNull sampled then []
                        else
                            (sampled :?> IEnumerable) |> Seq.cast<obj>
                            |> Seq.map (tryIntegerLiteral >> Option.get) |> Seq.toList)
                    Some(SqlExpr.InList(comparison, items, []))
                | _ -> None
            elif immutableLiteral && isFullyConstant source && native then
                let sourceValue = ExpressionEvaluation.evaluateExpr<obj> source
                match sourceValue with
                | :? IEnumerable as items when not (sourceValue :? string) ->
                    let literals =
                        items
                        |> Seq.cast<obj>
                        |> Seq.map tryIntegerLiteral
                        |> Seq.toList
                    if literals |> List.forall Option.isSome then
                        let literals = literals |> List.choose id
                        match literals with
                        | [] -> Some(predicate (SqlExpr.Literal SqlLiteral.Null) [])
                        | _ -> Some(predicate (visitComparisonDu value qb) literals)
                    else None
                | _ -> None
            else None
        let normalizedArray = normalizeContainsSource array
        match tryConstantIntegerInList normalizedArray with
        | Some expr -> expr
        | None ->
        let arrayQb = if qb.SourceAlias.IsNone then {qb with SourceAlias = Some "o"} else qb
        let produced = visitProduced normalizedArray arrayQb |> QueryProduced.sequence qb.SourceContext.NextAlias
        QueryProduced.consume qb.SourceContext.NextAlias (fun arrayExpr input ->
            let token = SqlExpr.FunctionCall("jsonb", [SqlExpr.Binary(arrayExpr, BinaryOperator.JsonText, SqlExpr.Column(Some "json_each", "fullkey"))])
            let element = elementInput input (SqlExpr.Column(Some "json_each", "key")) token
            let target = QueryProduced.scalarValue value.Type token { element with Carrier = CanonicalToken }
                         |> fun valueExpr -> QueryTranslatorCharacters.comparable value.Type valueExpr element
            let nativeIn = isRootParameter value && Utils.isIntegerBasedType value.Type && (QueryTranslatorNumeric.domain value.Type).IsNone
            if nativeIn then
                let values = { Ctes = []; Body = SingleSelect {
                    Distinct = false; Projections = Explicit({ Expr = target; Alias = None }, [])
                    Source = Some(FromJsonEach(arrayExpr, None)); Joins = []; Where = None
                    GroupBy = []; Having = None; OrderBy = []; Limit = None; Offset = None } }
                SqlExpr.InSubquery(visitComparisonDu value qb, values)
            else
                let whereExpr =
                    if isSqlScalarResult value.Type then
                        SqlExpr.Binary(target, BinaryOperator.Eq, visitComparisonDu value qb)
                    else
                        match qb.SourceContext.BindQueryValue with
                        | ValueSome binder when binder.IsValue value -> EqualityComparison.bound qb binder target value
                        | _ ->
                            if not (isFullyConstant value) then
                                raise (ArgumentException $"Cannot translate contains with this type of expression: {value.Type}")
                            EqualityComparison.known qb target value.Type (ExpressionEvaluation.evaluateExpr<obj> value)
                SqlExpr.Exists({ Ctes = []; Body = SelectBody.SingleSelect {
                    Distinct = false; Projections = Explicit({ Expr = SqlExpr.Literal(SqlLiteral.Integer 1L); Alias = None }, [])
                    Source = Some(TableSource.FromJsonEach(arrayExpr, None)); Joins = []
                    Where = Some whereExpr; GroupBy = []; Having = None; OrderBy = []; Limit = None; Offset = None } })
        ) produced

    let internal nested (visitProduced: Expression -> QueryBuilder -> QueryProducedValue) (visitDu: Expression -> QueryBuilder -> SqlExpr) (qb: QueryBuilder) (array: Expression) (whereFuncExpr: Expression) (isAll: bool) : SqlExpr =
        let expr =
            match whereFuncExpr with
            | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Quote -> ue.Operand
            | :? LambdaExpression as le -> le :> Expression
            | _ -> ExpressionEvaluation.evaluateExpr<Expression> whereFuncExpr
        let arrayQb = if qb.SourceAlias.IsNone then {qb with SourceAlias = Some "o"} else qb
        let lambda =
            match expr with
            | :? LambdaExpression as lambda -> lambda
            | _ -> raise (NotSupportedException("The array predicate must be a lambda expression."))
        let produced = visitProduced array arrayQb |> QueryProduced.sequence qb.SourceContext.NextAlias
        QueryProduced.consume qb.SourceContext.NextAlias (fun arrayExpr input ->
            let alias = qb.SourceContext.NextAlias()
            let fullKey = SqlExpr.Column(Some alias, "fullkey")
            let token = SqlExpr.FunctionCall("jsonb", [SqlExpr.Binary(arrayExpr, BinaryOperator.JsonText, fullKey)])
            let input = elementInput input (SqlExpr.Column(Some alias, "key")) token
            let input = { input with Carrier = if isSqlScalarResult lambda.Parameters[0].Type then SqlScalar else CanonicalToken }
            let innerQb = arrayQb.ForSubquery(alias, lambda.Parameters, OuterLayer, input)
            let element = QueryProduced.scalarValue lambda.Parameters.[0].Type token { input with Carrier = CanonicalToken }
            let predicateExpr =
                visitDu lambda.Body innerQb
                |> AliasRewrite.rewriteScopedAliasExpr (Map.ofList ["Value", element]) innerQb.SourceAlias.Value
            let whereExpr = if isAll then SqlExpr.Unary(UnaryOperator.Not, predicateExpr) else predicateExpr
            let joins = preparedJoinClauses innerQb.SourceContext.Joins
            let existsExpr = SqlExpr.Exists({ Ctes = []; Body = SelectBody.SingleSelect {
                Distinct = false; Projections = Explicit({ Expr = SqlExpr.Literal(SqlLiteral.Integer 1L); Alias = None }, [])
                Source = Some(TableSource.FromJsonEach(arrayExpr, Some alias))
                Joins = joins
                Where = Some whereExpr; GroupBy = []; Having = None; OrderBy = []; Limit = None; Offset = None } })
            if isAll then SqlExpr.Unary(UnaryOperator.Not, existsExpr) else existsExpr
        ) produced
