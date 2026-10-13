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
    let internal contains (visitProduced: Expression -> QueryBuilder -> QueryProducedValue) (visitComparisonDu: Expression -> QueryBuilder -> SqlExpr) (qb: QueryBuilder) (array: Expression) (value: Expression) : SqlExpr =
        let span = QuerySpanSource.trySource array
        let normalizedArray = span |> Option.map (fun source -> source.Value) |> Option.defaultValue array
        let elementType = span |> Option.map (fun source -> source.Element) |> Option.defaultWith (fun () -> ModelMetadata.SequenceElement normalizedArray.Type)
        let zero = SqlExpr.Literal(SqlLiteral.Integer 0L)
        let absent = SqlExpr.Literal SqlLiteral.Null
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
                | _ when span |> Option.exists (fun range -> range.Start.IsSome) -> false
                | :? NewArrayExpression as array -> array.Expressions |> Seq.forall (fun element -> element :? ConstantExpression)
                | _ -> qb.SourceContext.BindQueryValue.IsNone
            let predicate comparison literals =
                match literals with
                | [] -> SqlExpr.Binary(SqlExpr.Literal(SqlLiteral.Integer 1L), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L))
                | head :: tail -> SqlExpr.InList(comparison, head, tail)
            if not immutableLiteral && isFullyConstant array && native then
                match qb.SourceContext.BindQueryValue with
                | ValueSome binder when binder.PrepareList.IsSome && elementType.Equals value.Type ->
                    let acquired =
                        match span with
                        | Some range when range.Start.IsSome ->
                            let arguments = [|source; range.Start.Value; range.Length |> Option.defaultValue (Expression.Constant(null,typeof<obj>) :> Expression)|]
                            BoundValueCallExpression(arguments, typeof<obj>, Func<obj array,obj>(fun values ->
                                box(struct(values[0] :?> Array, unbox<int> values[1], if isNull values[2] then None else Some(unbox<int> values[2]))))) :> Expression
                        | _ -> source
                    let complete = binder.PrepareList.Value acquired
                    let comparison = visitComparisonDu value qb
                    let items = complete (fun sampled ->
                        match span with
                        | Some range when range.Start.IsSome ->
                            let struct(array,start,length) = unbox<struct(Array * int * int option)> sampled
                            let available = if isNull array then 0 else array.Length
                            if start < 0 || start > available || (length |> Option.exists (fun length -> length < 0 || length > available - start)) then
                                [QueryTranslatorSpanSource.call false (SqlExpr.Literal(SqlLiteral.Integer(int64 available)))
                                     (SqlExpr.Literal(SqlLiteral.Integer(int64 start)))
                                     (length |> Option.map (int64 >> SqlLiteral.Integer >> SqlExpr.Literal) |> Option.defaultValue absent) absent]
                            else
                                let count = length |> Option.defaultValue (available - start)
                                [for index in start .. start + count - 1 -> tryIntegerLiteral (array.GetValue index) |> Option.get]
                        | _ ->
                            if isNull sampled then []
                            else (sampled :?> IEnumerable) |> Seq.cast<obj> |> Seq.map (tryIntegerLiteral >> Option.get) |> Seq.toList)
                    Some(SqlExpr.InList(comparison, items, []))
                | _ -> None
            elif immutableLiteral && isFullyConstant array && native then
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
        let direct = if span |> Option.exists (fun source -> source.String) then None else tryConstantIntegerInList normalizedArray
        match direct with
        | Some expr -> expr
        | None ->
        let arrayQb = if qb.SourceAlias.IsNone then {qb with SourceAlias = Some "o"} else qb
        let produced = visitProduced normalizedArray arrayQb
        let produced = if span |> Option.exists (fun source -> source.String) then produced else QueryProduced.sequence qb.SourceContext.NextAlias produced
        let start = span |> Option.bind (fun source -> source.Start) |> Option.map (fun expression -> visitProduced expression qb) |> Option.defaultWith (fun () -> QueryProduced.typed typeof<int> zero)
        let length = span |> Option.bind (fun source -> source.Length) |> Option.map (fun expression -> visitProduced expression qb) |> Option.defaultWith (fun () -> QueryProduced.typed typeof<int> absent)
        let comparison = if isSqlScalarResult value.Type then Some(visitComparisonDu value qb) else None
        QueryProduced.construct qb.SourceContext.NextAlias (fun components ->
            let arrayExpr,input = components[0]
            let start,length = fst components[1],fst components[2]
            if span |> Option.exists (fun source -> source.String) then
                QueryTranslatorSpanSource.call true arrayExpr start length comparison.Value |> QueryProduced.typed typeof<bool>
            else
            let arraySource =
                if span |> Option.exists (fun source -> source.Start.IsSome) then QueryTranslatorSpanSource.array arrayExpr start length
                else arrayExpr
            let range =
                if span |> Option.exists (fun source -> source.Start.IsSome) then
                    let key = SqlExpr.Column(Some "json_each", "key")
                    let lower = SqlExpr.Binary(key, BinaryOperator.Ge, start)
                    let upperBound = if span.Value.Length.IsSome then SqlExpr.Binary(start, BinaryOperator.Add, length) else SqlExpr.FunctionCall("json_array_length",[arrayExpr])
                    let upper = SqlExpr.Binary(key, BinaryOperator.Lt, upperBound)
                    Some(SqlExpr.Binary(lower, BinaryOperator.And, upper))
                else None
            let token = SqlExpr.FunctionCall("jsonb", [SqlExpr.Binary(arrayExpr, BinaryOperator.JsonText, SqlExpr.Column(Some "json_each", "fullkey"))])
            let element = elementInput input (SqlExpr.Column(Some "json_each", "key")) token
            let target = QueryProduced.scalarValue value.Type token { element with Carrier = CanonicalToken }
                         |> fun valueExpr -> QueryTranslatorCharacters.comparable value.Type valueExpr element
            let nativeIn = isRootParameter value && Utils.isIntegerBasedType value.Type && (QueryTranslatorNumeric.domain value.Type).IsNone
            if nativeIn then
                let values = { Ctes = []; Body = SingleSelect {
                    Distinct = false; Projections = Explicit({ Expr = target; Alias = None }, [])
                    Source = Some(FromJsonEach(arraySource, None)); Joins = []; Where = range
                    GroupBy = []; Having = None; OrderBy = []; Limit = None; Offset = None } }
                SqlExpr.InSubquery(comparison.Value, values) |> QueryProduced.typed typeof<bool>
            else
                let whereExpr =
                    if isSqlScalarResult value.Type then
                        SqlExpr.Binary(target, BinaryOperator.Eq, comparison.Value)
                    else
                        match qb.SourceContext.BindQueryValue with
                        | ValueSome binder when binder.IsValue value -> EqualityComparison.bound qb binder target value
                        | _ ->
                            if not (isFullyConstant value) then
                                raise (ArgumentException $"Cannot translate contains with this type of expression: {value.Type}")
                            EqualityComparison.known qb target value.Type (ExpressionEvaluation.evaluateExpr<obj> value)
                SqlExpr.Exists({ Ctes = []; Body = SelectBody.SingleSelect {
                    Distinct = false; Projections = Explicit({ Expr = SqlExpr.Literal(SqlLiteral.Integer 1L); Alias = None }, [])
                    Source = Some(TableSource.FromJsonEach(arraySource, None)); Joins = []
                    Where = Some(match range with Some range -> SqlExpr.CaseExpr((range,whereExpr),[],Some zero) | None -> whereExpr); GroupBy = []; Having = None; OrderBy = []; Limit = None; Offset = None } }) |> QueryProduced.typed typeof<bool>
        ) [|produced;start;length|] |> QueryProduced.value qb.SourceContext.NextAlias

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
