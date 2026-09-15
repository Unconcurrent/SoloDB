namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.JsonSerializator
open QueryTranslatorBaseTypes
open SqlDu.Engine.C1.Spec

/// One comparison authority; callers supply known values or invocation accessors.
module internal EqualityComparison =
    open EqualityRules

    let private ignoresDiscriminators = function
        | SqlExpr.Column(Some "json_each", "Value") -> true
        | _ -> false

    let known (qb: QueryBuilder) target (targetType: Type) (value: obj) =
        if isPrimitiveSQLiteType targetType then
            SqlExpr.Binary(target, BinaryOperator.Is, qb.AllocateComparisonParamExpr value)
        else
            let ignoreDiscriminators = ignoresDiscriminators target
            let json =
                if ignoreDiscriminators then JsonValue.Serialize value
                else match value with :? JsonValue as json -> json | _ -> JsonValue.Serialize value
            let comparisons = ResizeArray<SqlExpr>()
            let emit segments comparison =
                let path = renderPath segments
                let stored = extract target (pathLiteral path)
                let result =
                    match comparison with
                    | Length count -> length stored (qb.AllocateComparisonParamExpr count)
                    | Scalar value ->
                        let parameter = if isNull value then SqlExpr.Literal(SqlLiteral.Null) else qb.AllocateComparisonParamExpr value
                        scalar stored parameter (SqlExpr.Literal(SqlLiteral.Boolean(not (isNull value) && path.EndsWith ".$type")))
                comparisons.Add result
            walk ignoreDiscriminators [] json emit
            conjunction (List.ofSeq comparisons)

    let private rowsMethod = typeof<Arguments>.GetMethod("Rows", Reflection.BindingFlags.Static ||| Reflection.BindingFlags.Public ||| Reflection.BindingFlags.NonPublic)

    let private dynamic (qb: QueryBuilder) (binder: QueryValueBinder) target (value: Expression) (missing: Expression) ignoreDiscriminators knownOperand =
        let rows = binder.Parameter(Expression.Call(rowsMethod.MakeGenericMethod(value.Type), value, missing, Expression.Constant(ignoreDiscriminators), Expression.Constant(knownOperand)))
        let iterator = qb.SourceContext.NextAlias()
        let name = qb.SourceContext.NextAlias()
        let item path = extract (SqlExpr.Column(Some iterator, "value")) (pathLiteral path)
        let kind = item "$[1]"
        let expected = SqlExpr.CaseExpr(
                           (SqlExpr.Binary(kind, BinaryOperator.Is, SqlExpr.Literal(SqlLiteral.Integer 2L)),
                            SqlExpr.Cast(item "$[2]", "REAL")), [], Some(item "$[2]"))
        let rowSource =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [
                  { Alias = Some "path"; Expr = item "$[0]" }
                  { Alias = Some "kind"; Expr = kind }
                  { Alias = Some "expected"; Expr = expected } ]
              Source = Some(FromJsonEach(rows, Some iterator))
              Joins = []; Where = None; GroupBy = []; Having = None; OrderBy = []; Limit = None; Offset = None }
        let column field = SqlExpr.Column(Some name, field)
        let stored = extract target (column "path")
        let discriminator = SqlExpr.Binary(
                                SqlExpr.FunctionCall("substr", [column "path"; SqlExpr.Literal(SqlLiteral.Integer -6L)]),
                                BinaryOperator.Is, pathLiteral ".$type")
        let comparison = SqlExpr.CaseExpr(
                             (SqlExpr.Binary(column "kind", BinaryOperator.Is, SqlExpr.Literal(SqlLiteral.Integer 1L)),
                              length stored (column "expected")), [], Some(scalar stored (column "expected") discriminator))
        let core =
            { rowSource with
                Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                Source = Some(BaseTable(name, None))
                Where = Some(SqlExpr.Unary(UnaryOperator.Not, comparison)) }
        let query =
            { Ctes = [{ Name = name; Materialized = true; Query = { Ctes = []; Body = SingleSelect rowSource } }]
              Body = SingleSelect core }
        SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists query)

    let private serializeMethod = typeof<JsonValue>.GetMethod("Serialize")
    let private scalarMethod = typeof<Arguments>.GetMethod("Scalar", Reflection.BindingFlags.Static ||| Reflection.BindingFlags.Public ||| Reflection.BindingFlags.NonPublic)

    let rec private scalarShape valueType =
        match JsonValue.SerializationShape valueType with
        | JsonSerializationShape.Scalar -> true
        | JsonSerializationShape.NullableValue underlying -> scalarShape underlying
        | _ -> false

    let bound (qb: QueryBuilder) (binder: QueryValueBinder) target (value: Expression) =
        let ancestors = Collections.Generic.HashSet<Type>()
        let ignoreDiscriminators = ignoresDiscriminators target
        let falseValue = Expression.Constant(false) :> Expression
        let nullValue = Expression.Constant(null, typeof<obj>) :> Expression
        let isFalse (expression: Expression) =
            match expression with
            | :? ConstantExpression as constant -> constant.Value :? bool && not (unbox<bool> constant.Value)
            | _ -> false
        let guarded (parentNull: Expression) (expression: Expression) =
            if isFalse parentNull then expression
            else Expression.Condition(parentNull, Expression.Default(expression.Type), expression) :> Expression
        let rec fields path (expression: Expression) (parentNull: Expression) includeType =
            let valueType = expression.Type
            let shape =
                if valueType.IsSealed && not (JsonFunctions.mustIncludeTypeInformationInSerializationFn valueType)
                   && not (ancestors.Contains valueType) then JsonValue.SerializationShape valueType
                else JsonSerializationShape.Dynamic
            let members, arrayShape =
                match shape with
                | JsonSerializationShape.ObjectMembers members -> members, false
                | JsonSerializationShape.ArrayMembers members -> members, true
                | _ -> null, false
            if isNull members then
                let value = guarded parentNull expression
                let value = if includeType then Expression.Convert(value, typeof<obj>) :> Expression else value
                dynamic qb binder (extract target (pathLiteral path)) value parentNull ignoreDiscriminators (path = "$")
            else
                ancestors.Add valueType |> ignore
                let local = binder.Local(guarded parentNull expression)
                let missing =
                    if valueType.IsValueType then parentNull
                    else
                        let ownNull = Expression.ReferenceEqual(Expression.Convert(local, typeof<obj>), nullValue)
                        if isFalse parentNull then ownNull :> Expression
                        else Expression.OrElse(parentNull, ownNull) :> Expression
                let missing = if isFalse missing then missing else binder.Local missing
                let stored = extract target (pathLiteral path)
                let comparisons = ResizeArray<SqlExpr>()
                if not (isFalse missing) then
                    // Null arguments require a null subtree. Non-null objects retain subset matching.
                    comparisons.Add(SqlExpr.Binary(
                        SqlExpr.Unary(UnaryOperator.Not, binder.Parameter missing), BinaryOperator.Or,
                        SqlExpr.Unary(UnaryOperator.IsNull, stored)))
                if arrayShape then
                    let count = Expression.Constant(members.Length, typeof<obj>) :> Expression
                    let count = if isFalse missing then count else Expression.Condition(missing, nullValue, count) :> Expression
                    comparisons.Add(length stored (binder.Scalar count))
                let typeName = if includeType && not ignoreDiscriminators && not arrayShape then Utils.typeToName valueType else None
                for i = 0 to members.Length - 1 do
                    let memberInfo = members.[i]
                    let access = Expression.MakeMemberAccess(local, memberInfo)
                    let memberPath = if arrayShape then sprintf "%s[%d]" path i else appendMember path memberInfo.Name
                    if memberInfo.Name = "$type" && (typeName.IsSome || ignoreDiscriminators) then
                        // SerializeWithType evaluates the member, then replaces its value.
                        binder.Local(guarded missing (Expression.Call(serializeMethod.MakeGenericMethod(access.Type), access))) |> ignore
                    elif scalarShape access.Type then
                        let scalarValue = Expression.Call(scalarMethod.MakeGenericMethod(access.Type), access) :> Expression
                        let scalarValue = if isFalse missing then scalarValue else Expression.Condition(missing, nullValue, scalarValue) :> Expression
                        let discriminator = SqlExpr.Literal(SqlLiteral.Boolean(memberPath.EndsWith ".$type"))
                        comparisons.Add(scalar (extract target (pathLiteral memberPath)) (binder.Scalar scalarValue) discriminator)
                    else comparisons.Add(fields memberPath access missing false)
                match typeName with
                | Some name ->
                    let typeName = Expression.Constant(name, typeof<obj>) :> Expression
                    let typeName = if isFalse missing then typeName else Expression.Condition(missing, nullValue, typeName) :> Expression
                    comparisons.Add(scalar (extract target (pathLiteral (appendMember path "$type")))
                                        (binder.Scalar typeName) (SqlExpr.Literal(SqlLiteral.Boolean true)))
                | None -> ()
                ancestors.Remove valueType |> ignore
                conjunction (List.ofSeq comparisons)
        fields "$" value falseValue true
