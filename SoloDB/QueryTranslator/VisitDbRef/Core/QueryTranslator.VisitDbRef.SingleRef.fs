namespace SoloDatabase

open System
open System.Linq.Expressions
open System.Reflection
open SqlDu.Engine.C1.Spec
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorVisitPostJoin
open DBRefTypeHelpers

/// Handles DBRef<T> single-reference navigation in expression trees.
/// Translates o.Ref.Id, o.Ref.HasValue, o.Ref.Value.Name patterns to SQL.
module internal QueryTranslatorVisitDbRefSingleRef =

    let internal handleDBRefExpression (qb: QueryBuilder) (exp: Expression) : bool =
        let tryMakeValueMemberFromInvokeArg (arg: Expression) =
            match unwrapConvert arg with
            | :? MemberExpression as dbrefPropExpr when isDBRefType dbrefPropExpr.Type ->
                let valueProp = dbrefPropExpr.Type.GetProperty("Value", BindingFlags.Public ||| BindingFlags.Instance)
                if isNull valueProp then ValueNone
                else ValueSome (Expression.MakeMemberAccess(dbrefPropExpr, valueProp))
            | _ -> ValueNone

        match exp with
        | :? MemberExpression as topMe when not (isNull topMe.Expression) ->
            let innerExpr = unwrapConvert topMe.Expression

            let inline prefixToAlias (prefix: string) =
                if String.IsNullOrEmpty prefix then None
                else Some(prefix.TrimEnd('.'))

            // Case 1: Direct member on DBRef<T> — o.Ref.Id, o.Ref.HasValue, o.Ref.TypedId (typed only)
            if isDBRefType innerExpr.Type then
                match topMe.Member.Name with
                | "Id" ->
                    match innerExpr with
                    | :? MemberExpression as dbrefPropExpr ->
                        let struct(prefix, prop) = resolveDBRefPropertyLocation qb dbrefPropExpr
                        qb.DuHandlerResult.Value <- ValueSome(SqlExpr.JsonExtractExpr(prefixToAlias prefix, "Value", JsonPath(prop, [])))
                        true
                    | _ -> false
                | "HasValue" ->
                    match innerExpr with
                    | :? MemberExpression as dbrefPropExpr ->
                        let struct(prefix, prop) = resolveDBRefPropertyLocation qb dbrefPropExpr
                        let extract = SqlExpr.JsonExtractExpr(prefixToAlias prefix, "Value", JsonPath(prop, []))
                        qb.DuHandlerResult.Value <- ValueSome(
                            SqlExpr.Binary(
                                SqlExpr.Unary(UnaryOperator.IsNotNull, extract),
                                BinaryOperator.And,
                                SqlExpr.Binary(extract, BinaryOperator.Ne, SqlExpr.Literal(SqlLiteral.Integer 0L))))
                        true
                    | _ -> false
                | "TypedId" when isDBRefTypedType innerExpr.Type ->
                    // o.Ref.TypedId: JOIN the target collection (excluded relations correctly throw via
                    // ensureDBRefJoin, mirroring .Value semantics) and read the target's [<SoloId>] field
                    // from its JSON Value column. Index already required by RelationsSchemaLinkTableDDL
                    // for typed relations, so this is a key lookup.
                    match innerExpr with
                    | :? MemberExpression as dbrefPropExpr ->
                        let targetType = (dbrefPropExpr.Type.GetGenericArguments()).[0]
                        match SoloIdAccessor.TryGetProperty targetType with
                        | ValueNone ->
                            raise (NotSupportedException(
                                sprintf "Cannot translate DBRef<%s,_>.TypedId: target type has no [<SoloId>] property." targetType.Name))
                        | ValueSome soloIdProp ->
                            let valueProp = dbrefPropExpr.Type.GetProperty("Value", BindingFlags.Public ||| BindingFlags.Instance)
                            if isNull valueProp then false
                            else
                                let valueME = Expression.MakeMemberAccess(dbrefPropExpr, valueProp)
                                let alias = ensureDBRefJoin qb valueME
                                qb.DuHandlerResult.Value <- ValueSome(SqlExpr.JsonExtractExpr(Some alias, "Value", JsonPath(soloIdProp.Name, [])))
                                true
                    | _ -> false
                | "TypedId" ->
                    raise (NotSupportedException(
                        sprintf "DBRef<%s>.TypedId is not available on the single-arg DBRef form. Use .Id for row-id comparisons, or declare the relation as DBRef<%s,'TId> for typed-id queries." innerExpr.Type.Name innerExpr.Type.Name))
                | _ -> false

            // Case 2: Property through DBRef<T>.Value — o.Ref.Value.Name
            else
                let rec findValueBoundary (expr: Expression) (above: string list) =
                    match expr with
                    | :? MemberExpression as me when isDBRefValueBoundary me ->
                        ValueSome struct(me, above)
                    | :? MemberExpression as me when not (isNull me.Expression) ->
                        findValueBoundary me.Expression (me.Member.Name :: above)
                    | :? MethodCallExpression as mc when mc.Method.Name = "Invoke" && mc.Arguments.Count = 1 ->
                        match tryMakeValueMemberFromInvokeArg mc.Arguments.[0] with
                        | ValueSome valueME -> ValueSome struct(valueME, above)
                        | ValueNone when not (isNull mc.Object) -> findValueBoundary mc.Object above
                        | ValueNone -> ValueNone
                    | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert ->
                        findValueBoundary ue.Operand above
                    | _ -> ValueNone

                match findValueBoundary topMe.Expression [topMe.Member.Name] with
                | ValueSome struct(valueME, propParts) ->
                    let alias = ensureDBRefJoin qb valueME
                    let du =
                        match propParts with
                        | ["Id"] -> SqlExpr.Column(Some alias, "Id")
                        | _ -> SqlExpr.JsonExtractExpr(Some alias, "Value", JsonPathOps.ofList propParts)
                    qb.DuHandlerResult.Value <- ValueSome du
                    true
                | ValueNone -> false
        | _ -> false
