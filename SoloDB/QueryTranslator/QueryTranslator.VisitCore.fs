namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Collections.ObjectModel
open System.Linq.Expressions
open System.Reflection
open System.Runtime.InteropServices
open System.Text
open JsonFunctions
open Utils
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorBase
open SqlDu.Engine.C1.Spec

module internal QueryTranslatorVisitCore =
    // ─── DU-constructing visitor (Batch 3: legacy visit path removed) ─────────
    // All expression families produce SqlExpr DU nodes via visitDu.
    // Remaining __raw__: pre-expression handler + unknown handler API boundaries
    // (external Func<QB, Expr, bool> callbacks whose type signature cannot change).

    /// Placeholder: legacy visit removed in Batch 3. All callers now use visitDu + emitExpr.
    let internal visit (_exp: Expression) (_qb: QueryBuilder) : unit =
        raise (NotSupportedException "Legacy visit path removed in Batch 3. Use visitDu + SqlDuMinimalEmit.emitExpr.")

    // Legacy visitor functions removed in Batch 3 (visitBinary, visitMemberAccess, visitMethodCall,
    // visitParameter, visitNot, visitNegate, visitNew, visitMemberInit, visitConvert, visitConstant,
    // visitListInit, visitTypeIs, visitIfElse, arrayIndex, visitProperty, castTo, visitMathMethod,
    // visitLambda, newObject, containsImpl, emitStringOperand).
    // All callers now use visitDu + SqlDuMinimalEmit.emitExpr.

    let rec private captureHandlerRawDu (qb: QueryBuilder) (sbBefore: int) : SqlExpr =
        let rawSql = qb.StringBuilder.ToString(sbBefore, qb.StringBuilder.Length - sbBefore)
        SqlExpr.FunctionCall("__raw__", [SqlExpr.Literal(SqlLiteral.String rawSql)])

    and private emitStringOperandDu (qb: QueryBuilder) (ignoreCase: bool) (expr: Expression) : SqlExpr =
        if ignoreCase then SqlExpr.FunctionCall("TO_LOWER", [visitDu expr qb])
        else visitDu expr qb

    and private castToDu (qb: QueryBuilder) (castToType: Type) (o: Expression) : SqlExpr =
        let sqlType =
            match castToType with
            | OfType float | OfType float32 | OfType decimal -> Some "REAL"
            | OfType bool | OfType int8 | OfType uint8 | OfType int16 | OfType uint16
            | OfType int32 | OfType uint32 | OfType int64 | OfType uint64
            | OfType nativeint | OfType unativeint -> Some "INTEGER"
            | _ -> None
        match sqlType with
        | None -> visitDu o qb
        | Some t -> SqlExpr.Cast(visitDu o {qb with UpdateMode = false}, t)

    and private parseMathFormatDu (format: string) (resolveArg: int -> SqlExpr) : SqlExpr =
        if format = "($1 - $2 * ROUND($1 / $2))" then
            SqlExpr.Binary(resolveArg 1, BinaryOperator.Sub, SqlExpr.Binary(resolveArg 2, BinaryOperator.Mul, SqlExpr.FunctionCall("ROUND", [SqlExpr.Binary(resolveArg 1, BinaryOperator.Div, resolveArg 2)])))
        elif format = "($1 * $2)" then
            SqlExpr.Binary(resolveArg 1, BinaryOperator.Mul, resolveArg 2)
        elif format = "($2 - $3 * ROUND($2 / $3))" then
            SqlExpr.Binary(resolveArg 2, BinaryOperator.Sub, SqlExpr.Binary(resolveArg 3, BinaryOperator.Mul, SqlExpr.FunctionCall("ROUND", [SqlExpr.Binary(resolveArg 2, BinaryOperator.Div, resolveArg 3)])))
        elif format = "($2 * $3)" then
            SqlExpr.Binary(resolveArg 2, BinaryOperator.Mul, resolveArg 3)
        else
            let parenIdx = format.IndexOf('(')
            let funcName = format.Substring(0, parenIdx)
            let argsStr = format.Substring(parenIdx + 1, format.Length - parenIdx - 2)
            let args =
                argsStr.Split(',')
                |> Array.map (fun s ->
                    let s = s.Trim()
                    if s.Length > 0 && s.[0] = '$' then resolveArg (int (s.Substring(1)))
                    else SqlExpr.Literal(SqlLiteral.Integer(int64 s)))
                |> Array.toList
            SqlExpr.FunctionCall(funcName, args)

    and private visitMathMethodDu (m: MethodCallExpression) (qb: QueryBuilder) : SqlExpr voption =
        match mathFunctionTransformation.TryGetValue((m.Arguments.Count, m.Method.Name)) with
        | false, _ -> ValueNone
        | true, format ->
            let resolveArg (n: int) =
                visitDu (match n with | 0 -> m.Object | d -> m.Arguments.[d - 1]) qb
            ValueSome(parseMathFormatDu format resolveArg)

    and private arrayIndexDu (array: Expression) (index: Expression) (qb: QueryBuilder) : SqlExpr =
        match index with
        | :? ConstantExpression as ce when ce.Type.IsPrimitive ->
            SqlExpr.FunctionCall("jsonb_extract", [visitDu array qb; SqlExpr.Literal(SqlLiteral.String(sprintf "$[%O]" ce.Value))])
        | _ -> raise (NotSupportedException("The index of the array must always be a constant value."))

    and private visitPropertyDu (o: Expression) (property: obj) (m: Expression) (qb: QueryBuilder) : SqlExpr =
        match property with
        | :? string as property ->
            let alias = if String.IsNullOrEmpty qb.TableNameDot then None else Some(qb.TableNameDot.TrimEnd([|'.'|]))
            if property = "Id" && o.NodeType = ExpressionType.Parameter && (m.Type = typeof<int64> || m.Type = typeof<int32>) then
                SqlExpr.Column(alias, "Id")
            else
                visitMemberAccessDu { Expression = o; MemberName = property; InputType = m.Type; ReturnType = typeof<obj>; OriginalExpression = None } qb
        | :? int as index ->
            visitMemberAccessDu { Expression = o; MemberName = $"[{index}]"; InputType = m.Type; ReturnType = typeof<obj>; OriginalExpression = None } qb
        | _ -> raise (NotSupportedException("Unable to translate property access."))

    and private newObjectDu (members: struct(string * SqlExpr) array) : SqlExpr =
        SqlExpr.JsonObjectExpr([for struct(name, expr) in members -> (name, expr)])

    and private visitParameterDu (m: ParameterExpression) (qb: QueryBuilder) : SqlExpr =
        let alias = if String.IsNullOrEmpty qb.TableNameDot then None else Some(qb.TableNameDot.TrimEnd([|'.'|]))
        if m.Type = typeof<int64>
            && ((qb.Parameters.IndexOf m = 0 && qb.Parameters.Count = 2)
                || qb.IdParameterIndex = qb.Parameters.IndexOf m) then
            SqlExpr.Column(alias, "Id")
        elif qb.UpdateMode then
            SqlExpr.Literal(SqlLiteral.String "$")
        elif qb.JsonExtractSelfValue && ((not << isPrimitiveSQLiteType) m.Type || (not << String.IsNullOrWhiteSpace) qb.TableNameDot) then
            SqlExpr.JsonExtractExpr(alias, "Value", JsonPath [])
        else
            SqlExpr.Column(alias, "Value")

    and private visitMemberAccessDu (m: MemberAccess) (qb: QueryBuilder) : SqlExpr =
        let alias = if String.IsNullOrEmpty qb.TableNameDot then None else Some(qb.TableNameDot.TrimEnd([|'.'|]))
        if m.MemberName = "Length" && m.InputType.GetInterface(typeof<IEnumerable>.FullName) <> null then
            if m.InputType = typeof<string> then SqlExpr.FunctionCall("length", [visitDu m.Expression qb])
            elif m.InputType = typeof<byte array> then SqlExpr.FunctionCall("length", [SqlExpr.FunctionCall("base64", [visitDu m.Expression qb])])
            else SqlExpr.FunctionCall("json_array_length", [visitDu m.Expression qb])
        elif m.MemberName = "Count" && m.InputType.GetInterface(typeof<IEnumerable>.FullName) <> null then
            SqlExpr.FunctionCall("json_array_length", [visitDu m.Expression qb])
        elif typedefof<System.Linq.IGrouping<_,_>>.IsAssignableFrom(m.InputType) then
            match m.MemberName with
            | "Key" -> SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$.Key")])
            | other ->
                let esc = escapeSQLiteString other
                SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String $"$.Items.{esc}")])
        elif (m.ReturnType = typeof<int64> && m.MemberName = "Id")
              || (m.MemberName = "Id" && m.Expression.NodeType = ExpressionType.Parameter && m.Expression.Type.FullName = "SoloDatabase.JsonSerializator.JsonValue") then
            SqlExpr.Column(alias, "Id")
        elif m.Expression <> null && isRootParameter m.Expression then
            let rec buildPath (expr: Expression) (accum: string list) : string list =
                match expr with
                | :? MemberExpression as inner -> buildPath inner.Expression (inner.Member.Name :: accum)
                | _ -> accum
            let pathParts = buildPath m.Expression [m.MemberName]
            match pathParts with
            | [] -> SqlExpr.Literal(SqlLiteral.Null)
            | parts ->
                if qb.UpdateMode then
                    let pathStr = (String.concat "." parts).Replace(".[", "[") |> escapeSQLiteString
                    SqlExpr.Literal(SqlLiteral.String $"$.{pathStr}")
                else SqlExpr.JsonExtractExpr(alias, "Value", JsonPath parts)
        else
            match m.OriginalExpression with
            | None ->
                let esc = escapeSQLiteString m.MemberName
                SqlExpr.FunctionCall("jsonb_extract", [visitDu m.Expression qb; SqlExpr.Literal(SqlLiteral.String $"$.{esc}")])
            | Some originalExpr ->
                if originalExpr.Expression = null then
                    qb.AllocateParamExpr((originalExpr.Member :?> PropertyInfo).GetValue null)
                elif isFullyConstant originalExpr then
                    qb.AllocateParamExpr(evaluateExpr originalExpr)
                else
                    raise (NotSupportedException(
                        sprintf "Error: Member access '%O' is not supported.\nReason: The member cannot be translated to SQL in this context.\nFix: Simplify the expression or move it after AsEnumerable()." originalExpr.Member.Name))

    and private containsImplDu (qb: QueryBuilder) (array: Expression) (value: Expression) : SqlExpr =
        let arrayQb = if qb.TableNameDot = "" then {qb with TableNameDot = "o."} else qb
        let arrayExpr = visitDu array arrayQb
        let whereExpr =
            if isPrimitiveSQLiteType value.Type then
                SqlExpr.Binary(SqlExpr.Column(Some "json_each", "Value"), BinaryOperator.Eq, visitDu value qb)
            else
                if not (isFullyConstant value) then
                    raise (ArgumentException $"Cannot translate contains with this type of expression: {value.Type}")
                compareKnownJsonDu qb (SqlExpr.Column(Some "json_each", "Value")) value.Type (evaluateExpr<obj> value)
        SqlExpr.Exists({ Ctes = []; Body = SelectBody.SingleSelect {
            Distinct = false; Projections = [{ Expr = SqlExpr.Literal(SqlLiteral.Integer 1L); Alias = None }]
            Source = Some(TableSource.FromJsonEach(arrayExpr, None)); Joins = []
            Where = Some whereExpr; GroupBy = []; Having = None; OrderBy = []; Limit = None; Offset = None } })

    and private visitNestedArrayPredicateDu (qb: QueryBuilder) (array: Expression) (whereFuncExpr: Expression) (isAll: bool) : SqlExpr =
        let expr =
            match whereFuncExpr with
            | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Quote -> ue.Operand
            | :? LambdaExpression as le -> le :> Expression
            | _ -> let exprFunc = Expression.Lambda<Func<Expression>>(whereFuncExpr).Compile(true) in exprFunc.Invoke()
        let arrayQb = if qb.TableNameDot = "" then {qb with TableNameDot = "o."} else qb
        let arrayExpr = visitDu array arrayQb
        let innerQb = {qb with TableNameDot = "json_each."; JsonExtractSelfValue = false}
        let predicateExpr = visitDu expr innerQb
        let whereExpr = if isAll then SqlExpr.Unary(UnaryOperator.Not, predicateExpr) else predicateExpr
        let existsExpr = SqlExpr.Exists({ Ctes = []; Body = SelectBody.SingleSelect {
            Distinct = false; Projections = [{ Expr = SqlExpr.Literal(SqlLiteral.Integer 1L); Alias = None }]
            Source = Some(TableSource.FromJsonEach(arrayExpr, None)); Joins = []
            Where = Some whereExpr; GroupBy = []; Having = None; OrderBy = []; Limit = None; Offset = None } })
        if isAll then SqlExpr.Unary(UnaryOperator.Not, existsExpr) else existsExpr

    and private visitBinaryDu (b: BinaryExpression) (qb: QueryBuilder) : SqlExpr =
        if b.NodeType = ExpressionType.Add && (b.Left.Type = typeof<string> || b.Right.Type = typeof<string>) then
            let expr = Expression.Call(typeof<String>.GetMethod("Concat", [|typeof<string seq>|]), Expression.NewArrayInit(typeof<string>, [|b.Left; b.Right|]))
            visitDu expr qb
        else
        let isLeftNull = match b.Left with :? ConstantExpression as c when c.Value = null -> true | _ -> false
        let isRightNull = match b.Right with :? ConstantExpression as c when c.Value = null -> true | _ -> false
        let isAnyNull = isLeftNull || isRightNull
        let struct(left, right) = if isLeftNull then struct(b.Right, b.Left) else struct(b.Left, b.Right)
        let shouldUseComplex = not isAnyNull && not (isPrimitiveSQLiteType left.Type || isPrimitiveSQLiteType right.Type) && (left.Type <> typeof<obj> && right.Type <> typeof<obj>) && (isFullyConstant left || isFullyConstant right)
        match struct(shouldUseComplex, b.NodeType) with
        | struct(true, ExpressionType.Equal) ->
            let struct(constant, expression) = if isFullyConstant left then struct(left, right) else struct(right, left)
            compareKnownJsonDu qb (visitDu expression qb) expression.Type (evaluateExpr<obj> constant)
        | struct(true, ExpressionType.NotEqual) ->
            let struct(constant, expression) = if isFullyConstant left then struct(left, right) else struct(right, left)
            SqlExpr.Unary(UnaryOperator.Not, compareKnownJsonDu qb (visitDu expression qb) expression.Type (evaluateExpr<obj> constant))
        | _ ->
        let leftDu = visitDu left qb
        let rightDu = visitDu right qb
        let op =
            match b.NodeType with
            | ExpressionType.And | ExpressionType.AndAlso -> BinaryOperator.And
            | ExpressionType.Or | ExpressionType.OrElse -> BinaryOperator.Or
            | ExpressionType.Equal -> if isAnyNull then BinaryOperator.Is else BinaryOperator.Eq
            | ExpressionType.NotEqual ->
                if isAnyNull || involvesDBRefValueAccess left || involvesDBRefValueAccess right
                then BinaryOperator.IsNot else BinaryOperator.Ne
            | ExpressionType.LessThan -> BinaryOperator.Lt
            | ExpressionType.LessThanOrEqual -> BinaryOperator.Le
            | ExpressionType.GreaterThan -> BinaryOperator.Gt
            | ExpressionType.GreaterThanOrEqual -> BinaryOperator.Ge
            | ExpressionType.Add -> BinaryOperator.Add
            | ExpressionType.Subtract -> BinaryOperator.Sub
            | ExpressionType.Multiply -> BinaryOperator.Mul
            | ExpressionType.Divide -> BinaryOperator.Div
            | ExpressionType.Modulo -> BinaryOperator.Mod
            | _ -> raise (NotSupportedException(sprintf "Error: Binary operator '%O' is not supported.\nReason: The operator cannot be translated to SQL for this expression.\nFix: Simplify the expression or move it after AsEnumerable()." b.NodeType))
        SqlExpr.Binary(leftDu, op, rightDu)

    and private visitMethodCallDu (m: MethodCallExpression) (qb: QueryBuilder) : SqlExpr =
        match visitMathMethodDu m qb with
        | ValueSome result -> result
        | ValueNone ->
        match m with
        | _ when m.Method.Name = "Invoke" && not (FSharp.Reflection.FSharpType.IsRecord m.Type) ->
            let rec stripConvert (expr: Expression) = match expr with :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert -> stripConvert ue.Operand | _ -> expr
            let targetExpr = if isNull m.Object then null else stripConvert m.Object
            match targetExpr with
            | :? LambdaExpression as le when le.Parameters.Count = m.Arguments.Count ->
                visitDu (inlineLambdaInvocation le m.Arguments) qb
            | _ when isFullyConstant (m :> Expression) -> qb.AllocateParamExpr(evaluateExpr<obj> (m :> Expression))
            | _ -> raise (NotSupportedException(sprintf "Error: Method '%s' is not supported.\nReason: The method has no SQL translation.\nFix: Rewrite the query or call AsEnumerable() before using this method." m.Method.Name))

        | OfShape0 null null "ToLowerInvariant" value | OfShape0 null null "ToLower" value ->
            SqlExpr.FunctionCall("TO_LOWER", [visitDu value qb])
        | OfShape0 null null "ToUpperInvariant" value | OfShape0 null null "ToUpper" value ->
            SqlExpr.FunctionCall("TO_UPPER", [visitDu value qb])
        | OfShape1 null null "GetArray" null (array, index) -> arrayIndexDu array index qb
        | OfShape1 null null "Like" null (str, likeWhat) ->
            SqlExpr.Binary(visitDu str qb, BinaryOperator.Like, visitDu likeWhat qb)
        // UpdateMode methods — DU path (Batch 3: replaces legacy visit-based UpdateMode handlers).
        | OfShape1 null null "Set" null (oldValue, newValue) when qb.UpdateMode ->
            let pathExpr = visitDu oldValue qb
            let valueExpr = visitDu newValue qb
            SqlExpr.FunctionCall("__update_fragment__", [pathExpr; valueExpr])

        | OfShape1 null null "Append" null (array, newValue)
        | OfShape1 null null "Add" null (array, newValue) when qb.UpdateMode ->
            let arrayPathExpr = visitDu array qb
            let modifiedPath =
                match arrayPathExpr with
                | SqlExpr.Literal(SqlLiteral.String path) -> SqlExpr.Literal(SqlLiteral.String $"{path}[#]")
                | other -> other
            let valueExpr = visitDu newValue qb
            SqlExpr.FunctionCall("__update_fragment__", [modifiedPath; valueExpr])

        | OfShape2 null null "SetAt" null null (array, indexExpr, newValue) when qb.UpdateMode ->
            let arrayPathExpr = visitDu array qb
            let indexVal = evaluateExpr<obj> indexExpr
            let modifiedPath =
                match arrayPathExpr with
                | SqlExpr.Literal(SqlLiteral.String path) -> SqlExpr.Literal(SqlLiteral.String $"{path}[{indexVal}]")
                | other -> other
            let valueExpr = visitDu newValue qb
            SqlExpr.FunctionCall("__update_fragment__", [modifiedPath; valueExpr])

        | OfShape1 null null "RemoveAt" null (array, indexExpr) when qb.UpdateMode ->
            let arrayPathExpr = visitDu array qb
            let indexVal = evaluateExpr<obj> indexExpr
            let tableNameDot = qb.GetTableNameDot()
            let alias = if String.IsNullOrEmpty tableNameDot then None else Some(tableNameDot.TrimEnd('.'))
            let valueExpr =
                SqlExpr.FunctionCall("jsonb_remove", [
                    SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(alias, "Value"); arrayPathExpr])
                    SqlExpr.Literal(SqlLiteral.String $"$[{indexVal}]")
                ])
            SqlExpr.FunctionCall("__update_fragment__", [arrayPathExpr; valueExpr])
        | OfShape1 null null "op_Dynamic" null (o, propExpr) ->
            visitPropertyDu o (propExpr |> unbox<ConstantExpression>).Value m qb
        | OfShape1 null null "Any" null (array, whereFuncExpr) ->
            visitNestedArrayPredicateDu qb array whereFuncExpr false
        | OfShape1 null null "All" null (array, whereFuncExpr) ->
            visitNestedArrayPredicateDu qb array whereFuncExpr true
        | OfShape2 null OfString "Contains" null OfStringComparison (text, what, comparisonExpr) ->
            let ic = isIgnoreCase (evaluateExpr<StringComparison> comparisonExpr)
            SqlExpr.Binary(SqlExpr.FunctionCall("INSTR", [emitStringOperandDu qb ic text; emitStringOperandDu qb ic what]), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L))
        | OfShape1 null OfString "Contains" null (text, what) ->
            SqlExpr.Binary(SqlExpr.FunctionCall("INSTR", [visitDu text qb; visitDu what qb]), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L))
        | OfShape1 null OfIEnum "Contains" null (array, value) -> containsImplDu qb array value
        | OfShape1 bool OfValueType "Contains" null (ros, value) when ros.Type.Name = "ReadOnlySpan`1" -> containsImplDu qb ros value
        | OfShape0 null null "QuotationToLambdaExpression" arg1 ->
            visitDu (arg1 :?> MethodCallExpression).Arguments.[0] qb
        | OfShape0 null null "op_Implicit" arg1 -> visitDu arg1 qb
        | OfShape0 null null "GetType" o when o.NodeType = ExpressionType.Parameter ->
            SqlExpr.FunctionCall("jsonb_extract", [visitDu o qb; SqlExpr.Literal(SqlLiteral.String "$.$type")])
        | OfShape0 (OfType) null "TypeOf" _ ->
            let t = m.Method.Invoke(null, Array.empty) :?> Type
            qb.AllocateParamExpr(match t |> typeToName with Some x -> x | None -> "")
        | OfShape0 null null "NewSqlId" arg1 -> visitDu arg1 qb
        | OfShape1 null null "get_Item" null (o, propExpr) when typeof<System.Collections.ICollection>.IsAssignableFrom o.Type || typeof<Array>.IsAssignableFrom o.Type || typeof<JsonSerializator.JsonValue>.IsAssignableFrom o.Type ->
            visitPropertyDu o (propExpr |> unbox<ConstantExpression>).Value m qb
        | OfShape1 null null "Dyn" (OfPropInfo) (o, propExpr) ->
            visitPropertyDu o (evaluateExpr<PropertyInfo> propExpr).Name m qb
        | OfShape1 null null "Dyn" null (o, propExpr) ->
            let property = match propExpr.Type with | t when t = typeof<string> || isIntegerBasedType t -> evaluateExpr<obj> propExpr | _ -> raise (NotSupportedException(sprintf "Cannot access dynamic property of %A" o))
            visitPropertyDu o property m qb
        | OfShape0 (OfString) null "Concat" _ ->
            let args =
                let len = m.Arguments.Count
                if len = 1 && typeof<IEnumerable<string>>.IsAssignableFrom m.Arguments.[0].Type && m.Arguments.[0].NodeType = ExpressionType.NewArrayInit then (m.Arguments.[0] :?> NewArrayExpression).Expressions
                elif len > 1 then m.Arguments
                else raise (NotSupportedException(sprintf "Unknown such concat function: %A" m.Method))
            SqlExpr.FunctionCall("CONCAT", [for arg in args -> visitDu arg qb])
        | OfShape2 null OfString "StartsWith" null OfStringComparison (arg, v, comparisonExpr) ->
            let ic = isIgnoreCase (evaluateExpr<StringComparison> comparisonExpr)
            SqlExpr.Binary(
                SqlExpr.FunctionCall("SUBSTR", [emitStringOperandDu qb ic arg; SqlExpr.Literal(SqlLiteral.Integer 1L); SqlExpr.FunctionCall("LENGTH", [visitDu v qb])]),
                BinaryOperator.Eq, emitStringOperandDu qb ic v)
        | OfShape1 null OfString "StartsWith" null (arg, v) ->
            if isFullyConstant v then
                let prefix = if v.Type = typeof<char> then (string << evaluateExpr<char>) v else evaluateExpr<string> v
                if prefix.Length = 0 then SqlExpr.Unary(UnaryOperator.IsNotNull, visitDu arg qb)
                else
                    let nextString (s: string) =
                        let chars = s.ToCharArray()
                        let rec bump i =
                            if i < 0 then s + "\u0000"
                            elif chars.[i] < System.Char.MaxValue then chars.[i] <- char (int chars.[i] + 1); new string(chars, 0, i + 1)
                            else bump (i - 1)
                        bump (chars.Length - 1)
                    SqlExpr.Binary(
                        SqlExpr.Binary(visitDu arg qb, BinaryOperator.Ge, qb.AllocateParamExpr prefix),
                        BinaryOperator.And,
                        SqlExpr.Binary(visitDu arg qb, BinaryOperator.Lt, qb.AllocateParamExpr(nextString prefix)))
            else
                SqlExpr.Binary(
                    SqlExpr.FunctionCall("SUBSTR", [visitDu arg qb; SqlExpr.Literal(SqlLiteral.Integer 1L); SqlExpr.FunctionCall("LENGTH", [visitDu v qb])]),
                    BinaryOperator.Eq, visitDu v qb)
        | OfShape2 null OfString "EndsWith" null OfStringComparison (arg, v, comparisonExpr) ->
            let ic = isIgnoreCase (evaluateExpr<StringComparison> comparisonExpr)
            SqlExpr.Binary(
                SqlExpr.FunctionCall("SUBSTR", [emitStringOperandDu qb ic arg; SqlExpr.Unary(UnaryOperator.Neg, SqlExpr.FunctionCall("LENGTH", [visitDu v qb]))]),
                BinaryOperator.Eq, emitStringOperandDu qb ic v)
        | OfShape1 null OfString "EndsWith" null (arg, v) ->
            SqlExpr.Binary(
                SqlExpr.FunctionCall("SUBSTR", [visitDu arg qb; SqlExpr.Unary(UnaryOperator.Neg, SqlExpr.FunctionCall("LENGTH", [visitDu v qb]))]),
                BinaryOperator.Eq, visitDu v qb)
        | OfShape2 null OfString "Equals" null OfStringComparison (arg, v, comparisonExpr) ->
            let ic = isIgnoreCase (evaluateExpr<StringComparison> comparisonExpr)
            SqlExpr.Binary(emitStringOperandDu qb ic arg, BinaryOperator.Eq, emitStringOperandDu qb ic v)
        | OfShape2 null null "Equals" OfString OfStringComparison (arg1, arg2, comparisonExpr) when m.Method.DeclaringType = typeof<string> && m.Method.IsStatic ->
            let ic = isIgnoreCase (evaluateExpr<StringComparison> comparisonExpr)
            SqlExpr.Binary(emitStringOperandDu qb ic arg1, BinaryOperator.Eq, emitStringOperandDu qb ic arg2)
        | OfShape2 null null "Compare" OfString OfStringComparison (arg1, arg2, comparisonExpr) when m.Method.DeclaringType = typeof<string> && m.Method.IsStatic ->
            let ic = isIgnoreCase (evaluateExpr<StringComparison> comparisonExpr)
            let a = emitStringOperandDu qb ic arg1
            let b = emitStringOperandDu qb ic arg2
            SqlExpr.CaseExpr([(SqlExpr.Binary(a, BinaryOperator.Lt, b), SqlExpr.Literal(SqlLiteral.Integer -1L)); (SqlExpr.Binary(a, BinaryOperator.Gt, b), SqlExpr.Literal(SqlLiteral.Integer 1L))], Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))
        | OfShape2 null OfString "IndexOf" null OfStringComparison (arg, v, comparisonExpr) ->
            let ic = isIgnoreCase (evaluateExpr<StringComparison> comparisonExpr)
            SqlExpr.Binary(SqlExpr.FunctionCall("INSTR", [emitStringOperandDu qb ic arg; emitStringOperandDu qb ic v]), BinaryOperator.Sub, SqlExpr.Literal(SqlLiteral.Integer 1L))
        | OfShape0 null null "ToObject" o when typeof<JsonSerializator.JsonValue>.IsAssignableFrom m.Method.DeclaringType || m.Method.DeclaringType.FullName = "SoloDatabase.MongoDB.BsonDocument" ->
            castToDu qb (m.Method.GetGenericArguments().[0]) o
        | OfShape0 null null "CastTo" arg -> castToDu qb (m.Method.GetGenericArguments().[0]) arg
        | OfShape0 null null "Parse" arg when
            m.Method.DeclaringType = typeof<SByte> || m.Method.DeclaringType = typeof<Byte> || m.Method.DeclaringType = typeof<Int16> || m.Method.DeclaringType = typeof<UInt16> ||
            m.Method.DeclaringType = typeof<Int32> || m.Method.DeclaringType = typeof<UInt32> || m.Method.DeclaringType = typeof<Int64> || m.Method.DeclaringType = typeof<UInt64> ||
            m.Method.DeclaringType = typeof<Single> || m.Method.DeclaringType = typeof<Double> || m.Method.DeclaringType = typeof<float> || m.Method.DeclaringType = typeof<float32> ->
            let targetType = match m.Method.DeclaringType with
                             | t when t = typeof<SByte> || t = typeof<Byte> || t = typeof<Int16> || t = typeof<UInt16> || t = typeof<Int32> || t = typeof<UInt32> || t = typeof<Int64> || t = typeof<UInt64> -> "INTEGER"
                             | t when t = typeof<Single> || t = typeof<Double> -> "REAL"
                             | _ -> raise (NotSupportedException(sprintf "Unsupported Parse type: %A" m.Method.DeclaringType))
            SqlExpr.Cast(visitDu arg qb, targetType)
        | OfShape2 null OfString "Substring" null null (str, start, length) ->
            SqlExpr.FunctionCall("SUBSTR", [visitDu str qb; SqlExpr.Binary(visitDu start qb, BinaryOperator.Add, SqlExpr.Literal(SqlLiteral.Integer 1L)); visitDu length qb])
        | OfShape1 null OfString "Substring" null (str, start) ->
            SqlExpr.FunctionCall("SUBSTR", [visitDu str qb; SqlExpr.Binary(visitDu start qb, BinaryOperator.Add, SqlExpr.Literal(SqlLiteral.Integer 1L))])
        | OfShape1 null OfString "get_Chars" null (str, index) ->
            SqlExpr.FunctionCall("SUBSTR", [visitDu str qb; SqlExpr.Binary(visitDu index qb, BinaryOperator.Add, SqlExpr.Literal(SqlLiteral.Integer 1L)); SqlExpr.Literal(SqlLiteral.Integer 1L)])
        | OfShape1 null null "GetString" null (str, index) ->
            SqlExpr.FunctionCall("SUBSTR", [visitDu str qb; SqlExpr.Binary(visitDu index qb, BinaryOperator.Add, SqlExpr.Literal(SqlLiteral.Integer 1L)); SqlExpr.Literal(SqlLiteral.Integer 1L)])
        | OfShape0 null null "Count" _ when let t = m.Arguments.[0].Type in t.IsConstructedGenericType && t.GetGenericTypeDefinition() = typedefof<System.Linq.IGrouping<_,_>> ->
            let alias = if String.IsNullOrEmpty qb.TableNameDot then None else Some(qb.TableNameDot.TrimEnd([|'.'|]))
            SqlExpr.FunctionCall("json_array_length", [SqlExpr.Column(alias, "Value"); SqlExpr.Literal(SqlLiteral.String "$.Items")])
        | OfShape0 null null "Items" _ when let t = m.Arguments.[0].Type in t.IsConstructedGenericType && t.GetGenericTypeDefinition() = typedefof<System.Linq.IGrouping<_,_>> ->
            let alias = if String.IsNullOrEmpty qb.TableNameDot then None else Some(qb.TableNameDot.TrimEnd([|'.'|]))
            SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(alias, "Value"); SqlExpr.Literal(SqlLiteral.String "$.Items")])
        | OfShape0 null null "Invoke" _ when FSharp.Reflection.FSharpType.IsRecord m.Type ->
            let rec collectArgs (expr: Expression) (args: ResizeArray<struct (ParameterExpression * Expression)>) =
                match expr with
                | :? MethodCallExpression as mc when mc.Method.Name = "Invoke" && mc.Arguments.Count = 1 ->
                    match mc.Object with :? LambdaExpression as le -> args.Add(struct (le.Parameters.[0], mc.Arguments.[0])) | _ -> ()
                    collectArgs mc.Object args
                | :? LambdaExpression as le -> collectArgs le.Body args
                | _ -> struct (expr, args)
            let struct (recordCtorExpr, argsList) = collectArgs (m :> Expression) (ResizeArray())
            let fields = FSharp.Reflection.FSharpType.GetRecordFields m.Type
            let constrArgs = (recordCtorExpr :?> NewExpression).Arguments
            let members =
                seq { for field, arg in constrArgs |> Seq.zip fields do
                        match arg with
                        | :? ParameterExpression as pe ->
                            let struct(_, correspondingExpr) = argsList |> Seq.find(fun struct(p, _) -> p = pe)
                            struct (field.Name, correspondingExpr)
                        | other -> struct (field.Name, other) }
                |> Seq.map(fun struct(name, expr) -> struct (name, visitDu expr qb)) |> Seq.toArray
            newObjectDu members
        | OfShape0 null OfString "Trim" str -> SqlExpr.FunctionCall("TRIM", [visitDu str qb])
        | OfShape0 null OfString "TrimStart" str -> SqlExpr.FunctionCall("LTRIM", [visitDu str qb])
        | OfShape0 null OfString "TrimEnd" str -> SqlExpr.FunctionCall("RTRIM", [visitDu str qb])
        | OfShape2 null OfString "Replace" null null (str, oldValue, newValue) ->
            SqlExpr.FunctionCall("REPLACE", [visitDu str qb; visitDu oldValue qb; visitDu newValue qb])
        | OfShape0 null OfString "IsNullOrEmpty" str ->
            SqlExpr.Binary(SqlExpr.Unary(UnaryOperator.IsNull, visitDu str qb), BinaryOperator.Or, SqlExpr.Binary(visitDu str qb, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.String "")))
        | OfShape1 null OfString "IsMatch" (OfString) (input, pattern) when m.Method.DeclaringType = typeof<System.Text.RegularExpressions.Regex> ->
            SqlExpr.Binary(visitDu input qb, BinaryOperator.Regexp, visitDu pattern qb)
        | _ ->
            raise (NotSupportedException(
                sprintf "Error: Method '%s' is not supported.\nReason: The method has no SQL translation.\nFix: Rewrite the query or call AsEnumerable() before using this method." m.Method.Name))

    /// DU-constructing visitor: builds SqlExpr tree from expression tree.
    /// ALL non-UpdateMode expression families return proper SqlExpr DU nodes.
    /// Only pre-expression/unknown handler callbacks (external API) use __raw__.
    /// Entry points clear SB after this returns and re-emit via SqlDuMinimalEmit.
    and internal visitDu (exp: Expression) (qb: QueryBuilder) : SqlExpr =
        // Pre-expression handlers (DBRef etc.) — external API boundary, captured as __raw__
        let sbBefore = qb.StringBuilder.Length
        if runHandler preExpressionHandler qb exp then
            captureHandlerRawDu qb sbBefore
        else

        // Fully-constant early-out (same guard as legacy visit)
        if exp.NodeType <> ExpressionType.Lambda
            && exp.NodeType <> ExpressionType.Quote
            && isFullyConstant exp
            && (match exp with :? ConstantExpression as ce when ce.Value = null -> false | _ -> true) then
            qb.AllocateParamExpr(evaluateExpr<obj> exp)
        else

        match exp.NodeType with
        | ExpressionType.And | ExpressionType.AndAlso | ExpressionType.Or | ExpressionType.OrElse
        | ExpressionType.Equal | ExpressionType.NotEqual | ExpressionType.LessThan | ExpressionType.LessThanOrEqual
        | ExpressionType.GreaterThan | ExpressionType.GreaterThanOrEqual
        | ExpressionType.Add | ExpressionType.Subtract | ExpressionType.Multiply | ExpressionType.Divide | ExpressionType.Modulo ->
            visitBinaryDu (exp :?> BinaryExpression) qb
        | ExpressionType.Not -> SqlExpr.Unary(UnaryOperator.Not, visitDu (exp :?> UnaryExpression).Operand qb)
        | ExpressionType.Negate | ExpressionType.NegateChecked -> SqlExpr.Unary(UnaryOperator.Neg, visitDu (exp :?> UnaryExpression).Operand qb)
        | ExpressionType.Lambda -> visitDu (exp :?> LambdaExpression).Body qb
        | ExpressionType.Call -> visitMethodCallDu (exp :?> MethodCallExpression) qb
        | ExpressionType.Constant ->
            let c = exp :?> ConstantExpression
            match c.Value with null -> SqlExpr.Literal(SqlLiteral.Null) | _ -> qb.AllocateParamExpr(c.Value)
        | ExpressionType.MemberAccess -> visitMemberAccessDu (exp :?> MemberExpression |> MemberAccess.From) qb
        | ExpressionType.Convert ->
            let m = exp :?> UnaryExpression
            if m.Type = typeof<obj> || m.Operand.Type = typeof<obj> then visitDu m.Operand qb
            else castToDu qb m.Type m.Operand
        | ExpressionType.New ->
            let m = exp :?> NewExpression
            if isTuple m.Type then SqlExpr.JsonArrayExpr([for arg in m.Arguments -> visitDu arg qb])
            elif m.Members.Count = m.Arguments.Count then
                newObjectDu [|for membr, expr in m.Arguments |> Seq.zip m.Members -> struct(membr.Name, visitDu expr qb)|]
            else raise (NotSupportedException(sprintf "Cannot construct new in SQL query %A" m.Type))
        | ExpressionType.Parameter -> visitParameterDu (exp :?> ParameterExpression) qb
        | ExpressionType.ArrayIndex ->
            match exp with
            | :? ParameterExpression as pe -> visitParameterDu pe qb
            | :? BinaryExpression as be -> arrayIndexDu be.Left be.Right qb
            | other -> raise (NotSupportedException(sprintf "Unknown array index expression of: %A" other))
        | ExpressionType.Index ->
            let indexExp = exp :?> IndexExpression
            if indexExp.Arguments.Count <> 1 then raise (NotSupportedException("The SQL translator does not support multiple args indexes."))
            let arge = indexExp.Arguments.[0]
            if not (isFullyConstant arge) then raise (NotSupportedException("The SQL translator does not support non constant index arg."))
            if arge.Type = typeof<string> then
                let arg = evaluateExpr<string> arge
                match arg with
                | "Id" when isRootParameter indexExp.Object ->
                    let alias = if String.IsNullOrEmpty qb.TableNameDot then None else Some(qb.TableNameDot.TrimEnd([|'.'|]))
                    SqlExpr.Column(alias, "Id")
                | arg -> visitPropertyDu indexExp.Object arg ({new Expression() with member this.Type = indexExp.Object.Type}) qb
            else arrayIndexDu indexExp.Object arge qb
        | ExpressionType.Quote -> visitDu (exp :?> UnaryExpression).Operand qb
        | ExpressionType.Invoke ->
            let invocation = exp :?> InvocationExpression
            match invocation.Expression with
            | :? LambdaExpression as le when le.Parameters.Count = invocation.Arguments.Count ->
                visitDu (inlineLambdaInvocation le invocation.Arguments) qb
            | _ when invocation.CanReduce -> visitDu (invocation.Reduce()) qb
            | _ when isFullyConstant exp -> qb.AllocateParamExpr(evaluateExpr<obj> exp)
            | _ -> raise (NotSupportedException("Error: Invoke expression is not reducible.\nReason: The invoked expression cannot be inlined or treated as a constant for SQL translation.\nFix: Inline the lambda or move the invocation after AsEnumerable()."))
        | ExpressionType.Conditional ->
            let c = exp :?> ConditionalExpression
            SqlExpr.CaseExpr([(visitDu c.Test qb, visitDu c.IfTrue qb)], Some(visitDu c.IfFalse qb))
        | ExpressionType.TypeIs ->
            let typeIsExp = exp :?> TypeBinaryExpression
            if not (mustIncludeTypeInformationInSerializationFn typeIsExp.Expression.Type) then
                raise (NotSupportedException(sprintf "Cannot translate TypeIs expression, because the DB will not store its type information for %A" typeIsExp.Type))
            let typeExpr = SqlExpr.FunctionCall("json_extract", [visitDu typeIsExp.Expression qb; SqlExpr.Literal(SqlLiteral.String "$.$type")])
            match typeIsExp.TypeOperand |> typeToName with
            | None -> raise (NotSupportedException(sprintf "Cannot translate TypeIs expression with the TypeOperand: %A" typeIsExp.TypeOperand))
            | Some typeName -> SqlExpr.Binary(typeExpr, BinaryOperator.Eq, qb.AllocateParamExpr typeName)
        | ExpressionType.MemberInit ->
            let m = exp :?> MemberInitExpression
            newObjectDu [|for binding in m.Bindings |> Seq.cast<MemberAssignment> -> struct(binding.Member.Name, visitDu binding.Expression qb)|]
        | ExpressionType.ListInit ->
            let listExp = exp :?> ListInitExpression
            SqlExpr.FunctionCall("jsonb_array", [for item in listExp.Initializers -> visitDu (item.Arguments |> Seq.exactlyOne) qb])
        // Unknown handler fallback — external API boundary, captured as __raw__
        | _ ->
            if not (runHandler unknownExpressionHandler qb exp) then
                raise (ArgumentOutOfRangeException $"QueryTranslator.{nameof unknownExpressionHandler} did not handle the expression of type: {exp.NodeType}")
            captureHandlerRawDu qb sbBefore

    // ─── DBRef relation query translation ────────────────────────────────────────
