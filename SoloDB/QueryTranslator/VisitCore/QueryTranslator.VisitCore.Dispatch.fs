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
    // ─── DU-constructing visitor (legacy visit path removed) ─────────
    // All expression families produce SqlExpr DU nodes via visitDu.
    // Pre-expression and unknown-expression handlers now return DU via DuHandlerResult.

    /// Placeholder: legacy visit removed. All callers now use visitDu + emitExpr.
    let internal visit (_exp: Expression) (_qb: QueryBuilder) : unit =
        raise (NotSupportedException "Legacy visit path removed. Use visitDu + SqlDuMinimalEmit.emitExpr.")

    // Legacy visitor functions removed (visitBinary, visitMemberAccess, visitMethodCall,
    // visitParameter, visitNot, visitNegate, visitNew, visitMemberInit, visitConvert, visitConstant,
    // visitListInit, visitTypeIs, visitIfElse, arrayIndex, visitProperty, castTo, visitMathMethod,
    // visitLambda, newObject, containsImpl, emitStringOperand).
    // All callers now use visitDu + SqlDuMinimalEmit.emitExpr.

    let private isByRefLikeType (t: Type) =
        t.CustomAttributes
        |> Seq.exists (fun attr -> attr.AttributeType.FullName = "System.Runtime.CompilerServices.IsByRefLikeAttribute")

    let rec private emitStringOperandDu (qb: QueryBuilder) (ignoreCase: bool) (expr: Expression) : SqlExpr =
        if ignoreCase then SqlExpr.FunctionCall("TO_LOWER", [visitDu expr qb])
        else visitDu expr qb

    and private stripTypeDiscriminatorDu (json: JsonSerializator.JsonValue) : JsonSerializator.JsonValue =
        match json with
        | JsonSerializator.JsonValue.Object dict ->
            let normalized = Dictionary<string, JsonSerializator.JsonValue>()
            for KeyValue(k, v) in dict do
                if not (StringComparer.Ordinal.Equals(k, "$type")) then
                    normalized.[k] <- stripTypeDiscriminatorDu v
            JsonSerializator.JsonValue.Object normalized
        | JsonSerializator.JsonValue.List items ->
            let normalized = ResizeArray<JsonSerializator.JsonValue>(items.Count)
            for item in items do
                normalized.Add(stripTypeDiscriminatorDu item)
            JsonSerializator.JsonValue.List normalized
        | _ -> json

    and private normalizeKnownJsonForJsonEachValueDu (targetExpr: SqlExpr) (knownObject: obj) : obj =
        match targetExpr with
        | SqlExpr.Column(Some sourceAlias, "Value") when StringComparer.Ordinal.Equals(sourceAlias, "json_each") ->
            knownObject
            |> JsonSerializator.JsonValue.Serialize
            |> stripTypeDiscriminatorDu
            :> obj
        | _ -> knownObject

    and private castToDu (qb: QueryBuilder) (castToType: Type) (o: Expression) : SqlExpr =
        QueryTranslatorVisitCoreCastAggregate.castToDu visitDu qb castToType o

    and private visitMathMethodDu (m: MethodCallExpression) (qb: QueryBuilder) : SqlExpr voption =
        QueryTranslatorVisitCoreCastAggregate.visitMathMethodDu visitDu m qb

    and private arrayIndexDu (array: Expression) (index: Expression) (qb: QueryBuilder) : SqlExpr =
        match index with
        | :? ConstantExpression as ce when ce.Type.IsPrimitive ->
            SqlExpr.FunctionCall("jsonb_extract", [visitDu array qb; SqlExpr.Literal(SqlLiteral.String(sprintf "$[%O]" ce.Value))])
        | _ -> raise (NotSupportedException("The index of the array must always be a constant value."))

    /// Resolve the correct SQL alias for a ParameterExpression.
    /// If the parameter belongs to the current scope, use qb.TableNameDot.
    /// If it's an outer-captured parameter, look up OuterParameterAliases.
    and private resolveAliasForParameter (paramExpr: ParameterExpression) (qb: QueryBuilder) : string option =
        if qb.Parameters |> Seq.exists (fun p -> obj.ReferenceEquals(p, paramExpr)) then
            // Current scope parameter
            if String.IsNullOrEmpty qb.TableNameDot then None
            else Some(qb.TableNameDot.TrimEnd([|'.'|]))
        else
            // Outer scope parameter — check dictionary
            match qb.OuterParameterAliases.TryGetValue(paramExpr) with
            | true, outerAlias -> Some outerAlias
            | false, _ ->
                // Fallback: use current scope (pre-existing behavior for edge cases)
                if String.IsNullOrEmpty qb.TableNameDot then None
                else Some(qb.TableNameDot.TrimEnd([|'.'|]))

    and private visitPropertyDu (o: Expression) (property: obj) (m: Expression) (qb: QueryBuilder) : SqlExpr =
        match property with
        | :? string as property ->
            let alias =
                match o with
                | :? ParameterExpression as pe -> resolveAliasForParameter pe qb
                | _ -> if String.IsNullOrEmpty qb.TableNameDot then None else Some(qb.TableNameDot.TrimEnd([|'.'|]))
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
        let alias = resolveAliasForParameter m qb
        if m.Type = typeof<int64>
            && ((qb.Parameters.IndexOf m = 0 && qb.Parameters.Count = 2)
                || qb.IdParameterIndex = qb.Parameters.IndexOf m) then
            SqlExpr.Column(alias, "Id")
        elif qb.UpdateMode then
            SqlExpr.Literal(SqlLiteral.String "$")
        elif qb.JsonExtractSelfValue && ((not << isPrimitiveSQLiteType) m.Type || (not << String.IsNullOrWhiteSpace) qb.TableNameDot) then
            SqlExpr.JsonRootExtract(alias, "Value")
        else
            SqlExpr.Column(alias, "Value")

    and private visitMemberAccessDu (m: MemberAccess) (qb: QueryBuilder) : SqlExpr =
        let alias =
            match m.Expression with
            | :? ParameterExpression as pe -> resolveAliasForParameter pe qb
            | _ -> if String.IsNullOrEmpty qb.TableNameDot then None else Some(qb.TableNameDot.TrimEnd([|'.'|]))
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
        elif m.InputType = typeof<DateTime> || m.InputType = typeof<DateTimeOffset> then
            DateTimeFunctions.translateDateTimeMember m.MemberName (visitDu m.Expression qb) m.InputType
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
                else SqlExpr.JsonExtractExpr(alias, "Value", JsonPathOps.ofList parts)
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
                elif (not (isPrimitiveSQLiteType m.InputType)) || typeof<JsonSerializator.JsonValue>.IsAssignableFrom m.InputType then
                    let esc = escapeSQLiteString m.MemberName
                    SqlExpr.FunctionCall("jsonb_extract", [visitDu originalExpr.Expression qb; SqlExpr.Literal(SqlLiteral.String $"$.{esc}")])
                else
                    raise (NotSupportedException(
                        sprintf "Error: Member access '%O' is not supported.\nReason: The member cannot be translated to SQL in this context.\nFix: Simplify the expression or move it after AsEnumerable()." originalExpr.Member.Name))

    and private containsImplDu (qb: QueryBuilder) (array: Expression) (value: Expression) : SqlExpr =
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
        let arrayQb = if qb.TableNameDot = "" then {qb with TableNameDot = "o."} else qb
        let arrayExpr = visitDu (normalizeContainsSource array) arrayQb
        let whereExpr =
            if isPrimitiveSQLiteType value.Type then
                SqlExpr.Binary(SqlExpr.Column(Some "json_each", "Value"), BinaryOperator.Eq, visitDu value qb)
            else
                if not (isFullyConstant value) then
                    raise (ArgumentException $"Cannot translate contains with this type of expression: {value.Type}")
                let normalizedKnownJson =
                    evaluateExpr<obj> value
                    |> normalizeKnownJsonForJsonEachValueDu (SqlExpr.Column(Some "json_each", "Value"))
                compareKnownJsonDu qb (SqlExpr.Column(Some "json_each", "Value")) value.Type normalizedKnownJson
        SqlExpr.Exists({ Ctes = []; Body = SelectBody.SingleSelect {
            Distinct = false; Projections = Explicit({ Expr = SqlExpr.Literal(SqlLiteral.Integer 1L); Alias = None }, [])
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
            Distinct = false; Projections = Explicit({ Expr = SqlExpr.Literal(SqlLiteral.Integer 1L); Alias = None }, [])
            Source = Some(TableSource.FromJsonEach(arrayExpr, None)); Joins = []
            Where = Some whereExpr; GroupBy = []; Having = None; OrderBy = []; Limit = None; Offset = None } })
        if isAll then SqlExpr.Unary(UnaryOperator.Not, existsExpr) else existsExpr

    and private visitBinaryDu (b: BinaryExpression) (qb: QueryBuilder) : SqlExpr =
        QueryTranslatorVisitCoreBinary.visitBinaryDu visitDu normalizeKnownJsonForJsonEachValueDu b qb

    and private visitMethodCallDu (m: MethodCallExpression) (qb: QueryBuilder) : SqlExpr =
        QueryTranslatorVisitCoreMethodCall.visitMethodCallDu visitDu visitMathMethodDu arrayIndexDu visitPropertyDu containsImplDu visitNestedArrayPredicateDu castToDu newObjectDu emitStringOperandDu m qb

    /// DU-constructing visitor: builds SqlExpr tree from expression tree.
    /// ALL expression families return proper SqlExpr DU nodes.
    /// Pre-expression handlers return DU via qb.DuHandlerResult.
    and internal visitDu (exp: Expression) (qb: QueryBuilder) : SqlExpr =
        qb.StepTranslation()
        // Pre-expression handlers (DBRef etc.) — return DU directly via DuHandlerResult
        qb.DuHandlerResult.Value <- ValueNone
        if runHandler preExpressionHandler qb exp then
            match qb.DuHandlerResult.Value with
            | ValueSome duResult -> duResult
            | ValueNone -> raise (InvalidOperationException "Pre-expression handler returned true but did not set DuHandlerResult")
        else

        // Fully-constant early-out (same guard as legacy visit)
        if exp.NodeType <> ExpressionType.Lambda
            && exp.NodeType <> ExpressionType.Quote
            && not (isByRefLikeType exp.Type)
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
        | ExpressionType.Coalesce ->
            let b = exp :?> BinaryExpression
            SqlExpr.Coalesce(visitDu b.Left qb, [visitDu b.Right qb])
        | ExpressionType.Not ->
            // SARGABLE form: NOT(expr) → (expr = 0) for boolean operands.
            // LINQ only generates NOT on boolean expressions, so this is safe.
            // Enables index utilization on boolean fields.
            let operand = visitDu (exp :?> UnaryExpression).Operand qb
            SqlExpr.Binary(operand, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L))
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
            if m.Type = typeof<DateTime> then
                if m.Arguments.Count = 3 then
                    SqlExpr.FunctionCall("printf", [SqlExpr.Literal(SqlLiteral.String "%04d-%02d-%02d"); visitDu m.Arguments.[0] qb; visitDu m.Arguments.[1] qb; visitDu m.Arguments.[2] qb])
                else raise (NotSupportedException($"new DateTime({m.Arguments.Count} arguments) is not supported in SQL translation. Only new DateTime(year, month, day) with 3 arguments is supported."))
            elif isTuple m.Type then SqlExpr.JsonArrayExpr([for arg in m.Arguments -> visitDu arg qb])
            elif m.Members.Count = m.Arguments.Count then
                let fieldQb = { qb with InsideJsonObjectProjection = true }
                newObjectDu [|for membr, expr in m.Arguments |> Seq.zip m.Members -> struct(membr.Name, visitDu expr fieldQb)|]
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
            SqlExpr.CaseExpr((visitDu c.Test qb, visitDu c.IfTrue qb), [], Some(visitDu c.IfFalse qb))
        | ExpressionType.TypeIs ->
            let typeIsExp = exp :?> TypeBinaryExpression
            if not (mustIncludeTypeInformationInSerializationFn typeIsExp.Expression.Type) then
                raise (NotSupportedException(sprintf "Cannot translate TypeIs expression, because the DB will not store its type information for %A" typeIsExp.Type))
            let typeExpr = SqlExpr.FunctionCall("jsonb_extract", [visitDu typeIsExp.Expression qb; SqlExpr.Literal(SqlLiteral.String "$.$type")])
            match typeIsExp.TypeOperand |> typeToName with
            | None -> raise (NotSupportedException(sprintf "Cannot translate TypeIs expression with the TypeOperand: %A" typeIsExp.TypeOperand))
            | Some typeName -> SqlExpr.Binary(typeExpr, BinaryOperator.Eq, qb.AllocateParamExpr typeName)
        | ExpressionType.MemberInit ->
            let m = exp :?> MemberInitExpression
            let fieldQb = { qb with InsideJsonObjectProjection = true }
            newObjectDu [|for binding in m.Bindings |> Seq.cast<MemberAssignment> -> struct(binding.Member.Name, visitDu binding.Expression fieldQb)|]
        | ExpressionType.ListInit ->
            let listExp = exp :?> ListInitExpression
            SqlExpr.FunctionCall("jsonb_array", [for item in listExp.Initializers -> visitDu (item.Arguments |> Seq.exactlyOne) qb])
        // Unknown handler fallback — returns DU via DuHandlerResult
        | _ ->
            qb.DuHandlerResult.Value <- ValueNone
            if not (runHandler unknownExpressionHandler qb exp) then
                raise (ArgumentOutOfRangeException $"QueryTranslator.{nameof unknownExpressionHandler} did not handle the expression of type: {exp.NodeType}")
            match qb.DuHandlerResult.Value with
            | ValueSome duResult -> duResult
            | ValueNone -> raise (InvalidOperationException $"Unknown expression handler returned true but did not set DuHandlerResult for: {exp.NodeType}")

    // ─── DBRef relation query translation ────────────────────────────────────────
