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

module internal QueryTranslatorVisitCoreMethodCall =
    let internal visitMethodCallDu (visitDu: Expression -> QueryBuilder -> SqlExpr) (visitMathMethodDu: MethodCallExpression -> QueryBuilder -> SqlExpr voption) (arrayIndexDu: Expression -> Expression -> QueryBuilder -> SqlExpr) (visitPropertyDu: Expression -> obj -> Expression -> QueryBuilder -> SqlExpr) (containsImplDu: QueryBuilder -> Expression -> Expression -> SqlExpr) (visitNestedArrayPredicateDu: QueryBuilder -> Expression -> Expression -> bool -> SqlExpr) (castToDu: QueryBuilder -> Type -> Expression -> SqlExpr) (newObjectDu: struct(string * SqlExpr) array -> SqlExpr) (emitStringOperandDu: QueryBuilder -> bool -> Expression -> SqlExpr) (m: MethodCallExpression) (qb: QueryBuilder) : SqlExpr =
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
        // UpdateMode methods — DU path replacing legacy visit-based UpdateMode handlers.
        | OfShape1 null null "Set" null (oldValue, newValue) when qb.UpdateMode ->
            let pathExpr = visitDu oldValue qb
            let valueExpr = visitDu newValue qb
            SqlExpr.UpdateFragment(pathExpr, valueExpr)

        | OfShape1 null null "Append" null (array, newValue)
        | OfShape1 null null "Add" null (array, newValue) when qb.UpdateMode ->
            let arrayPathExpr = visitDu array qb
            let modifiedPath =
                match arrayPathExpr with
                | SqlExpr.Literal(SqlLiteral.String path) -> SqlExpr.Literal(SqlLiteral.String $"{path}[#]")
                | other -> other
            let valueExpr = visitDu newValue qb
            SqlExpr.UpdateFragment(modifiedPath, valueExpr)

        | OfShape2 null null "SetAt" null null (array, indexExpr, newValue) when qb.UpdateMode ->
            let arrayPathExpr = visitDu array qb
            let indexVal = evaluateExpr<obj> indexExpr
            let modifiedPath =
                match arrayPathExpr with
                | SqlExpr.Literal(SqlLiteral.String path) -> SqlExpr.Literal(SqlLiteral.String $"{path}[{indexVal}]")
                | other -> other
            let valueExpr = visitDu newValue qb
            SqlExpr.UpdateFragment(modifiedPath, valueExpr)

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
            SqlExpr.UpdateFragment(arrayPathExpr, valueExpr)
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
            SqlExpr.CaseExpr((SqlExpr.Binary(a, BinaryOperator.Lt, b), SqlExpr.Literal(SqlLiteral.Integer -1L)), [(SqlExpr.Binary(a, BinaryOperator.Gt, b), SqlExpr.Literal(SqlLiteral.Integer 1L))], Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))
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
                |> Seq.map(fun struct(name, expr) -> struct (name, visitDu expr { qb with InsideJsonObjectProjection = true })) |> Seq.toArray
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
        | _ when m.Method.Name = "ToString" && not (isNull m.Object)
                 && DateTimeFunctions.supportsTemporalToStringTranslationType m.Object.Type ->
            DateTimeFunctions.translateDateTimeToStringCall
                (visitDu m.Object qb)
                m.Object
                m.Arguments.Count
                (if m.Arguments.Count >= 1 then Some m.Arguments.[0] else None)
        | _ when m.Method.Name = "ToUnixTimeMilliseconds" && not (isNull m.Object) && m.Object.Type = typeof<DateTimeOffset> ->
            visitDu m.Object qb
        | _ ->
            raise (NotSupportedException(
                sprintf "Error: Method '%s' is not supported.\nReason: The method has no SQL translation.\nFix: Rewrite the query or call AsEnumerable() before using this method." m.Method.Name))
