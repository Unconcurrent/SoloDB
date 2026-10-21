namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorExpressionAdmission

/// Ordinary acquisition shares producer ordering and parameter facts with compilation.
module internal QueryableValueBindings =
    type private PreparedLayout =
        | ScalarLayout of SqlExpr
        | ListLayout of SqlExpr list
        | ChoiceLayout of bool

    let create (variables: Dictionary<string, obj>) =
        let order = QueryValueBindingOrder()
        let acquisitions = ResizeArray<unit -> unit>()
        let layouts = Dictionary<string, PreparedLayout>(StringComparer.Ordinal)
        let add acquire =
            order.Add acquisitions.Count
            acquisitions.Add acquire
        let allocate name =
            variables.Add(name, null)
            SqlExpr.Parameter name
        let bind prepare encoded scalarJson withWriter (original: struct(string voption ref * ResizeArray<int array> * (seq<int array> -> obj -> obj)) voption) (expression: Expression) =
            let name = "dp" + variables.Count.ToString(Globalization.CultureInfo.InvariantCulture)
            let value = allocate name
            let json = if encoded then ValueSome(allocate (name + "j")) else ValueNone
            let writer = if withWriter then ValueSome(allocate (name + "w")) else ValueNone
            add (fun () ->
                let sampled = ExpressionEvaluation.evaluateExpr<obj> expression
                let struct(stored, isJson, writerType) = prepare sampled
                variables[name] <- stored
                match original with
                | ValueSome struct(slot,demanded,prepareOriginal) ->
                    match slot.Value with
                    | ValueSome name -> variables[name] <- if demanded.Count>0 then prepareOriginal demanded sampled else null
                    | ValueNone -> ()
                | ValueNone -> ()
                if encoded then variables[name + "j"] <- box isJson
                if withWriter then
                    TypeMetadata.registerType writerType
                    variables[name + "w"] <- box writerType.FullName)
            let value =
                match json with
                | ValueNone -> value
                | ValueSome flag ->
                    let token = SqlExpr.FunctionCall("jsonb", [value])
                    let token = if scalarJson then SqlExpr.FunctionCall("jsonb_extract", [token; SqlExpr.Literal(SqlLiteral.String "$")]) else token
                    SqlExpr.CaseExpr((flag, token), [], Some value)
            value, writer
        let produced comparison (expression: Expression) =
            let preparation = QueryStructuralOrdering.originalPreparation expression.Type
            let original = preparation |> ValueOption.map(fun struct(prepare,_) -> struct(ref ValueNone,ResizeArray<int array>(),prepare))
            let value, writer = bind (JsonFunctions.prepareQueryParameter comparison expression.Type)
                                    (JsonFunctions.parameterMayNeedJson expression.Type) false
                                    (not(JsonFunctions.parameterWriterIsFixed expression.Type)) original expression
            let produced = QueryProduced.parameter expression.Type value writer
            match original, preparation, produced with
            | ValueSome struct(slot,demanded,_), ValueSome struct(_,project), DirectValue(value, input) ->
                let units = lazy (
                    let name = "dp" + variables.Count.ToString(Globalization.CultureInfo.InvariantCulture)
                    let parameter = allocate name
                    slot.Value <- ValueSome name
                    parameter)
                DirectValue(value, { input with OriginalOrderOperand = ValueSome(QueryOriginalOrderOperand(expression.Type,units,SqlExpr.JsonArrayExpr [],0,(fun path -> demanded.Add path),project)) })
            | _ -> produced
        let scalar (expression: Expression) =
            bind (fun value -> struct(value, false, expression.Type)) false false false ValueNone expression |> fst
        let preparedScalar (expression: Expression) =
            bind (fun value -> let stored, encoded = unbox<obj * bool> value in struct(stored, encoded, expression.Type)) true true false ValueNone expression |> fst
        let prefix (expression: Expression) =
            bind (fun value -> let struct(stored, encoded) = JsonFunctions.toSQLParameterForComparison false value in struct(stored, encoded, expression.Type)) false false false ValueNone expression |> fst
        let local (expression: Expression) =
            let value = ref null
            add (fun () -> value.Value <- ExpressionEvaluation.evaluateExpr<obj> expression)
            BoundValueCallExpression(Array.empty, expression.Type, Func<obj array,obj>(fun _ -> value.Value)) :> Expression
        let value comparison expression =
            match produced comparison expression with
            | DirectValue(value, _) -> value
            | _ -> invalidOp "A captured parameter is direct."
        let prepareLayout (expression: Expression) initial =
            let name = "ql" + layouts.Count.ToString(Globalization.CultureInfo.InvariantCulture)
            layouts.Add(name, initial)
            let factory: (obj -> PreparedLayout) voption ref = ref ValueNone
            add (fun () ->
                let sampled = ExpressionEvaluation.evaluateExpr<obj> expression
                layouts[name] <- factory.Value.Value sampled)
            fun produce ->
                factory.Value <- ValueSome produce
                SqlExpr.Parameter name
        let prepareList expression =
            let complete = prepareLayout expression (ListLayout [])
            fun produce -> complete (produce >> ListLayout)
        let prepareChoice expression =
            let complete = prepareLayout expression (ChoiceLayout false)
            fun decide -> complete (decide >> ChoiceLayout)
        let prepareConstant (expression: Expression) target =
            match expression with
            | :? ConstantExpression as literal -> PreparedConstant literal.Value
            | _ ->
                let complete = prepareLayout expression (ScalarLayout(SqlExpr.Literal SqlLiteral.Null))
                DeferredConstant(fun produce ->
                    PackedValue(complete (produce >> QueryProduced.packet >> ScalarLayout),
                                (QueryProduced.typed target (SqlExpr.Literal SqlLiteral.Null)).Input))
        let rec resolveExpression expression =
            match expression with
            | SqlExpr.InList(value, SqlExpr.Parameter name, []) when layouts.ContainsKey name ->
                match layouts[name] with
                | ListLayout [] -> SqlExpr.Binary(SqlExpr.Literal(SqlLiteral.Integer 1L), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L))
                | ListLayout(head :: tail) -> SqlExpr.InList(resolveExpression value, head, tail)
                | ScalarLayout _ | ChoiceLayout _ -> SqlExprCombinators.mapChildren resolveExpression resolveQuery expression
            | SqlExpr.CaseExpr((SqlExpr.Parameter name, selected), [], Some otherwise) when layouts.ContainsKey name ->
                match layouts[name] with
                | ChoiceLayout decision -> resolveExpression (if decision then selected else otherwise)
                | _ -> SqlExprCombinators.mapChildren resolveExpression resolveQuery expression
            | SqlExpr.Parameter name when layouts.ContainsKey name ->
                match layouts[name] with
                | ScalarLayout value -> resolveExpression value
                | ChoiceLayout decision -> SqlExpr.Literal(SqlLiteral.Boolean decision)
                | ListLayout _ -> invalidOp "A captured list must occupy an IN operand."
            | _ -> SqlExprCombinators.mapChildren resolveExpression resolveQuery expression
        and resolveQuery query =
            SqlExprCombinators.mapSelectParts (SqlExprCombinators.mapCores (SqlExprCombinators.mapCoreExpressions resolveExpression resolveQuery)) resolveQuery query
        let binder = { Order = order; PrepareConstant = prepareConstant; PrepareList = ValueSome prepareList; PrepareChoice = ValueSome prepareChoice
                       Produced = produced; IsValue = isConstantParameter
                       Parameter = value false; Comparison = value true
                       Scalar = scalar; PreparedScalar = preparedScalar; Prefix = prefix; Local = local }
        binder, (fun query ->
            for index in order.Indices() do acquisitions[index]()
            resolveQuery query)
