namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open Microsoft.Data.Sqlite
open SQLiteTools
open SoloDatabase.SqlModel

module internal CompiledQueries =
    type private Substitute(replacements: (ParameterExpression * Expression) array) =
        inherit ExpressionVisitor()
        override _.VisitParameter parameter =
            replacements
            |> Array.tryPick (fun (key, value) -> if obj.ReferenceEquals(key, parameter) then Some value else None)
            |> Option.defaultValue (parameter :> Expression)

    // F# nests quoted predicates inside the outer expression. Rebind quotation
    // variables symbolically instead of evaluating SubstHelper with sample values.
    type private NormalizeQuotations() =
        inherit ExpressionVisitor()
        override this.VisitMethodCall call =
            if not (isNull call.Method.DeclaringType)
               && call.Method.DeclaringType.FullName = "Microsoft.FSharp.Linq.RuntimeHelpers.LeafExpressionConverter"
               && call.Method.Name = "QuotationToLambdaExpression" then
                let quoted, variables, values =
                    match call.Arguments.[0] with
                    | :? MethodCallExpression as substitution when substitution.Method.Name = "SubstHelper" ->
                        let quoted = (substitution.Arguments.[0] :?> ConstantExpression).Value :?> Microsoft.FSharp.Quotations.Expr
                        let variables = (substitution.Arguments.[1] :?> NewArrayExpression).Expressions
                                        |> Seq.map (fun e -> (e :?> ConstantExpression).Value :?> Microsoft.FSharp.Quotations.Var) |> Seq.toArray
                        let values = (substitution.Arguments.[2] :?> NewArrayExpression).Expressions |> Seq.toArray
                        quoted, variables, values
                    | :? ConstantExpression as constant ->
                        (constant.Value :?> Microsoft.FSharp.Quotations.Expr), [||], [||]
                    | _ -> raise (NotSupportedException "Unsupported F# query quotation.")
                let delegateType = Expression.GetDelegateType(Array.append (variables |> Array.map _.Type) [|quoted.Type|])
                let wrapper = Microsoft.FSharp.Quotations.Expr.NewDelegate(delegateType, Array.toList variables, quoted)
                let lambda = Microsoft.FSharp.Linq.RuntimeHelpers.LeafExpressionConverter.QuotationToExpression wrapper :?> LambdaExpression
                let replacements = values |> Array.mapi (fun i value -> lambda.Parameters.[i], (Expression.Convert(value, variables.[i].Type) :> Expression))
                let body = Substitute(replacements).Visit lambda.Body
                Expression.Quote(this.Visit(body) :?> LambdaExpression) :> Expression
            else base.VisitMethodCall call

    let private invalid message = raise (NotSupportedException("Cannot compile query: " + message))

    // Each expression is classified once per compilation, including shared subtrees.
    // Bit 1 denotes invocation values; bit 2 denotes document parameters.
    let private valueClassifier (argument: ParameterExpression) (locals: ResizeArray<ParameterExpression>) =
        let cache = Dictionary<Expression, int>()
        let rec flags (expression: Expression) =
            if isNull expression then 0 else
            match cache.TryGetValue expression with
            | true, value -> value
            | _ ->
                let fold expressions = Seq.fold (fun bits e -> bits ||| flags e) 0 expressions
                let value =
                    match expression with
                    | :? ParameterExpression as parameter -> if obj.ReferenceEquals(parameter, argument) || locals.Contains parameter then 1 else 2
                    | :? ConstantExpression -> 0
                    | :? MemberExpression as memberExpression ->
                        let parent = flags memberExpression.Expression
                        match memberExpression.Member with
                        | :? System.Reflection.FieldInfo as field when field.IsLiteral -> parent
                        | _ -> if parent = 0 then 1 else parent
                    | :? UnaryExpression as unary -> flags unary.Operand
                    | :? BinaryExpression as binary -> flags binary.Left ||| flags binary.Right ||| flags binary.Conversion
                    | :? MethodCallExpression as call ->
                        let inputs = flags call.Object ||| fold call.Arguments
                        if inputs = 0 then 1 else inputs
                    | :? NewExpression as created -> fold created.Arguments
                    | :? NewArrayExpression as array -> fold array.Expressions
                    | :? ConditionalExpression as conditional -> flags conditional.Test ||| flags conditional.IfTrue ||| flags conditional.IfFalse
                    | _ -> 2
                cache.Add(expression, value)
                value
        fun (expression: Expression) ->
            flags expression = 1 && not (typeof<IQueryable>.IsAssignableFrom expression.Type)

    let rec private checkRoot (source: ParameterExpression) (expression: Expression) =
        match expression with
        | :? ParameterExpression as parameter when obj.ReferenceEquals(source, parameter) -> ()
        | :? UnaryExpression as unary when unary.NodeType = ExpressionType.Convert -> checkRoot source unary.Operand
        | :? MethodCallExpression as call when call.Arguments.Count > 0
                                                    && typeof<IQueryable>.IsAssignableFrom(call.Arguments.[0].Type) ->
            checkRoot source call.Arguments.[0]
        | _ -> invalid "the query must be rooted in the supplied source parameter."

    type private Plan<'Args> = {
        Sql: string
        EmptyPrefixSql: string option
        Parameters: SQLiteToolsParams.ParameterValues
        Hydration: QueryableTranslationCore.BatchLoadContext voption
        Bind: Func<'Args, obj array, bool>
    }

    type private ParameterWriter =
        static member Add(parameters: obj array, slot: int, encoded: bool, prefix: bool, comparison: bool, value: obj) =
            let struct (value, json) = JsonFunctions.toSQLParameterForComparison comparison value
            parameters.[slot] <- value
            if encoded then parameters.[slot + 1] <- box json
            prefix && Object.Equals(value, "")

    let private writeParameter = typeof<ParameterWriter>.GetMethod("Add", Reflection.BindingFlags.Static ||| Reflection.BindingFlags.Public ||| Reflection.BindingFlags.NonPublic)

    let compile<'Source, 'Args, 'Result>
        (source: ISoloDBCollection<'Source>) (query: LambdaExpression)
        (argument: ParameterExpression) (arguments: Expression array) =
        if isNull (box source) then nullArg "source"
        if isNull query then nullArg "query"
        if source.InTransaction then invalid "transactional collections are not supported."
        let body = NormalizeQuotations().Visit query.Body
        checkRoot query.Parameters.[0] body
        let replacements =
            Array.append [|query.Parameters.[0], source.Expression|]
                (arguments |> Array.mapi (fun i value -> query.Parameters.[i + 1], value))
        let expression = Substitute(replacements).Visit body
        let build (connection: SqliteConnection) =
            let parameters = Expression.Parameter(typeof<obj array>, "parameters")
            let emptyPrefix = Expression.Variable(typeof<bool>, "emptyPrefix")
            let locals = ResizeArray<ParameterExpression>()
            let assignments = ResizeArray<Expression>()
            let isValue = valueClassifier argument locals
            let mutable bindingCount = 0
            let names = ResizeArray<string>()
            let mutable hasPrefix = false
            let bind comparison encoded prefix (value: Expression) =
                let name = "cq" + string bindingCount
                bindingCount <- bindingCount + 1
                let jsonName = if encoded then name + "j" else null
                hasPrefix <- hasPrefix || prefix
                let slot = names.Count
                names.Add name
                if encoded then names.Add jsonName
                let write = Expression.Call(writeParameter, parameters, Expression.Constant(slot),
                                Expression.Constant(encoded), Expression.Constant(prefix), Expression.Constant(comparison),
                                Expression.Convert(value, typeof<obj>))
                assignments.Add(Expression.Assign(emptyPrefix, Expression.Or(emptyPrefix, write)))
                let parameter = SqlExpr.Parameter name
                if not encoded then parameter else
                SqlExpr.CaseExpr((SqlExpr.Parameter jsonName, SqlExpr.FunctionCall("jsonb", [parameter])), [], Some parameter)
            let local (value: Expression) =
                let variable = Expression.Variable(value.Type, "value" + string locals.Count)
                locals.Add variable
                assignments.Add(Expression.Assign(variable, value))
                variable :> Expression
            let binder = { IsValue = isValue
                           Parameter = (fun value -> bind false (JsonFunctions.parameterMayNeedJson value.Type) false value)
                           Comparison = (fun value -> bind true (JsonFunctions.parameterMayNeedJson value.Type) false value)
                           Scalar = bind true false false
                           Prefix = (fun value -> bind false false (value.Type = typeof<string>) value)
                           Local = local }
            let sql, constants, hydration, _ = QueryableTranslationCore.startCompiledTranslation connection source expression binder false
            let slotNames = names.ToArray()
            let body = Expression.Block(Array.append [|emptyPrefix|] (locals.ToArray()),
                           Array.concat [ [|Expression.Assign(emptyPrefix, Expression.Constant(false)) :> Expression|]
                                          assignments.ToArray(); [|emptyPrefix :> Expression|] ])
            let run = Expression.Lambda<Func<'Args, obj array, bool>>(body, argument, parameters).Compile()
            let emptyPrefixSql =
                if hasPrefix then
                    // Both translations allocate the same parameter names in the same order.
                    bindingCount <- 0
                    names.Clear()
                    assignments.Clear()
                    locals.Clear()
                    let sql, _, _, _ = QueryableTranslationCore.startCompiledTranslation connection source expression binder true
                    Some sql
                else None
            { Sql = sql; EmptyPrefixSql = emptyPrefixSql; Hydration = hydration; Bind = run
              Parameters = { Constants = constants |> Seq.toArray; Names = slotNames; Values = [||] } }

        let current =
            use connection = source.GetInternalConnection()
            build connection

        fun (args: 'Args) ->
            let parameters =
                if current.Parameters.Names.Length = 0 then current.Parameters
                else { current.Parameters with Values = Array.zeroCreate current.Parameters.Names.Length }
            let emptyPrefix = current.Bind.Invoke(args, parameters.Values)
            let sql = if emptyPrefix then defaultArg current.EmptyPrefixSql current.Sql else current.Sql
            QueryableExecution.enumerate<'Source, 'Result> source sql parameters current.Hydration
