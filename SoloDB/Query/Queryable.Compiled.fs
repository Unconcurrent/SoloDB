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

    type private Plan<'Args, 'State> = {
        Handle: RetainedPreparedHandle
        EmptyPrefixHandle: RetainedPreparedHandle option
        Parameters: SQLiteToolsParams.ParameterValues
        Hydration: QueryableTranslationCore.BatchLoadContext voption
        Bind: Func<'Args, obj array, struct (bool * 'State)>
    }

    type private ParameterWriter =
        static member Add(parameters: obj array, slot: int, encoded: bool, prefix: bool, comparison: bool, value: obj) =
            let struct (value, json) = JsonFunctions.toSQLParameterForComparison comparison value
            parameters.[slot] <- value
            if encoded then parameters.[slot + 1] <- box json
            prefix && Object.Equals(value, "")

    let private writeParameter = typeof<ParameterWriter>.GetMethod("Add", Reflection.BindingFlags.Static ||| Reflection.BindingFlags.Public ||| Reflection.BindingFlags.NonPublic)

    let private prepare<'Source, 'Args, 'State>
        (source: ISoloDBCollection<'Source>) (query: LambdaExpression)
        (argument: ParameterExpression) (arguments: Expression array)
        (invocationState: Expression -> Expression option) =
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
            let translate empty =
                // Operator arguments are evaluated while constructing an ordinary query;
                // quoted predicates are evaluated later by its provider. Capture those
                // arguments first, preserving source-call and left-to-right order.
                let eager =
                    { new ExpressionVisitor() with
                        override _.VisitLambda<'Delegate>(lambda: Expression<'Delegate>) = lambda :> Expression
                        override this.VisitUnary unary =
                            if unary.NodeType = ExpressionType.Quote then unary :> Expression
                            else base.VisitUnary unary
                        override this.VisitMethodCall call =
                            if call.Method.DeclaringType = typeof<Queryable> then
                                let args = call.Arguments |> Seq.toArray
                                args.[0] <- this.Visit args.[0]
                                for i = 1 to args.Length - 1 do
                                    let value = args.[i]
                                    if not (typeof<Expression>.IsAssignableFrom value.Type) && isValue value then
                                        args.[i] <- local value
                                call.Update(call.Object, args) :> Expression
                            else base.VisitMethodCall call }
                let prepared = eager.Visit expression
                let state = invocationState prepared |> Option.defaultWith (fun () -> Expression.Default(typeof<'State>) :> Expression)
                let sql, constants, hydration, _ = QueryableTranslationCore.startCompiledTranslation connection source prepared binder empty
                sql, constants, hydration, state
            let sql, constants, hydration, state = translate false
            let slotNames = names.ToArray()
            let result = Expression.New(typeof<struct (bool * 'State)>.GetConstructor([|typeof<bool>; typeof<'State>|]),
                                        emptyPrefix, Expression.Convert(state, typeof<'State>))
            let body = Expression.Block(Array.append [|emptyPrefix|] (locals.ToArray()),
                           Array.concat [ [|Expression.Assign(emptyPrefix, Expression.Constant(false)) :> Expression|]
                                          assignments.ToArray(); [|result :> Expression|] ])
            let run = Expression.Lambda<Func<'Args, obj array, struct (bool * 'State)>>(body, argument, parameters).Compile()
            let emptyPrefixSql =
                if hasPrefix then
                    // Both translations allocate the same parameter names in the same order.
                    bindingCount <- 0
                    names.Clear()
                    assignments.Clear()
                    locals.Clear()
                    // Revisit the original expression so this translation owns all of
                    // its eager locals; no substituted local outlives its classifier.
                    let sql, _, _, _ = translate true
                    Some sql
                else None
            { Handle = RetainedPreparedHandle(sql)
              EmptyPrefixHandle = emptyPrefixSql |> Option.map RetainedPreparedHandle
              Hydration = hydration; Bind = run
              Parameters = { Constants = constants |> Seq.toArray; Names = slotNames; Values = [||] } }

        let current =
            use connection = source.GetInternalConnection()
            build connection

        let bind (args: 'Args) =
            let parameters =
                if current.Parameters.Names.Length = 0 then current.Parameters
                else { current.Parameters with Values = Array.zeroCreate current.Parameters.Names.Length }
            let struct (emptyPrefix, state) = current.Bind.Invoke(args, parameters.Values)
            let handle = if emptyPrefix then defaultArg current.EmptyPrefixHandle current.Handle else current.Handle
            struct (handle, parameters, current.Hydration, state)
        struct (expression, bind)

    let compile<'Source, 'Args, 'Elem>
        (source: ISoloDBCollection<'Source>) (query: LambdaExpression)
        (argument: ParameterExpression) (arguments: Expression array) =
        let struct (_, bind) = prepare<'Source, 'Args, unit> source query argument arguments (fun _ -> None)
        fun args ->
            let struct (handle, parameters, hydration, _) = bind args
            QueryableExecution.enumerate<'Source, 'Elem> source handle.Sql parameters hydration (ValueSome handle)

    // Reflection closes the element type once during compilation. Invocation uses
    // the resulting typed delegate, including for Enumerable materializers.
    type private ResultFactory =
        static member Sequence<'Source, 'Args, 'Elem>
            (source: ISoloDBCollection<'Source>, query: LambdaExpression, argument: ParameterExpression, arguments: Expression array) =
            let run = compile<'Source, 'Args, 'Elem> source query argument arguments
            Func<'Args, IEnumerable<'Elem>>(run)

        static member Scalar<'Source, 'Args, 'Result>
            (source: ISoloDBCollection<'Source>, query: LambdaExpression, argument: ParameterExpression, arguments: Expression array) =
            let struct (expression, bind) =
                prepare<'Source, 'Args, 'Result> source query argument arguments QueryableExecution.terminalDefaultExpression
            let methodName =
                match expression with
                | :? MethodCallExpression as call -> call.Method.Name
                | _ -> "Execute"
            let run args =
                let struct (handle, parameters, hydration, fallback) = bind args
                QueryableExecution.scalar source handle.Sql parameters hydration methodName (fun () -> fallback) (ValueSome handle)
            Func<'Args, 'Result>(run)

    let compileResult<'Source, 'Args, 'Result>
        (source: ISoloDBCollection<'Source>) (query: LambdaExpression)
        (argument: ParameterExpression) (arguments: Expression array) =
        if isNull query then nullArg "query"
        let body = NormalizeQuotations().Visit query.Body
        let replacements = arguments |> Array.mapi (fun i value -> query.Parameters.[i + 1], value)
        let mutable retained = false
        let rec rooted (expression: Expression) =
            match expression with
            | :? ParameterExpression as parameter -> obj.ReferenceEquals(parameter, query.Parameters.[0])
            | :? UnaryExpression as unary -> rooted unary.Operand
            | :? MethodCallExpression as call when call.Arguments.Count > 0
                                                        && typeof<IQueryable>.IsAssignableFrom(call.Arguments.[0].Type) -> rooted call.Arguments.[0]
            | _ -> false
        let visitor =
            { new ExpressionVisitor() with
                override this.Visit expression =
                    if isNull expression then null else
                    let sequence = typeof<IQueryable>.IsAssignableFrom expression.Type
                    let terminal =
                        match expression with
                        | :? MethodCallExpression as call -> call.Method.DeclaringType = typeof<Queryable>
                        | _ -> false
                    if (sequence || terminal) && rooted expression then
                        retained <- true
                        let resultType =
                            if sequence then (UtilsReflection.GenericTypeArgCache.Get expression.Type).[0]
                            else expression.Type
                        let name = if sequence then "Sequence" else "Scalar"
                        let factory = typeof<ResultFactory>.GetMethod(name, Reflection.BindingFlags.Static ||| Reflection.BindingFlags.Public ||| Reflection.BindingFlags.NonPublic).MakeGenericMethod(typeof<'Source>, typeof<'Args>, resultType)
                        let part = Expression.Lambda(expression, query.Parameters)
                        let run =
                            try factory.Invoke(null, [|box source; part; argument; arguments|])
                            with :? Reflection.TargetInvocationException as error when not (isNull error.InnerException) ->
                                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error.InnerException).Throw()
                                Unchecked.defaultof<obj>
                        let invocation = Expression.Invoke(Expression.Constant(run), argument)
                        invocation :> Expression
                    else base.Visit expression }
        let executable = visitor.Visit body |> Substitute(replacements).Visit
        if not retained then invalid "the query must be rooted in the supplied source parameter."
        let run = Expression.Lambda<Func<'Args, 'Result>>(executable, argument).Compile()
        fun args -> run.Invoke args
