namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open NativeSQLiteTools
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
    type private NormalizeQueryLambdas() =
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
                let delegateType = call.Type.GetGenericArguments().[0]
                let replacements = values |> Array.mapi(fun i value -> variables.[i], (Expression.Convert(value, variables.[i].Type) :> Expression))
                let lambda = QueryQuotationSyntax.fromQuotation delegateType replacements quoted
                Expression.Quote(this.Visit(lambda) :?> LambdaExpression) :> Expression
            elif typeof<Queryable>.Equals(call.Method.DeclaringType) then
                let arguments =
                    call.Arguments |> Seq.map (fun argument ->
                        match argument with
                        | (:? MemberExpression | :? ConstantExpression)
                            when typeof<LambdaExpression>.IsAssignableFrom argument.Type
                                 && QueryTranslatorBaseHelpers.isFullyConstant argument ->
                            match QueryTranslatorVisitPost.tryExtractLambdaExpression argument with
                            | ValueSome lambda -> Expression.Quote(this.Visit(lambda) :?> LambdaExpression) :> Expression
                            | ValueNone -> this.Visit argument
                        | _ -> this.Visit argument)
                    |> Seq.toArray
                call.Update(this.Visit call.Object, arguments) :> Expression
            else base.VisitMethodCall call

    let private invalid message = raise (NotSupportedException("Cannot compile query: " + message))

    // Each expression is classified once per compilation, including shared subtrees.
    // Bit 1 denotes invocation values; bit 2 denotes document parameters.
    let private valueClassifier (argument: ParameterExpression) (locals: ResizeArray<ParameterExpression>) =
        let cache = Dictionary<Expression, int>()
        let blockLocals = Dictionary<ParameterExpression,int>()
        let rec flags (expression: Expression) =
            if isNull expression then 0 else
            match expression with
            | :? ParameterExpression as parameter ->
                match blockLocals.TryGetValue parameter with
                | true, dependencies -> dependencies
                | _ -> if obj.ReferenceEquals(parameter,argument) || locals.Contains parameter then 1 else 2
            | _ ->
            match cache.TryGetValue expression with
            | true, value -> value
            | _ ->
                let fold (expressions: seq<Expression>) = Seq.fold (fun bits e -> bits ||| flags e) 0 expressions
                let value =
                    match expression with
                    | :? ConstantExpression | :? DefaultExpression -> 0
                    | :? BlockExpression as block ->
                        for variable in block.Variables do blockLocals.Add(variable,2)
                        let dependencies = fold block.Expressions
                        for variable in block.Variables do blockLocals.Remove variable |> ignore
                        dependencies
                    | :? BinaryExpression as assignment when assignment.NodeType = ExpressionType.Assign && (assignment.Left :? ParameterExpression) ->
                        let parameter = assignment.Left :?> ParameterExpression
                        let dependencies = flags assignment.Right
                        if blockLocals.ContainsKey parameter then blockLocals.[parameter] <- dependencies
                        dependencies
                    | :? IndexExpression as index -> flags index.Object ||| fold index.Arguments
                    | :? TypeBinaryExpression as test -> flags test.Expression
                    | :? MemberExpression as memberExpression ->
                        let parent = flags memberExpression.Expression
                        match memberExpression.Member with
                        | :? System.Reflection.FieldInfo as field when field.IsLiteral -> parent
                        | _ -> if parent = 0 then 1 else parent
                    | :? BoundValueAccessExpression as access -> flags access.Parent
                    | :? UnaryExpression as unary -> flags unary.Operand
                    | :? BinaryExpression as binary -> flags binary.Left ||| flags binary.Right ||| flags binary.Conversion
                    | :? MethodCallExpression as call ->
                        let inputs = flags call.Object ||| fold call.Arguments
                        if inputs = 0 then 1 else inputs
                    | :? BoundValueCallExpression as call -> fold call.Inputs
                    | :? InvocationExpression as invocation -> flags invocation.Expression ||| fold invocation.Arguments
                    | :? NewExpression as created -> let inputs = fold created.Arguments in if inputs = 0 then 1 else inputs
                    | :? MemberInitExpression as initialized ->
                        let rec binding (value: MemberBinding) =
                            match value with
                            | :? MemberAssignment as assignment -> flags assignment.Expression
                            | :? MemberMemberBinding as members -> members.Bindings |> Seq.fold(fun bits value -> bits ||| binding value) 0
                            | :? MemberListBinding as members -> members.Initializers |> Seq.fold(fun bits value -> bits ||| fold value.Arguments) 0
                            | _ -> 2
                        flags initialized.NewExpression ||| (initialized.Bindings |> Seq.fold(fun bits value -> bits ||| binding value) 0)
                    | :? ListInitExpression as initialized -> flags initialized.NewExpression ||| (initialized.Initializers |> Seq.fold(fun bits value -> bits ||| fold value.Arguments) 0)
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
        Parameters: ParameterValues
        Hydration: QueryableTranslationCore.BatchLoadPlan voption
        Bind: Func<'Args, struct (bool * 'State * obj array)>
    }

    type private ParameterWriter =
        static member Add(parameters: obj array, slot: int, encoded: bool, prefix: bool, prepare: obj -> struct(obj * bool * Type voption), value: obj) =
            let struct (value, json, writer) = prepare value
            parameters.[slot] <- value
            if encoded then parameters.[slot + 1] <- box json
            struct(prefix && Object.Equals(value, ""), writer)

    let private prepare<'Source, 'Args, 'State>
        (source: ISoloDBCollection<'Source>) (queryBody: Expression) (parameters: ParameterExpression array)
        (argument: ParameterExpression) (arguments: Expression array)
        (invocationState: Expression -> Expression option) =
        if isNull (box source) then nullArg "source"
        if isNull queryBody then nullArg "query"
        if source.InTransaction then invalid "transactional collections are not supported."
        let body = NormalizeQueryLambdas().Visit queryBody
        checkRoot parameters.[0] body
        let replacements =
            Array.append [|parameters.[0], source.Expression|]
                (arguments |> Array.mapi (fun i value -> parameters.[i + 1], value))
        let expression = Substitute(replacements).Visit body
        let build (connection: NativeSqliteConnection) =
            let variables = Dictionary<ParameterExpression, int>()
            variables.Add(argument, 0)
            let locals = ResizeArray<ParameterExpression>()
            let steps = ResizeArray<obj array -> int -> bool>()
            let bindingOrder = QueryValueBindingOrder()
            let addStep step =
                bindingOrder.Add steps.Count
                steps.Add step
            let isValue = valueClassifier argument locals
            let mutable bindingCount = 0
            let names = ResizeArray<string>()
            let mutable hasPrefix = false
            let bindWithInput prepare encoded scalarJson prefix withInput (value: Expression) =
                let name = "cq" + string bindingCount
                bindingCount <- bindingCount + 1
                let jsonName = if encoded then name + "j" else null
                hasPrefix <- hasPrefix || prefix
                let slot = names.Count
                names.Add name
                if encoded then names.Add jsonName
                let writerName = if withInput then name + "w" else null
                let writerSlot = names.Count
                if withInput then names.Add writerName
                let read = QueryValuePlan.bind variables (fun _ -> None) value
                addStep(fun frame offset ->
                    let sampled = read frame
                    let struct(empty, writer) = ParameterWriter.Add(frame, offset + slot, encoded, prefix, prepare, sampled)
                    if withInput then
                        let writer = writer.Value
                        TypeMetadata.registerType writer
                        frame[offset + writerSlot] <- box writer.FullName
                    empty)
                let parameter = SqlExpr.Parameter name
                let expression =
                    if not encoded then parameter else
                    let json = SqlExpr.FunctionCall("jsonb", [parameter])
                    let json = if scalarJson then SqlExpr.FunctionCall("jsonb_extract", [json;SqlExpr.Literal(SqlLiteral.String "$")]) else json
                    SqlExpr.CaseExpr((SqlExpr.Parameter jsonName,json), [], Some parameter)
                expression, (if withInput then ValueSome(SqlExpr.Parameter writerName) else ValueNone)
            let bind prepare encoded scalarJson prefix value =
                let prepare sampled = let struct(stored, json) = prepare sampled in struct(stored, json, ValueNone)
                bindWithInput prepare encoded scalarJson prefix false value |> fst
            let produced comparison (value: Expression) =
                let encoded = JsonFunctions.parameterMayNeedJson value.Type
                let dynamic = not(JsonFunctions.parameterWriterIsFixed value.Type)
                let prepare sampled =
                    let struct(stored, encoded, writer) = JsonFunctions.prepareQueryParameter comparison value.Type sampled
                    struct(stored, encoded, ValueSome writer)
                let expression, writer = bindWithInput prepare encoded false false dynamic value
                QueryProduced.parameter value.Type expression writer
            let local (value: Expression) =
                let variable = Expression.Variable(value.Type, "value" + string locals.Count)
                locals.Add variable
                let read = QueryValuePlan.bind variables (fun _ -> None) value
                let slot = variables.Count
                variables.Add(variable, slot)
                addStep(fun locals _ -> locals.[slot] <- read locals; false)
                variable :> Expression
            let binder = { Order = bindingOrder
                           PrepareConstant = fun expression _ -> PreparedConstant(ExpressionEvaluation.evaluateExpr<obj> expression)
                           PrepareList = ValueNone
                           Produced = produced
                           IsValue = isValue
                           Parameter = (fun value -> produced false value |> function DirectValue(value, _) -> value | _ -> invalidOp "A bound parameter is direct.")
                           Comparison = (fun value -> produced true value |> function DirectValue(value, _) -> value | _ -> invalidOp "A bound parameter is direct.")
                           Scalar = bind (fun value -> struct(value,false)) false false false
                           PreparedScalar = bind (fun value -> let stored,json = unbox<obj * bool> value in struct(stored,json)) true true false
                           Prefix = (fun value -> bind (JsonFunctions.toSQLParameterForComparison false) false false (typeof<string>.Equals(value.Type)) value)
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
                            if typeof<Queryable>.Equals(call.Method.DeclaringType) then
                                let args = call.Arguments |> Seq.toArray
                                args.[0] <- this.Visit args.[0]
                                for i = 1 to args.Length - 1 do
                                    let value = args.[i]
                                    if not (typeof<Expression>.IsAssignableFrom value.Type) && isValue value then
                                        args.[i] <- local value
                                call.Update(call.Object, args) :> Expression
                            else base.VisitMethodCall call }
                let prepared = eager.Visit expression
                let state =
                    match invocationState prepared with
                    | Some value ->
                        let read = QueryValuePlan.bind variables (fun _ -> None) value
                        fun locals -> unbox<'State> (read locals)
                    | None -> fun _ -> Unchecked.defaultof<'State>
                let sql, constants, hydration, _ = QueryableTranslationCore.startCompiledTranslation connection source prepared binder empty
                sql, constants, hydration, state
            let sql, constants, hydration, state = translate false
            let slotNames = names.ToArray()
            let invocationSteps = bindingOrder.Indices() |> Array.map (fun index -> steps[index])
            let localCount = variables.Count
            let run = Func<'Args, struct (bool * 'State * obj array)>(fun args ->
                let locals = Array.zeroCreate (localCount + slotNames.Length)
                locals.[0] <- box args
                let mutable emptyPrefix = false
                for step in invocationSteps do
                    let empty = step locals localCount
                    emptyPrefix <- emptyPrefix || empty
                struct (emptyPrefix, state locals, locals))
            let emptyPrefixSql =
                if hasPrefix then
                    // Both translations allocate the same parameter names in the same order.
                    bindingCount <- 0
                    names.Clear()
                    steps.Clear()
                    bindingOrder.Clear()
                    variables.Clear()
                    variables.Add(argument, 0)
                    locals.Clear()
                    // Revisit the original expression so this translation owns all of
                    // its eager locals; no substituted local outlives its classifier.
                    let sql, _, _, _ = translate true
                    Some sql
                else None
            { Handle = RetainedPreparedHandle(sql)
              EmptyPrefixHandle = emptyPrefixSql |> Option.map RetainedPreparedHandle
              Hydration = hydration; Bind = run
              Parameters = { Constants = constants |> Seq.toArray; Names = slotNames; Values = [||]; Offset = localCount } }

        let current =
            use connection = (source :?> INativeConnectionSource).AcquireNativeConnection()
            build connection

        let bind (args: 'Args) =
            let struct (emptyPrefix, state, frame) = current.Bind.Invoke args
            let parameters =
                if current.Parameters.Names.Length = 0 then current.Parameters
                else { current.Parameters with Values = frame }
            let handle = if emptyPrefix then defaultArg current.EmptyPrefixHandle current.Handle else current.Handle
            struct (handle, parameters, current.Hydration, state)
        struct (expression, bind)

    let compileExpression<'Source, 'Args, 'Elem>
        (source: ISoloDBCollection<'Source>) (body: Expression) (parameters: ParameterExpression array)
        (argument: ParameterExpression) (arguments: Expression array) =
        let struct (_, bind) = prepare<'Source, 'Args, unit> source body parameters argument arguments (fun _ -> None)
        fun args ->
            let struct (handle, parameters, hydration, _) = bind args
            QueryableExecution.enumerate<'Source, 'Elem> source (NativeStatementSource.Retained handle) parameters hydration

    let scalarExpression<'Source, 'Args, 'Result>
        (source: ISoloDBCollection<'Source>) (body: Expression) (parameters: ParameterExpression array)
        (argument: ParameterExpression) (arguments: Expression array) =
        let struct (expression, bind) =
            prepare<'Source, 'Args, 'Result> source body parameters argument arguments QueryableExecution.terminalDefaultExpression
        let methodName =
            match expression with
            | :? MethodCallExpression as call -> call.Method.Name
            | _ -> "Execute"
        fun (args: 'Args) ->
            let struct (handle, parameters, hydration, fallback) = bind args
            QueryableExecution.scalar<'Source, 'Result> source (NativeStatementSource.Retained handle) parameters hydration methodName (fun () -> fallback)

    let compile<'Source, 'Args, 'Elem>
        (source: ISoloDBCollection<'Source>) (query: LambdaExpression)
        (argument: ParameterExpression) (arguments: Expression array) =
        if isNull query then nullArg "query"
        compileExpression<'Source, 'Args, 'Elem> source query.Body (Seq.toArray query.Parameters) argument arguments

    let compileResult<'Source, 'Args, 'Result>
        (source: ISoloDBCollection<'Source>) (query: LambdaExpression)
        (argument: ParameterExpression) (arguments: Expression array) =
        if isNull query then nullArg "query"
        let body = NormalizeQueryLambdas().Visit query.Body
        let parameters = Seq.toArray query.Parameters
        let rec rooted (expression: Expression) =
            match expression with
            | :? ParameterExpression as parameter -> obj.ReferenceEquals(parameter, query.Parameters.[0])
            | :? UnaryExpression as unary -> rooted unary.Operand
            | :? MethodCallExpression as call when call.Arguments.Count > 0
                                                        && typeof<IQueryable>.IsAssignableFrom(call.Arguments.[0].Type) -> rooted call.Arguments.[0]
            | _ -> false
        match body with
        | :? MethodCallExpression as call when typeof<Queryable>.Equals(call.Method.DeclaringType) && rooted body ->
            scalarExpression<'Source, 'Args, 'Result> source body parameters argument arguments
        | _ ->
        let mutable retained = false
        let replacements = arguments |> Array.mapi (fun i value -> query.Parameters.[i + 1], value)
        let body = Substitute(replacements).Visit body
        let variables = Dictionary<ParameterExpression, int>()
        variables.Add(argument, 0)
        let replace (expression: Expression) =
            let sequence = typeof<IQueryable>.IsAssignableFrom expression.Type
            let terminal =
                match expression with
                | :? MethodCallExpression as call -> typeof<Queryable>.Equals(call.Method.DeclaringType)
                | _ -> false
            if (sequence || terminal) && rooted expression then
                retained <- true
                let resultType =
                    if sequence then (TypeMetadata.GenericTypeArgCache.Get expression.Type).[0]
                    else expression.Type
                let factory = CompiledQueryMetadata<'Source, 'Args>.Operation(sequence, resultType)
                let run = factory.Invoke(source, expression, parameters, argument, arguments)
                Some (fun (values: obj array) -> run.Invoke(unbox<'Args> (QueryStorage.value values.[0])))
            else None
        let run = QueryValuePlan.bind variables replace body
        if not retained then invalid "the query must be rooted in the supplied source parameter."
        fun args -> unbox<'Result> (run [|box args|])

    // Ordinary discovery closes these existing executors once and caches the delegate.
    let createSequenceDispatcher<'Source, 'Args, 'Element>() =
        Func<ISoloDBCollection<'Source>,Expression,ParameterExpression array,ParameterExpression,Expression array,Func<'Args,obj>>(fun source body parameters argument arguments ->
            let run = compileExpression<'Source, 'Args, 'Element> source body parameters argument arguments
            Func<'Args,obj>(fun values -> box(run values)))

    let createResultDispatcher<'Source, 'Args, 'Result>() =
        Func<ISoloDBCollection<'Source>,Expression,ParameterExpression array,ParameterExpression,Expression array,Func<'Args,obj>>(fun source body parameters argument arguments ->
            let run = scalarExpression<'Source, 'Args, 'Result> source body parameters argument arguments
            Func<'Args,obj>(fun values -> box(run values)))
