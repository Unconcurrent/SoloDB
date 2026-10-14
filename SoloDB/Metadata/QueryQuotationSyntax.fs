namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq.Expressions
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns

/// Lowers supplied quotation data into the existing query representation.
module internal QueryQuotationSyntax =
    let private numeric typ =
        typeof<sbyte>.Equals(typ) || typeof<byte>.Equals(typ) || typeof<int16>.Equals(typ) || typeof<uint16>.Equals(typ)
        || typeof<int32>.Equals(typ) || typeof<uint32>.Equals(typ) || typeof<int64>.Equals(typ) || typeof<uint64>.Equals(typ)
        || typeof<single>.Equals(typ) || typeof<double>.Equals(typ) || typeof<decimal>.Equals(typ)

    let private operator (method: System.Reflection.MethodInfo) (arguments: Expression array) =
        let owner = method.DeclaringType.FullName
        let basic = owner = "Microsoft.FSharp.Core.Operators"
        let checkedOwner = owner = "Microsoft.FSharp.Core.Operators+Checked"
        if not (basic || checkedOwner) then None else
        let binary kind = Some(Expression.MakeBinary(kind, arguments.[0], arguments.[1]) :> Expression)
        if arguments.Length = 1 && numeric arguments.[0].Type && numeric method.ReturnType &&
           (match method.Name with
            | "ToSByte" | "ToByte" | "ToInt16" | "ToUInt16" | "ToInt" | "ToInt32" | "ToUInt32"
            | "ToInt64" | "ToUInt64" | "ToSingle" | "ToDouble" | "ToDecimal" -> true
            | _ -> false) then
            Some(Expression.MakeUnary((if checkedOwner then ExpressionType.ConvertChecked else ExpressionType.Convert),arguments.[0],method.ReturnType) :> Expression)
        elif arguments.Length = 1 && numeric arguments.[0].Type && arguments.[0].Type.Equals(method.ReturnType)
             && method.Name = "op_UnaryNegation" then
            Some(Expression.MakeUnary((if checkedOwner then ExpressionType.NegateChecked else ExpressionType.Negate),arguments.[0],method.ReturnType) :> Expression)
        elif arguments.Length = 2 && arguments.[0].Type.Equals(arguments.[1].Type) &&
           (method.Name = "op_Equality" || method.Name = "op_Inequality") then
            let kind = if method.Name = "op_Equality" then ExpressionType.Equal else ExpressionType.NotEqual
            if numeric arguments.[0].Type then binary kind
            else Some(Expression.MakeBinary(kind,arguments.[0],arguments.[1],false,method) :> Expression)
        elif arguments.Length = 2 && numeric arguments.[0].Type && arguments.[0].Type.Equals(arguments.[1].Type) then
            match method.Name with
            | "op_Equality" -> binary ExpressionType.Equal
            | "op_Inequality" -> binary ExpressionType.NotEqual
            | "op_LessThan" -> binary ExpressionType.LessThan
            | "op_LessThanOrEqual" -> binary ExpressionType.LessThanOrEqual
            | "op_GreaterThan" -> binary ExpressionType.GreaterThan
            | "op_GreaterThanOrEqual" -> binary ExpressionType.GreaterThanOrEqual
            | "op_Addition" -> binary (if checkedOwner then ExpressionType.AddChecked else ExpressionType.Add)
            | "op_Subtraction" -> binary (if checkedOwner then ExpressionType.SubtractChecked else ExpressionType.Subtract)
            | "op_Multiply" -> binary (if checkedOwner then ExpressionType.MultiplyChecked else ExpressionType.Multiply)
            | "op_Division" -> binary ExpressionType.Divide
            | "op_Modulus" -> binary ExpressionType.Modulo
            | _ -> None
        elif arguments.Length = 1 && typeof<bool>.Equals(arguments.[0].Type) && method.Name = "Not" then
            Some(Expression.Not(arguments.[0]) :> Expression)
        else None

    let private delegateType (typ: Type) =
        if typ.IsGenericType && typedefof<Expression<_>>.Equals(typ.GetGenericTypeDefinition()) then
            Some(typ.GetGenericArguments().[0], true)
        elif typeof<Delegate>.IsAssignableFrom typ then Some(typ, false)
        else None

    let rec private lower (variables: Dictionary<Var, Expression>) (expected: Type option) quotation : Expression =
        let child quotation = lower variables None quotation
        let owner parent = parent |> Option.map child |> Option.defaultValue null
        let receiver parent (declaring: Type) =
            let value=owner parent
            if isNull value || declaring.IsAssignableFrom(value.Type) then value
            else Expression.Convert(value,declaring) :> Expression
        match quotation with
        | Var variable ->
            match variables.TryGetValue variable with
            | true, value -> value
            | _ -> raise(ArgumentException("A query quotation contains an unbound variable.", "quotation"))
        | WithValue(value, typ, _) | Value(value, typ) | ValueWithName(value, typ, _) -> Expression.Constant(value, typ) :> Expression
        | Lambda _ | NewDelegate _ ->
            match expected |> Option.bind delegateType with
            | Some(typ, quoted) ->
                let expression = lambda typ variables quotation
                if quoted then Expression.Quote expression :> Expression else expression :> Expression
            | None -> raise(NotSupportedException("A query quotation lambda requires its supplied delegate context."))
        | Call(_, method, [argument]) when method.DeclaringType.FullName = "Microsoft.FSharp.Linq.RuntimeHelpers.LeafExpressionConverter"
                                           && method.Name = "QuotationToLambdaExpression" ->
            let nested, replacements = quoted variables argument
            let context = Dictionary<Var, Expression>(variables, HashIdentity.Reference)
            for variable, value in replacements do context.[variable] <- value
            let typ = method.ReturnType.GetGenericArguments().[0]
            Expression.Quote(lambda typ context nested) :> Expression
        | Call(parent, method, arguments) ->
            let parameters = method.GetParameters()
            let arguments = arguments |> List.mapi(fun index argument -> lower variables (Some parameters.[index].ParameterType) argument) |> List.toArray
            match operator method arguments with
            | Some expression -> expression
            | None -> Expression.Call(receiver parent method.DeclaringType, method, arguments) :> Expression
        | PropertyGet(parent, property, arguments) ->
            if arguments.IsEmpty then Expression.Property(receiver parent property.DeclaringType, property) :> Expression
            else Expression.MakeIndex(receiver parent property.DeclaringType, property, arguments |> List.map child) :> Expression
        | PropertySet(parent, property, arguments, value) ->
            let destination =
                if arguments.IsEmpty then Expression.Property(receiver parent property.DeclaringType,property) :> Expression
                else Expression.MakeIndex(receiver parent property.DeclaringType,property,arguments |> List.map child) :> Expression
            Expression.Block(Expression.Assign(destination,child value),Expression.Default(typeof<unit>)) :> Expression
        | FieldGet(parent, field) -> Expression.Field(receiver parent field.DeclaringType, field) :> Expression
        | FieldSet(parent, field, value) ->
            Expression.Block(Expression.Assign(Expression.Field(receiver parent field.DeclaringType,field),child value),Expression.Default(typeof<unit>)) :> Expression
        | VarSet(variable,value) ->
            match variables.TryGetValue variable with
            | true, destination -> Expression.Block(Expression.Assign(destination,child value),Expression.Default(typeof<unit>)) :> Expression
            | false, _ -> raise(ArgumentException("A query quotation contains an unbound assignment variable.","quotation"))
        | NewObject(constructor, arguments) -> Expression.New(constructor, arguments |> List.map child) :> Expression
        | NewRecord(typ, arguments) ->
            let values = arguments |> List.map child |> List.toArray
            let fields =
                match TypeMetadata.recordFields typ with
                | ValueSome fields -> fields
                | ValueNone -> raise(NotSupportedException($"Query record construction for '{typ.FullName}' has no registered fields."))
            let create = QueryValueMetadata.Constructor(typ,values |> Array.map _.Type |> Array.toList)
            BoundValueCallExpression(values,typ,create,fields) :> Expression
        | NewTuple arguments | NewStructTuple arguments ->
            let rec tuple (typ: Type) (values: Expression array) =
                let types = typ.GetGenericArguments()
                let arguments =
                    if types.Length = 8 then
                        Array.append values.[0..6] [|tuple types.[7] values.[7..]|]
                    else values
                let create = QueryValueMetadata.Constructor(typ,Array.toList types)
                BoundValueCallExpression(arguments,typ,create) :> Expression
            tuple quotation.Type (arguments |> List.map child |> List.toArray)
        | TupleGet(value, ordinal) ->
            let rec item (value: Expression) ordinal =
                let name = if ordinal >= 7 then "Rest" else "Item" + string(ordinal + 1)
                match ModelMetadata.NamedMember(value.Type,name) with
                | Some(struct(typ,read)) ->
                    let access = BoundValueAccessExpression(value,typ,read,name) :> Expression
                    if ordinal >= 7 then item access (ordinal-7) else access
                | None -> raise(NotSupportedException($"Query tuple member '{value.Type.FullName}.{name}' has no direct operation."))
            item (child value) ordinal
        | NewArray(element, values) -> Expression.NewArrayInit(element, values |> List.map child) :> Expression
        | Coerce(value, typ) -> Expression.Convert(child value, typ) :> Expression
        | TypeTest(value, typ) -> Expression.TypeIs(child value, typ) :> Expression
        | DefaultValue typ -> Expression.Default typ :> Expression
        | IfThenElse(condition, yes, no) -> Expression.Condition(child condition, child yes, child no) :> Expression
        | Application(Lambda(variable,body),value) -> lower variables expected (Expr.Let(variable,value,body))
        | Application(functionValue,value) ->
            let functionExpression, argument = child functionValue, child value
            let call = QueryValueMetadata.Function(functionExpression.Type,argument.Type,quotation.Type)
            BoundValueCallExpression([|functionExpression;argument|],quotation.Type,
                Func<obj[],obj>(fun values -> call.Invoke(values.[0],[|values.[1]|]))) :> Expression
        | NewUnionCase(case,arguments) ->
            let struct(create,_) = QueryValueMetadata.UnionCase(case.DeclaringType,case.Tag)
            BoundValueCallExpression(arguments |> List.map child |> List.toArray,quotation.Type,create) :> Expression
        | UnionCaseTest(value,case) ->
            let struct(_,test) = QueryValueMetadata.UnionCase(case.DeclaringType,case.Tag)
            BoundValueAccessExpression(child value,typeof<bool>,Func<obj,obj>(fun value -> box(test.Invoke(value))),"Is" + case.Name) :> Expression
        | Let(variable,(Lambda _ as value),body) ->
            lower variables expected (body.Substitute(fun candidate -> if candidate = variable then Some value else None))
        | Sequential(first,second) -> Expression.Block(child first,lower variables expected second) :> Expression
        | Let(variable, value, body) ->
            // F# captures record arguments in source order before passing them in
            // field order. Keep that order in the existing named construction node.
            let rec capture bindings quotation =
                match quotation with
                | Let(variable,value,body) when not variable.IsMutable -> capture ((variable,value)::bindings) body
                | NewRecord(typ,arguments) -> Some(typ,List.rev bindings,arguments)
                | _ -> None
            let captured =
                match capture [] quotation with
                | Some(typ,bindings,arguments) ->
                    let variables = bindings |> List.map fst |> List.toArray
                    let contains variable = variables |> Array.exists(fun current -> obj.ReferenceEquals(current,variable))
                    let independent = bindings |> List.forall(fun (_,value) -> value.GetFreeVars() |> Seq.forall(contains >> not))
                    let direct = arguments |> List.forall(function
                        | Var variable when contains variable -> true
                        | argument -> argument.GetFreeVars() |> Seq.forall(contains >> not))
                    let once = variables |> Array.forall(fun variable ->
                        arguments |> List.filter(function Var current -> obj.ReferenceEquals(current,variable) | _ -> false) |> List.length = 1)
                    if independent && direct && once then Some(typ,bindings,arguments) else None
                | None -> None
            match captured with
            | Some(typ,bindings,arguments) ->
                let fields =
                    match TypeMetadata.recordFields typ with
                    | ValueSome fields -> fields
                    | ValueNone -> raise(NotSupportedException($"Query record construction for '{typ.FullName}' has no registered fields."))
                let inputs = ResizeArray<Expression>()
                let names = ResizeArray<string>()
                let positions = Array.zeroCreate arguments.Length
                for variable,value in bindings do
                    let index = arguments |> List.findIndex(function Var current -> obj.ReferenceEquals(current,variable) | _ -> false)
                    positions.[index] <- inputs.Count
                    inputs.Add(child value)
                    names.Add(fields.[index])
                for index,argument in arguments |> List.indexed do
                    match argument with
                    | Var variable when bindings |> List.exists(fun (current,_) -> obj.ReferenceEquals(current,variable)) -> ()
                    | _ ->
                        positions.[index] <- inputs.Count
                        inputs.Add(child argument)
                        names.Add(fields.[index])
                let create = QueryValueMetadata.Constructor(typ,arguments |> List.map _.Type)
                BoundValueCallExpression(inputs.ToArray(),typ,
                    Func<obj[],obj>(fun values -> create.Invoke(positions |> Array.map(fun index -> values.[index]))),names.ToArray()) :> Expression
            | None ->
                let local = Expression.Variable(variable.Type, variable.Name)
                let next = Dictionary<Var, Expression>(variables, HashIdentity.Reference)
                next.Add(variable, local)
                Expression.Block([|local|], [|Expression.Assign(local, child value) :> Expression; lower next expected body|]) :> Expression
        | AddressOf location -> child location
        | AddressSet(location,value) -> Expression.Block(Expression.Assign(child location,child value),Expression.Default(typeof<unit>)) :> Expression
        | WhileLoop _ | ForIntegerRangeLoop _ -> raise(NotSupportedException("Query quotation loops must run in a supplied client function, outside the SQL expression."))
        | LetRecursive _ -> raise(NotSupportedException("Recursive query quotation bindings must run in a supplied client function."))
        | TryFinally _ | TryWith _ -> raise(NotSupportedException("Query quotation exception handling must run in a supplied client function."))
        | QuoteTyped _ | QuoteRaw _ -> raise(NotSupportedException("Nested quotation data must be consumed by its query lambda conversion."))
        | _ -> raise(NotSupportedException("The supplied query quotation contains an unrecognized node."))

    and private quoted variables quotation =
        match quotation with
        | QuoteTyped nested | QuoteRaw nested -> nested, [||]
        | Value((:? Expr as nested), _) -> nested, [||]
        | Call(_, method, [argument; NewArray(_, bindings); NewArray(_, values)])
            when method.DeclaringType.FullName = "Microsoft.FSharp.Linq.RuntimeHelpers.LeafExpressionConverter" && method.Name = "SubstHelper" ->
            let nested, previous = quoted variables argument
            let bindings = bindings |> List.map(function
                | Value((:? Var as variable), _) -> variable
                | _ -> raise(NotSupportedException("A query quotation substitution requires supplied variable identities.")))
            let replacements = List.map2(fun (variable: Var) value ->
                let expression = lower variables None value
                let expression = if expression.Type.Equals(variable.Type) then expression else Expression.Convert(expression, variable.Type) :> Expression
                variable, expression) bindings values |> List.toArray
            nested, Array.append previous replacements
        | _ -> raise(NotSupportedException("A query quotation conversion requires supplied quotation data."))

    and private lambda delegateType (variables: Dictionary<Var, Expression>) quotation =
        let struct (types, create) = QueryValueMetadata.ExpressionLambda delegateType
        let locals = Dictionary<Var, Expression>(variables, HashIdentity.Reference)
        let parameters = ResizeArray<ParameterExpression>()
        let mutable body = quotation
        let add (variable: Var) typ =
            if not (variable.Type.Equals(typ)) then
                raise(ArgumentException("A query quotation does not match its supplied delegate parameter type.", "quotation"))
            let parameter = Expression.Parameter(typ, variable.Name)
            locals.Add(variable, parameter)
            parameters.Add parameter
        match quotation with
        | NewDelegate(typ, variables, rest) when (if isNull typ then isNull delegateType else typ.Equals(delegateType)) && variables.Length = types.Length ->
            List.iter2 add variables (Array.toList types)
            body <- rest
        | _ ->
          for typ in types do
            match body with
            | Lambda(variable, rest) ->
                add variable typ
                body <- rest
            | _ -> raise(ArgumentException("A query quotation does not match its supplied delegate parameters.", "quotation"))
        create.Invoke(lower locals None body, parameters.ToArray())

    let fromQuotation delegateType (replacements: (Var * Expression) array) quotation =
        if isNull (box quotation) then nullArg "query"
        let variables = Dictionary<Var, Expression>(HashIdentity.Reference)
        for variable, expression in replacements do variables.Add(variable, expression)
        lambda delegateType variables quotation

    let expression<'Delegate> quotation =
        fromQuotation typeof<'Delegate> [||] quotation :?> Expression<'Delegate>
