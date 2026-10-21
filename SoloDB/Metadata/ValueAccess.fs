namespace SoloDatabase

open System
open System.Linq.Expressions
open System.Reflection
open System.Reflection.Emit
open System.Collections.Generic
open SoloDatabase.Metadata.CodecGeneration

/// Upper bound shared by literal and invocation-bound ordinal prefixes.
type internal QueryPrefixBounds =
    static member Next(value: obj) =
        // Keep null strings null, as in ordinary prefix translation.
        let prefix = match value with :? char as c -> string c | _ -> value :?> string
        let chars = prefix.ToCharArray()
        let rec bump i =
            if i < 0 then prefix + "\u0000"
            elif chars.[i] < Char.MaxValue then
                chars.[i] <- char (int chars.[i] + 1)
                System.String(chars, 0, i + 1)
            else bump (i - 1)
        bump (chars.Length - 1)
    static member val Reader = Func<obj,obj>(fun value -> box(QueryPrefixBounds.Next(value)))

/// A translator value leaf carrying an already-bound typed member operation.
[<Sealed>]
type internal BoundValueAccessExpression(parent: Expression, valueType: Type, read: Func<obj, obj>, memberName: string) =
    inherit Expression()
    new(parent, valueType, read) = BoundValueAccessExpression(parent,valueType,read,null)
    member _.MemberName = memberName
    member _.Parent = parent
    member _.Read = read
    override _.NodeType = ExpressionType.Extension
    override _.Type = valueType
    override _.CanReduce = true
    override _.Reduce() =
        Expression.Convert(Expression.Invoke(Expression.Constant(read), Expression.Convert(parent, typeof<obj>)), valueType) :> Expression
    override this.VisitChildren(visitor) =
        let next = visitor.Visit parent
        if obj.ReferenceEquals(next, parent) then this :> Expression
        else BoundValueAccessExpression(next, valueType, read,memberName) :> Expression

/// An internal operation with explicit ordered inputs and a precompiled body.
[<Sealed>]
type internal BoundValueCallExpression(inputs: Expression array, valueType: Type, apply: Func<obj array,obj>, projectionNames: string array) =
    inherit Expression()
    new(inputs, valueType, apply) = BoundValueCallExpression(inputs,valueType,apply,null)
    member _.ProjectionNames = projectionNames
    member _.Inputs = inputs
    member _.Apply = apply
    override _.NodeType = ExpressionType.Extension
    override _.Type = valueType
    override _.CanReduce = isNull projectionNames
    override _.Reduce() =
        let arguments = Expression.NewArrayInit(typeof<obj>, inputs |> Array.map(fun input -> Expression.Convert(input,typeof<obj>) :> Expression))
        Expression.Convert(Expression.Invoke(Expression.Constant(apply),arguments),valueType) :> Expression
    override this.VisitChildren(visitor) =
        let next = inputs |> Array.map visitor.Visit
        if Array.forall2 (fun previous current -> obj.ReferenceEquals(previous,current)) inputs next then this :> Expression
        else BoundValueCallExpression(next,valueType,apply,projectionNames) :> Expression

/// Builds an ordered value plan from supplied expression facts and direct leaves.
module internal QueryValuePlan =
    [<System.Diagnostics.CodeAnalysis.DynamicDependency("op_Equality(System.String,System.String)", typeof<string>);
      System.Diagnostics.CodeAnalysis.DynamicDependency("op_Inequality(System.String,System.String)", typeof<string>)>]
    let rec bindScoped (variables: IDictionary<ParameterExpression, int>) (replace: IDictionary<ParameterExpression,int> -> Expression -> (obj array -> obj) option) expression =
        let rec build (expression: Expression) : obj array -> obj =
            if isNull expression then fun _ -> null else
            match replace variables expression with
            | Some run -> run
            | None ->
            match expression with
            | :? ConstantExpression as constant -> fun _ -> constant.Value
            | :? ParameterExpression as parameter ->
                match variables.TryGetValue parameter with
                | true, slot -> fun values -> QueryStorage.value values.[slot]
                | _ -> raise (ArgumentException("A query value contains an unbound parameter.", "expression"))
            | :? BlockExpression as block ->
                let slots = Dictionary<ParameterExpression,int>(variables)
                let start = slots.Count
                for variable in block.Variables do slots.Add(variable,slots.Count)
                let defaults = block.Variables |> Seq.map(fun variable -> QueryValueMetadata.Default variable.Type) |> Seq.toArray
                let statements = block.Expressions |> Seq.map(bindScoped slots replace) |> Seq.toArray
                fun values ->
                    let locals = Array.zeroCreate slots.Count
                    Array.Copy(values,locals,start)
                    for index = 0 to defaults.Length-1 do
                        locals.[start+index] <- System.Runtime.CompilerServices.RuntimeHelpers.GetObjectValue(defaults.[index])
                    let mutable result = null
                    try
                        for statement in statements do result <- statement locals
                        result
                    finally Array.Copy(locals,values,start)
            | :? BinaryExpression as assignment when assignment.NodeType = ExpressionType.Assign && (assignment.Left :? ParameterExpression) ->
                let variable = assignment.Left :?> ParameterExpression
                let slot =
                    match variables.TryGetValue variable with
                    | true, slot -> slot
                    | false, _ -> raise(ArgumentException("A query value contains an unbound assignment variable.","expression"))
                let value = build assignment.Right
                fun values ->
                    let result = value values
                    match values.[slot] with
                    | :? QueryVariable as storage -> storage.Value <- result
                    | _ -> values.[slot] <- if variable.Type.IsValueType then System.Runtime.CompilerServices.RuntimeHelpers.GetObjectValue result else result
                    result
            | :? BinaryExpression as assignment when assignment.NodeType = ExpressionType.Assign ->
                let value = build assignment.Right
                match assignment.Left with
                | :? MemberExpression as memberValue ->
                    let parent = receiver memberValue.Expression
                    let write = QueryValueMetadata.Setter memberValue.Member
                    fun values ->
                        let struct(owner,save) = parent values
                        let result = value values
                        try write.Invoke(owner,result)
                        finally save()
                        result
                | :? IndexExpression as index ->
                    let parent = build index.Object
                    let indexes = index.Arguments |> Seq.map build |> Seq.toArray
                    if isNull index.Indexer then
                        fun values ->
                            let array = parent values :?> Array
                            let ordinals = indexes |> Array.map(fun ordinal -> unbox<int>(ordinal values))
                            let result = value values
                            array.SetValue(result,ordinals)
                            result
                    else
                        let owner = receiver index.Object
                        let write = QueryValueMetadata.Call(index.Indexer.GetSetMethod(true))
                        fun values ->
                            let struct(target,save) = owner values
                            let arguments = indexes |> Array.map(fun argument -> argument values)
                            let result = value values
                            try write.Invoke(target,Array.append arguments [|result|]) |> ignore
                            finally save()
                            result
                | :? BinaryExpression as index when index.NodeType = ExpressionType.ArrayIndex ->
                    let parent, ordinal = build index.Left, build index.Right
                    fun values ->
                        let array = parent values :?> Array
                        let position = unbox<int>(ordinal values)
                        let result = value values
                        array.SetValue(result,position)
                        result
                | _ -> raise(NotSupportedException("The supplied query assignment has no writable destination."))
            | :? BoundValueAccessExpression as access ->
                let parent = build access.Parent
                fun values -> access.Read.Invoke(parent values)
            | :? BoundValueCallExpression as call ->
                let inputs = call.Inputs |> Array.map build
                fun values -> call.Apply.Invoke(inputs |> Array.map(fun input -> input values))
            | :? MemberExpression as memberExpression ->
                let parent = receiver memberExpression.Expression
                let receiverType = if isNull memberExpression.Expression then memberExpression.Member.DeclaringType else memberExpression.Expression.Type
                let read = QueryValueMetadata.Reader(memberExpression.Member,receiverType)
                fun values ->
                    let struct(owner,save) = parent values
                    try read.Invoke owner
                    finally save()
            | :? IndexExpression as index ->
                let parent = build index.Object
                let indexes = index.Arguments |> Seq.map build |> Seq.toArray
                if isNull index.Indexer then
                    fun values -> (parent values :?> Array).GetValue(indexes |> Array.map(fun ordinal -> unbox<int>(ordinal values)))
                else
                    let owner = receiver index.Object
                    let read = QueryValueMetadata.Call(index.Indexer.GetGetMethod(true))
                    fun values ->
                        let struct(target,save) = owner values
                        let arguments = indexes |> Array.map(fun argument -> argument values)
                        try read.Invoke(target,arguments)
                        finally save()
            | :? BinaryExpression as index when index.NodeType = ExpressionType.ArrayIndex ->
                let parent, ordinal = build index.Left, build index.Right
                fun values -> (parent values :?> Array).GetValue(unbox<int>(ordinal values))
            | :? UnaryExpression as unary when unary.NodeType = ExpressionType.Quote ->
                // A nested quoted predicate carries the current outer values,
                // while its own parameters remain bound by the quoted lambda.
                fun values ->
                    let shadowed = HashSet<ParameterExpression>()
                    let visitor =
                        { new ExpressionVisitor() with
                            override _.VisitParameter(parameter) =
                                match variables.TryGetValue parameter with
                                | true, slot when not(shadowed.Contains parameter) ->
                                    Expression.Constant(QueryStorage.value values.[slot],parameter.Type) :> Expression
                                | _ -> parameter :> Expression
                            override this.VisitLambda<'Delegate>(lambda: Expression<'Delegate>) =
                                let added = lambda.Parameters |> Seq.filter shadowed.Add |> Seq.toArray
                                try base.VisitLambda lambda
                                finally for parameter in added do shadowed.Remove parameter |> ignore
                            override this.VisitBlock(block) =
                                let added = block.Variables |> Seq.filter shadowed.Add |> Seq.toArray
                                try base.VisitBlock block
                                finally for parameter in added do shadowed.Remove parameter |> ignore }
                    visitor.Visit unary.Operand
            | :? UnaryExpression as unary when not(isNull unary.Method) &&
                not (typeof<decimal>.Equals(unary.Method.DeclaringType)) ->
                let input = build unary.Operand
                let call = QueryValueMetadata.Call unary.Method
                fun values ->
                    let value = input values
                    if unary.IsLifted && isNull value then null else call.Invoke(null,[|value|])
            | :? UnaryExpression as unary ->
                let input = build unary.Operand
                if unary.NodeType = ExpressionType.Convert &&
                   (typeof<obj>.Equals(unary.Type) || unary.Type.Equals(unary.Operand.Type) ||
                    (not unary.Operand.Type.IsValueType && unary.Type.IsAssignableFrom(unary.Operand.Type))) then input
                elif (unary.NodeType = ExpressionType.Convert || unary.NodeType = ExpressionType.TypeAs) &&
                     not unary.Type.IsValueType && not unary.Operand.Type.IsValueType then
                    fun values ->
                        let value = input values
                        if isNull value || unary.Type.IsInstanceOfType(value) then value
                        elif unary.NodeType = ExpressionType.TypeAs then null
                        else raise(InvalidCastException())
                else
                    let apply = QueryValueMetadata.Unary(unary.NodeType, unary.Operand.Type, unary.Type)
                    fun values -> apply.Invoke(input values)
            | :? BinaryExpression as binary when binary.NodeType = ExpressionType.AndAlso ->
                let left, right = build binary.Left, build binary.Right
                fun values -> box (unbox<bool> (left values) && unbox<bool> (right values))
            | :? BinaryExpression as binary when binary.NodeType = ExpressionType.OrElse ->
                let left, right = build binary.Left, build binary.Right
                fun values -> box (unbox<bool> (left values) || unbox<bool> (right values))
            | :? BinaryExpression as binary when binary.NodeType = ExpressionType.Coalesce && isNull binary.Conversion ->
                let left, right = build binary.Left, build binary.Right
                fun values -> let value = left values in if isNull value then right values else value
            | :? BinaryExpression as binary when isNull binary.Method &&
                (binary.NodeType = ExpressionType.Equal || binary.NodeType = ExpressionType.NotEqual) &&
                not binary.Left.Type.IsValueType && not binary.Right.Type.IsValueType ->
                let left, right = build binary.Left, build binary.Right
                fun values ->
                    let equal = obj.ReferenceEquals(left values,right values)
                    box (if binary.NodeType = ExpressionType.Equal then equal else not equal)
            | :? BinaryExpression as binary when not(isNull binary.Method) &&
                not (typeof<string>.Equals(binary.Method.DeclaringType)) && not (typeof<decimal>.Equals(binary.Method.DeclaringType)) ->
                let left, right = build binary.Left, build binary.Right
                let call = QueryValueMetadata.Call binary.Method
                fun values ->
                    let first = left values
                    let second = right values
                    if binary.IsLifted && (isNull first || isNull second) then
                        if binary.IsLiftedToNull then null
                        elif binary.NodeType = ExpressionType.Equal then box(isNull first && isNull second)
                        elif binary.NodeType = ExpressionType.NotEqual then box(not(isNull first && isNull second))
                        else box false
                    else call.Invoke(null,[|first;second|])
            | :? BinaryExpression as binary ->
                let left, right = build binary.Left, build binary.Right
                let apply =
                    match QueryScalarOperators.binaryMethod binary.Method with
                    | ValueSome operation -> operation
                    | ValueNone -> QueryValueMetadata.Binary(binary.NodeType, binary.Left.Type, binary.Right.Type, binary.Type)
                fun values ->
                    let l = left values
                    let r = right values
                    apply.Invoke(l, r)
            | :? TypeBinaryExpression as test ->
                let value = build test.Expression
                if test.NodeType = ExpressionType.TypeIs then
                    fun values -> box(test.TypeOperand.IsInstanceOfType(value values))
                else
                    fun values ->
                        let value = value values
                        box(not(isNull value) && value.GetType().Equals(test.TypeOperand))
            | :? ConditionalExpression as conditional ->
                let test, yes, no = build conditional.Test, build conditional.IfTrue, build conditional.IfFalse
                fun values -> if unbox<bool> (test values) then yes values else no values
            | :? LambdaExpression as lambda ->
                let slots = Dictionary<ParameterExpression, int>(variables)
                let start = slots.Count
                for i = 0 to lambda.Parameters.Count - 1 do slots.Add(lambda.Parameters.[i], start + i)
                let body = bindScoped slots replace lambda.Body
                let create = QueryValueMetadata.Lambda lambda.Type
                fun values ->
                    create.Invoke(start, slots.Count, Func<obj array, obj>(fun locals ->
                        Array.Copy(values, locals, start)
                        body locals))
            | :? DefaultExpression ->
                let value = QueryValueMetadata.Default expression.Type
                fun _ -> System.Runtime.CompilerServices.RuntimeHelpers.GetObjectValue value
            | :? NewExpression as created ->
                if isNull created.Constructor then
                    let value = QueryValueMetadata.Default created.Type
                    fun _ -> System.Runtime.CompilerServices.RuntimeHelpers.GetObjectValue value
                else
                    let create = QueryValueMetadata.Constructor(created.Type, created.Constructor.GetParameters() |> Array.map _.ParameterType |> Array.toList)
                    let inputs = arguments (created.Constructor.GetParameters()) created.Arguments
                    fun values ->
                        let struct(inputs,save) = inputs values
                        let result = create.Invoke(inputs)
                        save()
                        result
            | :? MemberInitExpression as initialized ->
                let create = build initialized.NewExpression
                let initialize = initialized.Bindings |> Seq.map binding |> Seq.toArray
                fun values ->
                    let result = create values
                    for initialize in initialize do initialize values result
                    result
            | :? ListInitExpression as initialized ->
                let create = build initialized.NewExpression
                let add = initialized.Initializers |> Seq.map initializer |> Seq.toArray
                fun values ->
                    let result = create values
                    for add in add do add values result
                    result
            | :? NewArrayExpression as array when array.NodeType = ExpressionType.NewArrayInit ->
                let inputs = array.Expressions |> Seq.map build |> Seq.toArray
                let create = QueryValueMetadata.Array array.Type
                fun values -> create.Invoke(inputs |> Array.map (fun input -> input values))
            | :? InvocationExpression as invocation ->
                let target = build invocation.Expression
                let method = invocation.Expression.Type.GetMethod("Invoke")
                let inputs = arguments (method.GetParameters()) invocation.Arguments
                let invoke = QueryValueMetadata.Call(method)
                fun values ->
                    let owner = target values
                    let struct(arguments,save) = inputs values
                    let result = invoke.Invoke(owner,arguments)
                    save()
                    result
            | :? MethodCallExpression as call when not(isNull call.Method.DeclaringType) && call.Method.DeclaringType.FullName = "System.MemoryExtensions" &&
                call.Method.Name = "Contains" && call.Arguments.Count = 2 && (QuerySpanSource.tryWholeArray call.Arguments.[0]).IsSome ->
                let source = (QuerySpanSource.tryWholeArray call.Arguments.[0]).Value
                let array, value = build source, build call.Arguments.[1]
                let contains = QueryValueMetadata.ArrayContains source.Type
                fun values ->
                    let items = array values
                    let expected = value values
                    box(contains.Invoke(items,expected))
            | :? MethodCallExpression as call ->
                let parent =
                    if not(isNull call.Object) && call.Object.Type.IsValueType then
                        let owner = address call.Object
                        fun values -> let struct(owner,_) = owner values in struct(box owner,fun () -> ())
                    else receiver call.Object
                let inputs = arguments (call.Method.GetParameters()) call.Arguments
                let apply = QueryValueMetadata.Call(call.Method)
                fun values ->
                    let struct(owner,saveOwner) = parent values
                    let struct(arguments,saveArguments) = inputs values
                    try
                        let result = apply.Invoke(owner, arguments)
                        saveArguments()
                        result
                    finally saveOwner()
            | _ -> raise (NotSupportedException(sprintf "Query value %O has no direct operation." expression.NodeType))
        and arguments (parameters: ParameterInfo[]) (expressions: IEnumerable<Expression>) =
            let noSave() = ()
            if parameters |> Array.forall(fun parameter -> not parameter.ParameterType.IsByRef) then
                let inputs = expressions |> Seq.map build |> Seq.toArray
                fun values -> struct((if inputs.Length = 0 then Array.empty else inputs |> Array.map(fun input -> input values)),noSave)
            else
            let inputs = expressions |> Seq.mapi(fun index input ->
                if parameters.[index].ParameterType.IsByRef then
                    let location = address input
                    fun values -> let struct(reference,save) = location values in struct(box reference,save)
                else
                    let value = build input
                    fun values -> struct(value values,fun () -> ())) |> Seq.toArray
            fun values ->
                let evaluated = inputs |> Array.map(fun input -> input values)
                struct(evaluated |> Array.map(fun struct(value,_) -> value),fun () ->
                    for struct(_,save) in evaluated do save())
        and address (expression: Expression) : obj[] -> struct(QueryReference * (unit -> unit)) =
            let noSave() = ()
            let temporary() =
                let read = build expression
                let storage = QueryValueMetadata.Storage expression.Type
                fun values ->
                    let variable = storage.Variable.Invoke(read values)
                    struct(storage.VariableAddress.Invoke(variable,[||]),noSave)
            let arrayAddress source ordinals =
                let parent = build source
                let indexes = ordinals |> Seq.map build |> Seq.toArray
                let locate = QueryValueMetadata.ArrayAddress source.Type
                fun values ->
                    let owner = parent values
                    let positions = indexes |> Array.map(fun input -> unbox<int>(input values))
                    struct(locate.Invoke(owner,positions),noSave)
            match expression with
            | :? ParameterExpression as parameter ->
                let slot = variables.[parameter]
                let storage = QueryValueMetadata.Storage parameter.Type
                fun values ->
                    let variable =
                        match values.[slot] with
                        | :? QueryVariable as variable -> variable
                        | value ->
                            let variable = storage.Variable.Invoke(value)
                            values.[slot] <- variable
                            variable
                    struct(storage.VariableAddress.Invoke(variable,[||]),noSave)
            | :? BoundValueAccessExpression as access when not(isNull access.MemberName) && access.Parent.Type.IsValueType ->
                let parent = address access.Parent
                let locate = QueryValueMetadata.FieldAddress(access.Parent.Type,access.MemberName,access.Type)
                fun values -> let struct(owner,_) = parent values in struct(locate.Invoke(owner,[||]),noSave)
            | :? MemberExpression as memberValue ->
                match memberValue.Member with
                | :? FieldInfo as field when not(field.IsInitOnly || field.IsLiteral) ->
                    let locate = QueryValueMetadata.FieldAddress field
                    let parent =
                        if not(isNull memberValue.Expression) && memberValue.Expression.Type.IsValueType then
                            let parent = address memberValue.Expression
                            fun values -> let struct(parent,_) = parent values in box parent
                        else build memberValue.Expression
                    fun values -> struct(locate.Invoke(parent values,[||]),noSave)
                | :? PropertyInfo as property when not(isNull(property.GetSetMethod(true))) ->
                    let parent,read,write =
                        if not(isNull memberValue.Expression) && memberValue.Expression.Type.IsValueType then
                            let owner = address memberValue.Expression
                            let get = QueryValueMetadata.Call(property.GetGetMethod(true))
                            let set = QueryValueMetadata.Call(property.GetSetMethod(true))
                            (fun values -> let struct(owner,_) = owner values in struct(box owner,noSave)),
                            Func<obj,obj>(fun owner -> get.Invoke(owner,[||])),
                            Action<obj,obj>(fun owner value -> set.Invoke(owner,[|value|]) |> ignore)
                        else receiver memberValue.Expression, QueryValueMetadata.Reader(property,property.DeclaringType), QueryValueMetadata.Setter property
                    let storage = QueryValueMetadata.Storage property.PropertyType
                    fun values ->
                        let struct(owner,saveOwner) = parent values
                        let variable = storage.Variable.Invoke(read.Invoke(owner))
                        struct(storage.VariableAddress.Invoke(variable,[||]),fun () ->
                            try write.Invoke(owner,variable.Value)
                            finally saveOwner())
                | _ -> temporary()
            | :? BinaryExpression as index when index.NodeType = ExpressionType.ArrayIndex ->
                arrayAddress index.Left [index.Right]
            | :? IndexExpression as index when isNull index.Indexer -> arrayAddress index.Object index.Arguments
            | :? MethodCallExpression as call when call.Method.Name = "Get" && not(isNull call.Object) && call.Object.Type.IsArray ->
                arrayAddress call.Object call.Arguments
            | :? IndexExpression as index when not(isNull(index.Indexer.GetSetMethod(true))) ->
                let parent =
                    if index.Object.Type.IsValueType then
                        let owner = address index.Object
                        fun values -> let struct(owner,_) = owner values in struct(box owner,noSave)
                    else receiver index.Object
                let indexes = index.Arguments |> Seq.map build |> Seq.toArray
                let read = QueryValueMetadata.Call(index.Indexer.GetGetMethod(true))
                let write = QueryValueMetadata.Call(index.Indexer.GetSetMethod(true))
                let storage = QueryValueMetadata.Storage index.Type
                fun values ->
                    let struct(owner,saveOwner) = parent values
                    let positions = indexes |> Array.map(fun input -> input values)
                    let variable = storage.Variable.Invoke(read.Invoke(owner,positions))
                    struct(storage.VariableAddress.Invoke(variable,[||]),fun () ->
                        try write.Invoke(owner,Array.append positions [|variable.Value|]) |> ignore
                        finally saveOwner())
            | :? UnaryExpression as unbox when unbox.NodeType = ExpressionType.Unbox ->
                let value = build unbox.Operand
                let locate = QueryValueMetadata.BoxAddress unbox.Type
                fun values -> struct(locate.Invoke(value values,[||]),noSave)
            | _ -> temporary()
        and initializer (initialization: ElementInit) =
            let inputs = initialization.Arguments |> Seq.map build |> Seq.toArray
            let add = QueryValueMetadata.Call initialization.AddMethod
            fun values owner -> add.Invoke(owner,inputs |> Array.map(fun input -> input values)) |> ignore
        and binding (initialization: MemberBinding) =
            match initialization with
            | :? MemberAssignment as assignment ->
                let value = build assignment.Expression
                let write = QueryValueMetadata.Setter assignment.Member
                fun values owner -> write.Invoke(owner,value values)
            | :? MemberMemberBinding as members ->
                let read = QueryValueMetadata.Reader(members.Member,members.Member.DeclaringType)
                let nested = members.Bindings |> Seq.map binding |> Seq.toArray
                let valueType =
                    match members.Member with
                    | :? FieldInfo as field -> field.FieldType
                    | :? PropertyInfo as property -> property.PropertyType
                    | _ -> invalidArg "initialization" "A member initializer requires a field or property."
                let write = if valueType.IsValueType then QueryValueMetadata.Setter members.Member else null
                fun values owner ->
                    let value = read.Invoke owner
                    try for initialize in nested do initialize values value
                    finally if not(isNull write) then write.Invoke(owner,value)
            | :? MemberListBinding as collection ->
                let read = QueryValueMetadata.Reader(collection.Member,collection.Member.DeclaringType)
                let add = collection.Initializers |> Seq.map initializer |> Seq.toArray
                fun values owner ->
                    let value = read.Invoke owner
                    for add in add do add values value
            | _ -> invalidArg "initialization" "The supplied member initializer has an unsupported binding."
        and receiver (expression: Expression) : obj array -> struct(obj * (unit -> unit)) =
            let noSave() = ()
            if isNull expression || not expression.Type.IsValueType then
                let value = build expression
                fun values -> struct(value values,noSave)
            else
                match expression with
                | :? ParameterExpression as parameter ->
                    let slot = variables.[parameter]
                    fun values ->
                        match values.[slot] with
                        | :? QueryVariable as variable ->
                            let value = variable.Value
                            struct(value,fun () -> variable.Value <- value)
                        | value -> struct(value,noSave)
                | :? MemberExpression as memberValue when
                    (match memberValue.Member with :? FieldInfo as field -> not(field.IsInitOnly || field.IsLiteral) | _ -> false) ->
                    let parent = receiver memberValue.Expression
                    let read = QueryValueMetadata.Reader(memberValue.Member,memberValue.Member.DeclaringType)
                    let write = QueryValueMetadata.Setter memberValue.Member
                    fun values ->
                        let struct(owner,saveParent) = parent values
                        let value = read.Invoke owner
                        struct(value,fun () ->
                            try write.Invoke(owner,value)
                            finally saveParent())
                | :? BinaryExpression as index when index.NodeType = ExpressionType.ArrayIndex ->
                    let parent, ordinal = build index.Left, build index.Right
                    fun values ->
                        let array = parent values :?> Array
                        let position = unbox<int>(ordinal values)
                        let value = array.GetValue position
                        struct(value,fun () -> array.SetValue(value,position))
                | :? IndexExpression as index when isNull index.Indexer ->
                    let parent = build index.Object
                    let indexes = index.Arguments |> Seq.map build |> Seq.toArray
                    fun values ->
                        let array = parent values :?> Array
                        let positions = indexes |> Array.map(fun ordinal -> unbox<int>(ordinal values))
                        let value = array.GetValue positions
                        struct(value,fun () -> array.SetValue(value,positions))
                | _ ->
                    let value = build expression
                    fun values -> struct(System.Runtime.CompilerServices.RuntimeHelpers.GetObjectValue(value values),noSave)
        build expression

    let bind variables replace expression =
        bindScoped variables (fun _ value -> replace value) expression

/// Cold JIT discovery creates a real member function; invocation never uses GetValue.
module internal DynamicMemberAccess =
    let reader (memberInfo: MemberInfo): Func<obj, obj> =
        let declaring = memberInfo.DeclaringType
        let valueType, accessor, field =
            match memberInfo with
            | :? PropertyInfo as property -> property.PropertyType, property.GetGetMethod(true), null
            | :? FieldInfo as field -> field.FieldType, null, field
            | _ -> invalidArg "memberInfo" "A serialized member must be a property or field."
        if not (isNull field) && field.IsLiteral then
            let value = field.GetRawConstantValue()
            Func<obj, obj>(fun _ -> value)
        else
        let body = DynamicMethod("ReadStoredMember", typeof<obj>, [|typeof<obj>|], declaring.Module, true)
        let isStatic = if isNull field then accessor.IsStatic else field.IsStatic
        let memberValue =
            { Name = memberInfo.Name; Type = valueType; Field = Option.ofObj field
              Getter = Option.ofObj accessor |> Option.map (fun value -> value :> MethodBase)
              Setter = None; Static = isStatic; Readable = true; Writable = false
              Ignored = false; Many = false }
        CodecEmitter.boxedMemberGetter (RuntimeCodecIL(body.GetILGenerator())) declaring declaring.IsValueType memberValue valueType.IsValueType
        body.CreateDelegate(typeof<Func<obj, obj>>) :?> Func<obj, obj>
