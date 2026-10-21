namespace SoloDatabase

open System
open System.Linq.Expressions
open System.Numerics
open System.Reflection
open SoloDatabase.Metadata.Generation
open SoloDatabase.Numerics
open SoloDatabase.SqlModel
open QueryTranslatorNumeric

/// Original closed method signatures supply mathematical and comparison operand roles.
module internal QueryTranslatorNumericMethods =
    type MathCall = NotApplicable | Unsupported | Translated of SqlExpr

    let private originalFSharpOwner name (owner: Type) =
        not(isNull owner) && owner.FullName=name && owner.Assembly.Equals(typeof<unit>.Assembly)

    type Comparison = private {
        Operator: BinaryOperator
        LeftTarget: Type voption
        RightTarget: Type voption
    }

    let relationalOperator = function
        | ExpressionType.Equal -> ValueSome BinaryOperator.Eq
        | ExpressionType.NotEqual -> ValueSome BinaryOperator.Ne
        | ExpressionType.LessThan -> ValueSome BinaryOperator.Lt
        | ExpressionType.LessThanOrEqual -> ValueSome BinaryOperator.Le
        | ExpressionType.GreaterThan -> ValueSome BinaryOperator.Gt
        | ExpressionType.GreaterThanOrEqual -> ValueSome BinaryOperator.Ge
        | _ -> ValueNone

    let comparisonOperation = function
        | "op_Equality" -> ExpressionType.Equal
        | "op_Inequality" -> ExpressionType.NotEqual
        | "op_LessThan" -> ExpressionType.LessThan
        | "op_LessThanOrEqual" -> ExpressionType.LessThanOrEqual
        | "op_GreaterThan" -> ExpressionType.GreaterThan
        | "op_GreaterThanOrEqual" -> ExpressionType.GreaterThanOrEqual
        | _ -> ExpressionType.Extension

    let originalComparison operation (method: MethodInfo) (left: Type) (right: Type) =
        if isNull method then left.Equals(right)
        else
            let parameters = method.GetParameters()
            let owner = method.DeclaringType
            method.IsStatic && not method.ContainsGenericParameters && typeof<bool>.Equals(method.ReturnType)
            && comparisonOperation method.Name = operation && parameters.Length = 2
            && parameters[0].ParameterType.Equals(left) && parameters[1].ParameterType.Equals(right)
            && (typeof<BigInteger>.Equals(owner) || typeof<decimal>.Equals(owner)
                || originalFSharpOwner "Microsoft.FSharp.Core.Operators" owner
                || originalFSharpOwner "Microsoft.FSharp.Core.Operators+Checked" owner)


    let supportsRelationalMethod operation (method: MethodInfo) left right =
        if isNull method || originalComparison operation method left right then true else
        let left,right = underlying left,underlying right
        let owner = method.DeclaringType
        let parameters = method.GetParameters()
        method.IsStatic && not method.ContainsGenericParameters && typeof<bool>.Equals(method.ReturnType) &&
        comparisonOperation method.Name = operation && parameters.Length = 2 &&
        parameters[0].ParameterType.Equals(left) && parameters[1].ParameterType.Equals(right) &&
        left.Equals(right) && owner.Equals(left) &&
        ((nativeCastType owner).IsSome || (TypeSupport.RawNumericCore owner.FullName).IsSome || Utils.isDateTimeLikeType owner)

    type MinMaxCall = OtherMethod | NativeMinMax | DefaultMinMax of Type | UnsupportedMinMax

    let minMaxCall (method: MethodInfo) argumentCount =
        let comparison =
            match method.Name with
            | "Min" | "Max" | "min" | "max" | "Min$W" | "Max$W" | "min$W" | "max$W" -> true
            | _ -> false
        if not comparison then OtherMethod else
        let owner = method.DeclaringType
        let parameters = method.GetParameters()
        let signature = method.IsStatic && not method.ContainsGenericParameters && argumentCount = 2 && parameters.Length = 2 &&
                        parameters[0].ParameterType.Equals(parameters[1].ParameterType) && method.ReturnType.Equals(parameters[0].ParameterType)
        if signature && originalFSharpOwner "Microsoft.FSharp.Core.Operators" owner && (method.Name = "Min" || method.Name = "Max") then
            DefaultMinMax parameters[0].ParameterType
        elif signature && (typeof<Math>.Equals(owner) || typeof<decimal>.Equals(owner) || typeof<BigInteger>.Equals(owner) ||
                           typeof<DecimalMath>.Equals(owner) || (owner.FullName = "System.MathF" && owner.Assembly.Equals(typeof<Math>.Assembly))) then NativeMinMax
        else UnsupportedMinMax

    /// Closed operator identity establishes operand promotion independently of its Boolean result.
    let comparison operation (method: MethodInfo) leftType rightType =
        match relationalOperator operation with
        | ValueNone -> ValueNone
        | ValueSome sqlOperator ->
            let left, right = underlying leftType, underlying rightType
            let original = originalComparison operation method left right
            if not original then ValueNone
            else
                match domain left, domain right with
                | ValueNone, ValueNone -> ValueNone
                | ValueSome _, ValueSome _ ->
                    ValueSome { Operator = sqlOperator; LeftTarget = ValueNone; RightTarget = ValueNone }
                | ValueSome _, ValueNone when nativeCastType right = ValueSome "INTEGER" && not(typeof<bool>.Equals(right)) ->
                    ValueSome { Operator = sqlOperator; LeftTarget = ValueNone; RightTarget = ValueSome left }
                | ValueNone, ValueSome _ when nativeCastType left = ValueSome "INTEGER" && not(typeof<bool>.Equals(left)) ->
                    ValueSome { Operator = sqlOperator; LeftTarget = ValueSome right; RightTarget = ValueNone }
                | _ -> ValueNone

    let comparableOperands comparison leftType rightType left right =
        let promote target original value =
            match target with
            | ValueSome target -> conversion false target original value
            | ValueNone -> value
        struct(promote comparison.LeftTarget leftType left, promote comparison.RightTarget rightType right)

    let tryComparisonCall (method: MethodInfo) argumentCount (originalArgument: int -> Expression) resolveArgument =
        if argumentCount <> 2 then ValueNone
        else
            let leftType, rightType = (originalArgument 0).Type, (originalArgument 1).Type
            match comparison (comparisonOperation method.Name) method leftType rightType with
            | ValueNone -> ValueNone
            | ValueSome comparison ->
                let left = resolveArgument 0
                let right = resolveArgument 1
                let struct(left, right) = comparableOperands comparison leftType rightType left right
                ValueSome(SqlExpr.Binary(left, comparison.Operator, right))

    let private witnessLambda (expression: Expression) =
        let rec collect parameters (expression: Expression) =
            match expression with
            | :? UnaryExpression as unary when unary.NodeType=ExpressionType.Quote -> collect parameters unary.Operand
            | :? MethodCallExpression as call when call.Method.IsStatic &&
                 originalFSharpOwner "Microsoft.FSharp.Core.FuncConvert" call.Method.DeclaringType &&
                 (call.Method.Name="ToFSharpFunc" || call.Method.Name="FromFunc") && call.Arguments.Count=1 ->
                collect parameters call.Arguments[0]
            | :? LambdaExpression as lambda -> collect (parameters @ List.ofSeq lambda.Parameters) lambda.Body
            | _ -> parameters,expression
        collect [] expression

    let private originalWitness target name count (expression: Expression) =
        let parameters,body=witnessLambda expression
        if parameters.Length<>count || parameters |> List.exists(fun parameter -> not(parameter.Type.Equals(target))) then false
        else
            match body with
            | :? MethodCallExpression as call when call.Method.IsStatic && call.Method.Name=name &&
                 (target.Equals(call.Method.DeclaringType) || typeof<Math>.Equals(call.Method.DeclaringType)) &&
                 call.Arguments.Count=count ->
                List.forall2 (fun parameter argument -> Object.ReferenceEquals(parameter,argument)) parameters (List.ofSeq call.Arguments)
            | :? MemberExpression as memberValue when name="Sign" && count=1 && typeof<BigInteger>.Equals(target) &&
                 memberValue.Member.Name="Sign" && target.Equals(memberValue.Member.DeclaringType) ->
                Object.ReferenceEquals(parameters.Head,memberValue.Expression)
            | _ -> false

    let private originalOne target (expression: Expression) =
        let parameters,body=witnessLambda expression
        if parameters.Length<>1 || not(typeof<unit>.Equals(parameters.Head.Type)) then false
        else
            match body with
            | :? MethodCallExpression as call when call.Method.IsStatic && call.Method.Name="GenericOne" &&
                 originalFSharpOwner "Microsoft.FSharp.Core.LanguagePrimitives" call.Method.DeclaringType &&
                 call.Method.ReturnType.Equals(target) && call.Arguments.Count=0 -> true
            | :? ConstantExpression as constant ->
                (typeof<decimal>.Equals(target) && (constant.Value :? decimal) && unbox<decimal> constant.Value=1M) ||
                (typeof<BigInteger>.Equals(target) && (constant.Value :? BigInteger) && (unbox<BigInteger> constant.Value).IsOne)
            | _ -> false

    let private tryFSharpMathCall (method: MethodInfo) argumentCount (originalArgument: int -> Expression) (resolveArgument: int -> SqlExpr) =
        let owner=method.DeclaringType
        if not method.IsStatic || not(originalFSharpOwner "Microsoft.FSharp.Core.Operators" owner) then NotApplicable
        else
            let name,offset =
                match method.Name with
                | "Abs" | "Abs$W" -> "ABS",(if method.Name="Abs" then 0 else 1)
                | "Floor" | "Floor$W" -> "FLOOR",(if method.Name="Floor" then 0 else 1)
                | "Ceiling" | "Ceiling$W" -> "CEIL",(if method.Name="Ceiling" then 0 else 1)
                | "Truncate" | "Truncate$W" -> "TRUNC",(if method.Name="Truncate" then 0 else 1)
                | "Round" | "Round$W" -> "ROUND",(if method.Name="Round" then 0 else 1)
                | "Sign" | "Sign$W" -> "SIGN",(if method.Name="Sign" then 0 else 1)
                | "Min" -> "MIN",0 | "Max" -> "MAX",0
                | "PowInteger" -> "POW_INTEGER",0 | "PowInteger$W" -> "POW_INTEGER",3
                | _ -> "",0
            let parameters=method.GetParameters()
            let count=if name="MIN" || name="MAX" || name="POW_INTEGER" then 2 else 1
            if name="" || parameters.Length<>argumentCount || argumentCount<>offset+count ||
               (name="POW_INTEGER" && not(typeof<int>.Equals(parameters[offset+1].ParameterType))) then NotApplicable
            else
                match domain parameters[offset].ParameterType with
                | ValueSome kind when (name="SIGN" && typeof<int>.Equals(method.ReturnType)) || domain method.ReturnType=ValueSome kind ->
                    let target=parameters[offset].ParameterType
                    let original=
                        if offset=0 then true
                        elif offset=3 then originalOne target (originalArgument 0) &&
                                           originalWitness target "op_Division" 2 (originalArgument 1) &&
                                           originalWitness target "op_Multiply" 2 (originalArgument 2)
                        else originalWitness target (method.Name.Substring(0,method.Name.Length-2)) 1 (originalArgument 0)
                    if not original then Unsupported
                    else
                        let arguments=[for index in offset .. argumentCount-1 -> resolveArgument index]
                        if name="ROUND" then
                            Translated(call kind name (arguments @ [SqlExpr.Literal(SqlLiteral.Integer 0L);SqlExpr.Literal(SqlLiteral.Integer 0L)]))
                        else Translated(call kind name arguments)
                | _ -> NotApplicable

    let private tryNativeMathCall (method: MethodInfo) argumentCount (resolveArgument: int -> SqlExpr) =
        let owner = method.DeclaringType
        if not method.IsStatic ||
           not(typeof<Math>.Equals(owner) || typeof<Decimal>.Equals(owner) || typeof<DecimalMath>.Equals(owner) || typeof<BigInteger>.Equals(owner)) then ValueNone
        else
            let parameters = method.GetParameters()
            if parameters.Length = 0 || parameters.Length <> argumentCount then ValueNone
            else
                match domain parameters[0].ParameterType with
                | ValueNone -> ValueNone
                | ValueSome kind ->
                    let name =
                        match method.Name with
                        | "Add" -> "ADD" | "Subtract" -> "SUB" | "Multiply" -> "MUL"
                        | "Divide" -> "DIV" | "Remainder" -> "MOD" | "Negate" -> "NEG"
                        | "Abs" -> "ABS" | "Sign" -> "SIGN" | "Min" -> "MIN" | "Max" -> "MAX"
                        | "Floor" -> "FLOOR" | "Ceiling" -> "CEIL" | "Truncate" -> "TRUNC"
                        | "Sqrt" -> "SQRT" | "Exp" -> "EXP" | "Log" when parameters.Length = 1 -> "LN"
                        | "Log" -> "LOG" | "Log10" -> "LOG10" | "Log2" -> "LOG2" | "Pow" -> "POW"
                        | "Sin" -> "SIN" | "Cos" -> "COS" | "Tan" -> "TAN"
                        | "Asin" -> "ASIN" | "Acos" -> "ACOS" | "Atan" -> "ATAN" | "Atan2" -> "ATAN2"
                        | "Sinh" -> "SINH" | "Cosh" -> "COSH" | "Tanh" -> "TANH"
                        | "Asinh" -> "ASINH" | "Acosh" -> "ACOSH" | "Atanh" -> "ATANH"
                        | "IEEERemainder" -> "IEEE_REM"
                        | "Degrees" -> "DEGREES" | "Radians" -> "RADIANS" | "Round" -> "ROUND"
                        | _ -> null
                    if isNull name then ValueNone
                    elif kind = NumericKind.ArbitraryInteger && typeof<double>.Equals(method.ReturnType) &&
                         (name = "LN" || name = "LOG" || name = "LOG10") &&
                         (parameters.Length = 1 || parameters.Length = 2 && typeof<double>.Equals(parameters[1].ParameterType)) then
                        ValueSome(call kind (name + "_BINARY") [for index in 0 .. argumentCount-1 -> resolveArgument index])
                    elif (name = "SIGN" && not(typeof<int>.Equals(method.ReturnType))) ||
                         (name <> "SIGN" && domain method.ReturnType <> ValueSome kind) then ValueNone
                    elif name = "ROUND" then
                        let integer value = SqlExpr.Literal(SqlLiteral.Integer value)
                        let roles =
                            match parameters.Length with
                            | 1 -> ValueSome(0,0)
                            | 2 when typeof<int>.Equals(parameters[1].ParameterType) -> ValueSome(1,0)
                            | 2 when typeof<MidpointRounding>.Equals(parameters[1].ParameterType) -> ValueSome(0,1)
                            | 3 when typeof<int>.Equals(parameters[1].ParameterType) && typeof<MidpointRounding>.Equals(parameters[2].ParameterType) -> ValueSome(1,2)
                            | _ -> ValueNone
                        match roles with
                        | ValueNone -> ValueNone
                        | ValueSome(digits,mode) ->
                            let value = resolveArgument 0
                            let digits = if digits = 0 then integer 0L else resolveArgument digits
                            let mode = if mode = 0 then integer 0L else resolveArgument mode
                            ValueSome(call kind name [value;digits;mode])
                    elif name = "POW" && kind = NumericKind.ArbitraryInteger && parameters.Length = 2 && typeof<int>.Equals(parameters[1].ParameterType) then
                        ValueSome(call kind name [for index in 0 .. argumentCount-1 -> resolveArgument index])
                    elif parameters |> Array.forall (fun parameter -> parameter.ParameterType.Equals(parameters[0].ParameterType)) then
                        ValueSome(call kind name [for index in 0 .. argumentCount-1 -> resolveArgument index])
                    else ValueNone

    let tryMathCall (method: MethodInfo) argumentCount originalArgument resolveArgument =
        match tryFSharpMathCall method argumentCount originalArgument resolveArgument with
        | NotApplicable ->
            match tryNativeMathCall method argumentCount resolveArgument with
            | ValueSome result -> Translated result
            | ValueNone when method.GetParameters() |> Array.exists(fun parameter -> (domain parameter.ParameterType).IsSome) -> Unsupported
            | ValueNone -> NotApplicable
        | result -> result
