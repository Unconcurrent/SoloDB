namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorSerialization
open SoloDatabase.QueryStructuralShape

/// Standard structural relations retain authored component order and operand roles.
module internal QueryStructuralRelations =
    type private Operand = Bound of Expression | Stored of QueryProducedValue

    let private target = function Leaf t | Optional(t,_) | Components(t,_) -> t
    let private boolean value = SqlExpr.Literal(SqlLiteral.Boolean value)
    let private sqlNull value = SqlExpr.Unary(UnaryOperator.IsNull,value)
    let private binary op left right = SqlExpr.Binary(left,op,right)
    let private choose condition yes no = SqlExpr.CaseExpr((condition,yes),[],Some no)

    let tryTranslate visitRaw (expression: BinaryExpression) (qb: QueryBuilder) =
        let method = expression.Method
        let relation =
            match expression.NodeType with
            | ExpressionType.LessThan -> ValueSome BinaryOperator.Lt
            | ExpressionType.LessThanOrEqual -> ValueSome BinaryOperator.Le
            | ExpressionType.GreaterThan -> ValueSome BinaryOperator.Gt
            | ExpressionType.GreaterThanOrEqual -> ValueSome BinaryOperator.Ge
            | _ -> ValueNone
        let original =
            relation.IsSome && expression.Left.Type.Equals expression.Right.Type && not(isNull method) &&
            method.DeclaringType.Assembly.Equals(typeof<unit>.Assembly) &&
            method.DeclaringType.FullName = "Microsoft.FSharp.Core.Operators" &&
            QueryTranslatorNumericMethods.originalComparison expression.NodeType method expression.Left.Type expression.Right.Type
        if not original then ValueNone else
        match qb.SourceContext.BindQueryValue,QueryStructuralShape.tryCreate expression.Left.Type with
        | ValueSome binder,ValueSome graph ->
            let shape = graph.Root
            let nextName = qb.SourceContext.NextAlias
            let relation = relation.Value
            let equalResult = boolean(relation = BinaryOperator.Le || relation = BinaryOperator.Ge)
            let nullObject = Expression.Constant(null,typeof<obj>)
            let boundMissing (value: Expression) =
                if value.Type.IsValueType && isNull(TypeMetadata.nullableUnderlying value.Type) then Expression.Constant(false) :> Expression
                else Expression.ReferenceEqual(Expression.Convert(value,typeof<obj>),nullObject) :> Expression
            let missing = function
                | Bound value -> binder.Scalar(boundMissing value)
                | Stored produced ->
                    QueryProduced.constructRaw nextName (fun parts ->
                        let value,input = parts[0]
                        QueryProduced.typed typeof<bool> (sqlNull(nullCarrier input.Carrier value))) [|produced|]
                    |> QueryProduced.value nextName
            let child name childShape = function
                | Bound parent ->
                    let childType = target childShape
                    let read = QueryStructuralShape.memberReader graph parent.Type name childType
                    let access = BoundValueAccessExpression(parent,childType,read,name) :> Expression
                    let guarded = Expression.Condition(boundMissing parent,Expression.Default childType,access)
                    Bound(binder.Local guarded)
                | Stored parent ->
                    let childType = target childShape
                    let finish = function
                        | DirectValue(raw,input) when typeof<float>.Equals childType || typeof<single>.Equals childType ->
                            match input.Source with
                            | ValueSome token ->
                                let fallback = QueryDefault.comparisonValue qb.Variables childType |> QueryProduced.value nextName
                                let value = choose (sqlNull token) fallback (QueryProduced.comparisonValue childType raw input)
                                DirectValue(value,{ input with Carrier = SqlScalar; Source = ValueSome(jsonToken value) })
                            | ValueNone -> DirectValue(raw,input)
                        | selected -> selected
                    Stored(QueryTranslatorVisitCoreMemberAccess.accessValueFinishing (QueryDefault.value qb.Variables) nextName finish (ClrMember(name,childType)) childType parent)
            let payload childShape = function
                | Bound _ as parent -> child "Value" childShape parent
                | Stored parent ->
                    Stored(QueryProduced.bind nextName (fun value input -> DirectValue(value,serializationPayload input)) parent)
            let scalar (t: Type) operand =
                let t,operand =
                    if not t.IsEnum then t,operand else
                    let underlying = TypeMetadata.enumUnderlying t
                    let operand =
                        match operand with
                        | Bound value -> Bound(BoundValueAccessExpression(value,underlying,Func<obj,obj>(fun value ->
                            (QueryStructuralShape.enumValue t value |> snd))))
                        | Stored _ -> operand
                    underlying,operand
                match operand with
                | Bound value when typeof<string>.Equals t ->
                    binder.Scalar(BoundValueAccessExpression(value,typeof<byte[]>,Func<obj,obj>(fun value ->
                        box(QueryTranslatorCharacters.ordinalBytes(unbox<string> value)))))
                | Bound value when typeof<char>.Equals t ->
                    binder.Scalar(BoundValueAccessExpression(value,typeof<int>,Func<obj,obj>(fun value -> box(int(unbox<char> value)))))
                | Bound value when typeof<float>.Equals t || typeof<single>.Equals t ->
                    // A visited nonnullable NaN is unordered, not a native REAL argument.
                    let safe = BoundValueAccessExpression(value,typeof<obj>,Func<obj,obj>(fun value ->
                        if typeof<float>.Equals t && Double.IsNaN(unbox<float> value) ||
                           typeof<single>.Equals t && Single.IsNaN(unbox<single> value) then null else value))
                    binder.Scalar safe
                | Bound value -> binder.Produced true value |> QueryProduced.consume nextName (QueryProduced.comparisonValue t)
                | Stored produced ->
                    QueryProduced.consume nextName (fun raw input ->
                        let value = QueryProduced.comparisonValue t raw input
                        if t.IsValueType && not(typeof<float>.Equals t || typeof<single>.Equals t) then
                            let defaultValue = QueryDefault.comparisonValue qb.Variables t |> QueryProduced.value nextName
                            SqlExpr.Coalesce(value,[defaultValue])
                        else value) produced
            let prepareScalar t operand value =
                if typeof<string>.Equals t then
                    match operand with Bound _ -> value | Stored _ -> QueryTranslatorCharacters.ordinalKey value
                elif DateTimeFunctions.isDateTimeLikeType t then DateTimeFunctions.canonicalizeForCompareOrOrder t value
                else value
            let preparedScalar t operand = prepareScalar t operand (scalar t operand)
            let necessaryRange (t: Type) reversed raw bound =
                if not t.IsValueType || typeof<float>.Equals t || typeof<single>.Equals t then ValueNone else
                let direction =
                    if (relation = BinaryOperator.Lt || relation = BinaryOperator.Le) <> reversed then BinaryOperator.Le
                    else BinaryOperator.Ge
                let raw = match raw with SqlExpr.Coalesce(value,_) -> value | value -> value
                let canonical value = if DateTimeFunctions.isDateTimeLikeType t then DateTimeFunctions.canonicalizeForCompareOrOrder t value else value
                ValueSome(binary BinaryOperator.Or (sqlNull raw) (binary direction (canonical raw) (canonical bound)))
            let rec render shape left right onEqual =
                let absent present =
                    let leftMissing,rightMissing = missing left,missing right
                    let less = boolean(relation = BinaryOperator.Lt || relation = BinaryOperator.Le)
                    let greater = boolean(relation = BinaryOperator.Gt || relation = BinaryOperator.Ge)
                    choose leftMissing (choose rightMissing onEqual less) (choose rightMissing greater present)
                match shape with
                | Optional(_,childShape) ->
                    let result,_ = render childShape (payload childShape left) (payload childShape right) onEqual
                    absent result,ValueNone
                | Components(t,fields) ->
                    // Acquire children in declaration order before composing lazy SQL branches.
                    let children = fields |> Array.map(fun (name,shape) -> shape,child name shape left,child name shape right)
                    let result,candidate = Array.foldBack(fun (shape,left,right) (rest,_) -> render shape left right rest) children (onEqual,ValueNone)
                    if t.IsValueType then result,candidate else absent result,ValueNone
                | Leaf t ->
                    let leftValue,rightValue = scalar t left,scalar t right
                    let candidate =
                        match left,right with
                        | Stored _,Bound _ -> necessaryRange t false leftValue rightValue
                        | Bound _,Stored _ -> necessaryRange t true rightValue leftValue
                        | _ -> ValueNone
                    let leftValue,rightValue = prepareScalar t left leftValue,prepareScalar t right rightValue
                    let equal = binary BinaryOperator.Eq leftValue rightValue
                    let ordered = binary relation leftValue rightValue
                    let result = if onEqual = equalResult then ordered else choose equal onEqual ordered
                    if typeof<float>.Equals t || typeof<single>.Equals t then
                        choose (binary BinaryOperator.Or (sqlNull leftValue) (sqlNull rightValue)) (boolean false) result,ValueNone
                    elif not t.IsValueType then absent result,ValueNone
                    else result,candidate
            let operands = ResizeArray<QueryProducedValue>()
            let acquire (value: Expression) =
                if binder.IsValue value || isFullyConstant value then Choice1Of2(Bound(binder.Local value))
                else
                    let index = operands.Count
                    operands.Add(visitRaw value qb)
                    Choice2Of2 index
            let left,right = acquire expression.Left,acquire expression.Right
            let result = QueryProduced.constructRaw nextName (fun parts ->
                if graph.Recursive then
                    let root = function
                        | Choice1Of2(Bound value) ->
                            let prepared = BoundValueAccessExpression(value,typeof<string>,Func<obj,obj>(fun value -> box(QueryStructuralShape.prepare graph value)))
                            QueryStructuralTraversal.Prepared(binder.Scalar prepared)
                        | Choice2Of2 index -> QueryStructuralTraversal.Selected parts[index]
                        | _ -> invalidOp "A recursive bound root requires its original value."
                    let operations: QueryStructuralTraversal.Operations = {
                        Child = fun name node produced ->
                            match child name node (Stored produced) with
                            | Stored produced -> produced
                            | _ -> invalidOp "A stored component requires its produced value."
                        Payload = fun node produced ->
                            match payload node (Stored produced) with
                            | Stored produced -> produced
                            | _ -> invalidOp "A stored payload requires its produced value."
                        Scalar = fun target produced -> preparedScalar target (Stored produced)
                        RawScalar = fun target produced -> scalar target (Stored produced)
                        NecessaryRange = necessaryRange }
                    QueryProduced.typed typeof<bool> (QueryStructuralTraversal.translate nextName relation operations graph (root left) (root right))
                else
                    let operand = function Choice1Of2 value -> value | Choice2Of2 index -> Stored(DirectValue parts[index])
                    let result,candidate = render shape (operand left) (operand right) equalResult
                    let result = match candidate with ValueSome candidate -> binary BinaryOperator.And candidate result | ValueNone -> result
                    QueryProduced.typed typeof<bool> result) (operands.ToArray())
            ValueSome(QueryProduced.value nextName result)
        | _ -> ValueNone
