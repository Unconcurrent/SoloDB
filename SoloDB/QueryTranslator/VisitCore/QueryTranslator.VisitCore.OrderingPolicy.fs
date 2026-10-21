namespace SoloDatabase

open System
open SoloDatabase.SqlModel
open SoloDatabase.QueryStructuralShape

/// Comparer policy belongs to an occurrence, independently of shared shape layout.
module internal QueryOrderingPolicy =
    type Role = Default | ObjectDefault | FSharp
    let children role (node: Node) =
        if role = FSharp || TypeMetadata.standardComparison node.Target || Utils.isOptionType node.Target then FSharp
        elif Utils.isTuple node.Target && not node.Target.IsValueType then ObjectDefault
        elif Utils.isTuple node.Target then Default
        else role
    let private text value = SqlExpr.Literal(SqlLiteral.String value)
    let private call name value = SqlExpr.FunctionCall(name,[value])
    let scalar role target value =
        if typeof<string>.Equals target then
            if role = FSharp then QueryTranslatorCharacters.ordinalKey value else QueryOrderingPrimitives.cultureString (role=ObjectDefault) value
        elif DateTimeFunctions.isDateTimeLikeType target then DateTimeFunctions.canonicalizeForCompareOrOrder target value
        else value
    let private normalized expression =
        SqlExpr.map (function
            | SqlExpr.Column(alias,name) -> SqlExpr.Column(alias |> Option.map AliasRewrite.normalizeSourceAlias,name)
            | SqlExpr.JsonRootExtract(alias,name) ->
                SqlExpr.FunctionCall("jsonb_extract",[SqlExpr.Column(alias |> Option.map AliasRewrite.normalizeSourceAlias,name);text "$"])
            | SqlExpr.JsonExtractExpr(alias,name,path) ->
                SqlExpr.FunctionCall("jsonb_extract",[SqlExpr.Column(alias |> Option.map AliasRewrite.normalizeSourceAlias,name);text(JsonPathOps.text(JsonPathOps.toList path))])
            | value -> value) expression
    let nonnull predicate expression =
        let expression = normalized expression
        let same value = normalized value = expression
        let rec mandatory = function
            | SqlExpr.Unary(UnaryOperator.IsNotNull,value) -> same value
            | SqlExpr.Binary(left,BinaryOperator.And,right) -> mandatory left || mandatory right
            | SqlExpr.Binary(left,(BinaryOperator.Eq|BinaryOperator.Ne|BinaryOperator.Lt|BinaryOperator.Le|BinaryOperator.Gt|BinaryOperator.Ge),right) -> same left || same right
            | _ -> false
        predicate |> Option.exists mandatory
    let finishDefault predicate = function
        | SqlExpr.Coalesce(value,_) when nonnull predicate value -> value
        | value -> value

    /// Hex digits plus a lower terminator form an order-preserving prefix-free word.
    let chunk role (target: Type) prepared =
        let target = if target.IsEnum then TypeMetadata.enumUnderlying target else target
        let floating = typeof<float>.Equals target || typeof<single>.Equals target
        let binaryCarrier = floating || Metadata.Generation.TypeSupport.NumericCore target.FullName = Some "System.Double"
        let bytes =
            if typeof<string>.Equals target then prepared
            elif (QueryTranslatorNumeric.domain target).IsSome then SqlExpr.Cast(prepared,"BLOB")
            elif binaryCarrier || (QueryTranslatorNumeric.nativeCastType target).IsSome || typeof<char>.Equals target || DateTimeFunctions.isDateTimeLikeType target then
                QueryOrderingPrimitives.number binaryCarrier prepared
            else SqlExpr.Cast(prepared,"BLOB")
        let present = SqlExpr.FunctionCall("concat",[text "1";call "hex" bytes;text "/"])
        if floating || target.IsValueType then present
        else SqlExpr.CaseExpr((SqlExpr.Unary(UnaryOperator.IsNull,prepared),text "0"),[],Some present)
