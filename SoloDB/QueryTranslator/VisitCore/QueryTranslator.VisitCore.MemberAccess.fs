namespace SoloDatabase

open System
open SoloDatabase.Metadata.Generation
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
open SoloDatabase.SqlModel
open SoloDatabase.JsonSerializator.JsonEqualitySupport
open SoloDatabase.QueryTranslatorSerialization
open SoloDatabase.QuerySerializationSql

/// Authored member identity produces the value and writer transition together.
module internal QueryTranslatorVisitCoreMemberAccess =
    let internal accessValue nextName updateMode access resultType (produced: QueryProducedValue) : QueryProducedValue =
        QueryProduced.bind nextName (fun value input ->
            let input = if input.Source.IsSome then input else { input with Source = ValueSome(jsonToken value) }
            let struct(child, segments) = serializationAccess input access
            let source = if updateMode then value else input.Source.Value
            let explicitToken =
                match JsonSerializator.JsonValue.SerializedValueFacts(Utils.optionalPayloadType resultType) with
                | JsonSerializator.JsonSerializationShape.Uninterpreted -> true
                | _ -> false
            let physicalIdentity =
                match segments, input.PhysicalOwner with
                | [Member "Id"], ValueSome owner when not updateMode &&
                    (typeof<JsonSerializator.JsonValue>.Equals resultType ||
                     (match access with StoredLabel(:? string as name) -> name = "Id" && (typeof<int64>.Equals resultType || typeof<int32>.Equals resultType) | _ -> false) ||
                     typeof<int64>.Equals resultType &&
                        (owner.Sources |> Array.forall (fun source -> ModelMetadata.MemberType(source.OwnerType, "Id", false) = Some typeof<int64>))) -> ValueSome owner
                | _ -> ValueNone
            let extracted =
                match segments, value with
                | [], _ when child.SourceOffset > 0 ->
                    let length =
                        match child.Facts with
                        | JsonSerializator.JsonSerializationShape.ArrayMembers members -> members.Length
                        | JsonSerializator.JsonSerializationShape.DeclaredArrayMembers members -> members.Length
                        | _ -> invalidOp "A retained tuple Rest requires its declared positional layout."
                    SqlExpr.JsonArrayExpr [for index in 0 .. length - 1 -> tupleLeafToken source (child.SourceOffset + index)]
                | [], _ -> value
                | _, SqlExpr.Literal(SqlLiteral.String path) when updateMode ->
                    SqlExpr.Literal(SqlLiteral.String(path + (JsonPathOps.text segments).Substring(1)))
                | _, _ when explicitToken && not updateMode ->
                    SqlExpr.FunctionCall("jsonb", [SqlExpr.Binary(source, BinaryOperator.JsonText, SqlExpr.Literal(SqlLiteral.String(JsonPathOps.text segments)))])
                | _, SqlExpr.JsonExtractExpr(alias, column, path) ->
                    SqlExpr.JsonExtractExpr(alias, column, JsonPathOps.ofSegments(JsonPathOps.toList path @ segments))
                | _, SqlExpr.FunctionCall("jsonb", [SqlExpr.Column(alias, column)]) ->
                    SqlExpr.JsonExtractExpr(alias, column, JsonPathOps.ofSegments segments)
                | _ -> SqlExpr.FunctionCall("jsonb_extract", [source; SqlExpr.Literal(SqlLiteral.String(JsonPathOps.text segments))])
            let extracted =
                match physicalIdentity with
                | ValueSome owner ->
                    match owner.Descriptor with
                    | SqlExpr.JsonArrayExpr _ -> owner.Identity
                    | _ -> SqlExpr.CaseExpr((SqlExpr.Unary(UnaryOperator.IsNotNull, owner.Descriptor), owner.Identity), [], Some extracted)
                | _ -> extracted
            let carrier = if updateMode || physicalIdentity.IsSome || isSqlScalarResult resultType then SqlScalar else CanonicalToken
            let child =
                if child.SourceOffset > 0 && segments.IsEmpty then { child with SourceOffset = 0; Source = ValueSome extracted }
                else child
            let child = if physicalIdentity.IsSome then { child with Source = ValueSome(jsonToken extracted); Operand = ValueNone } else child
            // A packed child retains its original scalar token until the actual
            // value or presence consumer chooses the owning scalar operation.
            let packedScalar = not updateMode && isSqlScalarResult resultType && (match produced with PackedValue _ -> true | _ -> false)
            if packedScalar then
                DirectValue(child.Source |> ValueOption.defaultWith (fun () -> jsonToken extracted), { child with Carrier = CanonicalToken })
            else DirectValue(extracted, { child with Carrier = carrier })) produced

    let internal access visitProduced parent access resultType (qb: QueryBuilder) =
        accessValue qb.SourceContext.NextAlias qb.UpdateMode access resultType (visitProduced parent qb)

    /// Identity and join consumers prepare the same authored reference input.
    let internal referenceInput (visitProduced: Expression -> QueryBuilder -> QueryProducedValue) (reference: Expression) (qb: QueryBuilder) =
        let bound = qb.SourceContext.BindQueryValue |> ValueOption.exists (fun binder -> binder.IsValue reference)
        if bound || isFullyConstant reference then struct(visitProduced reference qb, [], false)
        else
            match normalizeStoredConstruction reference with
            | :? MemberExpression as memberValue when not(isNull memberValue.Expression) ->
                let parent = visitProduced memberValue.Expression qb
                let access = ClrMember(memberValue.Member.Name, memberValue.Type)
                let struct(_, segments) = serializationAccess parent.Input access
                struct(accessValue qb.SourceContext.NextAlias qb.UpdateMode access memberValue.Type parent, segments, parent.Input.PhysicalOwner.IsSome)
            | _ -> struct(visitProduced reference qb, [], false)

    let internal memberAccess (visitProduced: Expression -> QueryBuilder -> QueryProducedValue) (visitDu: Expression -> QueryBuilder -> SqlExpr) (visitPresenceDu: Expression -> QueryBuilder -> SqlExpr) (visitPayload: Expression -> QueryBuilder -> QueryProducedValue) (m: MemberAccess) (qb: QueryBuilder) : QueryProducedValue =
        let scalar value = QueryProduced.typed m.ReturnType value
        let alias =
            match m.Expression with
            | :? ParameterExpression as pe -> (qb.ResolveParameterSource pe).Alias
            | _ -> qb.SourceAlias
        match TypeSupport.OptionalMember(m.InputType, m.MemberName) with
        | ValueSome OptionalAccess.Present -> scalar(SqlExpr.Unary(UnaryOperator.IsNotNull, visitPresenceDu m.Expression qb))
        | ValueSome OptionalAccess.Absent -> scalar(SqlExpr.Unary(UnaryOperator.IsNull, visitPresenceDu m.Expression qb))
        | ValueSome OptionalAccess.Payload ->
            QueryProduced.bind qb.SourceContext.NextAlias
                (fun value input -> DirectValue(value, serializationPayload input))
                (visitPayload m.Expression qb)
        | ValueNone ->
        if m.MemberName = "Length" && not (isNull (m.InputType.GetInterface(typeof<IEnumerable>.FullName))) then
            if typeof<string>.Equals(m.InputType) then scalar(SqlExpr.FunctionCall("length", [visitDu m.Expression qb]))
            elif typeof<byte array>.Equals(m.InputType) then scalar(SqlExpr.FunctionCall("length", [SqlExpr.FunctionCall("base64", [visitDu m.Expression qb])]))
            else scalar(SqlExpr.FunctionCall("json_array_length", [visitDu m.Expression qb]))
        elif m.MemberName = "Count" && not (isNull (m.InputType.GetInterface(typeof<IEnumerable>.FullName))) then
            scalar(SqlExpr.FunctionCall("json_array_length", [visitDu m.Expression qb]))
        elif isDateTimeLikeType m.InputType then
            let translated =
                if typeof<DateTimeOffset>.Equals(m.InputType) then
                    match m.Expression with
                    | :? NewExpression as ne ->
                        match DateTimeFunctions.tryTranslateDateTimeOffsetConstructorMember m.MemberName ne (fun expr -> visitDu expr qb) with
                        | Some value -> value
                        | None -> DateTimeFunctions.translateDateTimeMember m.MemberName (visitDu m.Expression qb) m.InputType
                    | _ -> DateTimeFunctions.translateDateTimeMember m.MemberName (visitDu m.Expression qb) m.InputType
                elif typeof<DateTime>.Equals(m.InputType) then DateTimeFunctions.translateDateTimeMember m.MemberName (visitDu m.Expression qb) m.InputType
                elif typeof<DateOnly>.Equals(m.InputType) then DateTimeFunctions.translateDateOnlyMember m.MemberName (visitDu m.Expression qb)
                elif typeof<TimeOnly>.Equals(m.InputType) then DateTimeFunctions.translateTimeOnlyMember m.MemberName (visitDu m.Expression qb)
                else DateTimeFunctions.translateTimeSpanMember m.MemberName (visitDu m.Expression qb)
            scalar translated
        elif (typeof<int64>.Equals(m.ReturnType) && m.MemberName = "Id" && isRootParameter m.Expression)
              || (m.MemberName = "Id" && m.Expression.NodeType = ExpressionType.Parameter && (typeof<JsonSerializator.JsonValue>.Equals(m.Expression.Type) || m.Expression.Type.FullName = "SoloDatabase.MongoDB.BsonDocument")) then
            let parent = visitProduced m.Expression qb
            if not qb.UpdateMode && parent.Input.PhysicalOwner.IsSome then
                let access =
                    if typeof<JsonSerializator.JsonValue>.Equals m.Expression.Type || m.Expression.Type.FullName = "SoloDatabase.MongoDB.BsonDocument" then StoredLabel(box m.MemberName)
                    else ClrMember(m.MemberName, m.ReturnType)
                accessValue qb.SourceContext.NextAlias false access m.ReturnType parent
            else scalar(SqlExpr.Column(alias, "Id"))
        else
            match m.OriginalExpression with
            | Some original when isNull original.Expression -> scalar(qb.AllocateParamExpr((original.Member :?> PropertyInfo).GetValue null))
            | Some original when isFullyConstant original -> scalar(qb.AllocateParamExpr(ExpressionEvaluation.evaluateExpr original))
            | Some original when not(isRootParameter original.Expression) && isPrimitiveSQLiteType m.InputType && not(typeof<JsonSerializator.JsonValue>.IsAssignableFrom m.InputType) ->
                raise(NotSupportedException(sprintf "Error: Member access '%O' is not supported.\nReason: The member cannot be translated to SQL in this context.\nFix: Simplify the expression or move it after AsEnumerable()." original.Member.Name))
            | _ -> access visitProduced m.Expression (ClrMember(m.MemberName, m.ReturnType)) m.ReturnType qb
