namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorSerialization
open SoloDatabase.QueryTranslatorBase
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.Utils

/// Computed collection access retains the selected element's original writer.
module internal QueryTranslatorIndex =
    let private indexerOwners =
        [| typedefof<Collections.Generic.List<_>>;typedefof<Collections.Generic.IList<_>>
           typedefof<Collections.Generic.IReadOnlyList<_>>;typedefof<list<_>>
           typedefof<Collections.ObjectModel.Collection<_>>;typedefof<Collections.ObjectModel.ReadOnlyCollection<_>>
           typedefof<Collections.Generic.Dictionary<_,_>>;typedefof<Collections.Generic.IDictionary<_,_>>
           typedefof<Collections.Generic.IReadOnlyDictionary<_,_>> |]

    let internal ownsIndexer (method: Reflection.MethodInfo) =
        let owner = method.DeclaringType
        if isNull owner then false
        elif owner.Equals typeof<Collections.ArrayList> || owner.Equals typeof<JsonSerializator.JsonValue> then true
        elif not owner.IsGenericType then false
        else
            let owner = owner.GetGenericTypeDefinition()
            indexerOwners |> Array.exists(fun expected -> owner.Equals expected)

    let private literal text = SqlExpr.Literal(SqlLiteral.String text)
    let private error kind message = SqlExpr.FunctionCall(JsonFunctions.rowErrorFunction,[runtimeErrorPayload kind message])
    let private nullError = error NullReferenceError "The indexed collection is null."
    let private nullSource source =
        SqlExpr.Binary(SqlExpr.Unary(UnaryOperator.IsNull,source),BinaryOperator.Or,
            SqlExpr.Binary(SqlExpr.FunctionCall("json_type",[source]),BinaryOperator.Eq,literal "null"))

    let private access createDefault nextName serializedKey (keyType: Type) resultType (parts: (SqlExpr * QuerySerializationInput) array) =
        let value,input = parts[0]
        let key,keyInput = parts[1]
        let key = if serializedKey then key else QueryProduced.scalarValue keyType key keyInput
        let source = input.Source |> ValueOption.defaultWith(fun () -> canonicalToken input.Carrier value)
        let dictionary = match input.Facts with JsonSerializator.JsonSerializationShape.DictionaryValues _ -> true | _ -> false
        if dictionary || typeof<string>.Equals keyType then
            let alias = nextName()
            let storedKey = SqlExpr.Column(Some alias,"key")
            let numericKey = dictionary && not(typeof<string>.Equals keyType)
            let comparison =
                if not numericKey then SqlExpr.Binary(storedKey,BinaryOperator.Eq,key)
                else
                    let expected =
                        if serializedKey then key
                        else
                            let token = keyInput.Source |> ValueOption.defaultWith(fun () -> canonicalToken keyInput.Carrier (fst parts[1]))
                            let fallback = QueryProduced.consume nextName (fun value input ->
                                SqlExpr.FunctionCall("json",[input.Source |> ValueOption.defaultWith(fun () -> canonicalToken input.Carrier value)])) (createDefault keyType)
                            SqlExpr.Coalesce(SqlExpr.FunctionCall("json",[token]),[fallback])
                    SqlExpr.FunctionCall("DICTIONARY_KEY_EQUALS",[literal(TypeMetadataCache.RegisterExactName keyType);storedKey;expected])
            let embeddedNul = SqlExpr.Binary(SqlExpr.FunctionCall("instr",[storedKey;SqlExpr.FunctionCall("char",[SqlExpr.Literal(SqlLiteral.Integer 0L)])]),
                                             BinaryOperator.Gt,SqlExpr.Literal(SqlLiteral.Integer 0L))
            let token = SqlExpr.FunctionCall("jsonb",[SqlExpr.CaseExpr((embeddedNul,
                SqlExpr.FunctionCall("JSON_PROPERTY_TOKEN",[SqlExpr.FunctionCall("json",[source]);storedKey])),[],
                Some(SqlExpr.Binary(source,BinaryOperator.JsonText,SqlExpr.Column(Some alias,"fullkey"))))])
            let child = elementInput input (SqlExpr.Column(Some alias,"key")) token
            let selected = DirectValue(jsonTokenValue resultType token,child)
            let selectedPacket = QueryProduced.packet selected
            let selectedPacket =
                if not numericKey then selectedPacket else
                SqlExpr.CaseExpr((SqlExpr.Binary(SqlExpr.FunctionCall("COUNT",[SqlExpr.Literal(SqlLiteral.Integer 1L)]),
                                                BinaryOperator.Gt,SqlExpr.Literal(SqlLiteral.Integer 1L)),
                                  error ArgumentError "Several stored names resolve to the same dictionary key."),[],
                                  Some(SqlExpr.FunctionCall("MAX",[selectedPacket])))
            let core = {
                Source = Some(FromJsonEach(source,Some alias)); Joins = []
                Projections = Explicit({ Alias = None; Expr = selectedPacket },[])
                Where = Some comparison
                GroupBy = []; Having = None; OrderBy = []; Limit = None; Offset = None; Distinct = false }
            let packet = SqlExpr.Coalesce(SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core },
                                         [error MissingKeyError "The requested dictionary key was not found."])
            let nullKey =
                if keyType.IsValueType && isNull(Nullable.GetUnderlyingType keyType) then []
                else [SqlExpr.Unary(UnaryOperator.IsNull,key),error NullArgumentError "The dictionary key is null."]
            let packet = SqlExpr.CaseExpr((nullSource source,nullError),nullKey,Some packet)
            let selected = PackedValue(packet,rebaseSerializationInput selected.Input)
            if input.PhysicalOwner.IsSome && typeof<JsonSerializator.JsonValue>.Equals input.OriginalType then
                let identity = QueryTranslatorVisitCoreMemberAccess.accessValue createDefault nextName false
                                   (StoredLabel(box "Id")) resultType (DirectValue(value,input))
                QueryProduced.choose resultType (SqlExpr.Binary(key,BinaryOperator.Eq,literal "Id")) identity selected
            else selected
        else
            let bounds = error (if input.OriginalType.IsArray then IndexError else RangeError) "The collection index is outside its bounds."
            let token = SqlExpr.Coalesce(SqlExpr.Binary(source,BinaryOperator.JsonText,key),[bounds])
            let token = SqlExpr.CaseExpr((nullSource source,nullError),
                [SqlExpr.Binary(key,BinaryOperator.Lt,SqlExpr.Literal(SqlLiteral.Integer 0L)),bounds],Some token)
            let token = SqlExpr.FunctionCall("jsonb",[token])
            let child = elementInput input key token
            let selected = DirectValue(jsonTokenValue resultType token,child)
            if typeof<byte>.Equals resultType && typeof<Collections.Generic.IEnumerable<byte>>.IsAssignableFrom input.OriginalType then
                let bytes = SqlExpr.FunctionCall("base64",[SqlExpr.FunctionCall("jsonb_extract",[source;literal "$"])])
                let binary = QueryProduced.typed resultType (SqlExpr.FunctionCall("BYTE_ELEMENT",[bytes;key;SqlExpr.Literal(SqlLiteral.Integer(if input.OriginalType.IsArray then 1L else 0L))]))
                QueryProduced.choose resultType (SqlExpr.Binary(SqlExpr.FunctionCall("json_type",[source]),BinaryOperator.Eq,literal "text")) binary selected
            else selected

    let internal lower (visit: Expression -> QueryBuilder -> QueryProducedValue) (parent: Expression) (index: Expression) resultType (qb: QueryBuilder) =
        let receiver = visit parent qb
        let dictionary = match receiver.Input.Facts with JsonSerializator.JsonSerializationShape.DictionaryValues _ -> true | _ -> false
        let serialized = dictionary && not(typeof<string>.Equals index.Type) &&
                         (isFullyConstant index || (qb.SourceContext.BindQueryValue |> ValueOption.exists(fun binder -> binder.IsValue index)))
        let argument =
            if not serialized then index else
            BoundValueCallExpression([|index|],typeof<string>,Func<obj array,obj>(fun values ->
                let text = JsonSerializator.JsonValue.WriteRuntimeBuffer(index.Type,values[0],false,false)
                box(System.String(text.Array,text.Offset,text.Count)))) :> Expression
        let key = visit argument qb
        QueryProduced.construct qb.SourceContext.NextAlias
            (access (QueryDefault.value qb.Variables) qb.SourceContext.NextAlias serialized index.Type resultType) [|receiver;key|]

    let internal tryCall visitProduced visitDu visitPropertyDu visitPreparedPropertyDu (m: MethodCallExpression) (qb: QueryBuilder) =
        match m with
        | OfShape1 null null "get_Item" null (o, key) when not qb.UpdateMode && ownsIndexer m.Method &&
                  (not(key :? ConstantExpression) || typeof<byte>.Equals m.Type && typeof<Collections.Generic.IEnumerable<byte>>.IsAssignableFrom o.Type ||
                   not(typeof<string>.Equals key.Type) && (match JsonSerializator.JsonValue.SerializedValueFacts o.Type with JsonSerializator.JsonSerializationShape.DictionaryValues _ -> true | _ -> false)) ->
            ValueSome(lower visitProduced o key m.Type qb)
        | OfShape1 null null "get_Item" null (o, key) when o.Type.IsGenericType && typedefof<Collections.Generic.IReadOnlyDictionary<_,_>>.Equals(o.Type.GetGenericTypeDefinition()) ->
            let produced = visitProduced o qb
            let keyValue = visitDu key qb
            ValueSome(QueryProduced.bind qb.SourceContext.NextAlias (fun value input ->
                let alias = qb.SourceContext.NextAlias()
                let token = SqlExpr.FunctionCall("jsonb", [SqlExpr.Binary(value, BinaryOperator.JsonText, SqlExpr.Column(Some alias, "fullkey"))])
                let child = QueryTranslatorSerialization.elementInput input (SqlExpr.Column(Some alias, "key")) token
                let selected = DirectValue(jsonTokenValue m.Type token, child)
                let core = { Source = Some(FromJsonEach(value, Some alias)); Joins = []
                             Projections = Explicit({ Alias = None; Expr = QueryProduced.packet selected }, [])
                             Where = Some(SqlExpr.Binary(SqlExpr.Column(Some alias, "key"), BinaryOperator.Eq, keyValue))
                             GroupBy = []; Having = None; OrderBy = []; Limit = None; Offset = None; Distinct = false }
                PackedValue(SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }, QueryTranslatorSerialization.rebaseSerializationInput selected.Input)) produced)
        | OfShape1 null null "get_Item" null (o, propExpr) when typeof<Collections.ICollection>.IsAssignableFrom o.Type || typeof<Array>.IsAssignableFrom o.Type || typeof<JsonSerializator.JsonValue>.IsAssignableFrom o.Type ->
            ValueSome(visitPropertyDu o (propExpr :?> ConstantExpression).Value m qb)
        | OfShape1 null null "Dyn" (OfPropInfo) (o, propExpr) ->
            ValueSome(visitPreparedPropertyDu o propExpr (fun value -> box((unbox<Reflection.PropertyInfo> value).Name)) m qb)
        | OfShape1 null null "Dyn" null (o, propExpr) ->
            if typeof<string>.Equals(propExpr.Type) || isIntegerBasedType propExpr.Type then ValueSome(visitPreparedPropertyDu o propExpr id m qb)
            else raise(NotSupportedException(sprintf "Cannot access dynamic property of %A" o))
        | _ -> ValueNone

    let internal register (connection: NativeSqliteConnection) =
        connection.RegisterManagedScalar("JSON_PROPERTY_TOKEN",2,RawSqliteFunctions.RawScalarFunc(fun result arguments ->
            let text = arguments[0].CopyToString()
            let mutable context = JsonSerializator.JsonReadContext(JsonSerializator.JsonLexer.fromString text,
                                      { Start = 0; Length = text.Length; Depth = 0 })
            let mutable child = JsonSerializator.JsonReadCursor.child &context context.Range
            if JsonSerializator.JsonReadCursor.tryProperty &context (arguments[1].CopyToString()) &child then
                result.SetText(text.Substring(child.Range.Start,child.Range.Length))
            else result.SetNull()),RawSqliteFunctions.SqliteFunctionFlags.Deterministic)
        connection.RegisterManagedScalar("DICTIONARY_KEY_EQUALS",3,RawSqliteFunctions.RawScalarFunc(fun context arguments ->
            let target = TypeMetadata.nameToType(arguments[0].CopyToString())
            let value = JsonSerializator.JsonValue.ReadDictionaryKey(target,arguments[1].CopyToString())
            let same = not arguments[2].IsNull && value.Equals(JsonSerializator.JsonValue.ParseRuntime(target,arguments[2].CopyToString()))
            context.SetInt32(if same then 1 else 0)),RawSqliteFunctions.SqliteFunctionFlags.Deterministic)
        connection.RegisterManagedScalar("BYTE_ELEMENT",3,RawSqliteFunctions.RawScalarFunc(fun context arguments ->
            if arguments[0].IsNull then raise(NullReferenceException "The indexed byte collection is null.")
            let bytes = arguments[0].GetBlobSpan()
            let index = arguments[1].GetInt64()
            if index < 0L || index >= int64 bytes.Length then
                if arguments[2].GetInt32() <> 0 then raise(IndexOutOfRangeException "The byte index is outside its bounds.")
                else raise(ArgumentOutOfRangeException("index","The byte index is outside its bounds."))
            context.SetInt32(int bytes[int index])),RawSqliteFunctions.SqliteFunctionFlags.Deterministic)
