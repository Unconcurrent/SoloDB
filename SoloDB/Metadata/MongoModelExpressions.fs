namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.JsonSerializator
open SoloDatabase.MongoDB

/// Member-path expression construction for the Mongo-style builders.
[<AbstractClass; Sealed>]
type internal MongoModelExpressions private () =
    /// <summary>
    /// Creates a property access expression from a string field path (e.g., "Customer.Address.Street").
    /// </summary>
    static member Property<'T,'TField>(fieldPath: string) : Expression<Func<'T,'TField>> =
        let parameter = Expression.Parameter(typeof<'T>, "x")
        let fields = fieldPath.Split('.')

        let rec buildExpression (expr: Expression) index : Expression =
            if index = fields.Length then expr else
            let name = if fields[index] = "_id" then "Id" else fields[index]
            let last = index + 1 = fields.Length
            let access =
                match ModelMetadata.NamedMember(expr.Type,name) with
                | Some struct(valueType,read) -> BoundValueAccessExpression(expr,valueType,read,name) :> Expression
                | None when typeof<BsonDocument>.Equals(expr.Type) || typeof<JsonValue>.Equals(expr.Type) ->
                    let bson = typeof<BsonDocument>.Equals(expr.Type)
                    let valueType = if last then typeof<'TField> else expr.Type
                    let read = Func<obj,obj>(fun owner ->
                        let value = if bson then box((unbox<BsonDocument> owner).[name]) else box((unbox<JsonValue> owner).[name])
                        if not last then value
                        elif name = "Id" then box(unbox<'TField> value)
                        elif bson then box((unbox<BsonDocument> value).ToObject<'TField>())
                        else box((unbox<JsonValue> value).ToObject<'TField>()))
                    BoundValueAccessExpression(expr,valueType,read,name) :> Expression
                | None ->
                    match ModelMetadata.StringIndexer(expr.Type) with
                    | ValueSome struct(valueType,read) ->
                        let read = Func<obj,obj>(fun owner -> read.Invoke(owner,name))
                        BoundValueAccessExpression(expr,valueType,read,name) :> Expression
                    | ValueNone -> raise(ArgumentNullException("indexer"))
            buildExpression access (index + 1)

        let finalExpr = buildExpression (parameter :> Expression) 0
        Expression.Lambda<Func<'T, 'TField>>(finalExpr, parameter)
