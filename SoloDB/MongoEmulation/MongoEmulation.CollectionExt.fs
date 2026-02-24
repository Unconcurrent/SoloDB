namespace SoloDatabase.MongoDB

open System.Linq.Expressions
open System
open System.Collections.Generic
open SoloDatabase
open SoloDatabase.JsonSerializator
open System.Runtime.CompilerServices
open System.Reflection
open System.Linq

/// <summary>
/// Represents the result of an InsertMany operation.
/// </summary>
type InsertManyResult = {
    /// <summary>The list of IDs of the inserted documents.</summary>
    Ids: IList<int64>
    /// <summary>The number of documents inserted.</summary>
    Count: int64
}

/// <summary>
/// An internal proxy class to resolve method overloading ambiguity for the F# compiler.
/// </summary>
[<AbstractClass; Sealed>]
type internal ProxyRef =
    /// <summary>
    /// Calls the ReplaceOne method on the collection with a specific parameter order.
    /// </summary>
    static member ReplaceOne (collection: SoloDatabase.ISoloDBCollection<'a>) (document: 'a) (filter: Expression<Func<'a, bool>>) =
        collection.ReplaceOne (filter, document)

    /// <summary>
    /// Calls the ReplaceMany method on the collection with a specific parameter order.
    /// </summary>
    static member ReplaceMany (collection: SoloDatabase.ISoloDBCollection<'a>) (document: 'a) (filter: Expression<Func<'a, bool>>) =
        collection.ReplaceMany (filter, document)

/// <summary>
/// Provides extension methods for <see cref="SoloDatabase.ISoloDBCollection{a}"/>
/// to offer a MongoDB-like driver API.
/// </summary>
[<Extension; AbstractClass; Sealed>]
type CollectionExtensions =
    /// <summary>
    /// Counts the number of documents matching the given filter.
    /// </summary>
    [<Extension>]
    static member CountDocuments<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, filter: Expression<Func<'a, bool>>) : int64 =
        collection.Where(filter).LongCount()

    /// <summary>
    /// Counts all documents in the collection.
    /// </summary>
    [<Extension>]
    static member CountDocuments<'a>(collection: SoloDatabase.ISoloDBCollection<'a>) : int64 =
        collection.LongCount()

    /// <summary>
    /// Finds all documents in the collection that match the given filter.
    /// </summary>
    [<Extension>]
    static member Find<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, filter: Expression<Func<'a, bool>>) =
        collection.Where(filter)

    /// <summary>
    /// Inserts a single document into the collection.
    /// </summary>
    [<Extension>]
    static member InsertOne<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, document: 'a) =
        collection.Insert document

    /// <summary>
    /// Inserts a sequence of documents into the collection.
    /// </summary>
    [<Extension>]
    static member InsertMany<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, documents: 'a seq) =
        let result = (collection.InsertBatch documents)
        {
            Ids = result
            Count = result.Count
        }

    /// <summary>
    /// Replaces a single document that matches the filter.
    /// </summary>
    [<Extension>]
    static member ReplaceOne<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, filter: Expression<Func<'a, bool>>, document: 'a) =
        ProxyRef.ReplaceOne collection document filter

    /// <summary>
    /// Replaces all documents that match the filter.
    /// </summary>
    [<Extension>]
    static member ReplaceMany<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, filter: Expression<Func<'a, bool>>, document: 'a) =
        ProxyRef.ReplaceMany collection document filter

    /// <summary>
    /// Deletes a single document that matches the filter.
    /// </summary>
    [<Extension>]
    static member DeleteOne<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, filter: Expression<Func<'a, bool>>) =
        collection.DeleteMany(filter)

    /// <summary>
    /// Deletes all documents that match the filter.
    /// </summary>
    [<Extension>]
    static member DeleteMany<'a>(collection: SoloDatabase.ISoloDBCollection<'a>, filter: Expression<Func<'a, bool>>) =
        collection.DeleteMany(filter)

/// <summary>
/// A private module containing helper functions for building expressions.
/// </summary>
module private Helper =
    /// <summary>
    /// Creates a property access expression from a string field path (e.g., "Customer.Address.Street").
    /// </summary>
    let internal getPropertyExpression (fieldPath: string) : Expression<Func<'T, 'TField>> =
        let parameter = Expression.Parameter(typeof<'T>, "x")
        let fields = fieldPath.Split('.')

        let rec buildExpression (expr: Expression) (fields: string list) : Expression =
            match fields with
            | [] -> expr
            | field :: rest ->
                let field =
                    if field = "_id" then "Id"
                    else field

                let propertyOrField =
                    if expr.Type.GetField(field) <> null || expr.Type.GetProperty(field) <> null then
                        Expression.PropertyOrField(expr, field) :> Expression
                    else
                        let indexExpr =
                            Expression.MakeIndex(expr, expr.Type.GetProperty "Item", [Expression.Constant field]) :> Expression

                        if rest.IsEmpty && field <> "Id" && (expr.Type = typeof<BsonDocument> || expr.Type = typeof<JsonValue>) then
                            Expression.Call(indexExpr, expr.Type.GetMethod("ToObject").MakeGenericMethod(typeof<'TField>))
                        elif field = "Id" then
                            Expression.Convert(Expression.Convert(indexExpr, typeof<obj>), typeof<'TField>)
                        else
                            indexExpr

                buildExpression propertyOrField rest

        let finalExpr = buildExpression (parameter :> Expression) (fields |> List.ofArray)
        Expression.Lambda<Func<'T, 'TField>>(finalExpr, parameter)
