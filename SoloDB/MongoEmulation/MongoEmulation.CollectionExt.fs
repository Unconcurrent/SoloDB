namespace SoloDatabase.MongoDB

open System.Linq.Expressions
open System
open System.Collections.Generic
open SoloDatabase
open SoloDatabase.JsonSerializator
open System.Runtime.CompilerServices
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
