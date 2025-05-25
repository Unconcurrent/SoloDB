namespace SoloDatabase

open System.Linq.Expressions
open System.Runtime.CompilerServices
open System.Linq
open System.Data

type ISoloDBCollection<'T> =
    inherit IOrderedQueryable<'T>

    abstract member Name: string with get
    abstract member InTransaction: bool with get
    abstract member IncludeType: bool with get

    abstract member Insert: item: 'T -> int64
    /// <summary>
    /// Will insert or replace the item in the DB based on its UNIQUE INDEXES.
    /// </summary>
    abstract member InsertOrReplace: item: 'T -> int64

    abstract member InsertBatch: items: 'T seq -> System.Collections.Generic.IList<int64>

    /// <summary>
    /// Will insert or replace the items in the DB based on its UNIQUE INDEXES, throwing if the Id is non zero.
    /// </summary>
    abstract member InsertOrReplaceBatch: items: 'T seq -> System.Collections.Generic.IList<int64>

    abstract member TryGetById: id: int64 -> 'T option

    abstract member GetById: id: int64 -> 'T

    abstract member TryGetById<'IdType when 'IdType : equality>: id: 'IdType -> 'T option

    abstract member GetById<'IdType when 'IdType : equality>: id: 'IdType -> 'T

    abstract member EnsureIndex<'R>: expression: Expression<System.Func<'T, 'R>> -> int

    abstract member EnsureUniqueAndIndex<'R>: expression: Expression<System.Func<'T, 'R>> -> int

    abstract member DropIndexIfExists<'R>: expression: Expression<System.Func<'T, 'R>> -> int
        
    /// <summary>
    /// Will ensure that all attribute indexes will exists, even if added after the collection's creation.
    /// </summary>
    abstract member EnsureAddedAttributeIndexes: unit -> unit

    /// <summary>
    /// Returns the internal SQLite connection object, do NOT use, as it is not intented for public usage.
    /// </summary>
    [<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
    abstract member GetInternalConnection: unit -> IDbConnection

    /// <summary>
    /// Will replace the item in the DB based on its Id, and if it does not have one then it will throw an InvalidOperationException.
    /// </summary>
    abstract member Update: item: 'T -> unit

    abstract member Delete: id: int64 -> int

    abstract member Delete<'IdType when 'IdType : equality>: id: 'IdType -> int

    abstract member DeleteMany: filter: Expression<System.Func<'T, bool>> -> int

    abstract member DeleteOne: filter: Expression<System.Func<'T, bool>> -> int

    abstract member ReplaceMany: filter: Expression<System.Func<'T, bool>> * item: 'T -> int

    abstract member ReplaceOne: filter: Expression<System.Func<'T, bool>> * item: 'T -> int

[<Extension>]
type UntypedCollectionExt =
    [<Extension>]
    static member InsertBatchObj(collection: ISoloDBCollection<JsonSerializator.JsonValue>, s: obj seq) =
        s |> Seq.map JsonSerializator.JsonValue.SerializeWithType |> collection.InsertBatch

    [<Extension>]
    static member InsertObj(collection: ISoloDBCollection<JsonSerializator.JsonValue>, o: obj) =
        o |> JsonSerializator.JsonValue.SerializeWithType |> collection.Insert