namespace SoloDatabase

open System.Linq.Expressions
open System

module Operators =
    type SoloDB with
        // The pipe operators.
        // If you want to use Expression<System.Func<'T, bool>> fluently, 
        // by just defining a normal function (fun (...) -> ...) you need to use a static member
        // with C# like arguments.

        static member collection<'T> (db: SoloDB) = db.GetCollection<'T>()
        static member collectionUntyped name (db: SoloDB) = db.GetUntypedCollection name

        static member drop<'T> (db: SoloDB) = db.DropCollection<'T>()
        static member dropByName name (db: SoloDB) = db.DropCollection name

        static member tryDrop<'T> (db: SoloDB) = db.DropCollectionIfExists<'T>()
        static member tryDropByName name (db: SoloDB) = db.DropCollectionIfExists name

        static member withTransaction func (db: SoloDB) = db.WithTransaction func
        static member optimize (db: SoloDB) = db.Optimize()

        static member ensureIndex<'T, 'R> (func: Expression<System.Func<'T, 'R>>) (collection: Collection<'T>) = collection.EnsureIndex func
        static member tryDropIndex<'T, 'R> (func: Expression<System.Func<'T, 'R>>) (collection: Collection<'T>) = collection.DropIndexIfExists func

        static member countWhere<'T> (func: Expression<System.Func<'T, bool>>) = fun  (collection: Collection<'T>) -> collection.CountWhere func
        static member count<'T> (collection: Collection<'T>) = collection.Count()
        static member countAll<'T> (collection: Collection<'T>) = collection.CountAll()
        static member countAllLimit<'T> (limit: uint64) (collection: Collection<'T>) = collection.CountAllLimit limit

        static member insert<'T> (item: 'T) (collection: Collection<'T>) = collection.Insert item
        static member insertBatch<'T> (items: 'T seq) (collection: Collection<'T>) = collection.InsertBatch items

        static member updateF<'T> (func: Expression<Action<'T>> array) = fun (collection: Collection<'T>) -> collection.Update func
        static member update<'T> (item: 'T) (collection: Collection<'T>) = collection.Update item
        static member replace<'T> (item: 'T) (collection: Collection<'T>) = collection.Replace item

        static member delete<'T> (collection: Collection<'T>) = collection.Delete()
        static member deleteById<'T> id (collection: Collection<'T>) = collection.DeleteById id

        static member select<'T, 'R> (func: Expression<System.Func<'T, 'R>>) = fun (collection: Collection<'T>) -> collection.Select func
        static member select<'T> () = fun (collection: Collection<'T>) -> collection.Select()
        static member selectUnique<'T, 'R> (func: Expression<System.Func<'T, 'R>>) = fun (collection: Collection<'T>) -> collection.SelectUnique func

        static member where<'a, 'b, 'c> (func: Expression<System.Func<'a, bool>>) = fun (builder: WhereBuilder<'a, 'b, 'c>) -> builder.Where func
        static member whereId (func: int64) (builder: WhereBuilder<'a, 'b, 'c>) = builder.WhereId func

        static member limit (count: uint64) (builder: FinalBuilder<'a, 'b, 'c>) = builder.Limit count
        static member offset (count: uint64) (builder: FinalBuilder<'a, 'b, 'c>) = builder.Offset count

        static member orderAsc (func: Expression<System.Func<'a, obj>>) = fun (builder: FinalBuilder<'a, 'b, 'c>) -> builder.OrderByAsc func
        static member orderDesc (func: Expression<System.Func<'a, obj>>) = fun (builder: FinalBuilder<'a, 'b, 'c>) -> builder.OrderByDesc func

        static member exec (builder: FinalBuilder<'a, 'b, 'c>) = builder.Execute()
        static member toSeq (builder: FinalBuilder<'a, 'b, 'c>) = builder.Enumerate()
        static member toList (builder: FinalBuilder<'a, 'b, 'c>) = builder.ToList()

        static member explain (builder: FinalBuilder<'a, 'b, 'c>) = builder.ExplainQueryPlan()

        static member getById (id: int64) (collection: Collection<'T>) = collection.GetById id
        static member tryGetById (id: int64) (collection: Collection<'T>) = collection.TryGetById id

        static member tryFirst<'T> (func: Expression<System.Func<'T, bool>>) = fun (collection: Collection<'T>) -> func |> collection.TryFirst

        static member any<'T> (func: Expression<System.Func<'T, bool>>) = fun (collection: Collection<'T>) -> func |> collection.Any