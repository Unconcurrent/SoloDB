namespace SoloDatabase

open System.Linq.Expressions
open System.Linq
open System

module Operators =
    type SoloDB with
        // The pipe operators.
        // If you want to use Expression<System.Func<'T, bool>> fluently, 
        // by just defining a normal function (fun (...) -> ...) you need to use a static member.

        static member collection<'T> (db: SoloDB) = db.GetCollection<'T>()
        static member collectionUntyped name (db: SoloDB) = db.GetUntypedCollection name

        static member drop<'T> (db: SoloDB) = db.DropCollection<'T>()
        static member dropByName name (db: SoloDB) = db.DropCollection name

        static member tryDrop<'T> (db: SoloDB) = db.DropCollectionIfExists<'T>()
        static member tryDropByName name (db: SoloDB) = db.DropCollectionIfExists name

        static member withTransaction func (db: SoloDB) = db.WithTransaction func
        static member optimize (db: SoloDB) = db.Optimize()

        static member ensureIndex<'T, 'R> (func: Expression<System.Func<'T, 'R>>) (collection: ISoloDBCollection<'T>) = collection.EnsureIndex func
        static member tryDropIndex<'T, 'R> (func: Expression<System.Func<'T, 'R>>) (collection: ISoloDBCollection<'T>) = collection.DropIndexIfExists func

        static member countWhere<'T> (func: Expression<System.Func<'T, bool>>) = fun (collection: ISoloDBCollection<'T>) -> collection.Where(func).LongCount()
        static member count<'T> (collection: ISoloDBCollection<'T>) = collection.LongCount()
        static member countAll<'T> (collection: ISoloDBCollection<'T>) = collection.LongCount()
        static member countAllLimit<'T> (limit: int32) (collection: ISoloDBCollection<'T>) = collection.Take(limit).LongCount()

        static member insert<'T> (item: 'T) (collection: ISoloDBCollection<'T>) = collection.Insert item
        static member insertBatch<'T> (items: 'T seq) (collection: ISoloDBCollection<'T>) = collection.InsertBatch items

        static member update<'T> (item: 'T) (collection: ISoloDBCollection<'T>) = collection.Update item
        static member replace<'T> (item: 'T) (collection: ISoloDBCollection<'T>) = collection.Update item

        static member delete<'T> (collection: ISoloDBCollection<'T>) = collection.DeleteMany(fun _ -> true)
        static member deleteById<'T> id (collection: ISoloDBCollection<'T>) = collection.Delete id

        static member select<'T, 'R> (func: Expression<System.Func<'T, 'R>>) = fun (collection: ISoloDBCollection<'T>) -> collection.Select func
        static member select<'T> () = fun (collection: ISoloDBCollection<'T>) -> collection
        static member selectUnique<'T, 'R> (func: Expression<System.Func<'T, 'R>>) = fun (collection: ISoloDBCollection<'T>) -> collection.Select(func).Distinct()

        static member where<'T> (func: Expression<System.Func<'T, bool>>) = fun (collection: ISoloDBCollection<'T>) -> collection.Where func
        static member whereId (id: int64) (builder: ISoloDBCollection<'T>) = builder.GetById id

        static member limit (count: int) (builder: IQueryable<'T>) = builder.Take count
        static member offset (count: int) (builder: IQueryable<'T>) = builder.Skip count

        static member orderAsc (func: Expression<System.Func<'T, 'K>>) = fun (builder: IQueryable<'T>) -> builder.OrderBy func
        static member orderDesc (func: Expression<System.Func<'T, 'K>>) = fun (builder: IQueryable<'T>) -> builder.OrderByDescending func

        static member explain (builder: IQueryable<'T>) = raise(NotImplementedException ())

        static member getById (id: int64) (collection: ISoloDBCollection<'T>) = collection.GetById id
        static member tryGetById (id: int64) (collection: ISoloDBCollection<'T>) = collection.TryGetById id

        static member tryFirst<'T> (func: Expression<System.Func<'T, bool>>) = fun (collection: ISoloDBCollection<'T>) -> func |> collection.Select |> Seq.tryHead

        static member any<'T> (func: Expression<System.Func<'T, bool>>) = fun (collection: ISoloDBCollection<'T>) -> func |> collection.Any

        static member toSeq (collection: ISoloDBCollection<'T>) = collection.AsEnumerable()
        static member toList (collection: ISoloDBCollection<'T>) = collection.ToList()