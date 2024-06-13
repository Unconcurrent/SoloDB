module PolymorphicTests

#nowarn "3391" // Implicit on SqlId

open System
open Microsoft.VisualStudio.TestTools.UnitTesting
open SoloDB
open SoloDbTypes
open Types
open TestUtils

[<TestClass>]
type PolymorphicTests() =
    let mutable db: SoloDB = Unchecked.defaultof<SoloDB>
    
    [<TestInitialize>]
    member this.Init() =
        let dbSource = $"memory:Test{Random.Shared.NextInt64()}"
        db <- SoloDB.instantiate dbSource
    
    [<TestCleanup>]
    member this.Cleanup() =
        db.Dispose()

    [<TestMethod>]
    member this.SelectType() =
        let animals = db.GetCollection<Animal>()
        
        animals.Insert (Cat()) |> ignore
        let tammedCat = Cat()
        tammedCat.Tammed <- true
        
        animals.Insert (tammedCat) |> ignore
        animals.Insert (Dog()) |> ignore
        animals.Insert (Tiger()) |> ignore
        
        let animalsNotCat = animals.Select(fun c -> (c.Id, c, c.GetType())).Where(fun a -> a.GetType() <> typeof<Cat>).ToList()
        let animalsNotCatType = animalsNotCat |> Seq.map(fun (id, a, t) -> a) |> Seq.map _.GetType() |> Seq.toList
        assertEqual (animalsNotCat.Length) 2 "Did not select all the non cats."

    [<TestMethod>]
    member this.InsertQueryAnimals() =
        let animals = db.GetCollection<Animal>()

        animals.Insert (Cat()) |> ignore
        let tammedCat = Cat()
        tammedCat.Tammed <- true

        animals.Insert (tammedCat) |> ignore
        animals.Insert (Dog()) |> ignore
        animals.Insert (Tiger()) |> ignore

        let cats = animals.Select().Where(fun a -> a.GetType() = typeof<Cat>).ToList()
        assertEqual (cats.Length) 2 "Did not select all the cats."
        let tammedCatsCount = animals.CountWhere(fun a -> a.GetType() = typeof<Cat> && a.Tammed)
        assertEqual tammedCatsCount 1 "Did not select all the cats."

        let animalsNotCat = animals.Select().Where(fun a -> a.GetType() <> typeof<Cat>).ToList()
        assertEqual (animalsNotCat.Length) 2 "Did not select all the non cats."

