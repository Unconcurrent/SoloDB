module PolymorphicTests

#nowarn "3391" // Implicit on SqlId

open System
open Microsoft.VisualStudio.TestTools.UnitTesting
open SoloDatabase
open SoloDatabase.Types
open SoloDatabase.JsonFunctions
open Types
open TestUtils

type IMakeSound =
    abstract member MakeNoise: unit -> string

type Mouse() =
    interface IMakeSound with
        override this.MakeNoise() =
            "Chiț"

type Fly() =
    interface IMakeSound with
        override this.MakeNoise() =
            "Bzzzz"

type Elephant() =
    interface IMakeSound with
        override this.MakeNoise() =
            "*Trumpets and rumbles noises*"

[<TestClass>]
type PolymorphicTests() =
    let mutable db: SoloDB = Unchecked.defaultof<SoloDB>
    
    [<TestInitialize>]
    member this.Init() =
        let dbSource = $"memory:Test{Random.Shared.NextInt64()}"
        db <- SoloDB.Instantiate dbSource
    
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
        assertTrue (animalsNotCatType |> Seq.forall(fun t -> t <> typeof<Cat>))

        let allTammedAnimals = animals.Select().Where(fun a -> a.Tammed).ToList()
        assertTrue (allTammedAnimals.Length = 2) // Dog also tammed.

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

    [<TestMethod>]
    member this.InsertQueryInterface() =
        let interfaceCol = db.GetCollection<IMakeSound>()
        let mouseCount = 4
        let flyCount = 9
        let elephantCount = 2

        for i in 1..mouseCount do
            interfaceCol.Insert (Mouse()) |> ignore

        for i in 1..flyCount do
            interfaceCol.Insert (Fly()) |> ignore

        for i in 1..elephantCount do
            interfaceCol.Insert (Elephant()) |> ignore

        let all = interfaceCol.Select().OnAll().ToList()

        assertEqual (all |> Seq.filter(fun i -> i.MakeNoise() = "Chiț") |> Seq.length) mouseCount "Unequal mouse count."
        assertEqual (all |> Seq.filter(fun i -> i.MakeNoise() = "Bzzzz") |> Seq.length) flyCount "Unequal fly count."
        assertEqual (all |> Seq.filter(fun i -> i.MakeNoise() = "*Trumpets and rumbles noises*") |> Seq.length) elephantCount "Unequal elephant count."

        ()

