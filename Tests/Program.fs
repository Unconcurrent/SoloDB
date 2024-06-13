module Tests


[<EntryPoint>]
let main argv =
    let test = PolymorphicTests.PolymorphicTests()
    test.Init()
    try test.SelectType()
    finally test.Cleanup()
    0