module Tests


[<EntryPoint>]
let main argv =
    let test = JsonTests.JsonTests()
    test.Init()
    try test.JsonSerializeDeserialize()
    finally test.Cleanup()
    0