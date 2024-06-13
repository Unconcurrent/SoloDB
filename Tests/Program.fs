module Tests

open StandardTests
open TransactionalTests


[<EntryPoint>]
let main argv =
    let test = SoloDBStandardTesting()
    test.Init()
    try test.AnyFalse()
    finally test.Cleanup()
    0