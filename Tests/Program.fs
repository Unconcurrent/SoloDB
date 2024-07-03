module Tests

open System
open System.Reflection
open Microsoft.VisualStudio.TestTools.UnitTesting
open System.Diagnostics

type TestResult =
    | Passed of string * int64
    | Failed of string * string

type TestRunner() =
    static member private PrintWithColor(colorCode: string, text: string) =
        printfn "\x1b[%sm%s\x1b[0m" colorCode text

    static member private RunTestMethods(testClass: Type) =
        let testInstance = Activator.CreateInstance(testClass)
        let meths = testClass.GetMethods()
        let stopwatch = Stopwatch()
        let results = System.Collections.Generic.List<TestResult>()

        try
            let classCleanupMethod = meths |> Seq.find (fun m -> m.GetCustomAttributes(typeof<TestCleanupAttribute>, true) |> Seq.isEmpty |> not)
            let classInitializeMethod = meths |> Seq.find (fun m -> m.GetCustomAttributes(typeof<TestInitializeAttribute>, true) |> Seq.isEmpty |> not)

            TestRunner.PrintWithColor("36", sprintf "Initializing tests in %s..." testClass.Name)

            for method in meths do
                if method.GetCustomAttributes(typeof<TestMethodAttribute>, true).Length > 0 then
                    stopwatch.Restart()
                    try
                        TestRunner.PrintWithColor("33", sprintf "Starting test %s..." method.Name)

                        classInitializeMethod.Invoke(testInstance, [||]) |> ignore
                        method.Invoke(testInstance, [||]) |> ignore                        

                        stopwatch.Stop()
                        results.Add(Passed(method.Name, stopwatch.ElapsedMilliseconds))
                        TestRunner.PrintWithColor("32", sprintf "Test %s passed in %d ms." method.Name stopwatch.ElapsedMilliseconds)
                    with
                    | :? Exception as ex ->
                        results.Add(Failed(method.Name, ex.Message))
                        TestRunner.PrintWithColor("31", sprintf "Test %s failed: %s" method.Name ex.Message)

                    TestRunner.PrintWithColor("36", sprintf "Cleaning up tests in %s..." testClass.Name)
                    classCleanupMethod.Invoke(testInstance, [| |]) |> ignore

        with
        | :? Exception as ex ->
            TestRunner.PrintWithColor("31", sprintf "Error during class initialization/cleanup: %s" ex.Message)

        results

    static member RunTests(assembly: Assembly) =
        let types = assembly.GetTypes()
        let allResults = System.Collections.Generic.List<TestResult>()
        let stopwatch = Stopwatch.StartNew()

        TestRunner.PrintWithColor("35", "Starting test run...")
        for testClass in types do
            if testClass.GetCustomAttributes(typeof<TestClassAttribute>, true).Length > 0 then
                allResults.AddRange(TestRunner.RunTestMethods(testClass))
        stopwatch.Stop()


        // Summary of results
        allResults |> Seq.iter (function
            | Passed(name, time) -> ()
            | Failed(name, message) -> TestRunner.PrintWithColor("31", sprintf "%s failed: %s" name message))

        let passed = allResults |> Seq.filter (function Passed _ -> true | _ -> false) |> Seq.length
        let failed = allResults |> Seq.filter (function Failed _ -> true | _ -> false) |> Seq.length
        TestRunner.PrintWithColor("35", $"Test summary: {passed} passed, {failed} failed in {stopwatch.Elapsed}")


[<EntryPoint>]
let main argv =
    TestRunner.RunTests(Assembly.GetExecutingAssembly())
    0