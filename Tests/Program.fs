module Tests

open System
open System.Reflection
open Microsoft.VisualStudio.TestTools.UnitTesting
open System.Diagnostics
open System.Text.RegularExpressions
open SoloDatabase
open SoloDatabase.Operators
open CSharpTests

type TestResult =
    | Passed of string * int64
    | Failed of string * exn

type TestRunner() =
    static member private PrintWithColor(colorCode: string, text: string) =
        printfn "\x1b[%sm%s\x1b[0m" colorCode text

    static member private RunTestMethods(testClass: Type) (filter) =
        let testInstance = Activator.CreateInstance(testClass)
        let meths = testClass.GetMethods()
        let stopwatch = Stopwatch()
        let results = System.Collections.Generic.List<TestResult>()

        try
            let classCleanupMethod = meths |> Seq.find (fun m -> m.GetCustomAttributes(typeof<TestCleanupAttribute>, true) |> Seq.isEmpty |> not)
            let classInitializeMethod = meths |> Seq.find (fun m -> m.GetCustomAttributes(typeof<TestInitializeAttribute>, true) |> Seq.isEmpty |> not)

            TestRunner.PrintWithColor("36", sprintf "Initializing tests in %s..." testClass.Name)

            for method in meths do
                if method.GetCustomAttributes(typeof<TestMethodAttribute>, true).Length > 0 && filter method then                    
                    
                    if method.ReturnType <> typeof<Void> then
                        failwithf "Non void return for %s." method.Name
                        ()

                    classInitializeMethod.Invoke(testInstance, [||]) |> ignore

                    stopwatch.Restart()

                    try
                        TestRunner.PrintWithColor("33", sprintf "Starting test %s..." method.Name)

                        
                        method.Invoke(testInstance, [||]) |> ignore                        

                        stopwatch.Stop()
                        results.Add(Passed(method.Name, stopwatch.ElapsedMilliseconds))
                        TestRunner.PrintWithColor("32", sprintf "Test %s passed in %d ms." method.Name stopwatch.ElapsedMilliseconds)
                    with ex ->
                        results.Add(Failed(method.Name, ex))
                        TestRunner.PrintWithColor("31", sprintf "Test %s failed: %s" method.Name ex.Message)

                    TestRunner.PrintWithColor("36", sprintf "Cleaning up tests in %s..." testClass.Name)
                    classCleanupMethod.Invoke(testInstance, [| |]) |> ignore

        with ex ->
            TestRunner.PrintWithColor("31", sprintf "Error during class initialization/cleanup: %s" ex.Message)

        results

    static member RunTests(tests: Type array) (filter) =
        let allResults = System.Collections.Generic.List<TestResult>()
        let stopwatch = Stopwatch.StartNew()

        TestRunner.PrintWithColor("35", "Starting test run...")
        for testClass in tests do
            allResults.AddRange(TestRunner.RunTestMethods(testClass)(filter))
        stopwatch.Stop()


        // Summary of results
        allResults |> Seq.iter (function
            | Passed(name, time) -> ()
            | Failed(name, ex) -> TestRunner.PrintWithColor("31", sprintf "%s failed: %s" name (ex.ToString())))

        let passed = allResults |> Seq.filter (function Passed _ -> true | _ -> false) |> Seq.length
        let failed = allResults |> Seq.filter (function Failed _ -> true | _ -> false) |> Seq.length
        TestRunner.PrintWithColor("35", $"Test summary: {passed} passed, {failed} failed in {stopwatch.Elapsed}")

    static member GetTests(assemblies: Assembly array) =
        /// Convert a glob pattern to a regular expression pattern.
        let globToRegex (globPattern: string) =
            let escapedPattern = Regex.Escape(globPattern)
            let patternWithStars = escapedPattern.Replace("\\*", ".*")
            let finalPattern = patternWithStars.Replace("\\?", ".")
            "^" + finalPattern + "$"
        
        /// Check if a text matches the converted glob pattern.
        let isMatch (text: string) (globPattern: string) =
            let regexPattern = globToRegex globPattern
            let regex = new Regex(regexPattern)
            regex.IsMatch(text)

        let testTypes = assemblies |> Seq.collect _.GetTypes() |> Seq.filter(fun t -> t.GetCustomAttributes(typeof<TestClassAttribute>, true).Length > 0) |> Seq.toArray

        {|
            Run = fun (testsFilterStr) ->
                TestRunner.RunTests testTypes (fun t -> isMatch t.Name testsFilterStr)
        |}

[<EntryPoint>]
let main argv =
    TestRunner.GetTests([|Assembly.GetExecutingAssembly()|]).Run "MoveFile3"
    0