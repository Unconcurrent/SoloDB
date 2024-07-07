module CSharpProxyTests

open CSharpTests
open Microsoft.VisualStudio.TestTools.UnitTesting

[<TestClass>]
type CSharpProxyStandardTests() =
    inherit StandardTests()

[<TestClass>]
type CSharpProxyDynamicTests() =
    inherit DynamicTests()

[<TestClass>]
type CSharpProxyReadMeTests() =
    inherit ReadMeTests()