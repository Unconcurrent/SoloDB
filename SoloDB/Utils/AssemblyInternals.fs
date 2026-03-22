namespace SoloDatabase

open System.Runtime.CompilerServices

[<assembly: InternalsVisibleTo("Tests")>]
[<assembly: InternalsVisibleTo("Tests.EventApi")>]
[<assembly: InternalsVisibleTo("CSharpTests")>]
[<assembly: InternalsVisibleTo("BenchMaster")>]
[<assembly: InternalsVisibleTo("SqlDu.Engine.C1.Spec")>]
[<assembly: InternalsVisibleTo("SqlDu.C1.Corpus")>]
[<assembly: InternalsVisibleTo("SingleSqlHydration.SliceA.Tests")>]
[<assembly: InternalsVisibleTo("BankingApp.Tests")>]
do ()
