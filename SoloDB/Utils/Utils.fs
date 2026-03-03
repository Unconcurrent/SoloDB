namespace SoloDatabase

open System.Runtime.CompilerServices

[<assembly: InternalsVisibleTo("Tests")>]
[<assembly: InternalsVisibleTo("Tests.EventApi")>]
[<assembly: InternalsVisibleTo("CSharpTests")>]
[<assembly: InternalsVisibleTo("BenchMaster")>]
do ()
