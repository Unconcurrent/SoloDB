namespace SoloDatabase

open System

/// One acquired scalar operation stays with its invocation and native argument binding.
[<Sealed>]
type internal NativeQueryEquality(equal: Func<string,string,bool>) =
    static member FunctionName = "QUERY_EQUAL"
    member _.Equal(left,right) = equal.Invoke(left,right)

[<AllowNullLiteral; Sealed>]
type internal NativeQueryFrame(database: NativeDatabase, equal: Func<int,string,string,bool>) =
    let generation = database.Generation
    member _.Equal(owner: NativeDatabase,index,left,right) =
        if not(Object.ReferenceEquals(owner,database)) then
            invalidOp "The query operation belongs to another connection."
        database.EnsureGeneration generation
        equal.Invoke(index,left,right)

