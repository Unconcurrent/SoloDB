namespace SoloDatabase.RawSqliteFunctions

open System
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Text
open System.Threading
open SQLitePCL
open SoloDatabase

/// Raw scalar registration shares one callback, result and lifetime owner.
[<Extension>]
type internal SqliteConnectionRawExtensions =
    [<Extension>]
    static member CreateRawFunction(connection: NativeSqliteConnection, name: string, argCount: int, func: RawScalarFunc,
            [<Optional; DefaultParameterValue(SqliteFunctionFlags.None)>] flags: SqliteFunctionFlags) =
        RawFunctionRegistration.create connection.DatabaseOwner name argCount func flags

    [<Extension>]
    static member CreateRawFunction(connection: NativeSqliteConnection, name: string, func: RawScalarFunc0,
            [<Optional; DefaultParameterValue(SqliteFunctionFlags.None)>] flags: SqliteFunctionFlags) =
        if isNull func then nullArg (nameof func)
        RawFunctionRegistration.create connection.DatabaseOwner name 0 (RawScalarFunc(fun context _ -> func.Invoke(&context))) flags

    [<Extension>]
    static member CreateRawFunction(connection: NativeSqliteConnection, name: string, func: RawScalarFunc1,
            [<Optional; DefaultParameterValue(SqliteFunctionFlags.None)>] flags: SqliteFunctionFlags) =
        if isNull func then nullArg (nameof func)
        RawFunctionRegistration.create connection.DatabaseOwner name 1 (RawScalarFunc(fun context args ->
            let value = args.[0]
            func.Invoke(&context, &value))) flags

    [<Extension>]
    static member CreateRawFunction(connection: NativeSqliteConnection, name: string, func: RawScalarFunc2,
            [<Optional; DefaultParameterValue(SqliteFunctionFlags.None)>] flags: SqliteFunctionFlags) =
        if isNull func then nullArg (nameof func)
        RawFunctionRegistration.create connection.DatabaseOwner name 2 (RawScalarFunc(fun context args ->
            let first = args.[0]
            let second = args.[1]
            func.Invoke(&context, &first, &second))) flags

    [<Extension>]
    static member CreateRawFunction(connection: NativeSqliteConnection, name: string, func: RawScalarFunc3,
            [<Optional; DefaultParameterValue(SqliteFunctionFlags.None)>] flags: SqliteFunctionFlags) =
        if isNull func then nullArg (nameof func)
        RawFunctionRegistration.create connection.DatabaseOwner name 3 (RawScalarFunc(fun context args ->
            let first = args.[0]
            let second = args.[1]
            let third = args.[2]
            func.Invoke(&context, &first, &second, &third))) flags

    [<Extension>]
    static member RemoveRawFunction(connection: NativeSqliteConnection, name: string, argCount: int) =
        RawFunctionRegistration.remove connection.Handle name argCount
