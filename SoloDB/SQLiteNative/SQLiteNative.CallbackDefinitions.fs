namespace SoloDatabase

open System
open SQLitePCL
open SoloDatabase.RawSqliteFunctions

/// The existing raw callback owner supplies observation, conversion and error containment.
type internal NativeAggregateStep<'State> = delegate of 'State * ReadOnlySpan<SqliteRawValue> -> 'State

[<AbstractClass>]
type internal NativeAggregateFunction() =
    abstract Install: NativeDatabase * string * int * SqliteFunctionFlags -> unit

[<Sealed>]
type private AggregateState<'State>(seed: 'State) =
    member val Value = seed with get, set
    member val Failure: exn = null with get, set

[<Sealed>]
type internal NativeAggregateFunction<'State, 'Result>(seed: 'State, accumulate: NativeAggregateStep<'State>, result: Func<'State, 'Result>, preserveManagedFailure: bool) =
    inherit NativeAggregateFunction()
    new(seed, accumulate, result) = NativeAggregateFunction<'State, 'Result>(seed, accumulate, result, false)
    override _.Install(database, name, arity, flags) =
        let state (context: sqlite3_context) =
            if isNull context.state then context.state <- AggregateState(seed)
            context.state :?> AggregateState<'State>
        let step = RawCallbackState(database, RawScalarFunc(fun context args ->
            let current = state context.Context
            if isNull current.Failure then
                try current.Value <- accumulate.Invoke(current.Value, args)
                with error ->
                    current.Failure <- error
                    if preserveManagedFailure then reraise()), preserveManagedFailure)
        let finish = RawCallbackState(database, RawScalarFunc(fun context _ ->
            let current = state context.Context
            try
                if isNull current.Failure then NativeScalarFunctions.write &context (result.Invoke(current.Value))
                elif preserveManagedFailure then context.SetNull()
                else Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(current.Failure).Throw()
            finally context.Context.state <- null), preserveManagedFailure)
        let stepCallback = delegate_function_aggregate_step(fun context _ args -> step.InvokeManaged(context, args))
        let finalCallback = delegate_function_aggregate_final(fun context _ -> finish.InvokeManaged(context, Array.empty))
        database.Check(raw.sqlite3_create_function(database.Handle, name, arity, raw.SQLITE_UTF8 ||| int flags, null, stepCallback, finalCallback))

[<Struct>]
type internal NativeRegisteredFunction =
    | Scalar of scalar: RawScalarFunc * scalarFlags: SqliteFunctionFlags
    | ManagedScalar of scalar: RawScalarFunc * scalarFlags: SqliteFunctionFlags
    | Aggregate of aggregate: NativeAggregateFunction * aggregateFlags: SqliteFunctionFlags

module internal NativeCallbackRegistration =
    let install database name arity = function
        | Scalar(callback, flags) -> RawFunctionRegistration.create database name arity callback flags
        | ManagedScalar(callback, flags) -> RawFunctionRegistration.createManaged database name arity callback flags
        | Aggregate(callback, flags) -> callback.Install(database, name, arity, flags)
    let collation (database: NativeDatabase) name (comparison: Comparison<string>) =
        let generation = database.Generation
        let callback =
            if isNull comparison then null else
            strdelegate_collation(fun _ left right ->
                database.ObserveCallback()
                if database.HasCallbackFailure then 0
                else
                    try comparison.Invoke(left, right)
                    with error -> database.RecordCallbackFailure(generation, error); 0)
        database.Check(raw.sqlite3_create_collation(database.Handle, name, null, callback))

/// A logical connection retains its registrations while a physical owner is idle.
[<Struct>]
type internal NativeConnectionRegistrations =
    { Functions: Collections.Generic.Dictionary<struct (string * int), struct (string * NativeRegisteredFunction)>
      Collations: Collections.Generic.Dictionary<string, Comparison<string>>
      Extensions: Collections.Generic.HashSet<struct (string * string)>
      ExtensionsEnabled: bool }
