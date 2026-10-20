namespace SoloDatabase

open System
open System.Runtime.ExceptionServices
open System.Threading.Tasks
open SQLitePCL

[<Struct>]
type private NativeScriptAdvance =
    | EndOfScript
    | CurrentStatement
    | ParserTail of source: NativeStatementSource * offset: int

/// Parser-tail progression belongs to dynamic SQL consumers, independently of statement ownership.
[<Struct>]
type internal NativeSqlScript =
    val private cache: NativeStatementCache
    val private root: NativeStatementScope
    val private arguments: INativeArgumentBinding
    val mutable private current: NativeStatementScope
    val mutable private offset: int
    val mutable private initialized: bool
    val mutable private available: bool
    val mutable private pending: bool
    val mutable private doneState: bool
    val mutable private closed: bool
    val mutable private failure: exn
    val mutable private affected: int
    val mutable private budget: NativeExecutionBudget
    new(cache: NativeStatementCache, source: NativeStatementSource, parameters: obj, timeout, checkReader) =
        let mutable root = new NativeStatementScope(cache, source, 0, NativeExecutionBudget(timeout), checkReader)
        let arguments =
            try root.Statement.Capture(parameters)
            with error -> root.Abort(error); reraise()
        { cache = cache; root = root; arguments = arguments; current = root; offset = 0
          initialized = false; available = true; pending = false; doneState = true; closed = false
          failure = null; affected = -1; budget = root.Budget }
    private new(cache: NativeStatementCache, root: NativeStatementScope, arguments: INativeArgumentBinding) =
        { cache = cache; root = root; arguments = arguments; current = root; offset = 0
          initialized = false; available = true; pending = false; doneState = true; closed = false
          failure = null; affected = -1; budget = root.Budget }
    static member Create<'Input, 'State>(cache: NativeStatementCache, sql, plan: SqlParameterPlan<'Input, 'State>, input: 'Input, timeout, checkReader) =
        let mutable root = new NativeStatementScope(cache, NativeStatementSource.Text sql, 0, NativeExecutionBudget(timeout), checkReader)
        let arguments =
            try root.Statement.Capture(plan, input)
            with error -> root.Abort(error); reraise()
        NativeSqlScript(cache, root, arguments)
    static member CreateAsync(cache: NativeStatementCache, source: NativeStatementSource, parameters: obj, timeout, checkReader) = task {
        let! prepared = NativeStatementScope.CreateAsync(cache, source, 0, NativeExecutionBudget(timeout), checkReader)
        let mutable root = prepared
        try
            let arguments = root.Statement.Capture(parameters)
            return NativeSqlScript(cache, root, arguments)
        with error ->
            root.Abort(error)
            return (ExceptionDispatchInfo.Capture(error).Throw(); Unchecked.defaultof<_>)
    }
    static member Continue(cache: NativeStatementCache, root: NativeStatementScope, arguments: INativeArgumentBinding) =
        let mutable script = NativeSqlScript(cache, root, arguments)
        script.initialized <- true
        script.doneState <- not root.HasRow
        script
    static member CompleteTail(cache: NativeStatementCache, root: NativeStatementScope, arguments: INativeArgumentBinding) =
        let mutable script = NativeSqlScript.Continue(cache, root, arguments)
        script.Dispose()
    static member ConsumeTail(cache: NativeStatementCache, root: NativeStatementScope, arguments: INativeArgumentBinding) =
        let mutable script = NativeSqlScript.Continue(cache, root, arguments)
        try
            let result = script.Consume()
            script.Dispose()
            result
        with error -> script.Abort(error); reraise()
    member private this.Check() =
        if this.closed then raise (ObjectDisposedException("SQL execution lease"))
        this.root.Statement |> ignore
    member this.TryBlobSource(result: NativeBlobResult) =
        this.Check()
        try result.Resolve(&this.current)
        finally this.budget <- this.current.Budget
    member this.Database = this.cache.Database
    member this.Generation = this.root.Generation
    member this.Row = this.Check(); this.current.Row
    member this.Statement = this.Check(); this.current.Statement
    member this.HasCurrentRow = not this.closed && this.available && this.current.HasRow
    member this.HasPendingRow = not this.closed && this.pending
    member this.FieldCount = if not this.available || not this.current.HasStatement then 0 else this.current.Statement.ColumnCount
    member this.RecordsAffected = this.affected
    member private this.ResetCurrent() =
        try
            if this.available then
                this.current.Reset()
                if this.current.RecordsAffected >= 0 then
                    this.affected <- max 0 this.affected + this.current.RecordsAffected
                this.budget <- this.current.Budget
                if this.offset <> 0 then this.current.Finish(null, false)
            this.pending <- false
            this.doneState <- true
        with error ->
            this.failure <- error
            reraise()
    member private this.Advance(result: NativeBlobResult) =
        if not this.initialized then
            this.initialized <- true
            CurrentStatement
        else
            let entry = this.current.Statement
            let next = entry.EndOffset
            let bytes = entry.Bytes
            if not (isNull result) then result.Dispose()
            this.ResetCurrent()
            if next >= bytes.Length - 1 || bytes.[next] = 0uy then
                this.available <- false
                EndOfScript
            else
                this.offset <- next
                this.available <- false
                ParserTail(NativeStatementSource.Prepared entry, next)
    member private this.SetCurrent(scope: NativeStatementScope) =
        this.current <- scope
        this.available <- true
    member private this.BeginResult(result: NativeBlobResult) =
        if not this.current.HasStatement then
            if this.offset <> 0 then
                try this.current.Finish(null, false)
                with error -> this.failure <- error; reraise()
            this.available <- false
            false
        else
            this.current.Bind(this.arguments)
            if not (isNull result) then result.Begin(this.cache, &this.current)
            true
    member private this.CompleteResult(row: bool, result: NativeBlobResult) =
        if not (isNull result) then result.Complete(&this.current)
        this.doneState <- not row
        if this.current.Statement.ColumnCount > 0 then
            this.pending <- row
            true
        else false
    member this.NextResult(result: NativeBlobResult) =
        this.Check()
        let mutable found = false
        let mutable more = this.available
        while more && not found do
            match this.Advance(result) with
            | EndOfScript -> more <- false
            | ParserTail(source, offset) ->
                this.SetCurrent(new NativeStatementScope(this.cache, source, offset, this.budget, false))
            | CurrentStatement -> ()
            if more then
                if this.BeginResult(result) then
                    found <- this.CompleteResult(this.current.Step(), result)
                else more <- false
        found
    static member private NextResultAsync(initialScript: NativeSqlScript) = task {
        let mutable script = initialScript
        try
            script.Check()
            let mutable found = false
            let mutable more = script.available
            while more && not found do
                match script.Advance(null) with
                | EndOfScript -> more <- false
                | ParserTail(source, offset) ->
                    let! scope = NativeStatementScope.CreateAsync(script.cache, source, offset, script.budget, false, script.Generation)
                    script.SetCurrent(scope)
                | CurrentStatement -> ()
                if more then
                    if script.BeginResult(null) then
                        let! struct (scope, row) = script.current.StepAsync()
                        script.current <- scope
                        found <- script.CompleteResult(row, null)
                    else more <- false
            return struct (script, found)
        with error ->
            script.Abort(error)
            return (ExceptionDispatchInfo.Capture(error).Throw(); Unchecked.defaultof<_>)
    }
    static member private ConsumeAsync(initialScript: NativeSqlScript) = task {
        let mutable script = initialScript
        try
            let mutable more = true
            while more do
                script.Check()
                if script.pending then script.pending <- false
                while script.available && not script.doneState do
                    let! struct (scope, row) = script.current.StepAsync()
                    script.current <- scope
                    script.doneState <- not row
                let! struct (updated, found) = NativeSqlScript.NextResultAsync(script)
                script <- updated
                more <- found
            return struct (script, script.affected)
        with error ->
            script.Abort(error)
            return (ExceptionDispatchInfo.Capture(error).Throw(); Unchecked.defaultof<_>)
    }
    member this.ConsumeAsync() = NativeSqlScript.ConsumeAsync(this)
    member this.NextResult() = this.NextResult(null)
    member this.Read() =
        this.Check()
        if this.pending then this.pending <- false; true
        elif this.doneState || not this.available then false
        else
            let row = this.current.Step()
            this.doneState <- not row
            row
    member this.Dispose() =
        if not this.closed then
            let mutable failure = this.failure
            if isNull failure && this.root.IsCurrent then
                try
                    if this.available then
                        while this.NextResult() do ()
                with error ->
                    // Successful early disposal retains best-effort script tail execution.
                    if this.root.IsCurrent && not (isNull this.cache.Database.Observer) then
                        try this.cache.Database.Observer.Failed(error) with _ -> ()
                    failure <- this.failure
                    if isNull failure && this.root.IsCurrent && this.current.IsCurrent
                       && not (Object.ReferenceEquals(this.root.Statement, this.current.Statement)) then
                        // Ignore the execution failure, while retaining cleanup-only failures.
                        try this.current.FinishFailedTail(error)
                        with cleanup -> failure <- cleanup
            try
                if this.available && this.offset <> 0 then this.current.Finish(failure, false)
            with error -> if isNull failure then failure <- error
            this.closed <- true
            let mutable root = this.root
            try root.Finish(failure, true)
            with error -> if isNull failure then failure <- error
            if not (isNull failure) then ExceptionDispatchInfo.Capture(failure).Throw()
    member this.Abort(error: exn) =
        this.failure <- error
        try this.Dispose() with _ -> ()
    member this.Consume() =
        let mutable more = true
        while more do
            while this.Read() do ()
            more <- this.NextResult()
        this.affected
