namespace SoloDatabase

open System
open SQLitePCL

/// Immutable lineage belongs to the producing borrow. Current view definitions
/// cannot authorize an old row, and validation touches only its physical source.
[<Sealed; AllowNullLiteral>]
type internal NativeBlobAdmission private (cache: NativeStatementCache, statement: NativePreparedStatement, query: NativeBlobIdentity.Query, initialBudget: NativeExecutionBudget) =
    let generation = statement.Generation
    let borrow = statement.Borrow
    let reprepare = raw.sqlite3_stmt_status(statement.Handle,5,0)
    let mutable budget = initialBudget
    let mutable callbacks = cache.Database.CallbackEpoch
    let mutable cookie = 0L
    let mutable proof = ValueNone
    let mutable physical = false
    let current () =
        statement.InUse && statement.Borrow=borrow && cache.Database.IsOpen
        && cache.Database.Generation=generation
    let unchanged () = current() && raw.sqlite3_stmt_status(statement.Handle,5,0)=reprepare
    let retire () = proof <- ValueNone
    member private _.Initialize() =
        let mutable physicalSchema = null
        let mutable physicalTable = null
        let mutable compatible = true
        for i = 0 to raw.sqlite3_column_count(statement.Handle)-1 do
            let schema=raw.sqlite3_column_database_name(statement.Handle,i).utf8_to_string()
            let table=raw.sqlite3_column_table_name(statement.Handle,i).utf8_to_string()
            if isNull schema || isNull table then compatible <- false
            elif isNull physicalSchema then physicalSchema <- schema; physicalTable <- table
            elif not (NativeBlobIdentity.equalIdentifier schema physicalSchema && NativeBlobIdentity.equalIdentifier table physicalTable) then compatible <- false
        if compatible && not (isNull physicalSchema) then
            use catalog = new NativeBlobCatalog(cache,budget)
            try
                let before = catalog.SourceVersion(physicalSchema)
                proof <- catalog.Resolve(physicalSchema,query)
                cookie <- catalog.SourceVersion(physicalSchema)
                if before<>cookie || not (catalog.Coherent && unchanged()) then retire()
            finally budget <- catalog.Budget
            match proof with
            | ValueSome source when not (NativeBlobIdentity.equalIdentifier source.Schema physicalSchema && NativeBlobIdentity.equalIdentifier source.Table physicalTable) -> retire()
            | _ -> ()
        callbacks <- cache.Database.CallbackEpoch
    static member Create(cache, scope: byref<NativeStatementScope>, query) =
        let admission = new NativeBlobAdmission(cache, scope.Statement, query, scope.Budget)
        try
            admission.Initialize()
            admission
        finally scope.SetBudget(admission.Budget)
    member _.Budget = budget
    member _.Complete(scope: NativeStatementScope) =
        budget <- scope.Budget
        if not (unchanged()) || cache.Database.CallbackEpoch<>callbacks then retire()
    member _.Resolve(scope: NativeStatementScope) =
        budget <- scope.Budget
        if not (unchanged()) then retire()
        match proof with
        | ValueNone -> ValueNone
        | ValueSome source ->
            use catalog = new NativeBlobCatalog(cache,budget)
            try
                let now = catalog.SourceVersion(source.Schema)
                if now<>cookie || not (catalog.Coherent && unchanged()) then retire()
                elif not physical then
                    proof <- catalog.Physical(source)
                    physical <- true
                    if catalog.SourceVersion(source.Schema)<>cookie || not (catalog.Coherent && unchanged()) then retire()
                proof
            finally budget <- catalog.Budget
    member _.Dispose() = retire()
    interface IDisposable with member this.Dispose() = this.Dispose()

/// One public command retains syntax for its most recently prepared result only.
[<Sealed; AllowNullLiteral>]
type internal NativeBlobSyntax() =
    let mutable sql: string = null
    let mutable finish = -1
    let mutable query = ValueNone
    member _.Query(statement: NativePreparedStatement) =
        if not (Object.ReferenceEquals(sql, statement.Sql)) || finish <> statement.EndOffset then
            let parsed = NativeBlobIdentity.tryQuery (raw.sqlite3_sql(statement.Handle).utf8_to_string())
            sql <- statement.Sql
            finish <- statement.EndOffset
            query <- parsed
        query

/// Optional physical-column admission is owned by one public reader execution.
[<Sealed; AllowNullLiteral>]
type internal NativeBlobResult(syntax: NativeBlobSyntax) =
    let mutable admission: NativeBlobAdmission = null
    member this.Begin(cache, scope: byref<NativeStatementScope>) =
        this.Dispose()
        if scope.Statement.ColumnCount > 1 then
            match syntax.Query(scope.Statement) with
            | ValueSome query -> admission <- NativeBlobAdmission.Create(cache, &scope, query)
            | _ -> ()
    member _.Complete(scope: byref<NativeStatementScope>) =
        if not (isNull admission) then
            try admission.Complete(scope)
            finally scope.SetBudget(admission.Budget)
    member _.Resolve(scope: byref<NativeStatementScope>) =
        if isNull admission then ValueNone else
        try admission.Resolve(scope)
        finally scope.SetBudget(admission.Budget)
    member _.Dispose() =
        if not (isNull admission) then admission.Dispose(); admission <- null
