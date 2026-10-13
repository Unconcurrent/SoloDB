namespace SoloDatabase

open CowByteSpanMap
open SoloDatabase.RawSqliteFunctions
open System
open SoloDatabase.Types
open Microsoft.FSharp.NativeInterop
open Utils
open System.Threading
open SoloDBEventsHelpers

// NativePtr operations for zero-copy JSON parsing from SQLite trigger callbacks
#nowarn "9"

module internal CollectionHandlerInvocation =
    let inserting (handler: InsertingHandlerSystem) connection session (struct (ptr: nativeptr<byte>, length: int)) =
        handler.Invoke(connection, session, ReadOnlySpan<byte>(NativePtr.toVoidPtr ptr, length))

    let deleting (handler: DeletingHandlerSystem) connection session (struct (ptr: nativeptr<byte>, length: int)) =
        handler.Invoke(connection, session, ReadOnlySpan<byte>(NativePtr.toVoidPtr ptr, length))

    let updating (handler: UpdatingHandlerSystem) connection session (struct (oldPtr, oldLength, newPtr, newLength)) =
        handler.Invoke(connection, session, oldPtr, oldLength, newPtr, newLength)

type internal EventSystem internal () =
    let mutable sessionIndex = 0L
    let globalLock = UtilsReflection.ReentrantSpinLock()
    let fileSystemDispatch = FileSystemEventDispatch(globalLock)
    member _.GlobalLock = globalLock
    member _.FileSystemDispatch = fileSystemDispatch
    member val internal InsertingHandlerMapping = CowByteSpanMap<EventHandlerList<InsertingHandlerSystem>>()
    member val internal DeletingHandlerMapping = CowByteSpanMap<EventHandlerList<DeletingHandlerSystem>>()
    member val internal UpdatingHandlerMapping = CowByteSpanMap<EventHandlerList<UpdatingHandlerSystem>>()
    member val internal InsertedHandlerMapping = CowByteSpanMap<EventHandlerList<InsertingHandlerSystem>>()
    member val internal DeletedHandlerMapping = CowByteSpanMap<EventHandlerList<DeletingHandlerSystem>>()
    member val internal UpdatedHandlerMapping = CowByteSpanMap<EventHandlerList<UpdatingHandlerSystem>>()

    member this.CreateFunctions(connection: NativeSqliteConnection) =
        fileSystemDispatch.CreateFunctions(connection)
        // Empty snapshots need no SQLite text-to-bytes conversion; validate arguments first.
        connection.CreateRawFunction("SHOULD_HANDLE_INSERTING", RawScalarFunc1(fun sqliteCtx sqliteCollectionName ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "The first argument of SHOULD_HANDLE_INSERTING must be a string"
            let should = this.InsertingHandlerMapping.Count > 0 && this.InsertingHandlerMapping.ContainsKey(sqliteCollectionName.GetBlobSpan())
            sqliteCtx.SetInt32(if should then 1 else 0)
        ))

        connection.CreateRawFunction("ON_INSERTING_HANDLER", RawScalarFunc2(fun sqliteCtx sqliteCollectionName jsonNew ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "First argument must be TEXT collection name"
            if not jsonNew.IsText then invalidArg (nameof jsonNew) "Second argument must be TEXT JSON (use json(NEW.Value) in trigger)"
            let nameUtf8 = sqliteCollectionName.GetBlobSpan()
            let mutable entry = Unchecked.defaultof<EventHandlerList<InsertingHandlerSystem>>
            if not (this.InsertingHandlerMapping.TryGetValue(nameUtf8, &entry)) then sqliteCtx.SetNull()
            else
            let newUtf8Size = jsonNew.GetByteCount()
            let newUtf8 = jsonNew.GetBlobPointer()
            let struct (failed, msg) =
                runHandlerBody this.GlobalLock &sessionIndex connection entry.Handlers
                    (struct (newUtf8, newUtf8Size)) CollectionHandlerInvocation.inserting
                    entry.RemoveMapping
                    "SoloDB inserting handler failed"
            if failed then sqliteCtx.SetText msg else sqliteCtx.SetNull()
        ))

        connection.CreateRawFunction("SHOULD_HANDLE_DELETING", RawScalarFunc1(fun sqliteCtx sqliteCollectionName ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "The first argument of SHOULD_HANDLE_DELETING must be a string"
            let should = this.DeletingHandlerMapping.Count > 0 && this.DeletingHandlerMapping.ContainsKey(sqliteCollectionName.GetBlobSpan())
            sqliteCtx.SetInt32(if should then 1 else 0)
        ))

        connection.CreateRawFunction("ON_DELETING_HANDLER", RawScalarFunc2(fun sqliteCtx sqliteCollectionName jsonOld ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "First argument must be TEXT collection name"
            if not jsonOld.IsText then invalidArg (nameof jsonOld) "Second argument must be TEXT JSON (use json(OLD.Value) in trigger)"
            let nameUtf8 = sqliteCollectionName.GetBlobSpan()
            let mutable entry = Unchecked.defaultof<EventHandlerList<DeletingHandlerSystem>>
            if not (this.DeletingHandlerMapping.TryGetValue(nameUtf8, &entry)) then sqliteCtx.SetNull()
            else
            let oldUtf8Size = jsonOld.GetByteCount()
            let oldUtf8 = jsonOld.GetBlobPointer()
            let struct (failed, msg) =
                runHandlerBody this.GlobalLock &sessionIndex connection entry.Handlers
                    (struct (oldUtf8, oldUtf8Size)) CollectionHandlerInvocation.deleting
                    entry.RemoveMapping
                    "SoloDB deleting handler failed"
            if failed then sqliteCtx.SetText msg else sqliteCtx.SetNull()
        ))

        connection.CreateRawFunction("SHOULD_HANDLE_UPDATING", RawScalarFunc1(fun sqliteCtx sqliteCollectionName ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "The first argument of SHOULD_HANDLE_UPDATING must be a string"
            let should = this.UpdatingHandlerMapping.Count > 0 && this.UpdatingHandlerMapping.ContainsKey(sqliteCollectionName.GetBlobSpan())
            sqliteCtx.SetInt32(if should then 1 else 0)
        ))

        connection.CreateRawFunction("ON_UPDATING_HANDLER", RawScalarFunc3(fun sqliteCtx sqliteCollectionName jsonOld jsonNew ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "First argument must be TEXT collection name"
            if not jsonOld.IsText then invalidArg (nameof jsonOld) "Second argument must be TEXT JSON (use json(OLD.Value) in trigger)"
            if not jsonNew.IsText then invalidArg (nameof jsonNew) "Third argument must be TEXT JSON (use json(NEW.Value) in trigger)"
            let nameUtf8 = sqliteCollectionName.GetBlobSpan()
            let mutable entry = Unchecked.defaultof<EventHandlerList<UpdatingHandlerSystem>>
            if not (this.UpdatingHandlerMapping.TryGetValue(nameUtf8, &entry)) then sqliteCtx.SetNull()
            else
            let oldUtf8Size = jsonOld.GetByteCount()
            let oldUtf8 = jsonOld.GetBlobPointer()
            let newUtf8Size = jsonNew.GetByteCount()
            let newUtf8 = jsonNew.GetBlobPointer()
            let struct (failed, msg) =
                runHandlerBody this.GlobalLock &sessionIndex connection entry.Handlers
                    (struct (oldUtf8, oldUtf8Size, newUtf8, newUtf8Size)) CollectionHandlerInvocation.updating
                    entry.RemoveMapping
                    "SoloDB updating handler failed"
            if failed then sqliteCtx.SetText msg else sqliteCtx.SetNull()
        ))

        connection.CreateRawFunction("SHOULD_HANDLE_INSERTED", RawScalarFunc1(fun sqliteCtx sqliteCollectionName ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "The first argument of SHOULD_HANDLE_INSERTED must be a string"
            let should = this.InsertedHandlerMapping.Count > 0 && this.InsertedHandlerMapping.ContainsKey(sqliteCollectionName.GetBlobSpan())
            sqliteCtx.SetInt32(if should then 1 else 0)
        ))

        connection.CreateRawFunction("ON_INSERTED_HANDLER", RawScalarFunc2(fun sqliteCtx sqliteCollectionName jsonNew ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "First argument must be TEXT collection name"
            if not jsonNew.IsText then invalidArg (nameof jsonNew) "Second argument must be TEXT JSON (use json(NEW.Value) in trigger)"
            let nameUtf8 = sqliteCollectionName.GetBlobSpan()
            let mutable entry = Unchecked.defaultof<EventHandlerList<InsertingHandlerSystem>>
            if not (this.InsertedHandlerMapping.TryGetValue(nameUtf8, &entry)) then sqliteCtx.SetNull()
            else
            let newUtf8Size = jsonNew.GetByteCount()
            let newUtf8 = jsonNew.GetBlobPointer()
            let struct (failed, msg) =
                runHandlerBody this.GlobalLock &sessionIndex connection entry.Handlers
                    (struct (newUtf8, newUtf8Size)) CollectionHandlerInvocation.inserting
                    entry.RemoveMapping
                    "SoloDB inserted handler failed"
            if failed then sqliteCtx.SetText msg else sqliteCtx.SetNull()
        ))

        connection.CreateRawFunction("SHOULD_HANDLE_DELETED", RawScalarFunc1(fun sqliteCtx sqliteCollectionName ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "The first argument of SHOULD_HANDLE_DELETED must be a string"
            let should = this.DeletedHandlerMapping.Count > 0 && this.DeletedHandlerMapping.ContainsKey(sqliteCollectionName.GetBlobSpan())
            sqliteCtx.SetInt32(if should then 1 else 0)
        ))

        connection.CreateRawFunction("ON_DELETED_HANDLER", RawScalarFunc2(fun sqliteCtx sqliteCollectionName jsonOld ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "First argument must be TEXT collection name"
            if not jsonOld.IsText then invalidArg (nameof jsonOld) "Second argument must be TEXT JSON (use json(OLD.Value) in trigger)"
            let nameUtf8 = sqliteCollectionName.GetBlobSpan()
            let mutable entry = Unchecked.defaultof<EventHandlerList<DeletingHandlerSystem>>
            if not (this.DeletedHandlerMapping.TryGetValue(nameUtf8, &entry)) then sqliteCtx.SetNull()
            else
            let oldUtf8Size = jsonOld.GetByteCount()
            let oldUtf8 = jsonOld.GetBlobPointer()
            let struct (failed, msg) =
                runHandlerBody this.GlobalLock &sessionIndex connection entry.Handlers
                    (struct (oldUtf8, oldUtf8Size)) CollectionHandlerInvocation.deleting
                    entry.RemoveMapping
                    "SoloDB deleted handler failed"
            if failed then sqliteCtx.SetText msg else sqliteCtx.SetNull()
        ))

        connection.CreateRawFunction("SHOULD_HANDLE_UPDATED", RawScalarFunc1(fun sqliteCtx sqliteCollectionName ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "The first argument of SHOULD_HANDLE_UPDATED must be a string"
            let should = this.UpdatedHandlerMapping.Count > 0 && this.UpdatedHandlerMapping.ContainsKey(sqliteCollectionName.GetBlobSpan())
            sqliteCtx.SetInt32(if should then 1 else 0)
        ))

        connection.CreateRawFunction("ON_UPDATED_HANDLER", RawScalarFunc3(fun sqliteCtx sqliteCollectionName jsonOld jsonNew ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "First argument must be TEXT collection name"
            if not jsonOld.IsText then invalidArg (nameof jsonOld) "Second argument must be TEXT JSON (use json(OLD.Value) in trigger)"
            if not jsonNew.IsText then invalidArg (nameof jsonNew) "Third argument must be TEXT JSON (use json(NEW.Value) in trigger)"
            let nameUtf8 = sqliteCollectionName.GetBlobSpan()
            let mutable entry = Unchecked.defaultof<EventHandlerList<UpdatingHandlerSystem>>
            if not (this.UpdatedHandlerMapping.TryGetValue(nameUtf8, &entry)) then sqliteCtx.SetNull()
            else
            let oldUtf8Size = jsonOld.GetByteCount()
            let oldUtf8 = jsonOld.GetBlobPointer()
            let newUtf8Size = jsonNew.GetByteCount()
            let newUtf8 = jsonNew.GetBlobPointer()
            let struct (failed, msg) =
                runHandlerBody this.GlobalLock &sessionIndex connection entry.Handlers
                    (struct (oldUtf8, oldUtf8Size, newUtf8, newUtf8Size)) CollectionHandlerInvocation.updating
                    entry.RemoveMapping
                    "SoloDB updated handler failed"
            if failed then sqliteCtx.SetText msg else sqliteCtx.SetNull()
        ))
        ()
