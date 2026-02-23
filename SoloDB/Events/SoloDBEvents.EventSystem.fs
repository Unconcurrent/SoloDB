namespace SoloDatabase

open CowByteSpanMap
open SoloDatabase.RawSqliteFunctions
open Microsoft.Data.Sqlite
open System
open SoloDatabase.Types
open Microsoft.FSharp.NativeInterop
open Utils
open System.Threading
open SoloDBEventsHelpers

// NativePtr operations for zero-copy JSON parsing from SQLite trigger callbacks
#nowarn "9"

type internal EventSystem internal () =
    let mutable sessionIndex = 0L
    member val internal GlobalLock = ReentrantSpinLock()
    member val internal InsertingHandlerMapping = CowByteSpanMap<ResizeArray<InsertingHandlerSystem>>()
    member val internal DeletingHandlerMapping = CowByteSpanMap<ResizeArray<DeletingHandlerSystem>>()
    member val internal UpdatingHandlerMapping = CowByteSpanMap<ResizeArray<UpdatingHandlerSystem>>()
    member val internal InsertedHandlerMapping = CowByteSpanMap<ResizeArray<InsertingHandlerSystem>>()
    member val internal DeletedHandlerMapping = CowByteSpanMap<ResizeArray<DeletingHandlerSystem>>()
    member val internal UpdatedHandlerMapping = CowByteSpanMap<ResizeArray<UpdatingHandlerSystem>>()

    member this.CreateFunctions(connection: SqliteConnection) =
        connection.CreateRawFunction("SHOULD_HANDLE_INSERTING", RawScalarFunc1(fun sqliteCtx sqliteCollectionName ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "The first argument of SHOULD_HANDLE_INSERTING must be a string"
            let sqliteCollectionNameUTF8 = sqliteCollectionName.GetBlobSpan()
            let should = if this.InsertingHandlerMapping.ContainsKey sqliteCollectionNameUTF8 then 1 else 0
            sqliteCtx.SetInt32 should
        ))

        connection.CreateRawFunction("ON_INSERTING_HANDLER", RawScalarFunc2(fun sqliteCtx sqliteCollectionName jsonNew ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "First argument must be TEXT collection name"
            if not jsonNew.IsText then invalidArg (nameof jsonNew) "Second argument must be TEXT JSON (use json(NEW.Value) in trigger)"
            let nameUtf8 = sqliteCollectionName.GetBlobSpan()
            let mutable handlers = Unchecked.defaultof<ResizeArray<InsertingHandlerSystem>>
            if not (this.InsertingHandlerMapping.TryGetValue(nameUtf8, &handlers)) then sqliteCtx.SetNull()
            else
            let nameBytes = nameUtf8.ToArray()
            let newUtf8Size = jsonNew.GetByteCount()
            let newUtf8 = jsonNew.GetBlobPointer()
            let struct (failed, msg) =
                runHandlerBody this.GlobalLock &sessionIndex connection handlers
                    (fun h conn session ->
                        let span = ReadOnlySpan<byte>(NativePtr.toVoidPtr newUtf8, newUtf8Size)
                        h.Invoke(conn, session, span))
                    (fun () -> ignore (this.InsertingHandlerMapping.Remove nameBytes))
                    "SoloDB inserting handler failed"
            if failed then sqliteCtx.SetText msg else sqliteCtx.SetNull()
        ))

        connection.CreateRawFunction("SHOULD_HANDLE_DELETING", RawScalarFunc1(fun sqliteCtx sqliteCollectionName ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "The first argument of SHOULD_HANDLE_DELETING must be a string"
            let sqliteCollectionNameUTF8 = sqliteCollectionName.GetBlobSpan()
            sqliteCtx.SetInt32(if this.DeletingHandlerMapping.ContainsKey sqliteCollectionNameUTF8 then 1 else 0)
        ))

        connection.CreateRawFunction("ON_DELETING_HANDLER", RawScalarFunc2(fun sqliteCtx sqliteCollectionName jsonOld ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "First argument must be TEXT collection name"
            if not jsonOld.IsText then invalidArg (nameof jsonOld) "Second argument must be TEXT JSON (use json(OLD.Value) in trigger)"
            let nameUtf8 = sqliteCollectionName.GetBlobSpan()
            let mutable handlers = Unchecked.defaultof<ResizeArray<DeletingHandlerSystem>>
            if not (this.DeletingHandlerMapping.TryGetValue(nameUtf8, &handlers)) then sqliteCtx.SetNull()
            else
            let nameBytes = nameUtf8.ToArray()
            let oldUtf8Size = jsonOld.GetByteCount()
            let oldUtf8 = jsonOld.GetBlobPointer()
            let struct (failed, msg) =
                runHandlerBody this.GlobalLock &sessionIndex connection handlers
                    (fun h conn session ->
                        let span = ReadOnlySpan<byte>(NativePtr.toVoidPtr oldUtf8, oldUtf8Size)
                        h.Invoke(conn, session, span))
                    (fun () -> ignore (this.DeletingHandlerMapping.Remove nameBytes))
                    "SoloDB deleting handler failed"
            if failed then sqliteCtx.SetText msg else sqliteCtx.SetNull()
        ))

        connection.CreateRawFunction("SHOULD_HANDLE_UPDATING", RawScalarFunc1(fun sqliteCtx sqliteCollectionName ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "The first argument of SHOULD_HANDLE_UPDATING must be a string"
            let sqliteCollectionNameUTF8 = sqliteCollectionName.GetBlobSpan()
            sqliteCtx.SetInt32(if this.UpdatingHandlerMapping.ContainsKey sqliteCollectionNameUTF8 then 1 else 0)
        ))

        connection.CreateRawFunction("ON_UPDATING_HANDLER", RawScalarFunc3(fun sqliteCtx sqliteCollectionName jsonOld jsonNew ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "First argument must be TEXT collection name"
            if not jsonOld.IsText then invalidArg (nameof jsonOld) "Second argument must be TEXT JSON (use json(OLD.Value) in trigger)"
            if not jsonNew.IsText then invalidArg (nameof jsonNew) "Third argument must be TEXT JSON (use json(NEW.Value) in trigger)"
            let nameUtf8 = sqliteCollectionName.GetBlobSpan()
            let mutable handlers = Unchecked.defaultof<ResizeArray<UpdatingHandlerSystem>>
            if not (this.UpdatingHandlerMapping.TryGetValue(nameUtf8, &handlers)) then sqliteCtx.SetNull()
            else
            let nameBytes = nameUtf8.ToArray()
            let oldUtf8Size = jsonOld.GetByteCount()
            let oldUtf8 = jsonOld.GetBlobPointer()
            let newUtf8Size = jsonNew.GetByteCount()
            let newUtf8 = jsonNew.GetBlobPointer()
            let struct (failed, msg) =
                runHandlerBody this.GlobalLock &sessionIndex connection handlers
                    (fun h conn session -> h.Invoke(conn, session, oldUtf8, oldUtf8Size, newUtf8, newUtf8Size))
                    (fun () -> ignore (this.UpdatingHandlerMapping.Remove nameBytes))
                    "SoloDB updating handler failed"
            if failed then sqliteCtx.SetText msg else sqliteCtx.SetNull()
        ))

        connection.CreateRawFunction("SHOULD_HANDLE_INSERTED", RawScalarFunc1(fun sqliteCtx sqliteCollectionName ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "The first argument of SHOULD_HANDLE_INSERTED must be a string"
            let sqliteCollectionNameUTF8 = sqliteCollectionName.GetBlobSpan()
            sqliteCtx.SetInt32(if this.InsertedHandlerMapping.ContainsKey sqliteCollectionNameUTF8 then 1 else 0)
        ))

        connection.CreateRawFunction("ON_INSERTED_HANDLER", RawScalarFunc2(fun sqliteCtx sqliteCollectionName jsonNew ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "First argument must be TEXT collection name"
            if not jsonNew.IsText then invalidArg (nameof jsonNew) "Second argument must be TEXT JSON (use json(NEW.Value) in trigger)"
            let nameUtf8 = sqliteCollectionName.GetBlobSpan()
            let mutable handlers = Unchecked.defaultof<ResizeArray<InsertingHandlerSystem>>
            if not (this.InsertedHandlerMapping.TryGetValue(nameUtf8, &handlers)) then sqliteCtx.SetNull()
            else
            let nameBytes = nameUtf8.ToArray()
            let newUtf8Size = jsonNew.GetByteCount()
            let newUtf8 = jsonNew.GetBlobPointer()
            let struct (failed, msg) =
                runHandlerBody this.GlobalLock &sessionIndex connection handlers
                    (fun h conn session ->
                        let span = ReadOnlySpan<byte>(NativePtr.toVoidPtr newUtf8, newUtf8Size)
                        h.Invoke(conn, session, span))
                    (fun () -> ignore (this.InsertedHandlerMapping.Remove nameBytes))
                    "SoloDB inserted handler failed"
            if failed then sqliteCtx.SetText msg else sqliteCtx.SetNull()
        ))

        connection.CreateRawFunction("SHOULD_HANDLE_DELETED", RawScalarFunc1(fun sqliteCtx sqliteCollectionName ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "The first argument of SHOULD_HANDLE_DELETED must be a string"
            let sqliteCollectionNameUTF8 = sqliteCollectionName.GetBlobSpan()
            sqliteCtx.SetInt32(if this.DeletedHandlerMapping.ContainsKey sqliteCollectionNameUTF8 then 1 else 0)
        ))

        connection.CreateRawFunction("ON_DELETED_HANDLER", RawScalarFunc2(fun sqliteCtx sqliteCollectionName jsonOld ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "First argument must be TEXT collection name"
            if not jsonOld.IsText then invalidArg (nameof jsonOld) "Second argument must be TEXT JSON (use json(OLD.Value) in trigger)"
            let nameUtf8 = sqliteCollectionName.GetBlobSpan()
            let mutable handlers = Unchecked.defaultof<ResizeArray<DeletingHandlerSystem>>
            if not (this.DeletedHandlerMapping.TryGetValue(nameUtf8, &handlers)) then sqliteCtx.SetNull()
            else
            let nameBytes = nameUtf8.ToArray()
            let oldUtf8Size = jsonOld.GetByteCount()
            let oldUtf8 = jsonOld.GetBlobPointer()
            let struct (failed, msg) =
                runHandlerBody this.GlobalLock &sessionIndex connection handlers
                    (fun h conn session ->
                        let span = ReadOnlySpan<byte>(NativePtr.toVoidPtr oldUtf8, oldUtf8Size)
                        h.Invoke(conn, session, span))
                    (fun () -> ignore (this.DeletedHandlerMapping.Remove nameBytes))
                    "SoloDB deleted handler failed"
            if failed then sqliteCtx.SetText msg else sqliteCtx.SetNull()
        ))

        connection.CreateRawFunction("SHOULD_HANDLE_UPDATED", RawScalarFunc1(fun sqliteCtx sqliteCollectionName ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "The first argument of SHOULD_HANDLE_UPDATED must be a string"
            let sqliteCollectionNameUTF8 = sqliteCollectionName.GetBlobSpan()
            sqliteCtx.SetInt32(if this.UpdatedHandlerMapping.ContainsKey sqliteCollectionNameUTF8 then 1 else 0)
        ))

        connection.CreateRawFunction("ON_UPDATED_HANDLER", RawScalarFunc3(fun sqliteCtx sqliteCollectionName jsonOld jsonNew ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "First argument must be TEXT collection name"
            if not jsonOld.IsText then invalidArg (nameof jsonOld) "Second argument must be TEXT JSON (use json(OLD.Value) in trigger)"
            if not jsonNew.IsText then invalidArg (nameof jsonNew) "Third argument must be TEXT JSON (use json(NEW.Value) in trigger)"
            let nameUtf8 = sqliteCollectionName.GetBlobSpan()
            let mutable handlers = Unchecked.defaultof<ResizeArray<UpdatingHandlerSystem>>
            if not (this.UpdatedHandlerMapping.TryGetValue(nameUtf8, &handlers)) then sqliteCtx.SetNull()
            else
            let nameBytes = nameUtf8.ToArray()
            let oldUtf8Size = jsonOld.GetByteCount()
            let oldUtf8 = jsonOld.GetBlobPointer()
            let newUtf8Size = jsonNew.GetByteCount()
            let newUtf8 = jsonNew.GetBlobPointer()
            let struct (failed, msg) =
                runHandlerBody this.GlobalLock &sessionIndex connection handlers
                    (fun h conn session -> h.Invoke(conn, session, oldUtf8, oldUtf8Size, newUtf8, newUtf8Size))
                    (fun () -> ignore (this.UpdatedHandlerMapping.Remove nameBytes))
                    "SoloDB updated handler failed"
            if failed then sqliteCtx.SetText msg else sqliteCtx.SetNull()
        ))
        ()
