namespace SoloDatabase

open CowByteSpanMap
open Microsoft.Data.Sqlite
open System.Collections.Concurrent
open System.Collections.Generic
open System
open SoloDatabase.Types
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open Microsoft.FSharp.NativeInterop
open Utils
open Connections
open System.Text
open SoloDBEventsHelpers

// NativePtr operations for zero-copy JSON parsing from SQLite trigger callbacks
#nowarn "9"

type internal CollectionEventSystem<'T> internal (collectionName: string, eventSystem: EventSystem, createDb: SqliteConnection -> (unit -> unit) -> ISoloDB) =
    let insertingHandlerMap = Dictionary<InsertingHandler<'T>, ResizeArray<InsertingHandlerSystem>>(HashIdentity.Reference)
    let deletingHandlerMap = Dictionary<DeletingHandler<'T>, ResizeArray<DeletingHandlerSystem>>(HashIdentity.Reference)
    let updatingHandlerMap = Dictionary<UpdatingHandler<'T>, ResizeArray<UpdatingHandlerSystem>>(HashIdentity.Reference)
    let insertedHandlerMap = Dictionary<InsertedHandler<'T>, ResizeArray<InsertingHandlerSystem>>(HashIdentity.Reference)
    let deletedHandlerMap = Dictionary<DeletedHandler<'T>, ResizeArray<DeletingHandlerSystem>>(HashIdentity.Reference)
    let updatedHandlerMap = Dictionary<UpdatedHandler<'T>, ResizeArray<UpdatingHandlerSystem>>(HashIdentity.Reference)
    interface ISoloDBCollectionEvents<'T> with
        member this.OnInserting(handler: InsertingHandler<'T>): unit =
            let collectionNameBytes = Encoding.UTF8.GetBytes collectionName
            let list = eventSystem.InsertingHandlerMapping.GetOrAdd(collectionNameBytes, CowByteSpanMapValueFactory(fun _span -> ResizeArray()))
            try
                eventSystem.GlobalLock.Enter()
                let ctxs = ConcurrentStack<SoloDBItemEventContext<'T>>()
                let mutable sysHandlerRef = Unchecked.defaultof<InsertingHandlerSystem>
                let removeFromMap () =
                    match insertingHandlerMap.TryGetValue handler with
                    | true, handlers ->
                        handlers.Remove sysHandlerRef |> ignore
                        if handlers.Count = 0 then insertingHandlerMap.Remove handler |> ignore
                    | _ -> ()
                let sysHandler = InsertingHandlerSystem(fun conn session json ->
                    let mutable ctx = Unchecked.defaultof<SoloDBItemEventContext<'T> | null>
                    if not (ctxs.TryPop(&ctx)) then ctx <- new SoloDBItemEventContext<'T>(collectionName, createDb)
                    let jsonPtr =
                        if json.IsEmpty then NativePtr.nullPtr<byte>
                        else NativePtr.ofVoidPtr (Unsafe.AsPointer(&MemoryMarshal.GetReference json))
                    let result =
                        try
                            ctx.Reset (conn, session, jsonPtr, json.Length)
                            Connections.EnterEventHandlerScope(conn)
                            try handler.Invoke ctx |> Option.ofObj |> Option.defaultValue EventHandled
                            finally Connections.ExitEventHandlerScope(conn)
                        finally ctx.MarkDisposed(); ctxs.Push ctx
                    if result = RemoveHandler then removeFromMap ()
                    result)
                sysHandlerRef <- sysHandler
                let _added = list.Add(sysHandler)
                match insertingHandlerMap.TryGetValue handler with
                | true, handlers -> handlers.Add sysHandler
                | false, _ ->
                    let handlers = ResizeArray()
                    handlers.Add sysHandler
                    insertingHandlerMap.[handler] <- handlers
                ()
            finally eventSystem.GlobalLock.Exit()

        member this.OnDeleting(handler: DeletingHandler<'T>): unit =
            let collectionNameBytes = Encoding.UTF8.GetBytes collectionName
            let list = eventSystem.DeletingHandlerMapping.GetOrAdd(collectionNameBytes, CowByteSpanMapValueFactory(fun _span -> ResizeArray()))
            try
                eventSystem.GlobalLock.Enter()
                let ctxs = ConcurrentStack<SoloDBItemEventContext<'T>>()
                let mutable sysHandlerRef = Unchecked.defaultof<DeletingHandlerSystem>
                let removeFromMap () =
                    match deletingHandlerMap.TryGetValue handler with
                    | true, handlers ->
                        handlers.Remove sysHandlerRef |> ignore
                        if handlers.Count = 0 then deletingHandlerMap.Remove handler |> ignore
                    | _ -> ()
                let sysHandler = DeletingHandlerSystem(fun conn session json ->
                    let mutable ctx = Unchecked.defaultof<SoloDBItemEventContext<'T> | null>
                    if not (ctxs.TryPop(&ctx)) then ctx <- new SoloDBItemEventContext<'T>(collectionName, createDb)
                    let jsonPtr =
                        if json.IsEmpty then NativePtr.nullPtr<byte>
                        else NativePtr.ofVoidPtr (Unsafe.AsPointer(&MemoryMarshal.GetReference json))
                    let result =
                        try
                            ctx.Reset (conn, session, jsonPtr, json.Length)
                            Connections.EnterEventHandlerScope(conn)
                            try handler.Invoke ctx |> Option.ofObj |> Option.defaultValue EventHandled
                            finally Connections.ExitEventHandlerScope(conn)
                        finally ctx.MarkDisposed(); ctxs.Push ctx
                    if result = RemoveHandler then removeFromMap ()
                    result)
                sysHandlerRef <- sysHandler
                let _added = list.Add(sysHandler)
                match deletingHandlerMap.TryGetValue handler with
                | true, handlers -> handlers.Add sysHandler
                | false, _ ->
                    let handlers = ResizeArray()
                    handlers.Add sysHandler
                    deletingHandlerMap.[handler] <- handlers
                ()
            finally eventSystem.GlobalLock.Exit()

        member this.OnUpdating(handler: UpdatingHandler<'T>): unit =
            let collectionNameBytes = Encoding.UTF8.GetBytes collectionName
            let list = eventSystem.UpdatingHandlerMapping.GetOrAdd(collectionNameBytes, CowByteSpanMapValueFactory(fun _span -> ResizeArray()))
            try
                eventSystem.GlobalLock.Enter()
                let ctxs = ConcurrentStack<SoloDBUpdatingEventContext<'T>>()
                let mutable sysHandlerRef = Unchecked.defaultof<UpdatingHandlerSystem>
                let removeFromMap () =
                    match updatingHandlerMap.TryGetValue handler with
                    | true, handlers ->
                        handlers.Remove sysHandlerRef |> ignore
                        if handlers.Count = 0 then updatingHandlerMap.Remove handler |> ignore
                    | _ -> ()
                let sysHandler = UpdatingHandlerSystem(fun conn session jsonOld jsonOldSize jsonNew jsonNewSize ->
                    let mutable ctx = Unchecked.defaultof<SoloDBUpdatingEventContext<'T> | null>
                    if not (ctxs.TryPop(&ctx)) then ctx <- new SoloDBUpdatingEventContext<'T>(collectionName, createDb)
                    let result =
                        try
                            ctx.Reset (conn, session, jsonOld, jsonOldSize, jsonNew, jsonNewSize)
                            Connections.EnterEventHandlerScope(conn)
                            try handler.Invoke ctx |> Option.ofObj |> Option.defaultValue EventHandled
                            finally Connections.ExitEventHandlerScope(conn)
                        finally ctx.MarkDisposed(); ctxs.Push ctx
                    if result = RemoveHandler then removeFromMap ()
                    result)
                sysHandlerRef <- sysHandler
                let _added = list.Add(sysHandler)
                match updatingHandlerMap.TryGetValue handler with
                | true, handlers -> handlers.Add sysHandler
                | false, _ ->
                    let handlers = ResizeArray()
                    handlers.Add sysHandler
                    updatingHandlerMap.[handler] <- handlers
                ()
            finally eventSystem.GlobalLock.Exit()

        member this.OnInserted(handler: InsertedHandler<'T>): unit =
            let collectionNameBytes = Encoding.UTF8.GetBytes collectionName
            let list = eventSystem.InsertedHandlerMapping.GetOrAdd(collectionNameBytes, CowByteSpanMapValueFactory(fun _span -> ResizeArray()))
            try
                eventSystem.GlobalLock.Enter()
                let ctxs = ConcurrentStack<SoloDBItemEventContext<'T>>()
                let mutable sysHandlerRef = Unchecked.defaultof<InsertingHandlerSystem>
                let removeFromMap () =
                    match insertedHandlerMap.TryGetValue handler with
                    | true, handlers ->
                        handlers.Remove sysHandlerRef |> ignore
                        if handlers.Count = 0 then insertedHandlerMap.Remove handler |> ignore
                    | _ -> ()
                let sysHandler = InsertingHandlerSystem(fun conn session json ->
                    let mutable ctx = Unchecked.defaultof<SoloDBItemEventContext<'T> | null>
                    if not (ctxs.TryPop(&ctx)) then ctx <- new SoloDBItemEventContext<'T>(collectionName, createDb)
                    let jsonPtr =
                        if json.IsEmpty then NativePtr.nullPtr<byte>
                        else NativePtr.ofVoidPtr (Unsafe.AsPointer(&MemoryMarshal.GetReference json))
                    let result =
                        try
                            ctx.Reset (conn, session, jsonPtr, json.Length)
                            Connections.EnterEventHandlerScope(conn)
                            try handler.Invoke ctx |> Option.ofObj |> Option.defaultValue EventHandled
                            finally Connections.ExitEventHandlerScope(conn)
                        finally ctx.MarkDisposed(); ctxs.Push ctx
                    if result = RemoveHandler then removeFromMap ()
                    result)
                sysHandlerRef <- sysHandler
                let _added = list.Add(sysHandler)
                match insertedHandlerMap.TryGetValue handler with
                | true, handlers -> handlers.Add sysHandler
                | false, _ ->
                    let handlers = ResizeArray()
                    handlers.Add sysHandler
                    insertedHandlerMap.[handler] <- handlers
                ()
            finally eventSystem.GlobalLock.Exit()

        member this.OnDeleted(handler: DeletedHandler<'T>): unit =
            let collectionNameBytes = Encoding.UTF8.GetBytes collectionName
            let list = eventSystem.DeletedHandlerMapping.GetOrAdd(collectionNameBytes, CowByteSpanMapValueFactory(fun _span -> ResizeArray()))
            try
                eventSystem.GlobalLock.Enter()
                let ctxs = ConcurrentStack<SoloDBItemEventContext<'T>>()
                let mutable sysHandlerRef = Unchecked.defaultof<DeletingHandlerSystem>
                let removeFromMap () =
                    match deletedHandlerMap.TryGetValue handler with
                    | true, handlers ->
                        handlers.Remove sysHandlerRef |> ignore
                        if handlers.Count = 0 then deletedHandlerMap.Remove handler |> ignore
                    | _ -> ()
                let sysHandler = DeletingHandlerSystem(fun conn session json ->
                    let mutable ctx = Unchecked.defaultof<SoloDBItemEventContext<'T> | null>
                    if not (ctxs.TryPop(&ctx)) then ctx <- new SoloDBItemEventContext<'T>(collectionName, createDb)
                    let jsonPtr =
                        if json.IsEmpty then NativePtr.nullPtr<byte>
                        else NativePtr.ofVoidPtr (Unsafe.AsPointer(&MemoryMarshal.GetReference json))
                    let result =
                        try
                            ctx.Reset (conn, session, jsonPtr, json.Length)
                            Connections.EnterEventHandlerScope(conn)
                            try handler.Invoke ctx |> Option.ofObj |> Option.defaultValue EventHandled
                            finally Connections.ExitEventHandlerScope(conn)
                        finally ctx.MarkDisposed(); ctxs.Push ctx
                    if result = RemoveHandler then removeFromMap ()
                    result)
                sysHandlerRef <- sysHandler
                let _added = list.Add(sysHandler)
                match deletedHandlerMap.TryGetValue handler with
                | true, handlers -> handlers.Add sysHandler
                | false, _ ->
                    let handlers = ResizeArray()
                    handlers.Add sysHandler
                    deletedHandlerMap.[handler] <- handlers
                ()
            finally eventSystem.GlobalLock.Exit()

        member this.OnUpdated(handler: UpdatedHandler<'T>): unit =
            let collectionNameBytes = Encoding.UTF8.GetBytes collectionName
            let list = eventSystem.UpdatedHandlerMapping.GetOrAdd(collectionNameBytes, CowByteSpanMapValueFactory(fun _span -> ResizeArray()))
            try
                eventSystem.GlobalLock.Enter()
                let ctxs = ConcurrentStack<SoloDBUpdatingEventContext<'T>>()
                let mutable sysHandlerRef = Unchecked.defaultof<UpdatingHandlerSystem>
                let removeFromMap () =
                    match updatedHandlerMap.TryGetValue handler with
                    | true, handlers ->
                        handlers.Remove sysHandlerRef |> ignore
                        if handlers.Count = 0 then updatedHandlerMap.Remove handler |> ignore
                    | _ -> ()
                let sysHandler = UpdatingHandlerSystem(fun conn session jsonOld jsonOldSize jsonNew jsonNewSize ->
                    let mutable ctx = Unchecked.defaultof<SoloDBUpdatingEventContext<'T> | null>
                    if not (ctxs.TryPop(&ctx)) then ctx <- new SoloDBUpdatingEventContext<'T>(collectionName, createDb)
                    let result =
                        try
                            ctx.Reset (conn, session, jsonOld, jsonOldSize, jsonNew, jsonNewSize)
                            Connections.EnterEventHandlerScope(conn)
                            try handler.Invoke ctx |> Option.ofObj |> Option.defaultValue EventHandled
                            finally Connections.ExitEventHandlerScope(conn)
                        finally ctx.MarkDisposed(); ctxs.Push ctx
                    if result = RemoveHandler then removeFromMap ()
                    result)
                sysHandlerRef <- sysHandler
                let _added = list.Add(sysHandler)
                match updatedHandlerMap.TryGetValue handler with
                | true, handlers -> handlers.Add sysHandler
                | false, _ ->
                    let handlers = ResizeArray()
                    handlers.Add sysHandler
                    updatedHandlerMap.[handler] <- handlers
                ()
            finally eventSystem.GlobalLock.Exit()

        member this.Unregister(handler: InsertingHandler<'T>): unit =
            unregisterHandler eventSystem.GlobalLock collectionName eventSystem.InsertingHandlerMapping insertingHandlerMap handler
        member this.Unregister(handler: DeletingHandler<'T>): unit =
            unregisterHandler eventSystem.GlobalLock collectionName eventSystem.DeletingHandlerMapping deletingHandlerMap handler
        member this.Unregister(handler: UpdatingHandler<'T>): unit =
            unregisterHandler eventSystem.GlobalLock collectionName eventSystem.UpdatingHandlerMapping updatingHandlerMap handler
        member this.Unregister(handler: InsertedHandler<'T>): unit =
            unregisterHandler eventSystem.GlobalLock collectionName eventSystem.InsertedHandlerMapping insertedHandlerMap handler
        member this.Unregister(handler: DeletedHandler<'T>): unit =
            unregisterHandler eventSystem.GlobalLock collectionName eventSystem.DeletedHandlerMapping deletedHandlerMap handler
        member this.Unregister(handler: UpdatedHandler<'T>): unit =
            unregisterHandler eventSystem.GlobalLock collectionName eventSystem.UpdatedHandlerMapping updatedHandlerMap handler
