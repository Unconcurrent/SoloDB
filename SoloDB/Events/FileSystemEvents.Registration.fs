namespace SoloDatabase

open System
open System.Collections.Generic
open SoloDBCollectionEventDispatch

module internal FileSystemEventRegistration =
    let get (events: EventSystem) createDb =
        let dispatch = events.FileSystemDispatch
        events.GlobalLock.Enter()
        try
            if isNull (box dispatch.Registration) then
                let maps = Array.init 18 (fun _ -> Dictionary<Delegate, ResizeArray<FileSystemHandlerSystem>>(HashIdentity.Reference))
                let register kind (handler: Delegate) createContext reset invoke =
                    if isNull handler then nullArg (nameof handler)
                    events.GlobalLock.Enter()
                    try
                        let map = maps.[int kind]
                        let list = dispatch.Handlers.[int kind].Handlers
                        let slots = CallbackSlots(createContext)
                        let mutable systemHandler = Unchecked.defaultof<FileSystemHandlerSystem>
                        systemHandler <- FileSystemHandlerSystem(fun connection session facts ->
                            let context: EventContextBase = slots.Borrow()
                            let result =
                                try
                                    reset context connection session facts
                                    withHandlerFaultProtocol connection (fun () ->
                                        let value = invoke handler context
                                        if isNull (box value) then EventHandled else value)
                                finally
                                    context.End()
                                    slots.Return()
                            if result = RemoveHandler then
                                match map.TryGetValue handler with
                                | true, entries ->
                                    entries.Remove(systemHandler) |> ignore
                                    if entries.Count = 0 then map.Remove(handler) |> ignore
                                | _ -> ()
                            result)
                        list.Add(systemHandler)
                        match map.TryGetValue handler with
                        | true, entries -> entries.Add(systemHandler)
                        | _ -> map.Add(handler, ResizeArray([|systemHandler|]))
                    finally events.GlobalLock.Exit()
                let unregister kind (handler: Delegate) =
                    if events.GlobalLock.IsHeldByCurrentThread then
                        raise (InvalidOperationException("Cannot call Unregister from within a handler. Return SoloDBEventsResult.RemoveHandler instead."))
                    events.GlobalLock.Enter()
                    try
                        let map = maps.[int kind]
                        match map.TryGetValue handler with
                        | true, entries ->
                            let list = dispatch.Handlers.[int kind].Handlers
                            for handler in entries do list.Remove(handler) |> ignore
                            map.Remove(handler) |> ignore
                        | _ -> ()
                    finally events.GlobalLock.Exit()
                let createRow () = new FileSystemRowEventContext(createDb) :> EventContextBase
                let resetRow (context: EventContextBase) connection session facts =
                    (context :?> FileSystemRowEventContext).Reset(connection, session, facts)
                let createDirectory () = new FileSystemDirectoryEventContext(createDb) :> EventContextBase
                let resetDirectory (context: EventContextBase) connection session facts =
                    (context :?> FileSystemDirectoryEventContext).Reset(connection, session, facts)
                let createMetadata () = new FileSystemMetadataEventContext(createDb) :> EventContextBase
                let resetMetadata (context: EventContextBase) connection session facts =
                    (context :?> FileSystemMetadataEventContext).Reset(connection, session, facts)
                let createContent () = new FileSystemContentEventContext(createDb) :> EventContextBase
                let resetContent (context: EventContextBase) connection session facts =
                    (context :?> FileSystemContentEventContext).Reset(connection, session, facts)
                dispatch.Registration <-
                    { new ISoloDBFileSystemEvents with
                        member _.OnFileCreating(handler) =
                            register FileSystemEventKind.FileCreating handler createRow resetRow
                                (fun h context -> (h :?> FileCreatingHandler).Invoke((context :> obj) :?> ISoloDBFileEventContext))
                        member _.Unregister(handler: FileCreatingHandler) = unregister FileSystemEventKind.FileCreating handler
                        member _.OnFileCreated(handler) =
                            register FileSystemEventKind.FileCreated handler createRow resetRow
                                (fun h context -> (h :?> FileCreatedHandler).Invoke((context :> obj) :?> ISoloDBFileEventContext))
                        member _.Unregister(handler: FileCreatedHandler) = unregister FileSystemEventKind.FileCreated handler
                        member _.OnFileDeleting(handler) =
                            register FileSystemEventKind.FileDeleting handler createRow resetRow
                                (fun h context -> (h :?> FileDeletingHandler).Invoke((context :> obj) :?> ISoloDBFileEventContext))
                        member _.Unregister(handler: FileDeletingHandler) = unregister FileSystemEventKind.FileDeleting handler
                        member _.OnFileDeleted(handler) =
                            register FileSystemEventKind.FileDeleted handler createRow resetRow
                                (fun h context -> (h :?> FileDeletedHandler).Invoke((context :> obj) :?> ISoloDBFileEventContext))
                        member _.Unregister(handler: FileDeletedHandler) = unregister FileSystemEventKind.FileDeleted handler
                        member _.OnFileUpdating(handler) =
                            register FileSystemEventKind.FileUpdating handler createRow resetRow
                                (fun h context -> (h :?> FileUpdatingHandler).Invoke((context :> obj) :?> ISoloDBFileUpdatingEventContext))
                        member _.Unregister(handler: FileUpdatingHandler) = unregister FileSystemEventKind.FileUpdating handler
                        member _.OnFileUpdated(handler) =
                            register FileSystemEventKind.FileUpdated handler createRow resetRow
                                (fun h context -> (h :?> FileUpdatedHandler).Invoke((context :> obj) :?> ISoloDBFileUpdatingEventContext))
                        member _.Unregister(handler: FileUpdatedHandler) = unregister FileSystemEventKind.FileUpdated handler
                        member _.OnDirectoryCreating(handler) =
                            register FileSystemEventKind.DirectoryCreating handler createDirectory resetDirectory
                                (fun h context -> (h :?> DirectoryCreatingHandler).Invoke((context :> obj) :?> ISoloDBDirectoryEventContext))
                        member _.Unregister(handler: DirectoryCreatingHandler) = unregister FileSystemEventKind.DirectoryCreating handler
                        member _.OnDirectoryCreated(handler) =
                            register FileSystemEventKind.DirectoryCreated handler createDirectory resetDirectory
                                (fun h context -> (h :?> DirectoryCreatedHandler).Invoke((context :> obj) :?> ISoloDBDirectoryEventContext))
                        member _.Unregister(handler: DirectoryCreatedHandler) = unregister FileSystemEventKind.DirectoryCreated handler
                        member _.OnDirectoryDeleting(handler) =
                            register FileSystemEventKind.DirectoryDeleting handler createDirectory resetDirectory
                                (fun h context -> (h :?> DirectoryDeletingHandler).Invoke((context :> obj) :?> ISoloDBDirectoryEventContext))
                        member _.Unregister(handler: DirectoryDeletingHandler) = unregister FileSystemEventKind.DirectoryDeleting handler
                        member _.OnDirectoryDeleted(handler) =
                            register FileSystemEventKind.DirectoryDeleted handler createDirectory resetDirectory
                                (fun h context -> (h :?> DirectoryDeletedHandler).Invoke((context :> obj) :?> ISoloDBDirectoryEventContext))
                        member _.Unregister(handler: DirectoryDeletedHandler) = unregister FileSystemEventKind.DirectoryDeleted handler
                        member _.OnDirectoryUpdating(handler) =
                            register FileSystemEventKind.DirectoryUpdating handler createDirectory resetDirectory
                                (fun h context -> (h :?> DirectoryUpdatingHandler).Invoke((context :> obj) :?> ISoloDBDirectoryUpdatingEventContext))
                        member _.Unregister(handler: DirectoryUpdatingHandler) = unregister FileSystemEventKind.DirectoryUpdating handler
                        member _.OnDirectoryUpdated(handler) =
                            register FileSystemEventKind.DirectoryUpdated handler createDirectory resetDirectory
                                (fun h context -> (h :?> DirectoryUpdatedHandler).Invoke((context :> obj) :?> ISoloDBDirectoryUpdatingEventContext))
                        member _.Unregister(handler: DirectoryUpdatedHandler) = unregister FileSystemEventKind.DirectoryUpdated handler
                        member _.OnFileMetadataChanging(handler) =
                            register FileSystemEventKind.FileMetadataChanging handler createMetadata resetMetadata
                                (fun h context -> (h :?> FileMetadataChangingHandler).Invoke((context :> obj) :?> ISoloDBFileSystemMetadataEventContext))
                        member _.Unregister(handler: FileMetadataChangingHandler) = unregister FileSystemEventKind.FileMetadataChanging handler
                        member _.OnFileMetadataChanged(handler) =
                            register FileSystemEventKind.FileMetadataChanged handler createMetadata resetMetadata
                                (fun h context -> (h :?> FileMetadataChangedHandler).Invoke((context :> obj) :?> ISoloDBFileSystemMetadataEventContext))
                        member _.Unregister(handler: FileMetadataChangedHandler) = unregister FileSystemEventKind.FileMetadataChanged handler
                        member _.OnDirectoryMetadataChanging(handler) =
                            register FileSystemEventKind.DirectoryMetadataChanging handler createMetadata resetMetadata
                                (fun h context -> (h :?> DirectoryMetadataChangingHandler).Invoke((context :> obj) :?> ISoloDBFileSystemMetadataEventContext))
                        member _.Unregister(handler: DirectoryMetadataChangingHandler) = unregister FileSystemEventKind.DirectoryMetadataChanging handler
                        member _.OnDirectoryMetadataChanged(handler) =
                            register FileSystemEventKind.DirectoryMetadataChanged handler createMetadata resetMetadata
                                (fun h context -> (h :?> DirectoryMetadataChangedHandler).Invoke((context :> obj) :?> ISoloDBFileSystemMetadataEventContext))
                        member _.Unregister(handler: DirectoryMetadataChangedHandler) = unregister FileSystemEventKind.DirectoryMetadataChanged handler
                        member _.OnFileContentChanging(handler) =
                            register FileSystemEventKind.FileContentChanging handler createContent resetContent
                                (fun h context -> (h :?> FileContentChangingHandler).Invoke((context :> obj) :?> ISoloDBFileContentEventContext))
                        member _.Unregister(handler: FileContentChangingHandler) = unregister FileSystemEventKind.FileContentChanging handler
                        member _.OnFileContentChanged(handler) =
                            register FileSystemEventKind.FileContentChanged handler createContent resetContent
                                (fun h context -> (h :?> FileContentChangedHandler).Invoke((context :> obj) :?> ISoloDBFileContentEventContext))
                        member _.Unregister(handler: FileContentChangedHandler) = unregister FileSystemEventKind.FileContentChanged handler
                    }
            dispatch.Registration
        finally events.GlobalLock.Exit()
