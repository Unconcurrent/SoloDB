namespace SoloDatabase

open System
open NativeSQLiteTools
open FileStorageCoreChunks

module internal FileStorageContentEvents =
    let private dispatcher (connection: NativeSqliteConnection) =
        match connection with
        | :? NativeCachingDbConnection as cached -> cached.FileSystemEventDispatch
        | _ -> Unchecked.defaultof<IFileSystemEventDispatch>

    let version (connection: NativeSqliteConnection) =
        match connection with
        | :? NativeCachingDbConnection as cached -> cached.EventDispatchState.MutationVersion
        | _ -> 0L

    let prepare connection (facts: FileSystemContentFacts) =
        let events = dispatcher connection
        if facts.Length = 0L || isNull (box events) || not events.HasContentHandlers then facts
        else
            let before = version connection
            events.Dispatch(connection, FileSystemEventKind.FileContentChanging, Content facts)
            if version connection = before then facts
            else
                // Only callback-side mutations require a second database read. Primitive
                // callbacks leave the validated identity and length intact.
                let currentLength = getFileLengthById connection facts.FileId
                match facts.Kind with
                | FileContentChangeKind.Write ->
                    { facts with OldLength = currentLength; NewLength = max currentLength (facts.Offset + facts.Length) }
                | FileContentChangeKind.Resize ->
                    { facts with Offset = min currentLength facts.NewLength
                                 Length = abs (currentLength - facts.NewLength); OldLength = currentLength }
                | _ -> { facts with OldLength = currentLength }

    let completed (connection: NativeSqliteConnection) (facts: FileSystemContentFacts) =
        let events = dispatcher connection
        if not (isNull (box events)) then
            match connection with
            | :? NativeCachingDbConnection as cached -> cached.EventDispatchState.RecordMutation()
            | _ -> ()
            if facts.Length <> 0L && events.HasContentHandlers then
                events.Dispatch(connection, FileSystemEventKind.FileContentChanged, Content facts)
