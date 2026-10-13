namespace SoloDatabase

open System

#nowarn "9"

/// Describes the logical content mutation, independently of compressed storage.
type FileContentChangeKind =
    | Write = 0
    | Resize = 1
    | Copy = 2

/// Describes an actual metadata row mutation. Replacement may remove then add a row.
type FileMetadataChangeKind =
    | Add = 0
    | Remove = 1
    | Update = 2

type internal FileSystemEventKind =
    | FileCreating = 0
    | FileCreated = 1
    | FileDeleting = 2
    | FileDeleted = 3
    | FileUpdating = 4
    | FileUpdated = 5
    | DirectoryCreating = 6
    | DirectoryCreated = 7
    | DirectoryDeleting = 8
    | DirectoryDeleted = 9
    | DirectoryUpdating = 10
    | DirectoryUpdated = 11
    | FileMetadataChanging = 12
    | FileMetadataChanged = 13
    | DirectoryMetadataChanging = 14
    | DirectoryMetadataChanged = 15
    | FileContentChanging = 16
    | FileContentChanged = 17

[<Struct>]
type internal BorrowedEventText =
    { Pointer: nativeint; Length: int }
    member this.Read() =
        if this.Length < 0 then null
        elif this.Length = 0 then String.Empty
        else System.Text.Encoding.UTF8.GetString(Microsoft.FSharp.NativeInterop.NativePtr.ofNativeInt<byte> this.Pointer, this.Length)

[<Struct>]
type internal FileSystemRowFacts =
    { Path: BorrowedEventText; Name: BorrowedEventText
      Length: int64; Created: int64; Modified: int64 }

[<Struct>]
type internal FileSystemMetadataFacts =
    { Path: BorrowedEventText; Key: BorrowedEventText
      OldValue: BorrowedEventText; NewValue: BorrowedEventText
      Kind: FileMetadataChangeKind }

[<Struct>]
type internal FileSystemContentFacts =
    { FileId: int64; Offset: int64; Length: int64
      OldLength: int64; NewLength: int64; Kind: FileContentChangeKind }

[<Struct>]
type internal FileSystemChange =
    | Row of oldRow: FileSystemRowFacts * newRow: FileSystemRowFacts
    | Metadata of metadataFacts: FileSystemMetadataFacts
    | Content of contentFacts: FileSystemContentFacts

type internal IFileSystemEventDispatch =
    abstract HasContentHandlers: bool
    abstract Dispatch: NativeSqliteConnection * FileSystemEventKind * FileSystemChange -> unit

/// Statement checkpoints preserve outer native facts during nested callbacks.
type internal IFileSystemCallbackResources =
    abstract Depth: int
    abstract Restore: int -> unit
    abstract Reset: unit -> unit
