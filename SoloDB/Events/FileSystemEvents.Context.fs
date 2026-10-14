namespace SoloDatabase

open System
open NativeSQLiteTools

module private FileSystemText =
    let inline read (cache: string byref) (text: BorrowedEventText) =
        if isNull cache then cache <- text.Read()
        cache

/// Borrowed row facts remain stable during recursive SQL callbacks.
type internal FileSystemRowEventContext(createDb) =
    inherit EventContextBase(createDb)
    let mutable oldRow = Unchecked.defaultof<FileSystemRowFacts>
    let mutable row = Unchecked.defaultof<FileSystemRowFacts>
    let mutable path, name, oldPath, oldName = null, null, null, null
    member this.Reset(connection, session, change) =
        match change with
        | Row(oldFacts, newFacts) -> oldRow <- oldFacts; row <- newFacts
        | _ -> invalidArg (nameof change) "Expected header row facts."
        path <- null; name <- null; oldPath <- null; oldName <- null
        this.Begin(connection, session)
    override _.ClearInvocation() =
        oldRow <- Unchecked.defaultof<_>
        row <- Unchecked.defaultof<_>
        path <- null; name <- null; oldPath <- null; oldName <- null
    interface ISoloDBFileEventContext with
        member this.Path = this.Check(); FileSystemText.read &path row.Path
        member this.Name = this.Check(); FileSystemText.read &name row.Name
        member this.Length = this.Check(); row.Length
        member this.Created = this.Check(); DateTimeOffset.FromUnixTimeMilliseconds(row.Created)
        member this.Modified = this.Check(); DateTimeOffset.FromUnixTimeMilliseconds(row.Modified)
    interface ISoloDBFileUpdatingEventContext with
        member this.OldPath = this.Check(); FileSystemText.read &oldPath oldRow.Path
        member this.OldName = this.Check(); FileSystemText.read &oldName oldRow.Name
        member this.OldLength = this.Check(); oldRow.Length
        member this.OldCreated = this.Check(); DateTimeOffset.FromUnixTimeMilliseconds(oldRow.Created)
        member this.OldModified = this.Check(); DateTimeOffset.FromUnixTimeMilliseconds(oldRow.Modified)

type internal FileSystemMetadataEventContext(createDb) =
    inherit EventContextBase(createDb)
    let mutable facts = Unchecked.defaultof<FileSystemMetadataFacts>
    let mutable path, key, oldValue, newValue = null, null, null, null
    member this.Reset(connection, session, change) =
        match change with
        | Metadata value -> facts <- value
        | _ -> invalidArg (nameof change) "Expected metadata facts."
        path <- null; key <- null; oldValue <- null; newValue <- null
        this.Begin(connection, session)
    override _.ClearInvocation() =
        facts <- Unchecked.defaultof<_>
        path <- null; key <- null; oldValue <- null; newValue <- null
    interface ISoloDBFileSystemMetadataEventContext with
        member this.Path = this.Check(); FileSystemText.read &path facts.Path
        member this.Key = this.Check(); FileSystemText.read &key facts.Key
        member this.Kind = this.Check(); facts.Kind
        member this.OldValue = this.Check(); FileSystemText.read &oldValue facts.OldValue
        member this.NewValue = this.Check(); FileSystemText.read &newValue facts.NewValue

type internal FileSystemContentEventContext(createDb) =
    inherit EventContextBase(createDb)
    let mutable facts = Unchecked.defaultof<FileSystemContentFacts>
    let mutable path: string = null
    member this.Reset(connection, session, change) =
        match change with
        | Content value -> facts <- value
        | _ -> invalidArg (nameof change) "Expected content facts."
        path <- null
        this.Begin(connection, session)
    override _.ClearInvocation() =
        facts <- Unchecked.defaultof<_>
        path <- null
    interface ISoloDBFileContentEventContext with
        member this.Path =
            this.Check()
            if isNull path then
                let mutable statement = this.Connection.OpenStatement("SELECT FullPath FROM SoloDBFileHeader WHERE Id = @Id")
                try
                    statement.BindInt64(1, facts.FileId)
                    path <- if statement.Step() then statement.Row.GetString(0) else Seq.head (Seq.empty<string>)
                    statement.Dispose()
                with error -> statement.Abort(error); reraise()
            path
        member this.Offset = this.Check(); facts.Offset
        member this.Length = this.Check(); facts.Length
        member this.OldLength = this.Check(); facts.OldLength
        member this.NewLength = this.Check(); facts.NewLength
        member this.Kind = this.Check(); facts.Kind

/// Borrowed row facts remain stable during recursive SQL callbacks.
type internal FileSystemDirectoryEventContext(createDb) =
    inherit EventContextBase(createDb)
    let mutable oldRow = Unchecked.defaultof<FileSystemRowFacts>
    let mutable row = Unchecked.defaultof<FileSystemRowFacts>
    let mutable path, name, oldPath, oldName = null, null, null, null
    member this.Reset(connection, session, change) =
        match change with
        | Row(oldFacts, newFacts) -> oldRow <- oldFacts; row <- newFacts
        | _ -> invalidArg (nameof change) "Expected header row facts."
        path <- null; name <- null; oldPath <- null; oldName <- null
        this.Begin(connection, session)
    override _.ClearInvocation() =
        oldRow <- Unchecked.defaultof<_>
        row <- Unchecked.defaultof<_>
        path <- null; name <- null; oldPath <- null; oldName <- null
    interface ISoloDBDirectoryEventContext with
        member this.Path = this.Check(); FileSystemText.read &path row.Path
        member this.Name = this.Check(); FileSystemText.read &name row.Name
        member this.Created = this.Check(); DateTimeOffset.FromUnixTimeMilliseconds(row.Created)
        member this.Modified = this.Check(); DateTimeOffset.FromUnixTimeMilliseconds(row.Modified)
    interface ISoloDBDirectoryUpdatingEventContext with
        member this.OldPath = this.Check(); FileSystemText.read &oldPath oldRow.Path
        member this.OldName = this.Check(); FileSystemText.read &oldName oldRow.Name
        member this.OldCreated = this.Check(); DateTimeOffset.FromUnixTimeMilliseconds(oldRow.Created)
        member this.OldModified = this.Check(); DateTimeOffset.FromUnixTimeMilliseconds(oldRow.Modified)
