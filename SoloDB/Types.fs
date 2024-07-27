namespace SoloDatabase.Types

open System.Linq.Expressions
open System
open System.Threading
open System.Collections.Generic
open System.Runtime.CompilerServices

type internal DisposableMutex =
    val mutex: Mutex

    internal new(name: string) =
        let mutex = new Mutex(false, name)
        mutex.WaitOne() |> ignore

        { mutex = mutex }


    interface IDisposable with
        member this.Dispose() =
            this.mutex.ReleaseMutex()
            this.mutex.Dispose()


[<CLIMutable>]
type DbObjectRow = {
    Id: int64
    ValueJSON: string
}


[<CLIMutable>]
type Metadata = {
    Key: string
    Value: string
}

[<CLIMutable>]
type SoloDBFileHeader = {
    Id: int64
    Name: string
    FullPath: string
    DirectoryId: int64
    Length: int64
    Created: DateTimeOffset
    Modified: DateTimeOffset
    Hash: byte array
    Metadata: IReadOnlyDictionary<string, string>
}

[<CLIMutable>]
type SoloDBDirectoryHeader = {
    Id: int64
    Name: string
    FullPath: string
    ParentId: Nullable<int64>
    Created: DateTimeOffset
    Modified: DateTimeOffset
    Metadata: IReadOnlyDictionary<string, string>
}

[<Struct>]
type SoloDBEntryHeader = 
    | File of file: SoloDBFileHeader
    | Directory of directory: SoloDBDirectoryHeader

    member this.Name = 
        match this with
        | File f -> f.Name
        | Directory d -> d.Name

    member this.FullPath = 
        match this with
        | File f -> f.FullPath
        | Directory d -> d.FullPath

    member this.DirectoryId = 
        match this with
        | File f -> f.DirectoryId |> Nullable
        | Directory d -> d.ParentId

    member this.Created = 
        match this with
        | File f -> f.Created
        | Directory d -> d.Created

    member this.Modified = 
        match this with
        | File f -> f.Modified
        | Directory d -> d.Modified

    member this.Metadata = 
        match this with
        | File f -> f.Metadata
        | Directory d -> d.Metadata

[<CLIMutable>]
type internal SoloDBFileChunk = {
    Id: int64
    FileId: int64
    Number: int64
    Data: byte array
}