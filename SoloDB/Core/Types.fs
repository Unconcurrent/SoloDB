namespace SoloDatabase.Types

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Runtime.InteropServices

/// <summary>
/// This is for internal use only, don't touch.
/// </summary>
[<CLIMutable>]
type internal DbObjectRow = {
    /// If the Id is NULL then the ValueJSON is a error message encoded in a JSON string.
    Id: Nullable<int64>
    ValueJSON: string
    /// JSON object containing hydrated DBRefMany collection data.
    /// Null when no DBRefMany hydration is active (non-queryable paths, types without DBRefMany).
    /// Shape: {"PropName": [{"Id": n, "Value": {...}}, ...], ...}
    HydrationJSON: string
}

[<CLIMutable>]
type internal Metadata = {
    Key: string
    Value: string
}

/// <summary>
/// Header information for a file stored in the SoloDB file system.
/// </summary>
[<CLIMutable>]
type SoloDBFileHeader = {
    Id: int64
    Name: string
    FullPath: string
    DirectoryId: int64
    Length: int64
    Created: DateTimeOffset
    Modified: DateTimeOffset
    Metadata: IReadOnlyDictionary<string, string>
}

/// <summary>
/// Header information for a directory in the SoloDB file system.
/// </summary>
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

[<CLIMutable>]
type BulkFileData = {
    /// <summary>The full path where the file should be stored.</summary>
    FullPath: string
    /// <summary>The binary content of the file.</summary>
    Data: byte array
    /// <summary>Optional. The creation timestamp to set for the file.</summary>
    Created: Nullable<DateTimeOffset>
    /// <summary>Optional. The modification timestamp to set for the file.</summary>
    Modified: Nullable<DateTimeOffset>
}

/// <summary>
/// A discriminated union representing either a file or directory entry in the SoloDB file system.
/// </summary>
[<Struct>]
type SoloDBEntryHeader = 
    /// <summary>A file entry.</summary>
    | File of file: SoloDBFileHeader
    /// <summary>A directory entry.</summary>
    | Directory of directory: SoloDBDirectoryHeader

    /// <summary>The name of the file or directory.</summary>
    member this.Name = 
        match this with
        | File f -> f.Name
        | Directory d -> d.Name

    /// <summary>The full path of the file or directory.</summary>
    member this.FullPath = 
        match this with
        | File f -> f.FullPath
        | Directory d -> d.FullPath

    /// <summary>The Id of the parent directory, or null for root entries.</summary>
    member this.DirectoryId = 
        match this with
        | File f -> f.DirectoryId |> Nullable
        | Directory d -> d.ParentId

    /// <summary>The creation timestamp.</summary>
    member this.Created = 
        match this with
        | File f -> f.Created
        | Directory d -> d.Created

    /// <summary>The last modification timestamp.</summary>
    member this.Modified = 
        match this with
        | File f -> f.Modified
        | Directory d -> d.Modified

    /// <summary>The key-value metadata associated with the entry.</summary>
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

type internal SoloDBConfiguration = {
    /// The general switch to enable or disable caching.
    /// By disabling it, any cached data will be automatically cleared.
    mutable CachingEnabled: bool
}

/// <summary>
/// Specifies the field to sort by when listing files or directories.
/// </summary>
type SortField =
    /// <summary>Sort by name (case-insensitive).</summary>
    | Name = 0
    /// <summary>Sort by file size (only applicable to files; directories treated as 0).</summary>
    | Size = 1
    /// <summary>Sort by creation date.</summary>
    | Created = 2
    /// <summary>Sort by modification date.</summary>
    | Modified = 3

/// <summary>
/// Specifies the sort direction.
/// </summary>
type SortDirection =
    /// <summary>Sort in ascending order (A-Z, smallest first, oldest first).</summary>
    | Ascending = 0
    /// <summary>Sort in descending order (Z-A, largest first, newest first).</summary>
    | Descending = 1


[<Struct; IsByRefLike; StructLayout(LayoutKind.Sequential)>]
type internal SoloDBLazyItem<'T> =
    // If jsonB <> null then value is None
    // If jsonB = null then value is Some
    val mutable internal valueUnsafe: 'T
    val mutable internal jsonB: ReadOnlySpan<byte>

    internal new (valueUnsafe: 'T, jsonB: ReadOnlySpan<byte>) =
        { valueUnsafe = valueUnsafe
          jsonB = jsonB }

    member inline internal this.HasValue () = this.jsonB.IsEmpty
