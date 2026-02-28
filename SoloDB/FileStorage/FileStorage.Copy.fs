namespace SoloDatabase

open System
open System.IO
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Threading.Tasks
open SoloDatabase.Types

/// Internal fast-path interface for copy operations, implemented only by SoloDB's internal FileSystem.
type internal IFileSystemCopyInternal =
    abstract member CopyFile: fromPath: string * toPath: string * copyMetadata: bool -> SoloDBFileHeader
    abstract member CopyFileAsync: fromPath: string * toPath: string * copyMetadata: bool -> Task<SoloDBFileHeader>
    abstract member CopyReplaceFile: fromPath: string * toPath: string * copyMetadata: bool -> SoloDBFileHeader
    abstract member CopyReplaceFileAsync: fromPath: string * toPath: string * copyMetadata: bool -> Task<SoloDBFileHeader>
    abstract member CopyDirectory: fromPath: string * toPath: string * recursive: bool * copyMetadata: bool -> SoloDBDirectoryHeader
    abstract member CopyDirectoryAsync: fromPath: string * toPath: string * recursive: bool * copyMetadata: bool -> Task<SoloDBDirectoryHeader>
    abstract member CopyReplaceDirectory: fromPath: string * toPath: string * recursive: bool * copyMetadata: bool -> SoloDBDirectoryHeader
    abstract member CopyReplaceDirectoryAsync: fromPath: string * toPath: string * recursive: bool * copyMetadata: bool -> Task<SoloDBDirectoryHeader>

/// Extension methods providing copy operations for IFileSystem.
/// These are non-breaking additions that do not add abstract members to IFileSystem.
[<Extension>]
type FileSystemCopyExtensions =

    /// <summary>
    /// Copies a file from one path to another. Fails if the destination already exists.
    /// </summary>
    /// <param name="fs">The file system instance.</param>
    /// <param name="fromPath">The source file path.</param>
    /// <param name="toPath">The destination file path.</param>
    /// <param name="copyMetadata">If true, copies key-value metadata from source to destination. Timestamps are always set to NOW.</param>
    /// <returns>The header of the newly created file.</returns>
    /// <exception cref="FileNotFoundException">Thrown if the source file does not exist.</exception>
    /// <exception cref="IOException">Thrown if a file already exists at the destination.</exception>
    /// <exception cref="ArgumentException">Thrown if source and destination are the same path.</exception>
    [<Extension>]
    static member CopyFile(fs: IFileSystem, fromPath: string, toPath: string, [<Optional; DefaultParameterValue(false)>] copyMetadata: bool) : SoloDBFileHeader =
        match fs with
        | :? IFileSystemCopyInternal as fast -> fast.CopyFile(fromPath, toPath, copyMetadata)
        | _ ->
            // Portable fallback: download source, upload to destination
            if fs.Exists toPath then raise (IOException("File already exists."))
            use ms = new MemoryStream()
            fs.Download(fromPath, ms)
            ms.Position <- 0L
            fs.Upload(toPath, ms)
            let dest = fs.GetAt toPath
            if copyMetadata then
                let src = fs.GetAt fromPath
                if not (isNull src.Metadata) then
                    for kv in src.Metadata do
                        fs.SetMetadata(dest, kv.Key, kv.Value)
            dest

    /// <summary>
    /// Asynchronously copies a file from one path to another. Fails if the destination already exists.
    /// </summary>
    [<Extension>]
    static member CopyFileAsync(fs: IFileSystem, fromPath: string, toPath: string, [<Optional; DefaultParameterValue(false)>] copyMetadata: bool) : Task<SoloDBFileHeader> =
        match fs with
        | :? IFileSystemCopyInternal as fast -> fast.CopyFileAsync(fromPath, toPath, copyMetadata)
        | _ -> Task.FromResult(FileSystemCopyExtensions.CopyFile(fs, fromPath, toPath, copyMetadata))

    /// <summary>
    /// Copies a file from one path to another. If the destination exists, it is replaced atomically.
    /// </summary>
    /// <param name="fs">The file system instance.</param>
    /// <param name="fromPath">The source file path.</param>
    /// <param name="toPath">The destination file path.</param>
    /// <param name="copyMetadata">If true, copies key-value metadata from source to destination. Timestamps are always set to NOW.</param>
    /// <returns>The header of the newly created file.</returns>
    /// <exception cref="FileNotFoundException">Thrown if the source file does not exist.</exception>
    /// <exception cref="ArgumentException">Thrown if source and destination are the same path.</exception>
    [<Extension>]
    static member CopyReplaceFile(fs: IFileSystem, fromPath: string, toPath: string, [<Optional; DefaultParameterValue(false)>] copyMetadata: bool) : SoloDBFileHeader =
        match fs with
        | :? IFileSystemCopyInternal as fast -> fast.CopyReplaceFile(fromPath, toPath, copyMetadata)
        | _ ->
            // Portable fallback: delete destination if exists, then upload
            if fs.DeleteFileAt toPath then ()
            use ms = new MemoryStream()
            fs.Download(fromPath, ms)
            ms.Position <- 0L
            fs.Upload(toPath, ms)
            let dest = fs.GetAt toPath
            if copyMetadata then
                let src = fs.GetAt fromPath
                if not (isNull src.Metadata) then
                    for kv in src.Metadata do
                        fs.SetMetadata(dest, kv.Key, kv.Value)
            dest

    /// <summary>
    /// Asynchronously copies a file, replacing the destination if it exists.
    /// </summary>
    [<Extension>]
    static member CopyReplaceFileAsync(fs: IFileSystem, fromPath: string, toPath: string, [<Optional; DefaultParameterValue(false)>] copyMetadata: bool) : Task<SoloDBFileHeader> =
        match fs with
        | :? IFileSystemCopyInternal as fast -> fast.CopyReplaceFileAsync(fromPath, toPath, copyMetadata)
        | _ -> Task.FromResult(FileSystemCopyExtensions.CopyReplaceFile(fs, fromPath, toPath, copyMetadata))

    /// <summary>
    /// Copies a directory from one path to another. Fails if the destination already exists.
    /// </summary>
    /// <param name="fs">The file system instance.</param>
    /// <param name="fromPath">The source directory path.</param>
    /// <param name="toPath">The destination directory path.</param>
    /// <param name="recursive">If true, copies all subdirectories and files recursively. Defaults to true.</param>
    /// <param name="copyMetadata">If true, copies key-value metadata. Timestamps are always set to NOW.</param>
    /// <returns>The header of the newly created directory.</returns>
    /// <exception cref="DirectoryNotFoundException">Thrown if the source or destination parent directory does not exist.</exception>
    /// <exception cref="IOException">Thrown if the destination directory already exists, or if recursive=false and source has children.</exception>
    /// <exception cref="ArgumentException">Thrown if source and destination are the same path or destination is inside source.</exception>
    [<Extension>]
    static member CopyDirectory(fs: IFileSystem, fromPath: string, toPath: string, [<Optional; DefaultParameterValue(true)>] recursive: bool, [<Optional; DefaultParameterValue(false)>] copyMetadata: bool) : SoloDBDirectoryHeader =
        match fs with
        | :? IFileSystemCopyInternal as fast -> fast.CopyDirectory(fromPath, toPath, recursive, copyMetadata)
        | _ -> raise (NotSupportedException("CopyDirectory is not supported by this IFileSystem implementation."))

    /// <summary>
    /// Asynchronously copies a directory. Fails if the destination already exists.
    /// </summary>
    [<Extension>]
    static member CopyDirectoryAsync(fs: IFileSystem, fromPath: string, toPath: string, [<Optional; DefaultParameterValue(true)>] recursive: bool, [<Optional; DefaultParameterValue(false)>] copyMetadata: bool) : Task<SoloDBDirectoryHeader> =
        match fs with
        | :? IFileSystemCopyInternal as fast -> fast.CopyDirectoryAsync(fromPath, toPath, recursive, copyMetadata)
        | _ -> raise (NotSupportedException("CopyDirectoryAsync is not supported by this IFileSystem implementation."))

    /// <summary>
    /// Copies a directory, replacing the destination if it exists.
    /// </summary>
    /// <param name="fs">The file system instance.</param>
    /// <param name="fromPath">The source directory path.</param>
    /// <param name="toPath">The destination directory path.</param>
    /// <param name="recursive">If true, copies all subdirectories and files recursively. Defaults to true.</param>
    /// <param name="copyMetadata">If true, copies key-value metadata. Timestamps are always set to NOW.</param>
    /// <returns>The header of the newly created directory.</returns>
    [<Extension>]
    static member CopyReplaceDirectory(fs: IFileSystem, fromPath: string, toPath: string, [<Optional; DefaultParameterValue(true)>] recursive: bool, [<Optional; DefaultParameterValue(false)>] copyMetadata: bool) : SoloDBDirectoryHeader =
        match fs with
        | :? IFileSystemCopyInternal as fast -> fast.CopyReplaceDirectory(fromPath, toPath, recursive, copyMetadata)
        | _ -> raise (NotSupportedException("CopyReplaceDirectory is not supported by this IFileSystem implementation."))

    /// <summary>
    /// Asynchronously copies a directory, replacing the destination if it exists.
    /// </summary>
    [<Extension>]
    static member CopyReplaceDirectoryAsync(fs: IFileSystem, fromPath: string, toPath: string, [<Optional; DefaultParameterValue(true)>] recursive: bool, [<Optional; DefaultParameterValue(false)>] copyMetadata: bool) : Task<SoloDBDirectoryHeader> =
        match fs with
        | :? IFileSystemCopyInternal as fast -> fast.CopyReplaceDirectoryAsync(fromPath, toPath, recursive, copyMetadata)
        | _ -> raise (NotSupportedException("CopyReplaceDirectoryAsync is not supported by this IFileSystem implementation."))
