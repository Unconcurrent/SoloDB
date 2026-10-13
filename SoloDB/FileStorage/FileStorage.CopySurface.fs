namespace SoloDatabase

open SoloDatabase.Connections
open FileStorageHelpers

module internal FileStorageCopySurface =
    let copyFile (connection: NativeConnection) fromPath toPath copyMetadata =
        connection.WithTransaction(fun db ->
            copyFileMustBeWithinTransaction db fromPath toPath false copyMetadata
        )

    let copyFileAsync (connection: NativeConnection) fromPath toPath copyMetadata = task {
        return connection.WithTransaction(fun db ->
            copyFileMustBeWithinTransaction db fromPath toPath false copyMetadata
        )
    }

    let copyReplaceFile (connection: NativeConnection) fromPath toPath copyMetadata =
        connection.WithTransaction(fun db ->
            copyFileMustBeWithinTransaction db fromPath toPath true copyMetadata
        )

    let copyReplaceFileAsync (connection: NativeConnection) fromPath toPath copyMetadata = task {
        return connection.WithTransaction(fun db ->
            copyFileMustBeWithinTransaction db fromPath toPath true copyMetadata
        )
    }

    let copyDirectory (connection: NativeConnection) fromPath toPath recursive copyMetadata =
        connection.WithTransaction(fun db ->
            copyDirectoryMustBeWithinTransaction db fromPath toPath false recursive copyMetadata
        )

    let copyDirectoryAsync (connection: NativeConnection) fromPath toPath recursive copyMetadata = task {
        return connection.WithTransaction(fun db ->
            copyDirectoryMustBeWithinTransaction db fromPath toPath false recursive copyMetadata
        )
    }

    let copyReplaceDirectory (connection: NativeConnection) fromPath toPath recursive copyMetadata =
        connection.WithTransaction(fun db ->
            copyDirectoryMustBeWithinTransaction db fromPath toPath true recursive copyMetadata
        )

    let copyReplaceDirectoryAsync (connection: NativeConnection) fromPath toPath recursive copyMetadata = task {
        return connection.WithTransaction(fun db ->
            copyDirectoryMustBeWithinTransaction db fromPath toPath true recursive copyMetadata
        )
    }
