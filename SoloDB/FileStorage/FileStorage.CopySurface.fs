namespace SoloDatabase

open SoloDatabase.Connections
open FileStorageHelpers

module FileStorageCopySurface =
    let copyFile (connection: Connection) fromPath toPath copyMetadata =
        connection.WithTransaction(fun db ->
            copyFileMustBeWithinTransaction db fromPath toPath false copyMetadata
        )

    let copyFileAsync (connection: Connection) fromPath toPath copyMetadata = task {
        return connection.WithTransaction(fun db ->
            copyFileMustBeWithinTransaction db fromPath toPath false copyMetadata
        )
    }

    let copyReplaceFile (connection: Connection) fromPath toPath copyMetadata =
        connection.WithTransaction(fun db ->
            copyFileMustBeWithinTransaction db fromPath toPath true copyMetadata
        )

    let copyReplaceFileAsync (connection: Connection) fromPath toPath copyMetadata = task {
        return connection.WithTransaction(fun db ->
            copyFileMustBeWithinTransaction db fromPath toPath true copyMetadata
        )
    }

    let copyDirectory (connection: Connection) fromPath toPath recursive copyMetadata =
        connection.WithTransaction(fun db ->
            copyDirectoryMustBeWithinTransaction db fromPath toPath false recursive copyMetadata
        )

    let copyDirectoryAsync (connection: Connection) fromPath toPath recursive copyMetadata = task {
        return connection.WithTransaction(fun db ->
            copyDirectoryMustBeWithinTransaction db fromPath toPath false recursive copyMetadata
        )
    }

    let copyReplaceDirectory (connection: Connection) fromPath toPath recursive copyMetadata =
        connection.WithTransaction(fun db ->
            copyDirectoryMustBeWithinTransaction db fromPath toPath true recursive copyMetadata
        )

    let copyReplaceDirectoryAsync (connection: Connection) fromPath toPath recursive copyMetadata = task {
        return connection.WithTransaction(fun db ->
            copyDirectoryMustBeWithinTransaction db fromPath toPath true recursive copyMetadata
        )
    }
