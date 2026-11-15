namespace SoloDatabase

open System
open System.ComponentModel
open System.IO
open System.Runtime.InteropServices
open Microsoft.Win32.SafeHandles

/// Disk barriers after the supplied Stream has drained its managed buffers.
module internal StorageFileSync =
    [<DllImport("libc", EntryPoint = "fsync", CallingConvention = CallingConvention.Cdecl, SetLastError = true)>]
    extern int private fsync(int descriptor)

    [<DllImport("libc", EntryPoint = "fcntl", CallingConvention = CallingConvention.Cdecl, SetLastError = true)>]
    extern int private fullSync(int descriptor, int command)

    let private macOS = RuntimeInformation.IsOSPlatform(OSPlatform.OSX)
    let private affected =
        let major = Environment.Version.Major
        major >= 6 && major < 11 &&
        (macOS || RuntimeInformation.IsOSPlatform(OSPlatform.Linux))

    let private synchronize (handle: SafeFileHandle) =
        let mutable held = false
        try
            handle.DangerousAddRef(&held)
            let descriptor = handle.DangerousGetHandle().ToInt32()
            let mutable completed = false
            while not completed do
                // Darwin F_FULLFSYNC is 51; ordinary fsync is a weaker barrier there.
                let result = if macOS then fullSync(descriptor, 51) else fsync(descriptor)
                if result = 0 then completed <- true
                else
                    let error = Marshal.GetLastWin32Error()
                    if error <> 4 then // EINTR on both supported Unix platforms.
                        raise(IOException("File synchronization failed.", Win32Exception(error)))
        finally
            if held then handle.DangerousRelease()

    let flushToDisk (file: FileStream) =
        // Derived streams retain their virtual flushing authority. On affected
        // runtimes the standard FileStream barrier can silently consume an error;
        // a second barrier cannot recover that report, so invoke only our barrier.
        if affected && file.GetType().Equals(typeof<FileStream>) then
            synchronize file.SafeFileHandle
        else
            file.Flush(true)
