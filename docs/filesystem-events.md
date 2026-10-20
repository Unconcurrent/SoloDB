# Filesystem events

Subscribe through `db.FileSystem`. Registrations also apply to filesystem operations through a transaction or an event context from the same database instance.

| Change | Before | After | Context |
|---|---|---|---|
| File creation | `OnFileCreating` | `OnFileCreated` | File header facts |
| File deletion | `OnFileDeleting` | `OnFileDeleted` | File header facts |
| File update | `OnFileUpdating` | `OnFileUpdated` | Old and new file header facts |
| Directory creation | `OnDirectoryCreating` | `OnDirectoryCreated` | Directory header facts |
| Directory deletion | `OnDirectoryDeleting` | `OnDirectoryDeleted` | Directory header facts |
| Directory update | `OnDirectoryUpdating` | `OnDirectoryUpdated` | Old and new directory header facts |
| File metadata | `OnFileMetadataChanging` | `OnFileMetadataChanged` | Path, key, kind, old and new values |
| Directory metadata | `OnDirectoryMetadataChanging` | `OnDirectoryMetadataChanged` | Path, key, kind, old and new values |
| File content | `OnFileContentChanging` | `OnFileContentChanged` | Path, byte range, kind, old and new lengths |

## File creation timing

`OnFileCreating` runs before insertion of the file header. `OnFileCreated` runs immediately after that header is inserted, inside the same SQLite statement and transaction. The new header is visible through the callback context; upload or copy contents need not be present yet. A copy can initialize the header with the source length before copying its bytes.

Subsequent content changes have their own range events. `OnFileCreated` does not wait for upload completion, stream disposal or transaction commit. If later work fails or the transaction rolls back, the inserted file can be rolled back even though the callback already ran.

## Content ranges

`Offset` and `Length` describe the half-open range `[Offset, Offset + Length)` in logical file bytes. They describe file contents, independently of compression and chunk storage.

| Kind | Range meaning |
|---|---|
| `Write` | The requested bytes, including writes that repeat their existing values. `OldLength` and `NewLength` describe any extension. |
| `Resize` | The interval between the old and new lengths. Growth exposes zeros; shrink removes bytes. |
| `Copy` | The copied destination interval starting at zero. Sparse holes remain logical zeros. `OldLength` describes the initialized destination header before content copying; it can already equal the source length. |

Empty writes, unchanged `SetLength` calls and empty copies raise no content event. A large write can produce one or multiple content events. Content after-events run after the corresponding bytes and length changes have been applied and blob handles have been closed. `Flush` and `Dispose` do not repeat content events; they can update timestamps.

Content paths follow the current file identity. An open stream's `FullPath` retains its opening path after a move. Header updates expose old and new paths; moves are updates. Directory moves and deletes include affected descendants. Metadata replacement follows the actual removal and addition of rows. `null` metadata values mean absence, while an empty string is a present value.

## C#

```csharp
using SoloDatabase;

using var db = new SoloDB("example.db");
FileContentChangedHandler written = ctx => {
    Console.WriteLine($"{ctx.Path}: {ctx.Kind} at {ctx.Offset}, {ctx.Length} bytes");
    return SoloDBEventsResult.EventHandled;
};
db.FileSystem.OnFileContentChanged(written);

using (var file = db.FileSystem.OpenOrCreateAt("/report")) {
    file.Write(new byte[] { 1, 2, 3 }, 0, 3);
}

db.FileSystem.Unregister(written);
```

## F#

```fsharp
open SoloDatabase

use db = new SoloDB("example.db")
let written = FileContentChangedHandler(fun ctx ->
    printfn "%s: %A at %d, %d bytes" ctx.Path ctx.Kind ctx.Offset ctx.Length
    SoloDBEventsResult.EventHandled)
db.FileSystem.OnFileContentChanged(written)

use file = db.FileSystem.OpenOrCreateAt("/report")
file.Write([|1uy; 2uy; 3uy|], 0, 3)
db.FileSystem.Unregister(written)
```

## Transactions and callback lifetime

Handlers run synchronously inside the operation's transaction. An after-event can still be followed by rollback of the enclosing transaction; it is not a commit notification. Throwing rejects the operation and rolls back its database changes. A swallowed handler-side database fault also prevents commit. External effects such as writing to a console cannot be rolled back.

Use `ctx` for database access inside handlers and keep that work on the callback thread. Normal `ctx.FileSystem` and collection operations participate in the active transaction. Explicit `ctx.WithTransaction` and `ctx.WithTransactionAsync` overloads throw `NotSupportedException`. Ordinary transactions outside handlers retain nested savepoint behavior.

Contexts are borrowed for one callback. Do not retain a context, its borrowed values or a returned database, collection, filesystem or stream adapter. Returned adapters reject access after their original invocation, including during a later reuse of the pooled context. A retained context reference itself can alias a later invocation; it is not a durable event record. Copy the values you need while the handler is active.

Handlers can perform recursive operations. Outer facts remain stable while nested callbacks run. Callback changes can affect what later callbacks observe. A before-content callback that changes the target causes the operation to revalidate its identity and length.

Returning `RemoveHandler` unregisters that registration after active dispatches finish, including recursive dispatches. A later handler failure does not undo an earlier successful removal. Duplicate registrations execute separately. `Unregister` removes registrations for the same delegate reference; calling it from a handler throws. Return `RemoveHandler` inside a handler instead. Registrations added during a callback begin with a later dispatch.

## Allocation and coverage

Warmed dispatch through the bundled SQLite engine adds no managed allocation for absent handlers, no-op handlers or primitive-only handlers, including warmed recursion. Registration, initial recursion capacity, requested strings, database adapters, file I/O and user code have their own costs. Paths and metadata strings are decoded lazily and cached for the invocation; content path resolution also reads the current header. Native SQLite work still has a cost.

Header and metadata SQL changes on SoloDB-owned connections participate in event delivery. External connections and direct compressed or incremental blob writes do not provide logical content events. Use the filesystem and stream APIs for content notifications.
