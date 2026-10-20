# Filesystem references

Documents can reference existing files and directories with `FileRef`, `DirectoryRef`, `FileRefMany` and `DirectoryRefMany`. Saving a reference requires its target to exist in the same database. A target cannot be deleted until every protecting reference is removed. This also prevents deleting an ancestor directory or replacing a protected destination. Deleting a document or dropping its collection removes its references and preserves the targets.

## C#

```csharp
using SoloDatabase;

public sealed class Report
{
    public long Id { get; set; }
    public FileRef MainFile { get; set; }
    public DirectoryRef Folder { get; set; }
    public FileRefMany Attachments { get; set; } = new();
    public DirectoryRefMany RelatedFolders { get; set; } = new();
}

using var db = new SoloDB("reports.db");
var file = FileRef.From(db.FileSystem.GetOrCreateAt("/reports/main.txt"));
var folder = DirectoryRef.From(db.FileSystem.GetDirAt("/reports"));
var reports = db.GetCollection<Report>();
var report = new Report { MainFile = file, Folder = folder };
report.Attachments.Add(file);
report.RelatedFolders.Add(folder);
long id = reports.Insert(report);

// These operations execute in SQLite and apply to the matched document set.
reports.UpdateMany(x => x.Id == id, x => x.MainFile.Set(file));
reports.UpdateMany(x => x.Id == id, x => x.Attachments.Add(file));
reports.UpdateMany(x => x.Id == id, x => x.Attachments.Remove(file));
reports.UpdateMany(x => x.Id == id, x => x.Attachments.Clear());
reports.UpdateMany(x => x.Id == id, x => x.RelatedFolders.Add(folder));
reports.UpdateMany(x => x.Id == id, x => x.MainFile.Set(FileRef.None));

string path = reports.GetById(id).Folder.Value.FullPath;
var matching = reports.AsQueryable()
    .Where(x => x.Folder.HasValue && x.Folder.Id == folder.Id)
    .Select(x => x.Folder.Value.FullPath).ToArray();
```

The four reference types also work with custom document IDs and documents without a public `Id` property. Filesystem references themselves always use the existing header's integer `Id`. `Add` and `Remove` take reference values, not file paths, headers or custom document IDs. Directory Many supports the same `Add`, `Remove` and `Clear` forms. Reference transforms compose with ordinary JSON transforms and admitted document-reference chains. A failure rolls back the complete document and membership mutation.

## F#

```fsharp
open SoloDatabase

[<CLIMutable>]
type Report = {
    Id: int64
    MainFile: FileRef
    Folder: DirectoryRef
    Attachments: FileRefMany
    RelatedFolders: DirectoryRefMany
}

use db = new SoloDB("reports.db")
let file = FileRef.From(db.FileSystem.GetOrCreateAt("/reports/main.txt"))
let folder = DirectoryRef.From(db.FileSystem.GetDirAt("/reports"))
let reports = db.GetCollection<Report>()
let id = reports.Insert({
    Id = 0L; MainFile = file; Folder = folder
    Attachments = FileRefMany(); RelatedFolders = DirectoryRefMany()
})
reports.UpdateMany((fun (r: Report) -> r.Id = id), fun r -> r.Attachments.Add(file)) |> ignore
reports.UpdateMany((fun (r: Report) -> r.Id = id), fun r -> r.RelatedFolders.Add(folder)) |> ignore
reports.UpdateMany((fun (r: Report) -> r.Id = id), fun r -> r.Attachments.Remove(file) |> ignore) |> ignore
reports.UpdateMany((fun (r: Report) -> r.Id = id), fun r -> r.Attachments.Clear()) |> ignore
reports.UpdateMany((fun (r: Report) -> r.Id = id), fun r -> r.MainFile.Set(FileRef.None)) |> ignore
```

## Identity and snapshots

`FileRef.To(id)` and `DirectoryRef.To(id)` create unloaded references to positive header IDs. `From(header)` captures an existing header and its identity. `None` or the default Single value is empty; check `HasValue` before use. `Value` throws when empty or unloaded. A document save never creates a file or directory from a reference.

Equality compares the reference kind and header ID, not the loaded snapshot. Ordinary and compiled queries support reference equality; an empty Single has an `Id` of zero in both CLR and SQL queries.

Identity is local to a database and the reference kind. Values do not prove database provenance. Detached IDs can be reused after their target has been deleted; they are not permanent generation identities. Moves and renames retain a live target's ID and protection. Loaded `Value` is a header and metadata snapshot, with no file bytes or connection. It retains its old path after a move; query again to load current facts.

## Loading and mutation

References load by default through `GetById` and queries. `Include`, `Exclude` and `ThenInclude` compose with document references. Single IDs remain available when excluded, while their `Value` is unloaded. Many loads are batched by document and target IDs. Ordinary and compiled queries support IDs, `HasValue`, header facts and metadata, Many membership and supported sequence operators, and projections of reference values.

File and directory references each count as one terminal reference hop when checking document mutations. The referenced header has no outgoing document-reference edge; directory parent hierarchy does not extend the document path. Newly introduced reference edges must leave affected final paths acyclic and no longer than 10 hops, including paths through existing document ancestors. Admission checks the final stored graph, including bulk and callback effects; a cycle or an overlong path throws `InvalidOperationException` and the rejected operation cannot commit partial effects. `Include` and `Exclude` control loading, not write admission. Existing graphs remain readable, and unchanged final membership or edge removals remain permitted for incremental repair.

An excluded Many is an unloaded local collection, not proof that the stored set is empty. Unrelated document updates preserve unknown membership. Local `Add`, `Remove` and `Clear` record explicit intent even while unloaded; call `Update` to persist it. Removing a locally absent item can return `false` while still recording removal of its stored identity. Many membership is a set with unspecified order. Duplicate `Add` is idempotent; `None` cannot be a Many member. Replacements and upserts supply the replacement membership rather than patching an unknown set.

Nontransactional saves use last-writer-wins: a later successful write can overwrite earlier state, whether the owner was loaded, freshly constructed or cloned. Loading an owner does not grant stale-write rejection or automatic merging of independent read/modify/write intentions. Membership operations and replacements retain their meanings described above. Explicit transactions retain their SQLite isolation, rollback and failure behavior. Reload after a failed save before retrying, because in-memory tracker state may no longer match the database.

Single identities are stored as integers or null in document JSON; Many membership is stored separately. Declaring a Single over existing canonical scalar identities establishes target protection without rewriting those JSON values and rejects missing targets. Incompatible cardinality or target declarations on an existing collection/property are rejected.

Filesystem deletion failures use the existing SQLite constraint error behavior. Reference writes use the existing relation error boundary. Protection does not require filesystem event subscriptions.
