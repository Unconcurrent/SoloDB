# Full-text search

Declare the stored string fields to search, then use `Search` or `SearchIn` with ordinary LINQ. SoloDB maintains the index when documents change.

The first successful `EnsureSearchIndex` advances the database to schema 7. **SoloDB 1.4.1 and earlier then refuse to open that database.** Upgrade every process that uses it before declaring an index.

## Declare fields and search

```csharp
using System;
using System.Linq;
using SoloDatabase;

using var db = new SoloDB("articles.db");
var articles = db.GetCollection<Article>();
articles.EnsureIndex(a => a.Tenant);
articles.EnsureSearchIndex("text", 1, builder => {
    builder.Field(a => a.Title, 4.0, "title")
           .Field(a => a.Body, 1.0, "body");
});

var results = articles.SearchIn("text", SearchQuery.TypeAhead("sqlite pag"))
    .Where(hit => hit.Item.Tenant == 3)
    .Take(10)
    .Select(hit => new {
        hit.Item.Id,
        hit.Score,
        Title = hit.Text(a => a.Title),
        Preview = hit.Snippet(a => a.Body, "[", "]", 24)
    })
    .ToArray();

public sealed class Article
{
    public long Id { get; set; }
    public string Title { get; set; } = "";
    public string Body { get; set; } = "";
    public int Tenant { get; set; }
}
```

`EnsureSearchIndex(builder => ...)` declares the index named `default` at version 1. `EnsureSearchIndex(name, builder => ...)` also uses version 1. `Search(text)` and `Search(query)` use `default`; `SearchIn(name, ...)` selects another index.

Field selectors must follow fixed stored members to a string. Method calls, array/indexer traversal and reference traversal are unsupported. Stored names follow the model's serializer. A missing, null or nontext stored value contributes empty text. A stored path can have several distinct aliases and weights. Aliases follow SQLite identifier comparison; `rank` and `rowid` are reserved. Weights must be finite; negative and zero weights are valid. Null required arguments raise `ArgumentNullException`. A nonpositive definition version raises `ArgumentOutOfRangeException`.

The builder defaults to `SearchTextStorage.Shared` and tokenizer `unicode61`. Set `Storage`, `Tokenizer` and `PrefixLengths` during configuration:

```csharp
articles.EnsureSearchIndex("completion", 1, builder => {
    builder.Storage = SearchTextStorage.Copied;
    builder.Tokenizer = "unicode61 remove_diacritics 0";
    builder.PrefixLengths = new[] { 2, 3 };
    builder.Field(a => a.Title);
});
```

Prefix lengths are positive, copied, sorted and deduplicated. Tokenizer text is passed to SQLite. Invalid native configuration fails the operation and rolls back its changes.

## Query text

`Search("sqlite pages")` means all literal words are required. Use a `SearchQuery` factory to choose another interpretation. Its `Kind` and original `Text` are read-only.

| Factory | Meaning |
|---|---|
| `AllWords(text)` | Quote each whitespace-separated chunk and require every chunk. NUL also separates chunks. |
| `LiteralChunks(text)` | The same literal chunk search, but reject NUL. |
| `Phrase(text)` | Quote the whole text as one phrase. |
| `Prefix(text)` | Search a phrase with its final token treated as a prefix. |
| `TypeAhead(text)` | Require all chunks; prefix the last nonempty chunk, including after trailing whitespace. |
| `Fts5Syntax(text)` | Use SQLite's FTS5 grammar directly. |

Literal factories escape quotes and do not interpret Boolean operators from the input. Empty literal searches return no hits. Null text is rejected. Factories other than `AllWords` and `TypeAhead` reject NUL. Raw syntax retains SQLite's parsing and errors:

```csharp
var matches = articles.SearchIn("text",
    SearchQuery.Fts5Syntax("title : sqlite AND body : page*"));
```

## Hits and selected text

`SearchHit<Article>` contains the original `Item` and a read-only `Score`. The score is the negation of the configured native BM25 rank. A single search defaults to native rank ascending, then document ID ascending. An explicit `OrderBy` replaces this order; add your own `ThenBy` when you need a tie-breaker.

`Text` returns a declared field's original text. `Snippet` returns one fragment, adds the supplied markers around matches, and uses `...` at truncated ends. SQLite clamps the token count to 0 through 64 and treats null opening or closing markers as empty text. Both methods belong inside a database query projection. Calling them on a hit already returned by `ToArray` or `ToList` throws `InvalidOperationException`. Select strings before materializing, as in the first example. Changing `Item` afterward does not save the document; call the collection's update operation.

Within a SoloDB query, `Union`, `Distinct`, `Intersect` and `Except` compare visible Item and Score using the usual SoloDB equality for the Item. Hidden search context does not affect equality. Union and Distinct keep the first surviving occurrence; Intersect and Except keep a left occurrence. Later Text/Snippet calls use that occurrence's original search text and declaration. Every possible source must declare the requested field and retain its query context. Appending or reconstructing a materialized hit does not restore that context. Bounds inside each arm stay inside that arm.

## Retain a query

```csharp
var find = articles.Compile((IQueryable<Article> source, string text, int tokens) =>
    articles.SearchIn("text", text).Take(10).Select(hit => new {
        hit.Item.Id,
        Preview = hit.Snippet(a => a.Body, "[", "]", tokens)
    }));

var first = find("sqlite", 24).ToArray();
var second = find("journal", 16).ToArray();
```

Retain the function. Text, markers and token counts supplied as arguments bind on invocation; enumeration reads current rows. A transactional collection can compile and execute within its transaction callback; finish every enumeration before that callback returns. The index name and declaration are fixed when compiling. An ordinary query resolves the current declaration when executed. A retained query never silently switches to a replacement declaration: recompile after replacing or dropping its index. Rebuild keeps the declaration identity.

An undefined or damaged index found during resolution raises `InvalidOperationException`. Starting a retained execution against an index retired by replacement or Drop raises `NativeSqliteException`; create a new compiled query against the current declaration. Preparation and reprepare errors preserve their native codes and cause. SoloDB does not infer their cause from a generic SQLite error or a changed catalog, and other native errors remain unchanged.

## F#

F# NativeAOT applications use `CompileQuotation` with the whole query lambda. This avoids FSharp.Core's unsupported conversion of ordinary query lambdas under NativeAOT. Ordinary query expressions also work under JIT. Register generated operations before opening the database; see [generated metadata](generated-metadata.md).

```fsharp
open System
open System.Linq
open SoloDatabase

[<CLIMutable>]
type Article = { Id: int64; Title: string; Body: string; Tenant: int }

type Supplied = SoloDBAOT<0>

let search () =
    Supplied.register()
    use db = new SoloDB("fsharp-articles.db")
    let articles = db.GetCollection<Article>()
    articles.EnsureSearchIndex("text", 1, Action<_>(fun builder ->
        builder.Field((fun item -> item.Title), 4.0, "title").Field((fun item -> item.Body), 1.0, "body")
        |> ignore))
    let find = articles.CompileQuotation<string, int64 * string * string>(
        <@ fun (source: IQueryable<Article>) (text: string) ->
            articles.SearchIn("text", text).Take(10).Select(fun hit ->
                hit.Item.Id, hit.Text(fun item -> item.Title),
                hit.Snippet((fun item -> item.Body), "[", "]", 24)) @>)
    find.Invoke("sqlite") |> Seq.toArray
```

## Change or repair an index

Ensure is explicit administration. Acquiring a collection or running a search does not create an index. Repeating the same declaration at the same version is a read-only check. Changing a definition at that version, or supplying an older version, fails. A higher version with unchanged semantics keeps the existing index. A higher version with changed semantics builds and backfills its replacement before retiring the old one, in one transaction.

```csharp
var declarations = articles.ListSearchIndexes();
var verification = articles.VerifySearchIndex("text");
if (verification.Findings.Any(f => f.Kind != SearchVerificationKind.UnownedArtifact))
    articles.RebuildSearchIndex("text");
foreach (var finding in verification.Findings)
    Console.Error.WriteLine($"{finding.Kind}: {finding.ObjectName}: {finding.Description}");
articles.OptimizeSearchIndex("text");
bool removed = articles.DropSearchIndexIfExists("completion");
```

List returns immutable declaration snapshots. Verify reports missing or altered artifacts, rank, integrity, content and ownership findings; genuine native failures remain exceptions. Verification requires a writable connection and holds a write transaction while inspecting the corpus, blocking other writers for its duration. Its work grows with the indexed data. Rebuild derives text from the documents and keeps the declaration version and identity. It restores owned triggers and views as well as missing configuration. An incompatible physical search table prevents that repair; a local artifact creation collision fails and rolls back. Unknown objects elsewhere do not block repair. Optimize is explicit native maintenance. Drop returns false if the declaration is absent. Dropping a collection also removes its owned search indexes.

`UnownedArtifact` findings cover the whole database, so they do not by themselves mean the selected index is damaged. `IsValid` means the complete findings list is empty. Rebuild leaves unknown objects untouched and therefore does not clear those findings. Inspect the exact reported object and its data; restore a consistent database backup if the object still belongs to a declaration whose catalog record was lost. Remove it only after confirming it is unowned and unnecessary.

For that deliberate administration, use the existing public ADO connection. Here `confirmedUnusedTable` is the exact reported name of a table the operator has inspected and chosen to remove; a finding alone does not supply that decision:

```csharp
static void DropConfirmedUnusedTable(SoloDB db, string confirmedUnusedTable)
{
    using var connection = db.Connection.Borrow();
    using var command = connection.CreateCommand();
    command.CommandText = "DROP TABLE main.\"" + confirmedUnusedTable.Replace("\"", "\"\"") + "\"";
    command.ExecuteNonQuery();
}
```

Never sweep the reserved prefix or change owned search objects manually. This exact-name removal of a confirmed unnecessary unowned table is an administration exception, not automatic search maintenance. Verify again after the operator action.

Document changes and search maintenance commit or roll back together. Lifecycle operations inside an existing transaction use its savepoint behavior. A running WAL reader can finish its old snapshot while a replacement is committed.

The first successful Ensure advances the database schema from 6 to 7. An ordinary open alone does not adopt search. Do not lower the database version or manually edit owned search objects. An adopted database missing its catalog is damage and is never treated as a fresh adoption.

## Storage and cost

Shared stores the selected text only in the documents and reads it there for text output. Copied stores another copy in FTS, increasing the stored text and bytes maintained while allowing text-only output without reading the document payload. On update or delete, Shared extracts the old field text from the document; Copied reads its FTS copy and also maintains that copy. Both retokenize the old text to remove its index entries. Neither mode promises faster writes: payload extraction, extra content writes and warm caches affect the result. Both retain document authority and transactional maintenance.

Search can use both its text index and ordinary member indexes. Ordering by Score can require scoring every match before returning a page. When you order by another field, SoloDB can choose the page first and score only its rows, although this may read the matches twice. Text and snippets can also be fetched after choosing the page. Matching, sorting and large `Skip` values still take work; a small `Take` does not make a broad search cheap.

`Union` and `Distinct` may need to retain many candidates before removing duplicates. Use `ExplainQueryPlan` to check index use, and measure the actual `ToArray` or `ToList`, including the fields you request, on representative data. Retain a compiled query to avoid translating it and reading its index declaration again on each execution.
