# Full-text search

Declare the stored string fields to search, then use `Search` or `SearchIn` with ordinary LINQ. SoloDB maintains the index when documents change.

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

Field selectors must follow fixed stored members to a string. Method calls, array/indexer traversal and reference traversal are unsupported. Stored names follow the model's serializer. A missing, null or nontext stored value contributes empty text. Each path can be declared once. Aliases follow SQLite identifier comparison; `rank` and `rowid` are reserved. Weights must be finite and nonnegative; zero is valid.

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

`Text` returns a declared field's original text. `Snippet` adds the supplied markers around matching text and uses `...` between fragments. Its token count must be from 1 through 64; opening and closing markers cannot be null. Both methods belong inside a database query projection. Calling them on a hit already returned by `ToArray` or `ToList` throws `InvalidOperationException`. Select strings before materializing, as in the first example. Changing `Item` afterward does not save the document; call the collection's update operation.

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

Retain the function. Text, markers and token counts supplied as arguments bind on invocation; enumeration reads current rows. The index name and declaration are fixed when compiling. An ordinary query resolves the current declaration when executed. A retained query never silently switches to a replacement declaration: recompile after replacing or dropping its index. Rebuild keeps the declaration identity.

An undefined or damaged index found during resolution raises `InvalidOperationException`. Native preparation and reprepare failures keep their `NativeSqliteException`, codes and cause unless a known structural preparation failure is tied to a confirmed retired source. A generic SQLite error and a changed catalog do not establish that cause. Other native errors remain unchanged.

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
if (!verification.IsValid)
    articles.RebuildSearchIndex("text");
articles.OptimizeSearchIndex("text");
bool removed = articles.DropSearchIndexIfExists("completion");
```

List returns immutable declaration snapshots. Verify reports missing or altered artifacts, rank, integrity, content and ownership findings; genuine native failures remain exceptions. Verification requires a writable connection. Rebuild derives text from the documents and keeps the declaration version and identity. It refuses an unsafe repair or an unowned artifact collision. Optimize is explicit native maintenance. Drop returns false if the declaration is absent. Dropping a collection also removes its owned search indexes.

Document changes and search maintenance commit or roll back together. Lifecycle operations inside an existing transaction use its savepoint behavior. A running WAL reader can finish its old snapshot while a replacement is committed.

The first successful Ensure advances the database schema from 6 to 7. An ordinary open alone does not adopt search. Coordinate schema administration with older SoloDB writers; do not lower the database version or edit private search objects. An adopted database missing its catalog is damage and is never treated as a fresh adoption.

## Storage and cost

Shared stores the selected text only in the documents and reads it there for text output. Copied stores another copy in FTS, using more storage and write work while allowing text-only output without reading the document payload. Both retain document authority and transactional maintenance.

FTS and ordinary member indexes can cooperate; SQLite chooses their access order. Broad terms still visit many matches. Narrow pages can select identities and order keys before fetching safe output; matching, sorting and deep offsets still cost work. Set deduplication may materialize candidates to retain the correct occurrence. Use `ExplainQueryPlan` on the actual query and measure complete enumeration with representative data. Ordinary translation reads the declaration; retained queries avoid repeating translation.
