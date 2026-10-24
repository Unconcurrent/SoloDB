# Generated metadata registration

The `SoloDB` package includes a C# source generator, an F# type provider and their build integration. Keep the package's build assets enabled; no separate generator package or manual build-target import is required for package consumers.

Both routes generate operations for your original model types, preserving their visibility and CLR identity. They share conversion and storage policies with ordinary runtime-discovered operations; registration does not select a different database format. The [numerical storage upgrade](#numerical-storage-and-migration) applies to both routes. You do not need replacement models or `partial` declarations on models. Generated operations can also be used in ordinary JIT applications.

## C#

Call the generated project-local `SoloDBAOT.Register()` before the first operation that needs generated metadata:

```csharp
using System;
using SoloDatabase;

internal sealed class Person
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
}

internal static class Program
{
    private static void Main()
    {
#if !DEBUG
        SoloDBAOT.Register();
#endif
        using var db = new SoloDB("memory:people");
        var people = db.GetCollection<Person>();
        long id = people.Insert(new Person { Name = "Ada" });
        Console.WriteLine(people.GetById(id).Name);
    }
}
```

The compiler discovers `Person` from the typed collection use. To select a model explicitly, add `[SoloDBAOT]` to its declaration. The attribute resolves through `using SoloDatabase`; the registration call uses the separate generated static class. Reserve the global name `SoloDBAOT` for that entry.

## F#

Declare a provider alias and call its lowercase `register` method before use:

```fsharp
namespace Example

open System
open System.Linq
open SoloDatabase

type Person() =
    member val Id = 0L with get, set
    member val Name = "" with get, set

#if !DEBUG
type AOTSupport = SoloDBAOT<0>
#endif

module Program =
    [<EntryPoint>]
    let main _ =
#if !DEBUG
        AOTSupport.register()
#endif
        use db = new SoloDB("memory:people")
        let people = db.GetCollection<Person>()
        people.Insert(Person(Name = "Ada")) |> ignore
        let count = people.CompileQuotation(fun query -> query.Count())
        let find = people.CompileQuotation(fun query (name: string) ->
            query.Where(fun person -> person.Name = name).ToArray())
        Console.WriteLine(count.Invoke())
        Console.WriteLine(find.Invoke("Ada").[0].Name)
        0
```

`0` is the provider's static argument, not a model identifier. Model roots are discovered from the current project's resolved SoloDB uses. `[<SoloDBAOT>]` on a model also selects it explicitly. Keep ordinary F# source ordering: declare the alias before calling it.

## Root discovery and registration lifetime

Active generation combines explicitly selected models with concrete types inferred from typed collection and JSON calls, closed generic helper uses and supported query signatures. It follows their required operation dependencies without generating every type in referenced assemblies. Explicit and inferred roots retain the same original types.

Raw SQL argument inputs have their own generated capture capability; they do not need JSON model roots. See [SQL argument capture](native-sql.md#typed-arguments-and-direct-mapping) for property selection, source availability and acquisition behavior.

An open-ended runtime-selected type cannot always be inferred. Resolve `SOLO003` warnings at the reported use: supply an explicit model root and a concrete closed signature where required. Attributes do not make an unsupported operation supported.

Each consuming project registers its own generated operations. A library can call its project-local registration from its own initialization method. Call registration before generated metadata is needed, including in libraries used by the application. Successful repeated calls, including concurrent calls, are no-ops; a failed registration can be retried. Registration does not invoke model constructors or getters.

## Conditional activation

Generation follows the registration call in the compiled source. Attributes or an F# provider alias alone do not activate full model generation. An ordinary runtime `if` does not disable generation; exclude the call with a preprocessor condition instead.

Use the standard `DEBUG` symbol to keep registration out of Debug builds. The examples above use `#if !DEBUG`: C# guards the registration call, and F# guards both the provider alias and its call. No project-file changes or custom symbols are needed.

In C#:

```csharp
#if !DEBUG
SoloDBAOT.Register();
#endif
```

In F#, declare the alias before the code that calls it:

```fsharp
#if !DEBUG
type AOTSupport = SoloDBAOT<0>
#endif

// Inside your startup function:
#if !DEBUG
AOTSupport.register()
#endif
```

With the standard Debug and Release configurations, this selects generated operations in Release and ordinary runtime discovery in Debug. Choose a source condition that includes every configuration you publish with NativeAOT. An ordinary runtime `if` does not exclude generation. The package handles activation changes during normal builds.

## JIT and NativeAOT deployment

When generation is inactive, ordinary JIT execution discovers supported type operations lazily at runtime. Registered generated operations take precedence when present. NativeAOT needs generated coverage; runtime discovery is not a substitute for missing operations. PublishAot belongs on the consuming executable project, including when its models live in a .NET Standard library.

Both C# and F# builds emit application operations into a project-specific runtime helper assembly. Package build integration generates and delivers the helper during normal builds and publishing. Keep it with managed application output; NativeAOT consumes it during native compilation. The generator and provider tooling are build-time dependencies and are not deployed with the application.

Generating operations requires .NET SDK 8 or later; installing SDK 10 separately is unnecessary. This compiler requirement is separate from the runtime requirements of ordinary SoloDB applications. F# analysis uses the installed SDK's .NET host. The runtime library requires FSharp.Core 6.0.7 or later and preserves newer consumer versions. The CSharp generator and FSharp provider use FSharp.Core 8 independently of the runtime library.

Application APIs have separate NativeAOT requirements. On .NET 8 linux-x64 with FSharp.Core 8.0.403, formatted `printfn` can fail with missing native generic code even in an executable without SoloDB. Model registration does not supply that framework code. Use compatible application output, such as `Console.WriteLine`, for that deployment.

Registration prepares type operations and query execution routes; it does not pregenerate SQL. LINQ translation and `collection.Compile` retain their existing responsibilities.

## F# query quotations

For F# NativeAOT applications, use `CompileQuotation` instead of passing an ordinary F# lambda to `Compile`. F# lambda-to-LINQ conversion through `LeafExpressionConverter` fails under NativeAOT when it accesses `MetadataToken`, before SoloDB receives the expression. Model registration does not fix that caller-side failure. `CompileQuotation` captures the whole query lambda before that conversion and uses SoloDB's existing query planning and execution. Existing `Compile` calls remain supported on JIT, and already constructed LINQ expressions retain their existing API.

Write an ordinary lambda, without a marker argument or quotation brackets:

```fsharp
open System.Linq

let count = people.CompileQuotation(fun query -> query.Count())
let total = count.Invoke()

let find = people.CompileQuotation(fun query id ->
    query.Single(fun person -> person.Id = id))
let person = find.Invoke(id)
```

Keep query construction, including `Where`, `OrderBy` and `Select` lambdas, inside the outer `CompileQuotation` lambda. Changing only the final method cannot repair conversion that already happened while building a query. Sequence, ordered sequence, scalar and client-result forms accept zero through three invocation arguments and retain their existing delegate and result semantics.

Some F# union field accessors need a metadata flag correction for NativeAOT quotation deserialization. For affected unsigned model libraries, the package automatically prepares private native compiler inputs with the necessary flag; original assemblies, model signatures and method bodies remain unchanged. No manual patch or extra package is required, and `FSharp.Core` is not modified. A signed model library needing this correction remains unchanged and stops the native build with a `SOLO005` error because signed-library correction is unsupported. Signed assemblies that do not need the correction remain supported.

## Direct typed JSON

Direct JSON operations share model conversion policies across ordinary execution and generated support.

`JsonValue.ParseInto<T>` reads a string, `ReadOnlySpan<byte>` containing UTF-8, or `ReadOnlySpan<char>` containing UTF-16 directly into a supported type. `JsonValue.WriteJson<T>` writes that type to a destination. Existing `Serialize<T>` still returns a `JsonValue` tree, and `Parse` still creates a tree. Writers take the model by `ref`, including class references. Mutable struct getters operate on that supplied location. Writers borrow proven mutable auto-property backing storage, so child getter mutations persist there; computed, effectful and readonly property results are captured once before their child writer runs. `ParseInto<T>` keeps its returning API.

The overloads below use C# API notation:

```text
int JsonValue.WriteJson<T>(ref T value, ref Span<byte> destination,
    JsonBufferGrow<byte> grow = null, JsonBufferRelease<byte> release = null);
int JsonValue.WriteJson<T>(ref T value, ref Span<char> destination,
    JsonBufferGrow<char> grow = null, JsonBufferRelease<char> release = null);
int JsonValue.WriteJson<T>(ref T value, IList<byte> destination);
int JsonValue.WriteJson<T>(ref T value, IList<char> destination);
```

`ParseInto<T>(null)` with the string overload returns the default value of `T`; an empty byte or character span raises `ArgumentException`.

`Parse` and `ParseInto<T>` accept at most 1000 nested object or array containers and raise `FormatException` beyond that limit. Sibling containers and braces inside strings do not increase nesting.

When `ParseInto<object>` infers an integral number, it permits up to 32768 additional decimal zero digits from positive exponent expansion. Beyond that budget it returns signed `Double` infinity. This is an inference work budget, not a maximum numerical value: `1e32769` saturates, while `10e32768` remains an exact `BigInteger` because one more coefficient digit is already written. Written coefficient digits are not capped, and explicitly requesting `BigInteger` retains its arbitrary precision within available runtime and memory limits. Encoded numerical parameters and symbolic exact arithmetic retain their exact representation.

Forward object readers convert recognized properties in input order. Mutable properties are assigned as encountered; immutable constructor arguments are held in typed locals and the model is constructed after the object ends. Missing mutable properties retain their constructor defaults, and missing constructor arguments use their typed defaults.

In forward object readers, each duplicate occurrence is converted: the last successfully converted value wins, including names made equivalent by JSON escapes. An invalid earlier occurrence fails even if a later occurrence would be valid. Constructors and setters may already have run when a later malformed value or trailing input causes failure; their effects are not rolled back. Explicit tree parsing and subsequent tree conversion retain their separate duplicate and conversion-order behavior. Optional wrappers, unions and directional collection readers, including Stack, retain their established policies.

A different assignable runtime type selected by a late `$type` discriminator uses an exceptional read of the object as that type. A failed declared-type read may also select this route when the object identifies a different assignable type. Earlier declared-type effects remain and the selected read can add effects. When the selected type is unchanged, the original failure is preserved. Missing or same-type discriminators use the ordinary direct path.

Each typed conversion uses a fixed operation for its type and encoding. Project registration supplies generated operations before use; ordinary JIT execution prepares its operation on first use. Runtime polymorphism preserves the actual type and its discriminator where required. Growth/release ownership and constructor, getter, setter and output callback effects retain their documented behavior.

Typed byte-sequence writers materialize the sequence before destination callbacks, then write base64 directly to the destination without constructing an intermediate base64 string. The explicit `JsonValue` tree APIs retain their owning string representation.

### Internal SQLite JSON reads

Immediate internal materialization on the bundled engine parses borrowed native UTF-16 text through the existing JSON reader. SQLite's text conversion occurs before user callbacks; typed materialization uses the declared stored-value policy. Configured providers without this native UTF-16 access retain owning text. This applies to ordinary and compiled query enumeration, Many relation row loops, and native JSON-tree projections. The text is consumed synchronously while the private producing statement remains active; no native span or pointer escapes into returned models or lazy results. Public UTF-8 codecs remain separate.

Deferred paths retain owning payloads where required by existing behavior: collection and scalar operations whose statement completion or cardinality checks precede construction, Single relation batches that gather rows before constructing per-owner instances, mutation payloads, and deferred hydration. Those copies preserve constructor and notification order and relation identity. Typed optional JSON reads through caller-controlled ADO readers also retain owning input: application code can advance or dispose that exact producer, so the private-statement lifetime proof does not apply. They are deliberate exceptions, not a claim that every internal read avoids a copy.

Public `GetString` and requested string projections still return owning strings. Materialized model strings, arrays and objects also own their data; borrowed input does not make results zero-copy or allocation-free. Caller APIs, forward-reader effects, application callbacks and output-buffer ownership remain unchanged.

### Internal JSON writes and parameters

Insert, update/replacement and cascade writes retain the shared writer's default-grown character array and used range, then bind that owned UTF-16 text without constructing a final JSON string. Structured stored query parameters use the same owned-text representation. Serialization stays at its existing capture point, preserving getter effects and rebinding the captured output without serializing again.

Compiled calls use one independent invocation frame and a bounded named-parameter range. Overlapping, lazy and recursive executions retain separate state; ref/out locations and exception writes remain intact. This changes internal transport, not public object parameter APIs or span/list writer callbacks and buffer ownership.

Unicode compatibility fallback can still create owning strings. SQLite's transient text binding copies the supplied text before returning; removal of an intermediate managed JSON string is not zero-copy native binding or allocation-free execution. Existing TEXT/JSONB storage semantics remain.

The callback types are in `SoloDatabase.JsonSerializator`:

```csharp
public delegate Span<T> JsonBufferGrow<T>(int minimumCapacity);
public delegate void JsonBufferRelease<T>(Span<T> storage);
```

For spans, writing starts at index zero. The returned count is bytes for UTF-8 or UTF-16 code units for characters. Use that prefix of the final span, which is returned through `ref destination`. Empty or default initial spans are valid. For example:

```csharp
using System;
using SoloDatabase.JsonSerializator;

Span<byte> destination = Span<byte>.Empty;
int written = JsonValue.WriteJson(ref person, ref destination);
ReadOnlySpan<byte> json = destination.Slice(0, written);
```

With no growth callback, SoloDB allocates a managed array when capacity is insufficient. Growth requests use checked, geometric capacity increases. Serialization does not restart or repeat getters and enumeration to size the output. A supplied growth callback must return at least the requested total capacity; an undersized result raises `InvalidOperationException` rather than switching to automatic allocation.

A release callback requires a custom growth callback. An invalid pairing raises `ArgumentException` before serialization or callbacks. Growth must leave old storage valid until SoloDB copies the written prefix. SoloDB then publishes the replacement through `ref destination` and calls release with the full old span, including an empty initial span. It never releases the successful final destination; the caller releases it after use.

When supplying release, initial and replacement buffers must follow the same allocator policy. Replacement storage must be independently owned and must not share an allocation that release will invalidate. Overlapping spans are rejected; disjoint spans alone cannot establish separate allocation ownership. The callbacks must retain their allocator's ownership information. A native reallocation that invalidates the old buffer before copying is unsuitable. Callback construction, including capturing lambdas, may allocate.

If growth throws, destination still identifies the old buffer. If release throws after replacement publication, destination identifies the new buffer. An invalid replacement leaves the original destination unchanged. A nonempty, nonoverlapping rejected replacement is passed to release when supplied; cleanup failure does not replace the original validation error. Allocator independence remains the callback's responsibility, including rejected replacements.

List overloads append to existing contents and return only the number of elements added. A null list raises `ArgumentNullException`; a read-only list raises `ArgumentException` before serialization. Writable lists use their append operations, with capacity growth for `List<byte>` and `List<char>`. No complete temporary JSON array is required merely to append. Callback, allocation, getter, enumeration and list failures propagate; partially written output is not rolled back. After a span failure, the caller retains responsibility for the buffer currently identified by destination.

Direct numeric conversion uses the declared integer, `double`, `float` or `decimal` type. Floating values avoid the decimal-backed tree's rounding and range limitations, so direct and tree output can differ for values outside Decimal precision or range. Direct floating-point writers use invariant numeric formatting, preserve negative zero as `-0`, and emit the JSON5 literals `NaN`, `Infinity` and `-Infinity` for nonfinite values. Explicit `JsonValue.Number` keeps its decimal representation. .NET Standard 2.0 UTF-16 numeric formatting may allocate a number-only string; direct output is not an allocation-free guarantee.

Direct floating readers retain a `NaN` number token, including in present nullable values, lists and Object-valued members. The explicit tree APIs have different directional rules: `JsonValue.Serialize` of a nonfinite floating value raises `OverflowException`, while `JsonValue.Parse("NaN")` produces a Decimal-backed zero. These tree conversions are not a native floating-point roundtrip.

### Numerical storage and migration

Typed document persistence stores Decimal and BigInteger values as canonical comparable strings; public direct JSON writes native numeric values. Typed numerical queries retain exact coefficients and exponents until the requested operation or conversion applies its native rounding and range rules. Ordinary string properties and numerical dictionary names retain their original meaning. Raw SQL extraction exposes the encoded text; SQLite's native casts and arithmetic do not decode that representation. Untyped collections expose the same physical JSON: an encoded numerical value is a String, and an explicit `JsonValue` write does not infer the numerical policy of another declared model. Writes through untyped JSON or raw SQL must preserve that stored representation if typed access will follow. Use typed collection methods for ordinary numerical reads and updates.

Collection persistence passes floating JSON through SQLite JSONB, which [interprets a `NaN` number token as JSON null](https://www.sqlite.org/json1.html#json5). Nonnullable `double` and `float` readers materialize that null as NaN; nullable floating values and Object-valued members materialize it as null. List elements follow their declared element type. Original NaN and genuine null cannot be distinguished after this normalization. A quoted `"NaN"` remains a String. Typed floating-point infinity remains infinity. Raw SQL parameter binding has its [own conversion rules](native-sql.md#typed-arguments-and-direct-mapping).

Before upgrading, stop every older SoloDB process using the database. Opening with SoloDB 1.4 advances `PRAGMA user_version` to 6 in the schema initialization transaction, so SoloDB 1.3 refuses a later opening. This does not revoke connections already held by an older process. Do not lower the version stamp to bypass the check. The database version is established at opening; numerical fields are converted later as their typed collections are initialized.

When an existing typed collection requires numerical migration, initialization converts its declared numerical fields in one transaction before returning the collection. A migration failure rolls back those changes and fails initialization. Stored numerical reads use the new representation after successful initialization; they do not retry a legacy representation. Canonical storage retains numerical value rather than Decimal scale bits.

Normal collection initialization samples the first present, non-null value independently for each declared Decimal or BigInteger field. A number selects the transactional conversion; text completes only that field check. Sibling fields are checked separately, including a field removed from the model during upgrade and later restored. Populated fields can finish in the first document; absent or all-null fields and unresolved runtime shapes can require searching later documents. This relies on the atomic format upgrade, rather than checking every document or repairing arbitrary mixed representations. If raw SQL or untyped writes left quoted numeric strings in fields intended as Decimal or BigInteger, normalize those known fields through SoloDB 1.3 typed reads and numeric writes before first opening the database with 1.4. Automatic conversion treats text as the current encoded representation; leaving those strings in place can produce incorrect values or conversion errors. Changing an application property from a native type to Decimal or BigInteger can require your own data migration.

Runtime discovery follows the stored shapes encountered while field samples are still needed. Once a runtime field's known numerical members have samples, initialization does not scan later documents for every unseen subtype. Numerical fields exclusive to those later shapes can require manual migration. See [manual numerical migration](numeric-migration.md) for an explicit-field recipe and producer-specific conversion.

Initialize collections that need conversion before accessing them from event handlers. Handler-context collection lookup does not scan or convert numerical fields. Callbacks run inside the active statement and transaction, so they can observe in-flight writes; ordinary collection initialization returns after conversion completes.

### Choosing a JSON API

Use `ParseInto<T>` and `WriteJson<T>` when your application needs typed values and JSON text or bytes. They avoid constructing an intermediate `JsonValue` tree. Use `Parse` and `Serialize` when you need to inspect or edit a JSON tree. Choose UTF-8 for byte-oriented interfaces and UTF-16 for character-oriented interfaces, so encoding conversion is not added unnecessarily.

The direct APIs share the database model conversion policies. Returned models own their data, and the caller owns the final output buffer. Buffer growth, model construction and required output copies can allocate.

## SQLite provider compatibility

SoloDB preserves a SQLitePCL provider selected by the application before its first connection. Database startup checks required JSONB and trigger RAISE() message-expression support (SQLite 3.47.0 and later); it does not reject an engine based on vulnerability-version labels or override a vendor backport.

The default native dependency supplies SQLite 3.53.3, including the WAL-reset fix. Package numbers alone do not identify the loaded engine; use `SELECT sqlite_version()` on the actual connection.

The document-store feature check does not govern direct `NativeSqliteConnection` or ADO use. Generated registration does not select or upgrade SQLite. See [direct SQL engine guidance](native-sql.md#sqlite-engine-and-wal-compatibility).

## F# Option and Result

Option uses the simple null/value format below in ordinary and generated conversion.

| F# value | JSON representation |
| --- | --- |
| `None` | `null` |
| `Some 7` | `7` |
| `Some "Ada"` | `"Ada"` |

Like `Nullable<T>`, an option stores absence as null and presence as the underlying value. This format does not preserve every possible option layer: `Some null` and `Some None` also write null, which reads back as `None`. Nested absent layers are not represented separately. Use an explicit model when your application needs to distinguish those states.

The same policy applies to explicit trees and direct UTF-8/UTF-16 conversion through ordinary and generated operations. The option reader also accepts the Nullable-compatible object form: `{"HasValue":true,"Value":7}` reads as `Some 7`, and `{"HasValue":false}` reads as `None`. A true wrapper constructs `Some` even when its decoded payload is null or another absent option; writing that result in the flat format still loses the separate presence. An object containing `HasValue` is interpreted as this compatibility wrapper before ordinary payload decoding. Writers continue using the simple null/value format. Existing `Nullable<T>` behavior remains unchanged. `Result<T, TError>` retains its existing union representation and case identity; it does not use the option null/value format.

General option conversion does not add option-wrapped relation properties. `option<DBRef<_>>` and `option<DBRefMany<_>>` remain unsupported; use the existing reference types and their empty-reference states.
