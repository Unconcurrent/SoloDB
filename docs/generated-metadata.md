# Generated metadata registration

To generate operations, build with .NET SDK 8 or later. The `SoloDB` package includes a C# source generator, an F# type provider and their build integration. Keep the package's build assets enabled; no separate generator package or manual build-target import is required for package consumers.

You can use your existing model classes and records in JIT and NativeAOT applications. Models do not need `partial` declarations or a separate generated version. Both builds use the same database format; see the [numerical storage upgrade](#numerical-storage-and-migration) when upgrading existing data.

## Conditional activation

The `#if !DEBUG` guards in the examples below keep generation out of Debug builds, so those builds pay no code-generation cost. SoloDB uses runtime discovery there. Release builds include generated operations for NativeAOT.

In F#, guard both the provider alias and its registration call. Remove the guards if you want generated operations in Debug too. A runtime `if` cannot skip code generation because it runs after the build.

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

## JIT and NativeAOT deployment

Use the registration shown above. For NativeAOT, add this to the executable project file, even when your models live in a .NET Standard library:

```xml
<PropertyGroup>
  <PublishAot Condition="'$(Configuration)' == 'Release'">true</PublishAot>
</PropertyGroup>
```

This example enables NativeAOT for Release builds and keeps ordinary runtime discovery in Debug. Publish for your target platform, for example Linux x64:

```bash
dotnet publish -c Release -r linux-x64
```

Install the SDK's [NativeAOT platform prerequisites](https://learn.microsoft.com/en-us/dotnet/core/deploying/native-aot/#prerequisites) before publishing. Keep registration enabled in the configuration you publish. For F# queries, use [`CompileQuotation`](#f-query-quotations).

The package includes generated operations in normal build and publish output. When copying a managed build, keep its generated helper assembly beside the application. NativeAOT publishing compiles those operations into the executable. Compiler tooling is needed only during the build.

The runtime library requires FSharp.Core 6.0.7 or later and supports newer application versions. The generation SDK requirement does not change the runtime requirements of ordinary SoloDB applications.

On .NET 8 linux-x64 with FSharp.Core 8.0.403, formatted `printfn` can fail with missing native generic code even without SoloDB. Use `Console.WriteLine`, as in the examples, for that deployment.

## Root discovery and registration lifetime

The generator follows the concrete types used by collection, JSON and query calls, including closed generic helpers. If a type is chosen only at runtime, it may need an explicit `[SoloDBAOT]` model declaration or a concrete query signature. Follow `SOLO003` warnings at the reported call.

Fixed recursive models, such as `Node<T>` containing another `Node<T>`, are supported. A model that requires infinitely many closed types, such as `Node<T>` containing `Node<List<T>>`, reports `SOLO001`; use a finite model for generated registration.

Raw SQL parameter objects have [separate argument capture](native-sql.md#typed-arguments-and-direct-mapping); they do not need JSON model declarations.

Each consuming project registers its own operations. A library can expose an initialization method that calls its project-local registration. Call it before using that library's generated operations. Successful repeated calls, including concurrent calls, are no-ops; a failed call can be retried. Registration does not run model constructors or getters.

## F# query quotations

Use `CompileQuotation` for F# NativeAOT queries. It captures the query before FSharp.Core's ordinary lambda-to-LINQ conversion, which can fail under NativeAOT. Existing `Compile` calls continue to work in JIT applications.

Write an ordinary lambda, without a marker argument or quotation brackets:

```fsharp
open System.Linq

let count = people.CompileQuotation(fun query -> query.Count())
let total = count.Invoke()

let find = people.CompileQuotation(fun query id ->
    query.Single(fun person -> person.Id = id))
let person = find.Invoke(id)
```

Keep every query operator inside the outer `CompileQuotation` lambda. Constructing a query with an ordinary F# lambda first can trigger conversion before `CompileQuotation` sees it. Sequence, ordered sequence, scalar and client-result forms support up to three invocation arguments.

The package prepares unsigned F# union libraries for NativeAOT when needed, without changing the original model libraries. If that preparation is required for a signed library, publishing stops with `SOLO005`. Other signed libraries remain supported.

## Direct typed JSON

Use direct JSON when you have a typed value and need JSON text or bytes, without building an intermediate tree. Use `Parse` and `Serialize` when you want to inspect or edit a `JsonValue` tree.

`ParseInto<T>` reads a string, UTF-8 byte span or UTF-16 character span directly into a supported type. `WriteJson<T>` writes that type to a span or appends it to a byte or character list:

```csharp
using System;
using SoloDatabase.JsonSerializator;

Span<byte> output = Span<byte>.Empty;
int[] source = { 1, 2, 3 };
int written = JsonValue.WriteJson(ref source, ref output);
ReadOnlySpan<byte> json = output.Slice(0, written);
int[] values = JsonValue.ParseInto<int[]>(json);
Console.WriteLine(values.Length);
```

This writes `[1,2,3]` as UTF-8 and prints `3`. SoloDB grows the initially empty span with a managed array. Use only the written prefix of the final span returned through `ref output`.

Choose UTF-8 for byte-oriented interfaces and UTF-16 when your input or output already uses characters. Returned models own their data; buffer growth, model construction and output copies can allocate. Ordinary execution and generated support use the same model conversion policies. See [registration](#conditional-activation) when publishing with NativeAOT.

### Direct JSON overloads

Writers take the model by `ref`, including class references. Mutable struct getters operate on that supplied location. Writers borrow proven mutable auto-property backing storage, so child getter mutations persist there; computed, effectful and readonly property results are captured once before their child writer runs. `ParseInto<T>` keeps its returning API.

The overloads below use C# API notation:

```text
int JsonValue.WriteJson<T>(ref T value, ref Span<byte> destination,
    JsonBufferGrow<byte> grow = null, JsonBufferRelease<byte> release = null);
int JsonValue.WriteJson<T>(ref T value, ref Span<char> destination,
    JsonBufferGrow<char> grow = null, JsonBufferRelease<char> release = null);
int JsonValue.WriteJson<T>(ref T value, IList<byte> destination);
int JsonValue.WriteJson<T>(ref T value, IList<char> destination);
```

### Reading objects and handling errors

`ParseInto<T>(null)` with the string overload returns the default value of `T`; an empty byte or character span raises `ArgumentException`.

`Parse` and `ParseInto<T>` accept at most 1000 nested object or array containers and raise `FormatException` beyond that limit. Sibling containers and braces inside strings do not increase nesting.

Forward object readers convert recognized properties in input order. Mutable properties are assigned as encountered; immutable constructor arguments are held in typed locals and the model is constructed after the object ends. Missing mutable properties retain their constructor defaults, and missing constructor arguments use their typed defaults.

For example, reading `{"Count":1,"Count":2}` into a model with an integer `Count` property assigns both values and ends with `2`. Reading `{"Count":"invalid","Count":2}` fails on the first value; the later valid value does not rescue it.

In forward object readers, each duplicate occurrence is converted: the last successfully converted value wins, including names made equivalent by JSON escapes. An invalid earlier occurrence fails even if a later occurrence would be valid. Constructors and setters may already have run when a later malformed value or trailing input causes failure; their effects are not rolled back. Explicit tree parsing and subsequent tree conversion retain their separate duplicate and conversion-order behavior; do not use tree conversion to predict direct-reader callback effects.

Collection conversion preserves the requested collection's ordering. For example, reading `[1,2,3]` into `Stack<int>` produces that enumeration order, rather than reversing it as three successive `Push` calls would. Options and Result use the [representations below](#f-option-and-result), rather than ordinary mutable-object assignment rules.

A different assignable runtime type selected by a late `$type` discriminator uses an exceptional read of the object as that type. A failed declared-type read may also select this route when the object identifies a different assignable type. Earlier declared-type effects remain and the selected read can add effects. When the selected type is unchanged, the original failure is preserved. Missing or same-type discriminators use the ordinary direct path.

Typed byte-sequence writers materialize the sequence before destination callbacks, then write base64 directly to the destination without constructing an intermediate base64 string. The explicit `JsonValue` tree APIs retain their owning string representation.

### Output buffers and callbacks

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

To choose a larger managed buffer yourself, supply a growth callback:

```csharp
Span<byte> output = Span<byte>.Empty;
int[] source = { 1, 2, 3 };
int written = JsonValue.WriteJson(ref source, ref output,
    minimumCapacity => new byte[Math.Max(minimumCapacity, 4096)]);
ReadOnlySpan<byte> json = output.Slice(0, written);
```

This callback receives the required total capacity. It returns independent managed storage; no release callback is needed. To use a pool or native allocator instead, provide a matching release callback and retain the ownership information your allocator needs. The rules below protect the old contents while the writer changes buffers.

With no growth callback, SoloDB allocates a managed array when capacity is insufficient. Growth requests use checked, geometric capacity increases. Serialization does not restart or repeat getters and enumeration to size the output. A supplied growth callback must return at least the requested total capacity; an undersized result raises `InvalidOperationException` rather than switching to automatic allocation.

A release callback requires a custom growth callback. An invalid pairing raises `ArgumentException` before serialization or callbacks. Growth must leave old storage valid until SoloDB copies the written prefix. SoloDB then publishes the replacement through `ref destination` and calls release with the full old span, including an empty initial span. It never releases the successful final destination; the caller releases it after use.

When supplying release, initial and replacement buffers must follow the same allocator policy. Replacement storage must be independently owned and must not share an allocation that release will invalidate. Overlapping spans are rejected; disjoint spans alone cannot establish separate allocation ownership. The callbacks must retain their allocator's ownership information. A native reallocation that invalidates the old buffer before copying is unsuitable. Callback construction, including capturing lambdas, may allocate.

If growth throws, destination still identifies the old buffer. If release throws after replacement publication, destination identifies the new buffer. An invalid replacement leaves the original destination unchanged. A nonempty, nonoverlapping rejected replacement is passed to release when supplied; cleanup failure does not replace the original validation error. Allocator independence remains the callback's responsibility, including rejected replacements.

List overloads append to existing contents and return only the number of elements added. A null list raises `ArgumentNullException`; a read-only list raises `ArgumentException` before serialization. Writable lists use their append operations, with capacity growth for `List<byte>` and `List<char>`. No complete temporary JSON array is required merely to append. Callback, allocation, getter, enumeration and list failures propagate; partially written output is not rolled back. After a span failure, the caller retains responsibility for the buffer currently identified by destination.

### JSON numbers

When `ParseInto<object>` infers an integral number, it permits up to 32768 additional decimal zero digits from positive exponent expansion. Beyond that budget it returns signed `Double` infinity. This is an inference work budget, not a maximum numerical value: `1e32769` saturates, while `10e32768` remains an exact `BigInteger` because one more coefficient digit is already written. Written coefficient digits are not capped, and explicitly requesting `BigInteger` retains its arbitrary precision within available runtime and memory limits. Encoded numerical parameters and symbolic exact arithmetic retain their exact representation.

Direct numeric conversion uses the declared integer, `double`, `float` or `decimal` type. Floating values avoid the decimal-backed tree's rounding and range limitations, so direct and tree output can differ for values outside Decimal precision or range. Direct floating-point writers use invariant numeric formatting, preserve negative zero as `-0`, and emit the JSON5 literals `NaN`, `Infinity` and `-Infinity` for nonfinite values. Explicit `JsonValue.Number` keeps its decimal representation. .NET Standard 2.0 UTF-16 numeric formatting may allocate a number-only string; direct output is not an allocation-free guarantee.

Direct floating readers retain a `NaN` number token, including in present nullable values, lists and Object-valued members. The explicit tree APIs have different directional rules: `JsonValue.Serialize` of a nonfinite floating value raises `OverflowException`, while `JsonValue.Parse("NaN")` produces a Decimal-backed zero. These tree conversions are not a native floating-point roundtrip.

### Numerical storage and migration

Typed document persistence stores Decimal and BigInteger values as canonical comparable strings; public direct JSON writes native numeric values. Typed numerical queries retain exact coefficients and exponents until the requested operation or conversion applies its native rounding and range rules. Ordinary string properties and numerical dictionary names retain their original meaning. Raw SQL extraction exposes the encoded text; SQLite's native casts and arithmetic do not decode that representation. Untyped collections expose the same physical JSON: an encoded numerical value is a String, and an explicit `JsonValue` write does not infer the numerical policy of another declared model. Writes through untyped JSON or raw SQL must preserve that stored representation if typed access will follow. Use typed collection methods for ordinary numerical reads and updates.

Collection persistence passes floating JSON through SQLite JSONB, which [interprets a `NaN` number token as JSON null](https://www.sqlite.org/json1.html#json5). Nonnullable `double` and `float` readers materialize that null as NaN; nullable floating values and Object-valued members materialize it as null. List elements follow their declared element type. Original NaN and genuine null cannot be distinguished after this normalization. A quoted `"NaN"` remains a String. Typed floating-point infinity remains infinity. Raw SQL parameter binding has its [own conversion rules](native-sql.md#typed-arguments-and-direct-mapping).

Before upgrading, stop every older SoloDB process using the database. A writable opening with SoloDB 1.4.2 advances `PRAGMA user_version` to 7 through normal schema migration, so SoloDB 1.4.1 and earlier refuse a later opening. This does not revoke connections already held by an older process. Do not lower the version stamp to bypass the check. The database version is established at opening; numerical fields are converted separately by explicit `MigrateDecimal<T>()` or `MigrateDecimal<T>(name)` calls.

Call `db.MigrateDecimal<T>()` for the default collection or `db.MigrateDecimal<T>(name)` for an existing named collection before reading legacy Decimal or BigInteger fields. The operation converts the declared numerical fields atomically. A root call starts a transaction; a transactional call uses a savepoint, and a failure rolls back the conversion. A missing collection raises `KeyNotFoundException`. Collection acquisition performs no numerical checks or conversion. Stored numerical reads use the current representation; they do not retry a legacy representation. Canonical storage retains numerical value rather than Decimal scale bits.

The explicit migration operation samples the first present, non-null value independently for each declared Decimal or BigInteger field. A number selects the transactional conversion; text completes only that field check. Sibling fields are checked separately, including a field removed from the model during upgrade and later restored. Populated fields can finish in the first document; absent or all-null fields and unresolved runtime shapes can require searching later documents. This relies on the atomic format upgrade, rather than checking every document or repairing arbitrary mixed representations. If raw SQL or untyped writes left quoted numeric strings in fields intended as Decimal or BigInteger, normalize those known fields through SoloDB 1.3 typed reads and numeric writes before first opening the database with 1.4. Migration treats text as the current encoded representation; leaving those strings in place can produce incorrect values or conversion errors. Changing an application property from a native type to Decimal or BigInteger can require your own data migration.

Runtime discovery follows the stored shapes encountered while field samples are still needed. Once a runtime field's known numerical members have samples, migration does not scan later documents for every unseen subtype. Numerical fields exclusive to those later shapes can require manual migration. See [manual numerical migration](numeric-migration.md) for an explicit-field recipe and producer-specific conversion.

Migrate collections before accessing legacy numerical fields from event handlers. Migration is not supported inside handlers, where an active statement prevents its required savepoint. Collection lookup performs no numerical scanning or conversion. Callbacks run inside the active statement and transaction and can observe in-flight writes.

### Choosing a JSON API

Use `ParseInto<T>` and `WriteJson<T>` for typed conversion, or `Parse` and `Serialize` to inspect or edit a tree. The [reader rules](#reading-objects-and-handling-errors), [buffer ownership rules](#output-buffers-and-callbacks) and [number formats](#json-numbers) describe the differences that matter when choosing between them.

### Internal SQLite JSON reads

Immediate internal materialization on the bundled engine parses borrowed native UTF-16 text through the existing JSON reader. SQLite's text conversion occurs before user callbacks; typed materialization uses the declared stored-value policy. Configured providers without this native UTF-16 access retain owning text. This applies to ordinary and compiled query enumeration, Many relation row loops, and native JSON-tree projections. The text is consumed synchronously while the private producing statement remains active; no native span or pointer escapes into returned models or lazy results. Public UTF-8 codecs remain separate.

Deferred paths retain owning payloads where required by existing behavior: collection and scalar operations whose statement completion or cardinality checks precede construction, Single relation batches that gather rows before constructing per-owner instances, mutation payloads, and deferred hydration. Those copies preserve constructor and notification order and relation identity. Typed optional JSON reads through caller-controlled ADO readers also retain owning input: application code can advance or dispose that exact producer, so the private-statement lifetime proof does not apply. They are deliberate exceptions, not a claim that every internal read avoids a copy.

Public `GetString` and requested string projections still return owning strings. Materialized model strings, arrays and objects also own their data; borrowed input does not make results zero-copy or allocation-free. Caller APIs, forward-reader effects, application callbacks and output-buffer ownership remain unchanged.

### Internal JSON writes and parameters

Insert, update/replacement and cascade writes retain the shared writer's default-grown character array and used range, then bind that owned UTF-16 text without constructing a final JSON string. Structured stored query parameters use the same owned-text representation. Serialization stays at its existing capture point, preserving getter effects and rebinding the captured output without serializing again.

Compiled calls use one independent invocation frame and a bounded named-parameter range. Overlapping, lazy and recursive executions retain separate state; ref/out locations and exception writes remain intact. This changes internal transport, not public object parameter APIs or span/list writer callbacks and buffer ownership.

Unicode compatibility fallback can still create owning strings. SQLite's transient text binding copies the supplied text before returning; removal of an intermediate managed JSON string is not zero-copy native binding or allocation-free execution. Existing TEXT/JSONB storage semantics remain.

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
| `Some null` | `null`, read back as `None` |
| `Some None` | `null`, read back as `None` |

Like `Nullable<T>`, an option stores absence as null and presence as the underlying value. This format does not preserve every possible option layer: `Some null` and `Some None` also write null, which reads back as `None`. Nested absent layers are not represented separately. Use an explicit model when your application needs to distinguish those states.

### Reading legacy option wrappers

The same policy applies to explicit trees and direct UTF-8/UTF-16 conversion through ordinary and generated operations. The option reader also accepts the Nullable-compatible object form: `{"HasValue":true,"Value":7}` reads as `Some 7`, and `{"HasValue":false}` reads as `None`. A true wrapper constructs `Some` even when its decoded payload is null or another absent option; writing that result in the flat format still loses the separate presence. An object containing `HasValue` is interpreted as this compatibility wrapper before ordinary payload decoding. Writers continue using the simple null/value format. Existing `Nullable<T>` behavior remains unchanged.

### Result cases

`Result<T, TError>` stores the selected case as an object, rather than using the option null/value format. For example, `Ok 7: Result<int,string>` writes:

```json
{"Tag":0,"IsOk":true,"IsError":false,"ResultValue":7,"ErrorValue":null}
```

`Error "failure"` writes:

```json
{"Tag":1,"IsOk":false,"IsError":true,"ResultValue":0,"ErrorValue":"failure"}
```

The inactive payload field keeps its typed default. These fields preserve case identity even when an error payload is null: `Error null` remains the Error case.

### Relation properties

General option conversion does not add option-wrapped relation properties. `option<DBRef<_>>` and `option<DBRefMany<_>>` remain unsupported; use the existing reference types and their empty-reference states.
