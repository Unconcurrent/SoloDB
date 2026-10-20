# Generated metadata registration

> This guide describes the upcoming SoloDB 1.4.0. The published NuGet release is 1.3.0; see the [README](../README.md#installation) for the release distinction.

Package-backed C# and F# examples and generated query controls have passed JIT and linux-x64 NativeAOT checks. These results do not establish every native platform or warning-free publication. F# NativeAOT queries use `CompileQuotation` as described below.

The `SoloDB` package includes a C# source generator, an F# type provider and their build integration. Keep the package's build assets enabled; no separate generator package or manual build-target import is required for package consumers.

Both routes generate operations for your original model types, preserving their visibility and CLR identity. They share conversion and storage policies with ordinary runtime-discovered operations; registration does not select a different database format. The [numerical storage upgrade](#numerical-storage-and-migration) applies to both routes. You do not need replacement models or `partial` declarations on models. Generated operations can also be used in ordinary JIT applications.

Private nested C# model controls, including closed generics, arrays, captured fields and anonymous client results, pass package-backed JIT with dynamic discovery disabled and linux-x64 NativeAOT checks.

## C#

Call the generated project-local `SoloDBAOT.Register()` before the first operation that needs generated metadata:

```csharp
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
        SoloDBAOT.Register();

        using (var db = new SoloDB("memory:people"))
        {
            var people = db.GetCollection<Person>();
            var id = people.Insert(new Person { Name = "Ada" });
            var person = people.GetById(id);
        }
    }
}
```

The compiler discovers `Person` from the typed collection use. To select a model explicitly, add `[SoloDBAOT]` to its declaration. The attribute resolves through `using SoloDatabase`; the registration call uses the separate generated static class. Reserve the global name `SoloDBAOT` for that entry.

## F#

Declare a provider alias and call its lowercase `register` method before use:

```fsharp
namespace Example

open SoloDatabase

type Person() =
    member val Id = 0L with get, set
    member val Name = "" with get, set

type AOTSupport = SoloDBAOT<0>

module Startup =
    let run () =
        AOTSupport.register()

        use db = new SoloDB("memory:people")
        let people = db.GetCollection<Person>()
        let id = people.Insert(Person(Name = "Ada"))
        people.GetById(id)
```

`0` is the provider's static argument, not a model identifier. Model roots are discovered from the current project's resolved SoloDB uses. `[<SoloDBAOT>]` on a model also selects it explicitly. Keep ordinary F# source ordering: declare the alias before calling it.

## Root discovery and registration lifetime

Active generation combines explicitly selected models with concrete types inferred from typed collection and JSON calls, closed generic helper uses and supported query signatures. It follows their required operation dependencies without generating every type in referenced assemblies. Explicit and inferred roots retain the same original types.

Raw SQL argument inputs have their own generated capture capability; they do not need JSON model roots. See [SQL argument capture](native-sql.md#typed-arguments-and-direct-mapping) for property selection, source availability and acquisition behavior.

An open-ended runtime-selected type cannot always be inferred. Resolve `SOLO003` warnings at the reported use: supply an explicit model root and a concrete closed signature where required. Attributes do not make an unsupported operation supported.

Each consuming project registers its own generated operations. A library can call its project-local registration from its own initialization method. Call registration before generated metadata is needed, including in libraries used by the application. Successful repeated calls, including concurrent calls, are no-ops; a failed registration can be retried. Registration does not invoke model constructors or getters.

## Conditional activation

Generation follows the registration call in the compiled source. Attributes or an F# provider alias alone do not activate full model generation. An ordinary runtime `if` does not disable generation; exclude the call with a preprocessor condition instead.

For example, add a user-defined symbol to the consuming `.csproj` or `.fsproj`:

```xml
<PropertyGroup Condition="'$(Configuration)' == 'Release'">
  <DefineConstants>$(DefineConstants);SOLODB_GENERATED</DefineConstants>
</PropertyGroup>
```

Then guard the registration call in C#:

```csharp
#if SOLODB_GENERATED
SoloDBAOT.Register();
#endif
```

Or in F#:

```fsharp
#if SOLODB_GENERATED
AOTSupport.register()
#endif
```

This example activates generation in Release and omits it in Debug. Any user-defined symbol and configuration can be used; Release is not required. `RELEASE` is not a universally predefined SDK symbol. Choose conditions that also activate registration in every configuration you intend to publish with NativeAOT. Switching configurations or removing the active call updates the generated output through the package build integration.

## JIT and NativeAOT deployment

When generation is inactive, ordinary JIT execution discovers supported type operations lazily at runtime. Registered generated operations take precedence when present. Captured generic fields, nullable comparisons, exact constructors and nested predicates pass current compatibility controls. NativeAOT needs generated coverage; runtime discovery is not a substitute for missing operations. PublishAot belongs on the consuming executable project, including when its models live in a .NET Standard library.

The compiler and provider tooling are build-time dependencies and are not copied into application output. Keep generated runtime helper assemblies delivered by the package build integration: those contain application operations rather than the compiler tooling.

The build integration supports .NET SDK 8 and later; installing SDK 10 separately is unnecessary. F# analysis uses the installed SDK's .NET host. The runtime library requires FSharp.Core 8.0.100 or later, and projects can retain a newer FSharp.Core version.

Application APIs have separate NativeAOT requirements. On .NET 8 linux-x64 with FSharp.Core 8.0.403, formatted `printfn` can fail with missing native generic code even in an executable without SoloDB. Model registration does not supply that framework code. Use compatible application output, such as `Console.WriteLine`, for that deployment.

Registration prepares type operations and query execution routes; it does not pregenerate SQL. LINQ translation and `collection.Compile` retain their existing responsibilities. Validate the actual queries and deployment target you use.

## F# query quotations

For F# NativeAOT applications, use `CompileQuotation` instead of passing an ordinary F# lambda to `Compile`. F# lambda-to-LINQ conversion through `LeafExpressionConverter` fails under NativeAOT when it accesses `MetadataToken`, before SoloDB receives the expression. Model registration does not fix that caller-side failure. `CompileQuotation` captures the whole query lambda before that conversion and uses SoloDB's existing query planning and execution. Existing `Compile` calls remain supported on JIT, and already constructed LINQ expressions retain their existing API.

Write an ordinary lambda, without a marker argument or quotation brackets:

```fsharp
let count = people.CompileQuotation(fun query -> System.Linq.Queryable.Count(query))
let total = count.Invoke()

let find = people.CompileQuotation(fun query id ->
    System.Linq.Queryable.Single(query, fun person -> person.Id = id))
let person = find.Invoke(id)
```

Keep query construction, including `Where`, `OrderBy` and `Select` lambdas, inside the outer `CompileQuotation` lambda. Changing only the final method cannot repair conversion that already happened while building a query. Sequence, ordered sequence, scalar and client-result forms accept zero through three invocation arguments and retain their existing delegate and result semantics.

Package-backed expanded F# quotation controls pass on linux-x64 NativeAOT with dynamic discovery disabled. Record construction uses registered field order and direct constructor operations, without runtime constructor discovery or constructor-handle lookup. F# quotation construction still has its own metadata requirements; generated execution support does not imply application-wide zero reflection. These checks do not establish all-platform query coverage.

Some F# union field accessors need a metadata flag correction for NativeAOT quotation deserialization. For affected unsigned model libraries, the package automatically prepares private native compiler inputs with the necessary flag; original assemblies, model signatures and method bodies remain unchanged. No manual patch or extra package is required, and `FSharp.Core` is not modified. A signed model library needing this correction remains unchanged and stops the native build with a `SOLO005` error because signed-library correction is unsupported. Signed assemblies that do not need the correction remain supported.

## Direct typed JSON

Direct JSON operations share model conversion policies across ordinary execution and generated support. Package-backed C# and F# checks cover linux-x64 NativeAOT; these checks do not establish every native platform or warning-free publication.

`JsonValue.ParseInto<T>` reads a string, `ReadOnlySpan<byte>` containing UTF-8, or `ReadOnlySpan<char>` containing UTF-16 directly into a supported type. `JsonValue.WriteJson<T>` writes that type to a destination. Existing `Serialize<T>` still returns a `JsonValue` tree, and `Parse` still creates a tree.

The overloads below use C# API notation:

```text
int JsonValue.WriteJson<T>(T value, ref Span<byte> destination,
    JsonBufferGrow<byte> grow = null, JsonBufferRelease<byte> release = null);
int JsonValue.WriteJson<T>(T value, ref Span<char> destination,
    JsonBufferGrow<char> grow = null, JsonBufferRelease<char> release = null);
int JsonValue.WriteJson<T>(T value, IList<byte> destination);
int JsonValue.WriteJson<T>(T value, IList<char> destination);
```

`ParseInto<T>(null)` with the string overload returns the default value of `T`; an empty byte or character span raises `ArgumentException`.

`Parse` and `ParseInto<T>` accept at most 1000 nested object or array containers and raise `FormatException` beyond that limit. Sibling containers and braces inside strings do not increase nesting.

When `ParseInto<object>` infers an integral number, it permits up to 32768 additional decimal zero digits from positive exponent expansion. Beyond that budget it returns signed `Double` infinity. This is an inference work budget, not a maximum numerical value: `1e32769` saturates, while `10e32768` remains an exact `BigInteger` because one more coefficient digit is already written. Written coefficient digits are not capped, and explicitly requesting `BigInteger` retains its arbitrary precision within available runtime and memory limits. Encoded numerical parameters and symbolic exact arithmetic retain their exact representation.

Forward object readers convert recognized properties in input order. Mutable properties are assigned as encountered; immutable constructor arguments are held in typed locals and the model is constructed after the object ends. Missing mutable properties retain their constructor defaults, and missing constructor arguments use their typed defaults.

In forward object readers, each duplicate occurrence is converted: the last successfully converted value wins, including names made equivalent by JSON escapes. An invalid earlier occurrence fails even if a later occurrence would be valid. Constructors and setters may already have run when a later malformed value or trailing input causes failure; their effects are not rolled back. Explicit tree parsing and subsequent tree conversion retain their separate duplicate and conversion-order behavior. Optional wrappers, unions and directional collection readers, including Stack, retain their established policies.

A different assignable runtime type selected by a late `$type` discriminator uses an exceptional read of the object as that type. A failed declared-type read may also select this route when the object identifies a different assignable type. Earlier declared-type effects remain and the selected read can add effects; this is not effect-order parity with the former prescanned reader. When the selected type is unchanged, the original failure is preserved. Missing or same-type discriminators use the ordinary direct path.

Each typed conversion uses a fixed operation for its type and encoding. Project registration supplies generated operations before use; ordinary JIT execution prepares its operation on first use. Runtime polymorphism preserves the actual type and its discriminator where required. Growth/release ownership and constructor, getter, setter and output callback effects retain their documented behavior.

Typed byte-sequence writers materialize the sequence before destination callbacks, then write base64 directly to the destination without constructing an intermediate base64 string. The explicit `JsonValue` tree APIs retain their owning string representation.

### Internal SQLite JSON reads

Immediate internal materialization on the bundled engine parses borrowed native UTF-16 text through the existing JSON reader. SQLite's text conversion occurs before user callbacks; typed materialization uses the declared stored-value policy. Configured providers without this native UTF-16 access retain owning text. This applies to ordinary and compiled query enumeration, Many relation row loops, and native JSON-tree projections. The text is consumed synchronously while the private producing statement remains active; no native span or pointer escapes into returned models or lazy results. Public UTF-8 codecs remain separate. Earlier package-backed checks covered JIT and linux-x64 NativeAOT; other native platforms were not executed.

Deferred paths retain owning payloads where required by existing behavior: collection and scalar operations whose statement completion or cardinality checks precede construction, Single relation batches that gather rows before constructing per-owner instances, mutation payloads, and deferred hydration. Those copies preserve constructor and notification order and relation identity. Typed optional JSON reads through caller-controlled ADO readers also retain owning input: application code can advance or dispose that exact producer, so the private-statement lifetime proof does not apply. They are deliberate exceptions, not a claim that every internal read avoids a copy.

Public `GetString` and requested string projections still return owning strings. Materialized model strings, arrays and objects also own their data; borrowed input does not make results zero-copy or allocation-free. Caller APIs, forward-reader effects, application callbacks and output-buffer ownership remain unchanged. One checked warmed comparison against the preceding implementation measured 300 operations per case. Managed bytes per operation fell from 1,373,088 to 1,127,328 for document reads, 1,360,208 to 1,114,448 for compiled reads, and 630,244 to 602,090 for hydrated owners. Total elapsed times were 406.026 to 259.044 ms, 242.376 to 241.323 ms, and 245.498 to 253.228 ms respectively. This single nonstatistical sample shows mixed timing and retains the copying exceptions above; it establishes no universal speedup or competitor advantage.

### Internal JSON writes and parameters

Insert, update/replacement and cascade writes retain the shared writer's default-grown character array and used range, then bind that owned UTF-16 text without constructing a final JSON string. Structured stored query parameters use the same owned-text representation. Serialization stays at its existing capture point, preserving getter effects and rebinding the captured output without serializing again.

Compiled calls use one independent invocation frame and a bounded named-parameter range. Overlapping, lazy and recursive executions retain separate state; ref/out locations and exception writes remain intact. This changes internal transport, not public object parameter APIs or span/list writer callbacks and buffer ownership.

Unicode compatibility fallback can still create owning strings. SQLite's transient text binding copies the supplied text before returning; removal of an intermediate managed JSON string is not zero-copy native binding or allocation-free execution. Existing TEXT/JSONB storage semantics remain. Package-backed checks cover JIT and linux-x64 NativeAOT; other native platforms were not executed, and existing native warning qualifications remain.

One checked comparison of 300 operations measured document update/read allocation falling from 21,912 to 17,376 bytes per operation in ordinary execution and 21,320 to 16,784 bytes in generated execution. Total times were 38.897 to 42.032 ms and 35.972 to 35.366 ms respectively. Compiled scalar allocation fell from 264 to 248 bytes in both routes; times rose from 5.010 to 5.452 ms and 5.161 to 5.577 ms. Client-tail allocation remained 20,040 bytes, with times 38.576 to 38.808 ms and 38.136 to 38.441 ms. This single nonstatistical sample has mixed timing results; it establishes no universal speedup, zero-allocation behavior or competitor advantage.

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
int written = JsonValue.WriteJson(person, ref destination);
ReadOnlySpan<byte> json = destination.Slice(0, written);
```

With no growth callback, SoloDB allocates a managed array when capacity is insufficient. Growth requests use checked, geometric capacity increases. Serialization does not restart or repeat getters and enumeration to size the output. A supplied growth callback must return at least the requested total capacity; an undersized result raises `InvalidOperationException` rather than switching to automatic allocation.

A release callback requires a custom growth callback. An invalid pairing raises `ArgumentException` before serialization or callbacks. Growth must leave old storage valid until SoloDB copies the written prefix. SoloDB then publishes the replacement through `ref destination` and calls release with the full old span, including an empty initial span. It never releases the successful final destination; the caller releases it after use.

When supplying release, initial and replacement buffers must follow the same allocator policy. Replacement storage must be independently owned and must not share an allocation that release will invalidate. Overlapping spans are rejected; disjoint spans alone cannot establish separate allocation ownership. The callbacks must retain their allocator's ownership information. A native reallocation that invalidates the old buffer before copying is unsuitable. Callback construction, including capturing lambdas, may allocate.

If growth throws, destination still identifies the old buffer. If release throws after replacement publication, destination identifies the new buffer. An invalid replacement leaves the original destination unchanged. A nonempty, nonoverlapping rejected replacement is passed to release when supplied; cleanup failure does not replace the original validation error. Allocator independence remains the callback's responsibility, including rejected replacements.

List overloads append to existing contents and return only the number of elements added. A null list raises `ArgumentNullException`; a read-only list raises `ArgumentException` before serialization. Writable lists use their append operations, with capacity growth for `List<byte>` and `List<char>`. No complete temporary JSON array is required merely to append. Callback, allocation, getter, enumeration and list failures propagate; partially written output is not rolled back. After a span failure, the caller retains responsibility for the buffer currently identified by destination.

Direct numeric conversion uses the declared integer, `double`, `float` or `decimal` type. Floating values avoid the decimal-backed tree's rounding and range limitations, so values previously rounded or rejected cannot have a blanket byte-identical-output guarantee. Direct floating-point writers use invariant numeric formatting, preserve negative zero as `-0`, and emit the JSON5 literals `NaN`, `Infinity` and `-Infinity` for nonfinite values. Explicit `JsonValue.Number` keeps its decimal representation. .NET Standard 2.0 UTF-16 numeric formatting may allocate a number-only string; direct output is not an allocation-free guarantee.

### Numerical storage and migration

Typed document persistence stores Decimal and BigInteger values as canonical comparable strings; public direct JSON writes native numeric values. Typed numerical queries retain exact coefficients and exponents until the requested operation or conversion applies its native rounding and range rules. Ordinary string properties and numerical dictionary names retain their original meaning. Raw SQL extraction exposes the encoded text; SQLite's native casts and arithmetic do not decode that representation. Untyped collections expose the same physical JSON: an encoded numerical value is a String, and an explicit `JsonValue` write does not infer the numerical policy of another declared model. Writes through untyped JSON or raw SQL must preserve that stored representation if typed access will follow. Use typed collection methods for ordinary numerical reads and updates.

Before upgrading, stop every older SoloDB process using the database. Opening with SoloDB 1.4 advances `PRAGMA user_version` to 6 in the schema initialization transaction, so SoloDB 1.3 refuses a later opening. This does not revoke connections already held by an older process. Do not lower the version stamp to bypass the check. The database version is established at opening; numerical fields are converted later as their typed collections are initialized.

When an existing typed collection requires numerical migration, initialization converts its declared numerical fields in one transaction before returning the collection. A migration failure rolls back those changes and fails initialization. Stored numerical reads use the new representation after successful initialization; they do not retry a legacy representation. Canonical storage retains numerical value rather than Decimal scale bits.

Normal collection initialization samples the first present, non-null Decimal or BigInteger value. A number selects the transactional conversion; text identifies the current representation and stops the check. This relies on the atomic format upgrade, rather than checking every document or repairing arbitrary mixed representations. If raw SQL or untyped writes left quoted numeric strings in fields intended as Decimal or BigInteger, normalize those known fields through SoloDB 1.3 typed reads and numeric writes before first opening the database with 1.4. Automatic conversion treats text as the current encoded representation; leaving those strings in place can produce incorrect values or conversion errors. Changing an application property from a native type to Decimal or BigInteger can require your own data migration.

Initialize collections that need conversion before accessing them from event handlers. Handler-context collection lookup does not scan or convert numerical fields. Callbacks run inside the active statement and transaction, so they can observe in-flight writes; ordinary collection initialization returns after conversion completes.

### Earlier direct JSON measurements

Both packaged frontends have JIT and linux-x64 NativeAOT coverage. Other native platforms were not executed; existing native warning qualifications remain.

One checked comparison against the preceding implementation measured 1,000 complete write/read round trips after 100 warmups, using escaped Unicode text and integer arrays of 9 or 4,096 elements. UTF-16 elapsed times fell from 29.852 to 16.868 ms for the small array and 1,316.985 to 637.164 ms for the large array; UTF-8 fell from 26.852 to 16.416 ms and 1,158.034 to 517.965 ms. Managed allocation per operation fell from 6,288 to 4,008 bytes and 1,526,160 to 690,128 bytes for UTF-16, and from 6,048 to 3,768 bytes and 1,460,896 to 624,864 bytes for UTF-8. This single sequential sample is diagnostic, not a statistical or general speedup guarantee. Destination growth, typed arrays and lexical work still allocate. These results do not compare System.Text.Json or Newtonsoft.Json.

## JSON comparison scope

An earlier implementation passed the full functional gate and packaged JIT and linux-x64 NativeAOT comparison, forward-reader and collection checks. These retained results do not establish validation of later numerical changes. Other native platforms were not executed. A narrow typed workload on .NET 10 compares separately owned UTF-8 byte arrays and UTF-16 strings, and separate reads, against System.Text.Json generated serialization/metadata readers and Newtonsoft.Json 13.0.4 with reused contracts. Required output copying and transcoding are included. Strict common inputs preserve declared wide integer, decimal and finite double values. Five rotated warm samples per cell expose variability; setup observations are first calls in an already initialized process, not comparative cold-start measurements.

ASCII model, string and array outputs match exactly across the tested writers. Escape-rich output differs by encoder policy, so those cells compare equivalent values with different wire work. The supplied System.Text.Json writer invokes its actual generated handler where available; default owned APIs remain distinct measured routes. Applicable native checks compare SoloDB and System.Text.Json only; Newtonsoft native reflection support is not established.

The corrected ordinary 4,096-element array reads allocate 295,288 rather than 688,504 managed bytes per operation. Final ordinary median times change from 621.826 to 623.345 microseconds for UTF-16 and 521.054 to 517.675 for UTF-8, showing mixed, near-baseline array timing; generated-route observations are also near baseline with the same allocation reduction. Ordinary plain-string write medians change from 4.778 to 0.871 microseconds for UTF-16 and 4.358 to 0.720 for UTF-8. System.Text.Json remains faster in many cells. These bounded observations establish neither a universal fastest serializer nor allocation-free execution, statistical timing certainty, exact escape-rich compatibility or all-platform results. Exceptional model effects and output callbacks are covered separately from these plain-property performance models.

## SQLite provider compatibility

SoloDB preserves a SQLitePCL provider selected by the application before its first connection. Database startup checks required JSONB and trigger RAISE() message-expression support (SQLite 3.47.0 and later); it does not reject an engine based on vulnerability-version labels or override a vendor backport.

The default native dependency supplies SQLite 3.53.3, including the WAL-reset fix. Packaged JIT and linux-x64 NativeAOT controls report that engine. Static inspection of other platform assets establishes provenance, not execution on every platform; existing NativeAOT warning qualifications remain. Package numbers alone do not identify the loaded engine; use `SELECT sqlite_version()` on the actual connection.

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
