# Reading stored numerical strings

SoloDB stores typed `Decimal` and `BigInteger` leaves as comparable encoded strings. Typed collection reads already decode these leaves. Use `StoredNumeric` when a raw SQL or untyped read has extracted the encoded string itself.

```csharp
using System.Numerics;
using SoloDatabase;
using SoloDatabase.JsonSerializator;

decimal amount = StoredNumeric.DecodeDecimal("d-S"); // 12.5
BigInteger count = StoredNumeric.DecodeBigInteger("d-S"); // 12
bool valid = StoredNumeric.TryDecodeDecimal("d-S".AsSpan(), out amount);
```

`DecodeDecimal`, `DecodeBigInteger`, `TryDecodeDecimal` and `TryDecodeBigInteger` each accept `string` or `ReadOnlySpan<char>`. Their input is the unquoted encoded contents, not a complete JSON string token. Throwing methods report malformed keys with `FormatException`, and unrepresentable Decimal results with `OverflowException`. A null string raises `ArgumentNullException`. Try methods return false for null, malformed keys or target overflow and set the result to zero; unrelated execution failures propagate.

Decimal decoding uses the existing stored-value conversion: rounding to the representable precision with ties to even, underflow to zero and overflow outside the Decimal range. BigInteger decoding truncates fractional values toward zero. Decoding cannot restore Decimal trailing scale that the stored key omitted.

## Choose how typed JSON reads interpret strings

Existing JSON conversion signatures keep their ordinary numeric-text behavior. Encoded keys and ordinary numbers overlap: the JSON string `"11"` means eleven by default, but encodes negative 84 trillion. Select the representation explicitly; no automatic detection is performed.

```csharp
var options = JsonReadOptions.StoredNumericStrings;
var sameOptions = new JsonReadOptions(numericStrings: NumericStringFormat.StoredKey);

JsonValue.ParseInto<decimal>("\"11\"");          // 11
JsonValue.ParseInto<decimal>("\"11\"", options); // -84000000000000
JsonValue.ParseInto<decimal>("\"d-S\"", options); // 12.5
JsonValue.ParseInto<BigInteger>("\"M\"", options); // 0
JsonValue.ParseInto<string>("\"d-S\"", options); // literal d-S
JsonValue.ParseInto<decimal>("12.5", options);    // native JSON number, 12.5

JsonValue tree = JsonValue.Parse("\"d-S\""); // remains a String
decimal amount = tree.ToObject<decimal>(options);
object boxed = tree.ToObject(typeof(decimal), options);
```

The string, UTF-16 character-span and UTF-8 byte-span `ParseInto<T>` overloads all accept options. So do generic and runtime-Type `ToObject` conversions. The selection follows declared Decimal and BigInteger targets through model members, collections and optional wrappers, including generated readers. Other targets keep their existing policies. Tree parsing and native JSON Number input remain unchanged; ordinary String targets retain their contents. Malformed encoded contents fail the typed conversion.

`NumericStringFormat.NumberText` is the default. `JsonReadOptions` is a sealed immutable class with a get-only `NumericStrings` property, default and format constructors, and shared `Default` and `StoredNumericStrings` instances. Omitted or null options use `Default`. Options belong to one conversion and do not alter later conversions or stored database bytes.

```fsharp
let options = JsonReadOptions.StoredNumericStrings
let amount = JsonValue.ParseInto<decimal>("\"d-S\"", options)
let decoded = StoredNumeric.DecodeDecimal("d-S")
```

## Typed raw SQL results

Raw SQL reads also need an explicit representation choice: TEXT `11` is ambiguous, while SQLite INTEGER and REAL are native numbers. The SQL convenience overloads take the same `NumericStringFormat` enum directly after SQL. They do not treat SQL as JSON or require a separate options class.

```csharp
using static SoloDatabase.NativeSQLiteTools.IDbConnectionExtensions;

using var connection = new NativeSqliteConnection("Data Source=:memory:");
connection.Open();
var keys = NumericStringFormat.StoredKey;

decimal amount = connection.QueryFirst<decimal>("SELECT 'd-S'", keys); // 12.5
BigInteger wide = connection.QueryFirst<BigInteger>("SELECT 'x!5!'", keys); // 10^40
decimal plain = connection.QueryFirst<decimal>("SELECT '11'"); // 11
decimal encoded = connection.QueryFirst<decimal>("SELECT '11'", keys); // -84000000000000
var values = connection.Query<decimal>("SELECT @value", keys, parameters: new { value = "d-S" });
```

`Query<T>`, `QueryFirst<T>` and `QueryFirstOrDefault<T>` expose this overload on native connections and the public SQLite connection presentations, including pooled connections. Multi-mapping places the format after SQL and before the existing map, parameters and split-column arguments. The selected mode applies to both typed segments before the callback runs. Existing signatures, named parameters and defaults remain available.

The same mode follows typed scalar, model/record member, tuple, nullable and FSharp-option reads through ordinary and generated row mappers. It belongs to the current invocation, so alternating modes does not change cached mapper behavior. Only TEXT targeting Decimal or BigInteger is decoded as a key. Other SQL kinds, String/untyped results and existing NULL policies retain their meanings. Arbitrary-width integer keys go directly to BigInteger; they are not converted through Decimal or Double. Query enumeration remains lazy, including conversion errors.

For direct native rows, use `rows.Row.GetDecimal(ordinal, keys)` or `rows.Row.GetBigInteger(ordinal, keys)`. The concrete `SqliteDataReader` also offers `GetFieldValue<T>(ordinal, keys)` through its existing native conversion owner. Its ordinary BCL getter slots keep their defaults; code using a standard reader interface can instead call `StoredNumeric.DecodeDecimal(reader.GetString(ordinal))` or `DecodeBigInteger`. Explicit field access retains the existing getter's SQL-NULL and lifetime rules. Public multi-map callbacks receive converted typed values; internal row-mapper registration is not a new public API.

## Parameterless numerical query formatting

`rows.Select(x => x.Amount.ToString())` and the corresponding BigInteger call execute the native parameterless formatter after exact target conversion. They observe `CurrentCulture`, including customized number-format properties, when the query executes. Retained queries use the invocation's culture. Compare against the decoded/materialized value, rather than an original Decimal scale that storage removed.

Existing implicit numerical text in concatenation, composite formatting and public JSON output keeps its invariant policy. This addition does not admit other `ToString` format/provider overloads, boxed Object or Nullable calls. Exact numerical casts retain their existing conversion and overflow behavior.

## Numerical dictionary keys

Dictionary lookups in queries can take a captured, compiled or row-derived key, such as `rows.Select(row => row.Prices[row.Code])`. Numerical keys use the same conversion as typed dictionary reads. Decimal keys compare by value regardless of scale, and unsigned keys retain their full range.

A missing key raises `KeyNotFoundException`. A malformed stored key raises its conversion error; two stored names that convert to the same numerical key raise `ArgumentException`. String keys keep their complete contents, including embedded NUL characters.

See [numerical migration](numeric-migration.md) for format admission and historical data conversion.
