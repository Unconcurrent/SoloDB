# Query dates and durations

Date and time members, constructors, formatting and parsing run inside the SQLite query. Captured values bind when the query executes; retained queries accept new argument values on each invocation.

```csharp
var project = events.Compile<TimeSpan, long>((rows, offset) =>
    rows.Select(row => new DateTimeOffset(row.Clock, offset).ToUnixTimeMilliseconds()));

var east = project(TimeSpan.FromHours(5.5)).ToArray();
var west = project(TimeSpan.FromHours(-4)).ToArray();
```

`DateTime` constructors accept a literal, captured, row-derived or retained `DateTimeKind`. `DateTimeOffset(DateTime, TimeSpan)` uses the supplied clock and offset, including .NET's UTC/local-kind consistency checks. An invalid explicit offset raises its actual argument error. Existing component constructors keep their established SQL-NULL behavior for invalid row components; consuming that result as a nonnullable date fails.

A directly constructed or parsed `DateTimeOffset` still has its offset for member access and formatting:

```csharp
var text = events.Select(row =>
    new DateTimeOffset(row.Clock, row.Offset).ToString("yyyy-MM-dd HH:mm:ss zzz"));
```

Persistence stores `DateTimeOffset` as UTC milliseconds and discards its original offset. Reading a stored value does not recover that offset. The existing refusal of timezone format specifiers on such a value remains; retain the offset in a separate field when it matters. Materializing a constructed value also uses the existing millisecond representation.

`DateTime` stores its `ToBinary()` value. `Ticks`, `Kind`, `Date` and `TimeOfDay` use the decoded value. The established calendar-component, comparison and formatting SQL continues to normalize Local values to UTC-equivalent ticks. `TimeOnly` and `TimeSpan` retain millisecond storage; their stored `Ticks` reflect that precision.

Formatting uses the established invariant-culture policy. Its string argument may come from a row or a retained invocation:

```csharp
var format = events.Compile<string, string>((rows, pattern) =>
    rows.Select(row => row.Clock.ToString(pattern)));

var month = format("yyyy-MM").ToArray();
var day = format("dd/MM/yyyy").ToArray();
```

Literal formats retain their existing indexed SQL. Dynamic formats preserve the same UTC-equivalent and millisecond policy. DateOnly, TimeOnly and TimeSpan also accept string formats. Malformed consumed formats raise a format error.

The single-string `Parse` calls for Decimal, DateTime, DateTimeOffset, DateOnly, TimeOnly and TimeSpan use the corresponding .NET parser and execution-time current culture. They return SoloDB's existing stored representation. Invalid text raises the parser's format or range error; it does not become zero through a SQL cast. Decimal parsing retains its exact coefficient before storage encoding.
