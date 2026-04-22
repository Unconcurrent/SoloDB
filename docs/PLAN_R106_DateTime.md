# PLAN — R-106: DateTime and DateTimeOffset LINQ-to-SQL Translation

**Architect:** hipp  
**Phase:** III (Architecture)  
**Status:** PLAN-LOCK CANDIDATE  
**Governing triage:** `mail-board/2026-04-22_17-15-03-data-to-all-r106-triage-assessment.md` (amended)  
**Captain directives:** history.md 2026-04-22 17:32-17:38; Linus impossibility ruling 2026-04-22_17:38  
**Research:** `mail-board/2026-04-22_17-30-00-hipp-to-data_carmack_linus_axiom-arch-r106-research-report.md`

---

## PROBLEM STATEMENT

SoloDB throws `NotSupportedException` for any LINQ expression that accesses DateTime/DateTimeOffset members (`.Year`, `.Month`, etc.) or calls `.ToString(format)`. The primary trigger — GroupBy by year+month, Select with formatted date — fails entirely. All deterministic, expressible constructs must translate. Non-deterministic constructs (UTC offset data that was discarded at write time) throw with a clear named exception.

**Backward-compatibility law (Rule 1):** Storage format is immutable. DateTime stored as `ToBinary()` int64. DateTimeOffset stored as unix-milliseconds int64. No changes.

**Unification law (Rule 3):** DateTime and DateTimeOffset share identical public behavior, identical specifier support, identical error messages. A single `toUnixMs` preamble converts each to canonical unix-milliseconds; all downstream logic is shared.

**TDD law (Rule 4):** Every feature line is preceded by a failing test that fails for the correct reason.

**Zero-deferral law (Rule 5):** All specifiers ship this cycle. Any same-cycle deferral requires joint sign-off from Data, Linus, and Anvil-rev.

**InvariantCulture oracle:** The DB commits to InvariantCulture as the SQL-side format oracle for all standard specifiers. Tests compare against `.ToString(specifier, CultureInfo.InvariantCulture)` on the .NET side.

---

## RESEARCH FINDINGS

### R1 — Storage (FINAL)

```fsharp
// SoloDB/Json/JsonFunctions.fs:115-117
| :? DateTime    as x -> x.ToBinary() :> obj, false
| :? DateTimeOffset as x -> x.ToUnixTimeMilliseconds() :> obj, false
```

`DateTime.ToBinary()` bit layout:
- Kind=Unspecified (bits 63:62 = `00`): raw = ticks
- Kind=Utc (bits 63:62 = `01`): raw = ticks | 0x4000000000000000
- Kind=Local (bits 63:62 = `10`): ToBinary **pre-shifts to UTC ticks** before encoding. The mask formula recovers UTC-equivalent ticks → strftime formats UTC-equivalent wall-clock. Non-negotiable. Document in README; do NOT throw.

`DateTimeOffset.ToUnixTimeMilliseconds()` discards the offset component. UTC offset is permanently lost at write time. Timezone specifiers (`z`, `zz`, `zzz`) cannot be fulfilled → `NotSupportedException`.

### R2 — CONCAT is safe (SQLite 3.49.1 bundled, CONCAT added in 3.44.0)

Existing codebase uses `FunctionCall("CONCAT", ...)` at `VisitCore.MethodCall.fs:112` and `GroupByOps.fs:259,305`. Use consistently.

### R3 — BinaryOperator missing BitwiseAnd

Required for ToBinary kind-bit masking. Approved by Linus. One-line DU addition + one-line emitter addition.

### R4 — Test location

`/wrk/github/SoloDBTests/CSharpTests/` (MSTest). New file: `R106DateTimeTests.cs`.

---

## CHOSEN APPROACH

### Single preamble: `toUnixMs`

```fsharp
/// Convert raw stored SqlExpr to canonical unix-milliseconds.
/// All downstream member/specifier logic receives this unified expression.
let toUnixMs (raw: SqlExpr) (clrType: Type) : SqlExpr =
    if clrType = typeof<DateTime> then
        // ToBinary: ((raw & 0x3FFFFFFFFFFFFFFF) - 621355968000000000) / 10000
        let mask     = Literal(Integer 4611686018427387903L)   // 0x3FFFFFFFFFFFFFFF
        let epochTk  = Literal(Integer 621355968000000000L)    // ticks: 0001-01-01 → 1970-01-01 UTC
        let ticksMs  = Literal(Integer 10000L)                 // 100-ns ticks per millisecond
        Binary(Binary(Binary(raw, BitwiseAnd, mask), Sub, epochTk), Div, ticksMs)
    elif clrType = typeof<DateTimeOffset> then
        raw  // identity: ToUnixTimeMilliseconds is the stored value
    else
        raise (NotSupportedException $"toUnixMs: unsupported type {clrType.FullName}")
```

Derived from `unixMs`:
```fsharp
let unixSec  ms = Binary(ms, Div, Literal(Integer 1000L))
let millisPart ms = Binary(ms, Mod, Literal(Integer 1000L))
let strftimeCast fmt ms =
    Cast(FunctionCall("strftime", [Literal(String fmt); unixSec ms; Literal(String "unixepoch")]), "INTEGER")
```

---

## EDGE CASES — ALL ENUMERATED

1. **Kind=Local DateTime**: toUnixMs recovers UTC-equivalent ticks; strftime formats UTC-equivalent time. Not a bug — documented behavior. README section required.
2. **Kind=Utc / Unspecified**: correct results, as designed.
3. **Midnight (00:00:00)**: strftime('%H')='00', CAST=0. Correct.
4. **Leap year (Feb 29)**: strftime handles natively.
5. **Year boundary (Dec 31 / Jan 1)**: UTC-based; no DST risk. Test in G7.
6. **Min DateTime (0001-01-01)**: unix_ms = (0 - 621355968000000000) / 10000 = -62135596800000. SQLite strftime handles negative epoch. Test in G7.
7. **Max DateTime (9999-12-31)**: unix_ms ≈ 2.53e14. Within SQLite int64 range. Test in G7.
8. **Non-constant format string**: throw (see ERROR CONDITIONS).
9. **Empty or null format string**: treat as `"G"` — matches `.ToString("", CultureInfo.InvariantCulture)` which .NET defines as equivalent to `"G"`. Special-case at entry of `translateDateTimeToString`: if `fmt = null` or `fmt = ""`, substitute `"G"` before any dispatch. Do NOT throw.
10. **Single-char standard vs custom dispatch**: see TOKENIZER SPEC.
11. **Sub-millisecond digits (f4–f7, F4–F7)**: always '0' since storage is ms precision. printf zero-pads; F-family RTRIM produces empty string if all zero after ms. Correct and tested in G4.
12. **`new DateTime(y,m,d)` 3-arg**: → `printf('%04d-%02d-%02d', y, m, d)` ISO string.
13. **`new DateTime(...)` with arg count ≠ 3**: throw (see ERROR CONDITIONS).
14. **`new DateTime(y,m,d)` args may be non-constant**: visitDu on each arg. No constraint.
15. **`g.Key.OccurredAt.Year`** (3-level GroupBy key nesting): Not in R-106 scope. Throw with named message. Requires Data+Linus+Anvil sign-off as deferred item.
16. **`hh`/`h` 12-hour**: `(hour + 11) % 12 + 1`. hour=0→12, hour=12→12, hour=13→1. Matches .NET.
17. **`tt`/`t` midnight**: hour=0 → AM. Correct.
18. **`K` specifier**: always `'Z'` — both DateTime (UTC-equivalent recovered) and DateTimeOffset (UTC) store UTC-equivalent values. **Parity consequence for `o`/`O`:** .NET Kind=Unspecified emits no suffix via `"o"`; Kind=Utc emits `'Z'`. SQL always emits `'Z'`. G6 o/O tests must use `DateTime.SpecifyKind(dt, DateTimeKind.Utc)` so oracle matches SQL output.
19. **`g`/`gg`**: all stored dates in CE. Emit literal `'A.D.'`.
20. **`yyy` count=3**: same SQL as count=4 (all modern years are 4-digit; yyy for 2024 = "2024"). Correct.
21. **`%` lone char in custom format**: literal `%` pass-through (single-specifier prefix only applies when format.Length=1 and uses no `%`-prefix extension in our tokenizer).

---

## ERROR CONDITIONS — COMPLETE THROW LIST (exactly 4 cases per Linus ruling)

| # | Error | Detection | Exception message (exact) |
|---|-------|----------|--------------------------|
| 1 | `z`, `zz`, `zzz` in format string | tokenizer emits `Specifier('z', n)` | `"DateTime.ToString: format specifier '{z*n}' is not supported — UTC offset data is not stored (DateTimeOffset.ToUnixTimeMilliseconds discards the offset; DateTime.ToBinary does not store timezone)."` where `{z*n}` is `"z"`, `"zz"`, or `"zzz"` |
| 2 | `new DateTime(n-arg)` with n≠3 | `ExpressionType.New`, `m.Arguments.Count ≠ 3` | `"new DateTime({n} arguments) is not supported in SQL translation. Only new DateTime(year, month, day) with 3 arguments is supported."` |
| 3 | Unsupported DateTime/DateTimeOffset member | `translateDateTimeMember` default | `"DateTime/DateTimeOffset member access '.{name}' is not supported in SQL translation. Supported members: Year, Month, Day, Hour, Minute, Second, Millisecond, DayOfWeek, DayOfYear."` |
| 4 | Non-constant format string in `.ToString(fmt)` | `not (isFullyConstant fmtExpr)` in ToString dispatch | `"DateTime.ToString(format): the format argument must be a compile-time constant for SQL translation. Use a string literal, or call AsEnumerable() before ToString to evaluate client-side."` |

**No other `NotSupportedException` is thrown for format specifiers. Empty/null format is treated as `"G"` — not a throw.** Culture-sensitive standard specifiers expand to InvariantCulture equivalents (see STANDARD SPECIFIER EXPANSION TABLE).

---

## FORMAT TOKENIZER SPEC

### One-char standard vs custom disambiguation (the 's' landmine)

```
if format.Length = 1:
  if format[0] is in standard-specifier-char-set → dispatch as STANDARD
  else → dispatch as CUSTOM (single-char custom format, tokenize normally)
else:
  → dispatch as CUSTOM (tokenize multi-char string)
```

Standard-specifier char set: `d D f F g G M Y t T s u o O U R r`

Examples:
- `"s"` (len=1, in set) → standard → expand to `"yyyy-MM-ddTHH:mm:ss"` → tokenize that
- `"HH:mm:ss"` (len>1) → custom; at pos=6, `Specifier('s',2)` → 2-digit seconds
- `"ss"` (len=2) → custom; `Specifier('s',2)` → 2-digit seconds

### Standard specifier expansion table (InvariantCulture)

All standard specifiers expand to a custom format string and are fed back through the tokenizer (no recursion limit risk — standard specifiers never expand to other single-char standard specifiers).

| Specifier | InvariantCulture expansion |
|-----------|---------------------------|
| `d` | `"MM/dd/yyyy"` |
| `D` | `"dddd, dd MMMM yyyy"` |
| `f` | `"dddd, dd MMMM yyyy HH:mm"` |
| `F` | `"dddd, dd MMMM yyyy HH:mm:ss"` |
| `g` | `"MM/dd/yyyy HH:mm"` |
| `G` | `"MM/dd/yyyy HH:mm:ss"` |
| `M` / `m` | `"MMMM dd"` |
| `Y` / `y` | `"yyyy MMMM"` |
| `t` | `"HH:mm"` |
| `T` | `"HH:mm:ss"` |
| `s` | `"yyyy-MM-ddTHH:mm:ss"` (`T` is a literal in custom strings — not a format specifier) |
| `u` | `"yyyy-MM-dd HH:mm:ssZ"` (`Z` is a literal, not K) |
| `o` / `O` | `"yyyy-MM-ddTHH:mm:ss.fffffffK"` (digits 4-7 always '0'; K → 'Z'; **G6 seed values for o/O MUST use `DateTime.SpecifyKind(dt, DateTimeKind.Utc)` — see Fix 3 note below**) |
| `R` / `r` | `"ddd, dd MMM yyyy HH:mm:ss 'GMT'"` |
| `U` | `"dddd, dd MMMM yyyy HH:mm:ss"` (UTC-equivalent, same as F) |

Verification: in .NET custom format strings, `T`, `Z`, `G`, `M`, `Y`, `H`, `m`, `s`, `f`, `F`, `d` are all only specifiers when they are the *recognized* custom specifier chars. Specifically: `T` is NOT a custom specifier char — it is a literal in custom format strings. `Z` is NOT a custom specifier char — literal. This means `"yyyy-MM-ddTHH:mm:ss"` is correctly tokenized: `yyyy`, `-`, `MM`, `-`, `dd`, `T` (literal), `HH`, `:`, `mm`, `:`, `ss`.

### Format tokenizer algorithm (count-based, left-to-right)

Token types:
```fsharp
type FormatToken =
    | Specifier of char * count: int   // Specifier('y', 4) for "yyyy"
    | Literal   of string              // quoted or escaped literal text
    | Separator of char                // '/' or ':'
```

Specifier chars (the ONLY chars that form `Specifier` tokens): `y M d H h m s f F t z K g`

Note: ALL other chars (including `T`, `Z`, `G`, `U`, `R`, etc.) are emitted as `Literal` when encountered outside quotes.

```
procedure tokenize(fmt: string) -> FormatToken list:
  pos := 0
  tokens := []
  while pos < fmt.Length:
    ch := fmt[pos]

    // 1. Backslash escape: next char is literal
    if ch = '\\' and pos+1 < fmt.Length:
      tokens.append(Literal(string fmt[pos+1]))
      pos += 2; continue

    // 2. Quoted literal block
    if ch = '\'':
      close := fmt.IndexOf('\'', pos+1)
      if close < 0: close := fmt.Length  // unterminated: consume rest
      text := fmt.Substring(pos+1, close-(pos+1))
      tokens.append(Literal(text))
      pos := close+1; continue

    // 3. Culture separators
    if ch = '/' or ch = ':':
      tokens.append(Separator(ch))
      pos += 1; continue

    // 4. '%' prefix: forces NEXT char to be parsed as single-count specifier.
    //    '%s' = Specifier('s',1). Lone '%' with no following specifier = literal '%'.
    if ch = '%' and pos+1 < fmt.Length and fmt[pos+1] in {y,M,d,H,h,m,s,f,F,t,z,K,g}:
      tokens.append(Specifier(fmt[pos+1], 1))
      pos += 2; continue

    // 5. Specifier run: count consecutive identical specifier chars
    if ch in {y,M,d,H,h,m,s,f,F,t,z,K,g}:
      count := 1
      while pos+count < fmt.Length and fmt[pos+count] = ch:
        count += 1
      tokens.append(Specifier(ch, count))
      pos += count; continue

    // 6. Literal (all other chars including T, Z, G, %, etc.)
    tokens.append(Literal(string ch))
    pos += 1
```

### Specifier dispatch table (all consume `unixMs`)

**Fix 2 — FromIsoString mode**: `new DateTime(y,m,d)` produces midnight with no sub-second component. In `FromIsoString` mode, `millisPart` MUST be `Literal(Integer 0L)` — not `Binary(ms, Mod, 1000L)`, which would perform integer arithmetic on a TEXT value and return garbage. Every `f`/`F` token dispatch must branch on `TranslationMode`:
- `FromEpoch clrType`: use `millisPart = Binary(toUnixMs raw clrType, Mod, Lit 1000L)` — integer arithmetic on int64.
- `FromIsoString`: use `millisPart = Literal(Integer 0L)` — midnight, always zero.

The `hHour` and `sec` expressions in `FromIsoString` mode use `strftimeFromIso` (no 'unixepoch' modifier, SQLite parses ISO string).

Let `sec = unixSec(ms)`, `msPart = millisPart(ms)` (mode-dependent per above), `hHour = Cast(FunctionCall("strftime",[Lit"%H";sec;Lit"unixepoch"]),"INTEGER")`.

For `FromIsoString` mode, replace `strftime(fmt, sec, "unixepoch")` with `strftime(fmt, isoExpr)` throughout — no third argument.

`Separator('/')` → `Literal(String "/")`. `Separator(':')` → `Literal(String ":")`.

| Char | Count | SqlExpr |
|------|-------|---------|
| `y` | 1 | `Binary(Cast(strftime('%Y'),INTEGER), Mod, Lit 100L)` |
| `y` | 2 | `FunctionCall("printf",[Lit"%02d"; y1_expr])` |
| `y` | 3 | same as count ≥ 4 |
| `y` | ≥4 | `FunctionCall("strftime",[Lit"%Y"; sec; Lit"unixepoch"])` |
| `M` | 1 | `Cast(FunctionCall("strftime",[Lit"%m";sec;Lit"unixepoch"]),"INTEGER")` |
| `M` | 2 | `FunctionCall("strftime",[Lit"%m"; sec; Lit"unixepoch"])` |
| `M` | 3 | abbrev month CASE (see CASE TABLES below) |
| `M` | ≥4 | full month CASE (see CASE TABLES below) |
| `d` | 1 | `Cast(FunctionCall("strftime",[Lit"%d";sec;Lit"unixepoch"]),"INTEGER")` |
| `d` | 2 | `FunctionCall("strftime",[Lit"%d"; sec; Lit"unixepoch"])` |
| `d` | 3 | abbrev day-of-week CASE (see CASE TABLES below) |
| `d` | ≥4 | full day-of-week CASE (see CASE TABLES below) |
| `H` | 1 | `Cast(FunctionCall("strftime",[Lit"%H";sec;Lit"unixepoch"]),"INTEGER")` |
| `H` | ≥2 | `FunctionCall("strftime",[Lit"%H"; sec; Lit"unixepoch"])` |
| `h` | 1 | `Binary(Binary(Binary(hHour,Add,Lit 11L),Mod,Lit 12L),Add,Lit 1L)` |
| `h` | ≥2 | `FunctionCall("printf",[Lit"%02d"; h1_expr])` |
| `m` | 1 | `Cast(FunctionCall("strftime",[Lit"%M";sec;Lit"unixepoch"]),"INTEGER")` |
| `m` | ≥2 | `FunctionCall("strftime",[Lit"%M"; sec; Lit"unixepoch"])` |
| `s` | 1 | `Cast(FunctionCall("strftime",[Lit"%S";sec;Lit"unixepoch"]),"INTEGER")` |
| `s` | ≥2 | `FunctionCall("strftime",[Lit"%S"; sec; Lit"unixepoch"])` |
| `f` | 1 | `Binary(msPart, Div, Lit 100L)` |
| `f` | 2 | `FunctionCall("printf",[Lit"%02d"; Binary(msPart,Div,Lit 10L)])` |
| `f` | 3 | `FunctionCall("printf",[Lit"%03d"; msPart])` |
| `f` | 4 | `FunctionCall("printf",[Lit"%04d"; Binary(msPart,Mul,Lit 10L)])` |
| `f` | 5 | `FunctionCall("printf",[Lit"%05d"; Binary(msPart,Mul,Lit 100L)])` |
| `f` | 6 | `FunctionCall("printf",[Lit"%06d"; Binary(msPart,Mul,Lit 1000L)])` |
| `f` | ≥7 | `FunctionCall("printf",[Lit"%07d"; Binary(msPart,Mul,Lit 10000L)])` |
| `F` | 1 | `CaseExpr((Binary(Binary(msPart,Div,Lit 100L),Eq,Lit 0L), Lit""), [], Some(Cast(Binary(msPart,Div,Lit 100L),"TEXT")))` |
| `F` | 2 | `CaseExpr((Binary(Binary(msPart,Div,Lit 10L),Eq,Lit 0L), Lit""), [], Some(FunctionCall("RTRIM",[printf02d_ms10; Lit"0"])))` |
| `F` | 3 | `CaseExpr((Binary(msPart,Eq,Lit 0L), Lit""), [], Some(FunctionCall("RTRIM",[printf03d_ms; Lit"0"])))` |
| `F` | 4..7 | `CaseExpr((Binary(msPart,Eq,Lit 0L), Lit""), [], Some(FunctionCall("RTRIM",[printf_fn_fN; Lit"0"])))` where `printf_fn_fN` is the corresponding `f` expression |
| `t` | 1 | `CaseExpr((Binary(hHour,Lt,Lit 12L), Lit"A"), [], Some(Lit"P"))` |
| `t` | ≥2 | `CaseExpr((Binary(hHour,Lt,Lit 12L), Lit"AM"), [], Some(Lit"PM"))` |
| `K` | any | `Literal(String "Z")` |
| `g` | any | `Literal(String "A.D.")` |
| `z` | any | throw `NotSupportedException` (Error #1) |

**Multi-part assembly:** if only one token, return its SqlExpr. If multiple, `FunctionCall("CONCAT", [token1; token2; ...])`.

### CASE TABLES (month names and day names)

**Month CASE (M≥4, full):**
```sql
CASE strftime('%m', unixSec, 'unixepoch')
  WHEN '01' THEN 'January'   WHEN '02' THEN 'February'  WHEN '03' THEN 'March'
  WHEN '04' THEN 'April'     WHEN '05' THEN 'May'       WHEN '06' THEN 'June'
  WHEN '07' THEN 'July'      WHEN '08' THEN 'August'    WHEN '09' THEN 'September'
  WHEN '10' THEN 'October'   WHEN '11' THEN 'November'  WHEN '12' THEN 'December'
END
```

**Month CASE (M=3, abbrev):**
```sql
CASE strftime('%m', unixSec, 'unixepoch')
  WHEN '01' THEN 'Jan' WHEN '02' THEN 'Feb' WHEN '03' THEN 'Mar'
  WHEN '04' THEN 'Apr' WHEN '05' THEN 'May' WHEN '06' THEN 'Jun'
  WHEN '07' THEN 'Jul' WHEN '08' THEN 'Aug' WHEN '09' THEN 'Sep'
  WHEN '10' THEN 'Oct' WHEN '11' THEN 'Nov' WHEN '12' THEN 'Dec'
END
```

**Day-of-week CASE (d≥4, full):**
```sql
CASE strftime('%w', unixSec, 'unixepoch')
  WHEN '0' THEN 'Sunday'    WHEN '1' THEN 'Monday'    WHEN '2' THEN 'Tuesday'
  WHEN '3' THEN 'Wednesday' WHEN '4' THEN 'Thursday'  WHEN '5' THEN 'Friday'
  WHEN '6' THEN 'Saturday'
END
```

**Day-of-week CASE (d=3, abbrev):**
```sql
CASE strftime('%w', unixSec, 'unixepoch')
  WHEN '0' THEN 'Sun' WHEN '1' THEN 'Mon' WHEN '2' THEN 'Tue'
  WHEN '3' THEN 'Wed' WHEN '4' THEN 'Thu' WHEN '5' THEN 'Fri'
  WHEN '6' THEN 'Sat'
END
```

SqlExpr representation: `CaseExpr(first_branch, [rest_branches], None)` where each branch is `(Binary(strftimeMon, Eq, Lit "01"), Lit "January")` etc.

---

## MEMBER ACCESS TRANSLATION TABLE (unified, all consume unixMs)

| Member | SqlExpr |
|--------|---------|
| `.Year` | `Cast(FunctionCall("strftime",[Lit"%Y"; unixSec; Lit"unixepoch"]), "INTEGER")` |
| `.Month` | `Cast(FunctionCall("strftime",[Lit"%m"; unixSec; Lit"unixepoch"]), "INTEGER")` |
| `.Day` | `Cast(FunctionCall("strftime",[Lit"%d"; unixSec; Lit"unixepoch"]), "INTEGER")` |
| `.Hour` | `Cast(FunctionCall("strftime",[Lit"%H"; unixSec; Lit"unixepoch"]), "INTEGER")` |
| `.Minute` | `Cast(FunctionCall("strftime",[Lit"%M"; unixSec; Lit"unixepoch"]), "INTEGER")` |
| `.Second` | `Cast(FunctionCall("strftime",[Lit"%S"; unixSec; Lit"unixepoch"]), "INTEGER")` |
| `.Millisecond` | `Cast(Binary(unixMs, Mod, Lit 1000L), "INTEGER")` |
| `.DayOfWeek` | `Cast(FunctionCall("strftime",[Lit"%w"; unixSec; Lit"unixepoch"]), "INTEGER")` |
| `.DayOfYear` | `Cast(FunctionCall("strftime",[Lit"%j"; unixSec; Lit"unixepoch"]), "INTEGER")` |

Any other member name → throw (Error #3).

---

## `new DateTime(y, m, d)` CONTRACT

In `visitDu` `ExpressionType.New` branch, before existing `isTuple` / `m.Members` checks:

```fsharp
if m.Type = typeof<DateTime> then
    if m.Arguments.Count = 3 then
        // ISO date string for strftime — no unixepoch modifier needed
        FunctionCall("printf", [Literal(String "%04d-%02d-%02d")
                                visitDu m.Arguments.[0] qb
                                visitDu m.Arguments.[1] qb
                                visitDu m.Arguments.[2] qb])
    else
        raise (NotSupportedException $"new DateTime({m.Arguments.Count} arguments) is not supported ...")
```

The produced `printf` expression is an ISO date string (`"2024-03-01"`). When `.ToString(fmt)` is called on a `new DateTime(y,m,d)` receiver:
- Detect at `visitMethodCallDu`: `m.Object` is a `NewExpression` with `m.Object.Type = typeof<DateTime>`
- The ISO string is passed directly to `strftime(fmt_expanded, iso_string)` WITHOUT the `'unixepoch'` modifier (SQLite parses ISO strings natively)
- This means the tokenizer's strftime calls for the ISO string path omit the `Literal(String "unixepoch")` third argument

Two strftime invocation modes in `DateTimeFunctions.fs`:
```fsharp
// Mode A: from unix-ms field (unixepoch modifier required)
let strftimeFromEpoch fmt ms =
    FunctionCall("strftime", [Literal(String fmt); unixSec ms; Literal(String "unixepoch")])

// Mode B: from ISO string (new DateTime(y,m,d)) (no modifier)
let strftimeFromIso fmt isoExpr =
    FunctionCall("strftime", [Literal(String fmt); isoExpr])
```

`translateDateTimeToString` takes a `rawExpr: SqlExpr` and a `mode: TranslationMode`:
- `mode = FromEpoch clrType` → `toUnixMs rawExpr clrType` → use `strftimeFromEpoch`
- `mode = FromIsoString` → use `strftimeFromIso rawExpr`

---

## WHAT LINUS WILL CHECK — 5 SCRUTINY POINTS AND HOW ADDRESSED

**1. Tokenizer: does `"ddd"` produce three `Specifier('d',1)` tokens or one `Specifier('d',3)` token?**

Run-counting algorithm: at position 0, `ch='d'`, count consecutive `'d'` chars → count=3. Emit `Specifier('d',3)`. Advance 3. Result: one token. Dispatch: `d,3` → abbreviated day name CASE expression. Not three separate tokens. Proven by algorithm design.

**2. Standard vs custom disambiguation for `"s"`:**

- `"s"` (length=1, `'s'` in standard-char-set) → STANDARD → expand to `"yyyy-MM-ddTHH:mm:ss"` → tokenize that (no length=1 check recurses since the expansion is multi-char).
- `"HH:mm:ss"` (length=8) → CUSTOM → tokenize → `Specifier('s',2)` → 2-digit seconds.
- No ambiguity possible.

**3. BitwiseAnd precedence:**

Emitter wraps every `Binary` in `(...)`. `Binary(Binary(Binary(raw,BitwiseAnd,mask),Sub,epoch),Div,ticksMs)` emits `(((raw & 4611686018427387903) - 621355968000000000) / 10000)`. Inner parens force correct evaluation regardless of SQLite's lower `&` precedence.

**4. TDD sequencing is correct:**

Batch 1: Write ALL tests (G1-G8) + BitwiseAnd contract change. Run them. ALL fail (G1-G7 on wrong-result/generic-NSE; G8 on generic NSE with wrong message — the specific dispatch code does not exist yet). Commit red run as proof.  
Batch 2: Implement translator. G1-G8 all go green with exact messages.  
The discipline: no feature line written before a failing test exists for it.

**5. No silent wrong answers:**

- Kind=Local: documented. Users see UTC-equivalent, not local wall-clock. Explicit README section.
- z/zz/zzz: throw with exact message explaining offset is not stored.
- All other constructs either produce correct results or throw.

---

## DIVISION OF LABOR

### Batch 1 (Parallel — contract change + ALL G1-G9 tests written, all run to RED)

**DeveloperA — Hipp** (BitwiseAnd contract + all test infrastructure)

Files:
- `SoloDB/QueryTranslator/SqlDu/Contract/SqlDu.Contract.Primitives.fs`: add `| BitwiseAnd` to `BinaryOperator`
- `SoloDB/QueryTranslator/SqlDu/Emit/SqlDu.EmitExpr.fs`: add `| BitwiseAnd -> "&"` to `emitBinaryOp`
- `SoloDBTests/CSharpTests/R106DateTimeTests.cs` — CREATE with model, corpus, and all G1-G9 tests

Model and corpus:
- `DateTimeEvent`: `long Id`, `DateTime OccurredAt`, `DateTimeOffset OccurredAtOffset`
- `SeedCorpus`: 2020-01-01 00:00:00 UTC, 2024-03-15 14:30:45.123 UTC, 2024-02-29 12:00:00 UTC (leap), 2024-12-31 23:59:59.999 UTC, 2025-01-01 00:00:00 UTC. Use `DateTime.SpecifyKind(..., DateTimeKind.Utc)` for all DateTime values.

**G8 tests** — assert `NotSupportedException` thrown (message content checked post-Batch-2):
- G8_01: `z` in custom format
- G8_02: `zz` in custom format
- G8_03: `zzz` in custom format
- G8_04: non-constant format string (lambda capture)
- G8_05: `new DateTime(2024, 1, 1, 12, 0, 0)` (6-arg constructor)
- G8_06: `.OccurredAt.Ticks` member access
- G8_07: `.OccurredAt.Kind` member access

**G1 tests** (Where): `.Year == 2024`, `.Month == 3`, `.Hour > 12` — DateTime and DateTimeOffset  
**G2 tests** (OrderBy/Select): `OrderBy(x => x.OccurredAt.Year)`, `Select(x => x.OccurredAt.Month)`  
**G3 tests** (GroupBy): single key, compound key  
**G4 tests** (ToString specifiers): y1, y2, y4, M1, M2, M3, M4, d1, d2, d3, d4, H1, H2, h1, h2, m1, m2, s1, s2, f1, f2, f3, f4, f7, F1, F3, F7, tt, t1, K, g, combinations `"yyyy-MM"`, `"yyyy-MM-dd"`, `"yyyy-MM-ddTHH:mm:ss"`, month name, day name  
**G5 test** (full trigger): GroupBy year+month, `new DateTime(g.Key.Year, g.Key.Month, 1).ToString("yyyy-MM")`, Count, OrderBy  
**G6 tests** (standard specifiers): s, u, o, d, D, f, F, g, G, M, Y, t, T, U, R, r vs InvariantCulture oracle. o/O: seed with `DateTime.SpecifyKind(dt, DateTimeKind.Utc)`, comment: *"SQL treats all stored DateTimes as UTC-shape; oracle comparison for o/O requires Kind=Utc."*  
**G7 tests** (edge cases): midnight, leap Feb 29, year boundary Dec31→Jan1, min DateTime, max DateTime  
**G9 test** (deferred gap — MUST remain RED through Batch 1 AND Batch 2, fix deferred to R-107+): `GroupBy(x => x.OccurredAt)` then `Select(g => new { Year = g.Key.Year, Count = g.Count() })` — assert year values match LINQ-to-Objects oracle. Expected: test FAILS in Batch 1 (throws `NotSupportedException` on 3-level key nesting instead of returning year). Stays RED through Batch 2 (deferred, translator not updated for this case). Goes GREEN only when R-107 implements the 3-level GroupBy key nesting fix — that flip is the deferral-resolved signal. Do NOT assert `throws NSE`; that would pass immediately and never flip.

**Batch 1 exit criteria:**
- `dotnet build` succeeds
- ALL G1-G9 tests FAIL — G8 fail with generic "not supported" (specific dispatcher not yet wired); G1-G7 fail on wrong result or generic NSE; G9 fails because it asserts correct year value but gets NSE (3-level nesting not supported)
- Commit failing test run output as red-evidence

**DeveloperB — Carmack** writes G3 compound key and G5 trigger tests in parallel; coordinates with Hipp on file layout.

### Batch 2 (Sequential after Batch 1 red-evidence committed)

**DeveloperA — Hipp** (core translator implementation)

Files:
- `SoloDB/QueryTranslator/VisitCore/QueryTranslator.VisitCore.DateTimeFunctions.fs` — NEW:
  - `TranslationMode` DU: `FromEpoch of clrType: Type` | `FromIsoString`
  - `toUnixMs(raw, clrType)` — preamble
  - `unixSec(ms)`, `millisPart(ms)`, `strftimeFromEpoch(fmt,ms)`, `strftimeFromIso(fmt,isoExpr)`
  - `translateDateTimeMember(memberName, unixMs)` — member dispatch table
  - `tokenizeFormat(fmt)` — tokenizer (exact algorithm from spec)
  - `dispatchToken(token, ms, mode)` — token-to-SqlExpr per spec table
  - `buildMonthCase(full, sec)`, `buildDayOfWeekCase(full, sec)` — CASE expression builders
  - `translateDateTimeToString(rawExpr, mode, fmt)` — standard/custom dispatch + assembly

- `SoloDB/QueryTranslator/VisitCore/QueryTranslator.VisitCore.Dispatch.fs`:
  1. In `visitMemberAccessDu`, add DateTime/DateTimeOffset member intercept before the `isPrimitiveSQLiteType` check
  2. In `ExpressionType.New` branch, add `new DateTime(y,m,d)` → `printf` before existing branches

- `SoloDB/QueryTranslator/VisitCore/QueryTranslator.VisitCore.MethodCall.fs`:
  1. Add `DateTime.ToString(const_fmt)` case
  2. Add `DateTimeOffset.ToString(const_fmt)` case
  3. Add `DateTime.ToString()` / `DateTimeOffset.ToString()` zero-arg → translate as `"G"` standard

- `SoloDB/SoloDB.fsproj`: add `DateTimeFunctions.fs` to compile order before `Dispatch.fs`

### Batch 3 (Sequential after Batch 2)

**Linus** — review implementation against plan  
**Anvil-rev** — verify SQL emission and test correctness  

---

## ACCEPTANCE CRITERIA

1. `dotnet build` succeeds, zero errors
2. All G1-G8 tests pass (G9 remains red — deferred, fix not priority)
3. Full trigger query (G5) produces results matching in-memory LINQ for both DateTime and DateTimeOffset
4. Every supported member (.Year through .DayOfYear) works in Where, Select, OrderBy, GroupBy key for both DateTime and DateTimeOffset
5. Every format specifier produces output matching `.ToString(specifier, CultureInfo.InvariantCulture)` on the same value
6. All 4 G8 throw cases throw `NotSupportedException` with message text matching the ERROR CONDITIONS table
7. Zero regression in existing `CSharpTests/` tests
8. README includes "DateTime.Kind=Local behavior" note and "UTC offset specifiers (z, zz, zzz) are not supported" note

--- END OF PLAN ---
