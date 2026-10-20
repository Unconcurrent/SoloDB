# Decimal and BigInteger migration

SoloDB 1.3 stores typed Decimal and BigInteger values as JSON numbers. SoloDB 1.4 stores them as encoded JSON strings for exact numerical queries. Public JSON output still uses numbers.

Stop every older SoloDB process before opening a database with 1.4. Opening advances `PRAGMA user_version` to 6; 1.3 then refuses to reopen it. Do not lower that stamp. Numerical conversion happens separately when each typed collection is initialized.

## Automatic conversion

Initialization samples the first present, non-null value independently for each declared or discovered numerical field. A number selects complete transactional conversion of the model's numerical fields. A string completes that field's check. This covers independent sibling fields and fields removed from the model during upgrade and later restored. Missing or all-null fields can require searching later documents.

Runtime shapes are discovered while numerical samples are needed. If the first shape already supplies all known numerical fields, later unseen subtype-only fields may remain undiscovered. This does not prevent conversion of already encountered runtime shapes or independent dynamic fields. Handler-context collection lookup skips numerical verification; initialize collections needing conversion before using them in handlers.

A typed read of an incorrect stored representation raises `FormatException` with a link here. Some queries can filter out a row or operate without materializing that field, so successful query execution does not establish that every stored field has been converted.

## Declare known numerical paths explicitly

A migration tool can initialize an existing named collection through a sealed model containing its known numerical paths. Initialization uses the normal transactional migration to patch those leaves in the original documents. It does not materialize or replace documents through the migration model; other fields, runtime discriminators and document identities remain intact.

Use this recipe when each targeted path has the same numerical meaning in every affected shape, and at least one targeted field's first present value is still a native number. For example, a later subtype may add `ExtraAmount` and `ExtraCount` while earlier documents omit them:

```csharp
using System.Numerics;
using SoloDatabase;

public sealed class NumericMigrationFields
{
    public decimal? ExtraAmount { get; set; }
    public BigInteger? ExtraCount { get; set; }
}

public static class NumericMigration
{
    public static void Apply(string databasePath)
    {
        using var database = new SoloDB(databasePath);
        database.WithTransaction(transaction =>
        {
            transaction.GetCollection<NumericMigrationFields>("Values");
        });
    }
}
```

Use the existing collection name and exact serialized field names. Nested models, sequences and dictionaries must describe the actual stored shape. If the tool uses generated metadata, register its migration models too. After `Apply` returns, resume access through the application's original model and verify its exact numerical values and unrelated fields. A conversion failure rolls back the transaction and leaves the original documents unchanged.

This is not a force-migration API. If every targeted first sample is already a string, initialization does not select a later native occurrence of the same field. The sealed model also removes subtype-specific path gates: do not use it when the same path means Decimal in one shape and ordinary String in another.

## Mixed data and legacy quoted numbers

Raw SQL and untyped collections expose physical JSON. They do not infer a typed model's numerical storage policy. Native numbers written after a field became current can leave mixed representations that first-sample admission does not repair automatically.

An ordinary quoted number can also be a valid encoded key with a different value. Neither key validation nor the absence of an exception proves its intended meaning. Do not convert every string, decode it twice or use SQLite REAL casts to repair it.

Before the first 1.4 opening, normalize known legacy quoted numerical fields through the original 1.3 schema's typed reads and numerical writes, preserving the other fields and identities. Keep the original producer's range constraints: its BigInteger writer was Decimal-limited, so it cannot export arbitrary-width integers through that writer.

For conflicting subtype meanings or data outside the explicit-field recipe, use an application-specific typed export and rewrite based on the original schema and producer. Preserve original integer tokens and Decimal values; verify the complete records and identities in the destination. If the file has already been upgraded, use the original source or a pre-upgrade backup to build a separate current database, or perform a targeted current typed rewrite from independently known values. Reopening it with 1.3 by lowering `user_version` is not a migration method. Current bytes alone cannot recover an intended value that has already been overwritten.

See [numerical storage and migration](generated-metadata.md#numerical-storage-and-migration) for the storage and initialization contracts, and [raw SQL](native-sql.md) for physical access.
