# Recorded collection types

SoloDB records the serializer description of `T` when a writable database acquires a collection with `GetCollection<T>()` or `GetCollection<T>(name)`. No extra application call is required. Future migration tools and database viewers can use these facts without loading your application's assemblies.

The existing `SoloDBCollections` row contains two nullable BLOB fields holding native SQLite JSONB arrays:

```sql
SELECT Name, json(TypeInformation) AS Contracts,
       json(TypeAlternatives) AS KnownAlternatives
FROM SoloDBCollections;
```

`TypeInformation` keeps the latest ten distinct declared contracts, oldest first. Each entry contains the exact acquired `FullName` and `AssemblyQualifiedName`, a `Contract` describing its stored representation, and `Labels` containing the exact CLR identities for that graph's numbered definitions. The contract records type names and assembly identity without version labels; culture and public key identity still distinguish assemblies. Assembly-version labels alone do not append or rewrite an entry. Acquiring an existing contract leaves its position and contents unchanged. A new contract appends once and evicts the oldest when necessary.

Within `Contract`, `Type` identifies the CLR family and `Shape` describes the serialized value: stored members, scalar encodings, containers, enum values, F# union cases and references. For example, byte sequences, including byte groupings, use a base64 string rather than an array of numbers. Union cases retain their declared payload fields. Repeated types use local `Definition` and `Reference` numbers. A `GenericReference` supplies new arguments to an existing definition; original member and case signatures distinguish parameter positions from fixed types and preserve inherited substitutions. Repeated stored member names also have an ordered `Members` list. Container `TypeArguments` retain CLR parameter identities separately from the types that actually become stored children.

`TypeAlternatives` records known polymorphic alternatives separately. Each entry names its declared base in `For` and holds the concrete `Description`, with its own contract, labels and local definition numbers. Acquisition accumulates facts available from ordinary discovery or generated registration, including separate registration units and other database roots. A process knowing fewer alternatives does not erase previously recorded ones. Discovering another alternative enriches this array without consuming a contract-history slot. Ten limits history only: fields, union cases and recorded alternatives are never truncated.

These are recorded possibilities, not a complete inventory of rows. Several types may acquire the same collection, such as a full model and a projection. Older writers, raw SQL and partial updates can leave values that no retained contract explains completely. Unknown subtypes and information already lost by serialization are not guessed. The descriptions neither replace codecs nor control materialization, and recording one does not construct a model or execute its property getters.

Existing writable databases gain missing fields without changing `user_version` or rewriting documents. NULL means no facts have been recorded. Acquisition through read-only custom storage leaves the catalogue and database bytes unchanged. Dropping a collection removes both arrays with its row; recreating it starts empty.

Normal and generated C# and F# delivery use the same description policy. NativeAOT applications use their normal generated registration.
