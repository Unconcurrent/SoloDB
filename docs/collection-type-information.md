# Recorded collection types

SoloDB records the serializer description of `T` when a writable database acquires a collection with `GetCollection<T>()` or `GetCollection<T>(name)`. No extra application call is required. Future migration tools and database viewers can use these facts without loading your application's assemblies.

The existing `SoloDBCollections` row contains a nullable `TypeInformation` BLOB holding a native SQLite JSONB array:

```sql
SELECT Name, json(TypeInformation) AS RecordedTypes
FROM SoloDBCollections;
```

`TypeInformation` keeps the latest ten distinct complete model descriptions, oldest first. Each entry contains the acquired `FullName` and `AssemblyQualifiedName`, a `Shape` containing the declared `Contract` and its known `Alternatives`, and `Labels` containing exact CLR identities for their numbered definitions. Each graph is stored once. Contracts retain assembly name, culture and public key identity; assembly-version labels alone do not add or rewrite an entry. Equality follows decoded JSON meaning, so equivalent native JSON encodings do not create another entry.

Acquiring a description already covered by a retained entry makes no metadata write and leaves its position unchanged. Discovering another concrete identity enriches compatible retained entries without using another history slot, including discovery through an older retained model. Existing concrete revisions remain distinct. A changed declared contract or changed representation of a known concrete type records a new complete description. When an eleventh description is recorded, the oldest and the alternative revisions owned only by it are removed together.

Within `Contract`, `Type` identifies the CLR family and `Shape` describes the serialized value: stored members, scalar encodings, containers, enum values, F# union cases and references. For example, byte sequences, including byte groupings, use a base64 string rather than an array of numbers. Union cases retain their declared payload fields. Repeated types use local `Definition` and `Reference` numbers. A `GenericReference` supplies new arguments to an existing definition; original member and case signatures distinguish parameter positions from fixed types and preserve inherited substitutions. Repeated stored member names also have an ordered `Members` list. Container `TypeArguments` retain CLR parameter identities separately from the types that actually become stored children.

`Shape.Alternatives` contains the known polymorphic descriptions for that model. Each item names its declared base in `For`, its concrete identity in `Identity`, and its concrete `Contract` with independent local definition numbers. `Labels.Root` describes the declared graph; the aligned `Labels.Alternatives` entries describe the concrete graphs. Acquisition merges available ordinary and generated facts, including separate registration units and other database roots. A process knowing fewer types does not erase another writer's recorded facts. A new model revision preserves known types that this writer has no newer information about. Ten limits complete history entries: fields, union cases and concrete alternatives within a retained entry have no ten-item cap.

These are recorded possibilities, not a complete inventory of rows. Several types may acquire the same collection, such as a full model and a projection. Older writers, raw SQL and partial updates can leave values that no retained contract explains completely. Unknown subtypes and information already lost by serialization are not guessed. The descriptions neither replace codecs nor control materialization, and recording one does not construct a model or execute its property getters.

Existing writable databases gain the missing field without changing `user_version` or rewriting documents. NULL means no facts have been recorded. Acquisition through read-only custom storage leaves the catalogue and database bytes unchanged. Dropping a collection removes its recorded descriptions with its row; recreating it starts empty.

Normal and generated C# and F# delivery use the same description policy. NativeAOT applications use their normal generated registration.
