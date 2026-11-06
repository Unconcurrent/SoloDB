# Recorded collection types

SoloDB records a description of `T` when a writable database acquires a collection with `GetCollection<T>()` or `GetCollection<T>(name)`. No extra application call is required. The descriptions let future migration tools and database viewers understand stored fields without loading your application's assemblies.

The information lives in `SoloDBCollections.TypeInformation`, a nullable BLOB containing a SQLite JSONB array. You can inspect it with SQL:

```sql
SELECT Name, json(TypeInformation) AS TypeInformation
FROM SoloDBCollections;
```

Each entry contains the type's `FullName`, assembly-qualified identity, abstract flag, serializer kind and `Shape`. Nested descriptions cover stored members, scalar encodings, containers, enum values, F# union cases, references and known polymorphic alternatives. Union cases list their declared payload fields; stored member selection appears in the root and concrete case shapes. Repeated types use local `Definition` and `Reference` numbers. A `GenericReference` supplies new arguments to an existing definition; its original member and case signatures identify type-parameter positions without confusing them with fixed member types. Repeated stored member names also have an ordered `Members` list, preserving each occurrence and its signature. An array's shape describes its element type, rather than sampled values. Descriptions come from the same facts as serialization; recording one does not construct a model or execute its property getters.

The array keeps the latest ten distinct recorded shapes, oldest first. Acquiring an already recorded shape does not reorder or rewrite it. A new shape is appended and evicts the oldest entry when necessary. Assembly-version labels alone do not create a distinct shape. The limit applies to history entries: a retained entry keeps all its fields, union cases, nested types and known alternatives.

Several types may legitimately acquire the same collection, such as a full model and a projection. These entries describe those contracts; the last entry is not a declaration that every row has that shape. Older writers, raw SQL and partial updates can leave values that no retained description explains completely. Unknown runtime subtypes and information already lost by serialization are not guessed.

Existing writable databases gain the column without changing `user_version` or rewriting documents. A NULL value means no description has been recorded. Read-only acquisition leaves both the catalogue and database bytes unchanged. Dropping a collection removes its descriptions with the collection row; recreating it starts a new history.

Normal and generated C# and F# metadata use the same description policy. NativeAOT applications use their generated registration as usual. The recorded descriptions do not replace the codecs or control document materialization.
