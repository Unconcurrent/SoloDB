# Internal storage layout

This is implementation reference for tools that inspect SQLite directly. Applications can use collections, LINQ and `FileSystem` without depending on this layout.

A document collection has an `Id` primary key, a `Value` JSONB document and a `Metadata` column managed by SoloDB. Internal columns and catalogs belong to SoloDB and may change between releases; they are not extra application fields.

To inspect document values with SQL:

```sql
SELECT Id, json(Value) AS Document
FROM MyCollection;
```

`SoloDBCollections` catalogs collection names and recorded serializer information. The [collection model description](collection-type-information.md) explains the tooling format. File tables and search objects are also maintained by their public APIs; see [file storage](custom-storage.md) and [search](full-text-search.md).
