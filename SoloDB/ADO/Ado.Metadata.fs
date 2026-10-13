namespace SoloDatabase.Data.Sqlite

open System
open System.Data
open System.Data.Common
open SoloDatabase
open SQLitePCL

module internal AdoMetadata =
    let schema (session: AdoSession) name (restrictions: string[]) =
        session.Guard(fun () ->
            if not (isNull restrictions) && restrictions.Length <> 0 then invalidArg "restrictionValues" "This metadata collection has no restrictions."
            if String.Equals(name, DbMetaDataCollectionNames.MetaDataCollections, StringComparison.OrdinalIgnoreCase) then
                let table = new DataTable(DbMetaDataCollectionNames.MetaDataCollections)
                table.Columns.Add(DbMetaDataColumnNames.CollectionName, typeof<string>) |> ignore
                table.Columns.Add(DbMetaDataColumnNames.NumberOfRestrictions, typeof<int>) |> ignore
                table.Columns.Add(DbMetaDataColumnNames.NumberOfIdentifierParts, typeof<int>) |> ignore
                table.Rows.Add([|box DbMetaDataCollectionNames.MetaDataCollections; box 0; box 0|]) |> ignore
                table.Rows.Add([|box DbMetaDataCollectionNames.ReservedWords; box 0; box 0|]) |> ignore
                table
            elif String.Equals(name, DbMetaDataCollectionNames.ReservedWords, StringComparison.OrdinalIgnoreCase) then
                NativeDatabase.EnsureInitialized()
                let table = new DataTable(DbMetaDataCollectionNames.ReservedWords)
                table.Columns.Add(DbMetaDataColumnNames.ReservedWord, typeof<string>) |> ignore
                for i = 0 to raw.sqlite3_keyword_count() - 1 do
                    let mutable keyword = null
                    let result = raw.sqlite3_keyword_name(i, &keyword)
                    if result <> raw.SQLITE_OK then raise (NativeSqliteException.FromResult(null, result))
                    table.Rows.Add([|box keyword|]) |> ignore
                table
            else invalidArg "collectionName" "Unknown SQLite metadata collection.")
