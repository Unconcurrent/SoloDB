namespace SoloDatabase

open System
open System.Collections.Generic
open Microsoft.Data.Sqlite

type internal CollectionInsertOps<'T>() =

    static member Insert
        (item: 'T)
        (hasRelations: bool)
        (withTransaction: (SqliteConnection -> int64) -> int64)
        (ensureRelationTx: SqliteConnection -> Relations.RelationTxContext)
        (insertInner: SqliteConnection -> int64) =

        if isNull (box item) then raise (ArgumentNullException(nameof(item)))
        if hasRelations then
            withTransaction (fun conn ->
                let tx = ensureRelationTx conn
                let plan = Relations.prepareInsert tx (box item)
                let id = insertInner conn
                Relations.syncInsert tx id plan
                id
            )
        else
            withTransaction insertInner

    static member InsertOrReplace
        (item: 'T)
        (hasRelations: bool)
        (name: string)
        (withTransaction: (SqliteConnection -> int64) -> int64)
        (ensureRelationTx: SqliteConnection -> Relations.RelationTxContext)
        (tryLoadExistingOwnerForUpsert: SqliteConnection -> obj voption * int64 voption)
        (insertOrReplaceInner: SqliteConnection -> int64) =

        if isNull (box item) then raise (ArgumentNullException(nameof(item)))
        if hasRelations then
            withTransaction (fun conn ->
                let tx = ensureRelationTx conn
                let oldOwner, oldOwnerId = tryLoadExistingOwnerForUpsert conn

                match oldOwnerId with
                | ValueSome ownerId -> Relations.applyOwnerReplacePolicies tx name ownerId
                | ValueNone -> ()

                let plan = Relations.prepareUpsert tx oldOwner (box item)
                let id = insertOrReplaceInner conn
                Relations.syncUpsert tx id plan
                id
            )
        else
            withTransaction insertOrReplaceInner

    static member ValidateBatchItems(items: 'T seq) =
        if isNull items then raise (ArgumentNullException(nameof(items)))
        let items = items |> Seq.toArray
        for item in items do
            if isNull (box item) then raise (ArgumentNullException(nameof(items), "Batch contains a null element."))
        items

    static member InsertBatchCore
        (items: 'T array)
        (tx: Relations.RelationTxContext voption)
        (insertInner: 'T -> int64) =

        let ids = List<int64>()
        for item in items do
            match tx with
            | ValueSome tx ->
                let plan = Relations.prepareInsert tx (box item)
                let id = insertInner item
                Relations.syncInsert tx id plan
                ids.Add id
            | ValueNone ->
                insertInner item |> ids.Add
        ids

    static member InsertOrReplaceBatchCore
        (items: 'T array)
        (tx: Relations.RelationTxContext voption)
        (name: string)
        (tryLoadExistingOwnerForUpsert: 'T -> obj voption * int64 voption)
        (insertOrReplaceInner: 'T -> int64) =

        let ids = List<int64>()
        for item in items do
            match tx with
            | ValueSome tx ->
                let oldOwner, oldOwnerId = tryLoadExistingOwnerForUpsert item

                match oldOwnerId with
                | ValueSome ownerId -> Relations.applyOwnerReplacePolicies tx name ownerId
                | ValueNone -> ()

                let plan = Relations.prepareUpsert tx oldOwner (box item)
                let id = insertOrReplaceInner item
                Relations.syncUpsert tx id plan
                ids.Add id
            | ValueNone ->
                insertOrReplaceInner item |> ids.Add
        ids
