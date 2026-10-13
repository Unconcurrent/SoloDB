namespace SoloDatabase

open System
open System.Collections.Generic

type internal CollectionInsertOps<'T>() =

    static member Insert
        (item: 'T)
        (hasRelations: bool)
        (withTransaction: (NativeSqliteConnection -> int64) -> int64)
        (ensureRelationTx: NativeSqliteConnection -> RelationsTypes.RelationTxContext)
        (insertInner: NativeSqliteConnection -> int64) =

        if isNull (box item) then raise (ArgumentNullException(nameof(item)))
        if hasRelations then
            withTransaction (fun conn ->
                let tx = ensureRelationTx conn
                let plan = RelationsPlan.prepareInsert tx (box item)
                let id = insertInner conn
                RelationsGraphAdmission.inserted tx id
                RelationsSync.syncInsert tx id plan
                RelationsGraphAdmission.validate tx
                id
            )
        else
            withTransaction insertInner

    static member InsertOrReplace
        (item: 'T)
        (hasRelations: bool)
        (name: string)
        (withTransaction: (NativeSqliteConnection -> int64) -> int64)
        (ensureRelationTx: NativeSqliteConnection -> RelationsTypes.RelationTxContext)
        (tryLoadExistingOwnerForUpsert: NativeSqliteConnection -> obj voption * int64 voption)
        (insertOrReplaceInner: NativeSqliteConnection -> int64) =

        if isNull (box item) then raise (ArgumentNullException(nameof(item)))
        if hasRelations then
            withTransaction (fun conn ->
                let tx = ensureRelationTx conn
                let oldOwner, oldOwnerId = tryLoadExistingOwnerForUpsert conn

                match oldOwnerId with
                | ValueSome ownerId ->
                    RelationsGraphAdmission.capture tx ownerId
                    RelationsSync.applyOwnerReplacePolicies tx name ownerId
                | ValueNone -> ()

                let plan = RelationsPlan.prepareUpsert tx oldOwner (box item)
                let id = insertOrReplaceInner conn
                if oldOwnerId.IsNone then RelationsGraphAdmission.inserted tx id
                RelationsSync.syncUpsert tx id plan
                RelationsGraphAdmission.validate tx
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
        (tx: RelationsTypes.RelationTxContext voption)
        (insertInner: 'T -> int64) =

        let ids = List<int64>(items.Length)
        for item in items do
            match tx with
            | ValueSome tx ->
                let plan = RelationsPlan.prepareInsert tx (box item)
                let id = insertInner item
                RelationsGraphAdmission.inserted tx id
                RelationsSync.syncInsert tx id plan
                ids.Add id
            | ValueNone ->
                insertInner item |> ids.Add
        match tx with
        | ValueSome tx -> RelationsGraphAdmission.validate tx
        | ValueNone -> ()
        ids

    static member InsertOrReplaceBatchCore
        (items: 'T array)
        (tx: RelationsTypes.RelationTxContext voption)
        (name: string)
        (tryLoadExistingOwnerForUpsert: 'T -> obj voption * int64 voption)
        (insertOrReplaceInner: 'T -> int64) =

        let ids = List<int64>(items.Length)
        for item in items do
            match tx with
            | ValueSome tx ->
                let oldOwner, oldOwnerId = tryLoadExistingOwnerForUpsert item

                match oldOwnerId with
                | ValueSome ownerId ->
                        RelationsGraphAdmission.capture tx ownerId
                        RelationsSync.applyOwnerReplacePolicies tx name ownerId
                | ValueNone -> ()

                let plan = RelationsPlan.prepareUpsert tx oldOwner (box item)
                let id = insertOrReplaceInner item
                if oldOwnerId.IsNone then RelationsGraphAdmission.inserted tx id
                RelationsSync.syncUpsert tx id plan
                ids.Add id
            | ValueNone ->
                insertOrReplaceInner item |> ids.Add
        match tx with
        | ValueSome tx -> RelationsGraphAdmission.validate tx
        | ValueNone -> ()
        ids
