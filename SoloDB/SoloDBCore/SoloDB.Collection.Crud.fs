namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq.Expressions
open Microsoft.Data.Sqlite
open SoloDatabase.Types

module internal CollectionInstanceCrud =
    let insert<'T>
        (item: 'T)
        (hasRelations: bool)
        (withTransaction: (SqliteConnection -> int64) -> int64)
        (ensureRelationTx: SqliteConnection -> Relations.RelationTxContext)
        (insertInner: SqliteConnection -> int64) =
        CollectionInsertOps<'T>.Insert item hasRelations withTransaction ensureRelationTx insertInner

    let insertOrReplace<'T>
        (item: 'T)
        (hasRelations: bool)
        (name: string)
        (withTransaction: (SqliteConnection -> int64) -> int64)
        (ensureRelationTx: SqliteConnection -> Relations.RelationTxContext)
        (tryLoadExistingOwnerForUpsert: SqliteConnection -> obj voption * int64 voption)
        (insertOrReplaceInner: SqliteConnection -> int64) =
        CollectionInsertOps<'T>.InsertOrReplace item hasRelations name withTransaction ensureRelationTx tryLoadExistingOwnerForUpsert insertOrReplaceInner

    let tryGetByIdInt64<'T>
        (id: int64)
        (name: string)
        (hasRelations: bool)
        (getConnection: unit -> SqliteConnection)
        (hydrateRelations: SqliteConnection -> int64 -> obj -> unit) =
        CollectionReadDeleteOps<'T>.TryGetByIdInt64 id name hasRelations getConnection hydrateRelations

    let requireByIdInt64<'T> (id: int64) (name: string) (value: 'T option) =
        CollectionReadDeleteOps<'T>.RequireByIdInt64(id, name, value)

    let deleteByIdInt64<'T>
        (id: int64)
        (name: string)
        (withTransaction: (SqliteConnection -> int) -> int)
        (requiresRelationDeleteHandling: SqliteConnection -> bool)
        (ensureRelationTx: SqliteConnection -> Relations.RelationTxContext)
        (syncDeleteOwner: Relations.RelationTxContext -> int64 -> obj -> unit) =
        CollectionReadDeleteOps<'T>.DeleteByIdInt64 id name withTransaction requiresRelationDeleteHandling ensureRelationTx syncDeleteOwner

    let tryGetByCustomId<'T, 'IdType when 'IdType : equality>
        (id: 'IdType)
        (name: string)
        (hasRelations: bool)
        (getConnection: unit -> SqliteConnection)
        (hydrateRelations: SqliteConnection -> int64 -> obj -> unit) =
        CollectionReadDeleteOps<'T>.TryGetByCustomId id name hasRelations getConnection hydrateRelations

    let tryGetByIdWithFallback<'T, 'IdType when 'IdType : equality>
        (id: 'IdType)
        (tryGetByInt64: int64 -> 'T option)
        (tryGetByCustomId: unit -> 'T option) =
        CollectionReadDeleteOps<'T>.TryGetByIdWithFallback id (nameof id) tryGetByInt64 tryGetByCustomId

    let deleteByCustomId<'T, 'IdType when 'IdType : equality>
        (id: 'IdType)
        (name: string)
        (withTransaction: (SqliteConnection -> int) -> int)
        (requiresRelationDeleteHandling: SqliteConnection -> bool)
        (ensureRelationTx: SqliteConnection -> Relations.RelationTxContext)
        (syncDeleteOwner: Relations.RelationTxContext -> int64 -> obj -> unit) =
        CollectionReadDeleteOps<'T>.DeleteByCustomId id name withTransaction requiresRelationDeleteHandling ensureRelationTx syncDeleteOwner

    let deleteByIdWithFallback<'T, 'IdType when 'IdType : equality>
        (id: 'IdType)
        (deleteByInt64: int64 -> int)
        (deleteByCustomId: unit -> int) =
        CollectionReadDeleteOps<'T>.DeleteByIdWithFallback id (nameof id) deleteByInt64 deleteByCustomId

    let update<'T>
        (item: 'T)
        (name: string)
        (hasRelations: bool)
        (withTransaction: (SqliteConnection -> unit) -> unit)
        (ensureRelationTx: SqliteConnection -> Relations.RelationTxContext)
        (mkRelationPathSets: unit -> HashSet<string> * HashSet<string>)
        (setSerializedItem: IDictionary<string, obj> -> 'T -> unit) =
        CollectionMutationOps<'T>.Update item name hasRelations withTransaction ensureRelationTx mkRelationPathSets setSerializedItem

    let deleteMany<'T>
        (filter: Expression<Func<'T, bool>>)
        (name: string)
        (withTransaction: (SqliteConnection -> int) -> int)
        (requiresRelationDeleteHandling: SqliteConnection -> bool)
        (ensureRelationTx: SqliteConnection -> Relations.RelationTxContext)
        (selectMutationRows: SqliteConnection -> Expression<Func<'T, bool>> -> bool -> DbObjectRow array) =
        CollectionMutationOps<'T>.DeleteMany filter name withTransaction requiresRelationDeleteHandling ensureRelationTx selectMutationRows

    let deleteOne<'T>
        (filter: Expression<Func<'T, bool>>)
        (name: string)
        (withTransaction: (SqliteConnection -> int) -> int)
        (requiresRelationDeleteHandling: SqliteConnection -> bool)
        (ensureRelationTx: SqliteConnection -> Relations.RelationTxContext)
        (selectMutationRows: SqliteConnection -> Expression<Func<'T, bool>> -> bool -> DbObjectRow array) =
        CollectionMutationOps<'T>.DeleteOne filter name withTransaction requiresRelationDeleteHandling ensureRelationTx selectMutationRows

    let replaceMany<'T>
        (item: 'T)
        (filter: Expression<Func<'T, bool>>)
        (name: string)
        (hasRelations: bool)
        (withTransaction: (SqliteConnection -> int) -> int)
        (ensureRelationTx: SqliteConnection -> Relations.RelationTxContext)
        (mkRelationPathSets: unit -> HashSet<string> * HashSet<string>)
        (setSerializedItem: IDictionary<string, obj> -> 'T -> unit) =
        CollectionMutationOps<'T>.ReplaceMany item filter name hasRelations withTransaction ensureRelationTx mkRelationPathSets setSerializedItem

    let replaceOne<'T>
        (item: 'T)
        (filter: Expression<Func<'T, bool>>)
        (name: string)
        (hasRelations: bool)
        (withTransaction: (SqliteConnection -> int) -> int)
        (ensureRelationTx: SqliteConnection -> Relations.RelationTxContext)
        (mkRelationPathSets: unit -> HashSet<string> * HashSet<string>)
        (setSerializedItem: IDictionary<string, obj> -> 'T -> unit) =
        CollectionMutationOps<'T>.ReplaceOne item filter name hasRelations withTransaction ensureRelationTx mkRelationPathSets setSerializedItem

    let updateMany<'T>
        (transform: Expression<Action<'T>> array)
        (filter: Expression<Func<'T, bool>>)
        (name: string)
        (hasRelations: bool)
        (getConnection: unit -> SqliteConnection)
        (withTransaction: (SqliteConnection -> int) -> int)
        (ensureRelationTx: SqliteConnection -> Relations.RelationTxContext)
        (selectMutationRows: SqliteConnection -> Expression<Func<'T, bool>> -> bool -> DbObjectRow array)
        (executeJsonUpdateManyByRows: SqliteConnection -> DbObjectRow array -> ResizeArray<Expression<Action<'T>>> -> int) =
        CollectionMutationOps<'T>.UpdateMany transform filter name hasRelations getConnection withTransaction ensureRelationTx selectMutationRows executeJsonUpdateManyByRows
