module internal SoloDatabase.RelationsSchemaValidator

open System
open System.Reflection
open System.Collections.Generic
open Microsoft.FSharp.Reflection
open SoloDatabase
open SoloDatabase.Attributes
open SoloDatabase.Utils
open NativeSQLiteTools
open RelationsTypes
open RelationsSchemaBuilder

let internal tryGetSoloIdProperty (targetType: Type) =
    let members = CustomIdMetadata.GetMembers targetType
    if members.Length > 1 then
        raise (InvalidOperationException($"Error: Invalid [SoloId] declaration on '{targetType.FullName}'.\nReason: Multiple [SoloId] properties are not supported.\nFix: Keep exactly one [SoloId] property on relation targets."))
    if members.Length = 0 then ValueNone else ValueSome members.[0]

let private isFSharpUnionType (t: Type) =
    not (isNull t) && RelationMetadata.IsUnion t

let private containsUnionHostedRelationRefType (rootType: Type) =
    let visited = HashSet<Type>()

    let rec scan (t: Type) =
        if isNull t then false
        elif DBRefTypeHelpers.isOptionWrappedRelationRefType t || DBRefTypeHelpers.isAnyRelationRefType t then true
        elif typeof<string>.Equals(t) || t.IsPrimitive || t.IsEnum
             || typeof<decimal>.Equals(t) || typeof<Guid>.Equals(t) || isDateTimeLikeType t then false
        elif not (visited.Add t) then false
        elif t.IsArray then
            scan (t.GetElementType())
        elif isFSharpUnionType t then
            FSharpType.GetUnionCases(t, true)
            |> Array.exists (fun unionCase ->
                unionCase.GetFields()
                |> Array.exists (fun field -> scan field.PropertyType))
        elif FSharpType.IsRecord(t, true) then
            FSharpType.GetRecordFields(t, true)
            |> Array.exists (fun field -> scan field.PropertyType)
        elif t.IsGenericType && (t.GetGenericArguments() |> Array.exists scan) then
            true
        elif t.IsClass then
            t.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
            |> Array.exists (fun prop ->
                prop.CanRead
                && prop.GetIndexParameters().Length = 0
                && scan prop.PropertyType)
        else
            false

    match RelationMetadata.TryGetUnion rootType with
    | ValueSome hostedReferences -> hostedReferences
    | ValueNone -> isFSharpUnionType rootType && scan rootType

let private validateRelationTargetType (ownerType: Type) (prop: RelationMember) (targetType: Type) =
    if isFSharpUnionType targetType then
        raise (InvalidOperationException(
            $"Error: Invalid relation target on {ownerType.FullName}.{prop.Name}.\nReason: Target type '{targetType.FullName}' is an F# discriminated union. DBRef/DBRefMany targets require stable property paths for relation tracking, but DU cases serialize as tagged JSON values.\nFix: Use a record or class type instead."))
    elif targetType.IsInterface then
        raise (InvalidOperationException(
            $"Error: Invalid relation target on {ownerType.FullName}.{prop.Name}.\nReason: Target type '{targetType.FullName}' is an interface.\nFix: Use a concrete class with a writable int64 Id property."))
    elif targetType.IsAbstract && not (TypeMetadata.requiresTypeInformation targetType) then
        raise (InvalidOperationException(
            $"Error: Invalid relation target on {ownerType.FullName}.{prop.Name}.\nReason: Target type '{targetType.FullName}' is abstract without [Polymorphic] attribute.\nFix: Add [Polymorphic] to the abstract base class, or use a concrete class with a writable int64 Id property."))
    else
        requireWritableInt64Id targetType

let private buildRelationSpecs (ownerType: Type) (properties: RelationMember array) =
    if containsUnionHostedRelationRefType ownerType then
        raise (InvalidOperationException(
            $"Error: Invalid relation declaration on '{ownerType.FullName}'.\nReason: DBRef/DBRefMany properties inside F# discriminated unions are not supported. DU cases serialize as tagged JSON values without stable property paths for relation tracking.\nFix: Use a record or class type to hold relation properties."))

    properties
    |> Array.choose (fun prop ->
        if isNull prop.Read then
            None
        else
            let propType = prop.ReferenceType
            if DBRefTypeHelpers.isOptionWrappedRelationRefType propType then
                raise (InvalidOperationException(
                    $"Error: Invalid relation property '{ownerType.FullName}.{prop.Name}'.\nReason: Option-wrapped DBRef/DBRefMany is not supported.\nFix: Use DBRef<T>/DBRef<TTarget,'TId>/DBRefMany<T>/DBRefMany<TTarget,'TId> directly (non-option)."))
            elif DBRefTypeHelpers.isFileRefType propType || DBRefTypeHelpers.isFileRefManyType propType then
                None
            elif DBRefTypeHelpers.isAnyRelationRefType propType then
                if isNull prop.Write then
                    raise (InvalidOperationException(
                        $"Error: Invalid relation property '{ownerType.FullName}.{prop.Name}'.\nReason: Relation properties must be writable.\nFix: Add a public setter, or remove relation type from this property."))
                else
                    let targetType = DBRefTypeHelpers.referenceTargetType propType
                    let typedIdType = DBRefTypeHelpers.referenceIdentityType propType
                    let attr = prop.Policy
                    let onDelete = if isNull attr then DeletePolicy.Restrict else attr.OnDelete
                    let onOwnerDelete = if isNull attr then DeletePolicy.Deletion else attr.OnOwnerDelete
                    let isUnique = not (isNull attr) && attr.Unique
                    let kind = if DBRefTypeHelpers.isDBRefManyType propType then Many else Single

                    validateRelationTargetType ownerType prop targetType

                    match kind, onOwnerDelete with
                    | Single, DeletePolicy.Cascade
                    | Many, DeletePolicy.Cascade ->
                        raise (InvalidOperationException(
                            $"Error: Invalid relation policy on {ownerType.FullName}.{prop.Name}.\nReason: OnOwnerDelete cannot be Cascade.\nFix: Use Deletion instead."))
                    | Single, _
                    | Many, _ -> ()

                    match typedIdType with
                    | ValueSome idType ->
                        match tryGetSoloIdProperty targetType with
                        | ValueNone ->
                            raise (InvalidOperationException(
                                $"Error: Invalid typed relation on {ownerType.FullName}.{prop.Name}.\nReason: Target type '{targetType.FullName}' has no [SoloId] property.\nFix: Add exactly one [SoloId] property with type '{idType.FullName}'."))
                        | ValueSome soloIdProp when not (soloIdProp.IdType.Equals(idType)) ->
                            raise (InvalidOperationException(
                                $"Error: Invalid typed relation on {ownerType.FullName}.{prop.Name}.\nReason: DBRef id type '{idType.FullName}' does not match target [SoloId] type '{soloIdProp.IdType.FullName}'.\nFix: Align DBRef<'TTarget,'TId> with target [SoloId] property type."))
                        | ValueSome _ -> ()
                    | ValueNone -> ()

                    let orderBy = if isNull attr then DBRefOrder.Undefined else attr.OrderBy
                    Some (prop, kind, targetType, typedIdType, onDelete, onOwnerDelete, isUnique, orderBy)
            elif DBRefTypeHelpers.containsNestedRelationRefType propType then
                raise (InvalidOperationException(
                    $"Error: Invalid relation property '{ownerType.FullName}.{prop.Name}'.\nReason: Container-wrapped relation-like shapes are not supported.\nFix: Use direct DBRef<T>/DBRef<TTarget,'TId>/DBRefMany<T>/DBRefMany<TTarget,'TId> property types."))
            else
                None)


let internal getReferenceMembers (ownerType: Type) = RelationMetadata.Get ownerType

let private buildReferenceSpecs (ownerType: Type) (properties: RelationMember[]) =
    let documents = buildRelationSpecs ownerType properties
    let filesystem =
        properties |> Array.choose (fun prop ->
            let t = prop.ReferenceType
            if not (DBRefTypeHelpers.isFileRefType t || DBRefTypeHelpers.isFileRefManyType t) then None
            else
                if isNull prop.Read || isNull prop.Write then
                    raise (InvalidOperationException($"Filesystem reference property '{ownerType.FullName}.{prop.Name}' must be readable and writable."))
                let attr = prop.Policy
                if not (isNull attr) then
                    raise (InvalidOperationException($"Filesystem reference '{ownerType.FullName}.{prop.Name}' always restricts target deletion and unlinks on owner deletion; SoloRef policies apply to document references."))
                let isFile = typeof<FileRef>.Equals(t) || typeof<FileRefMany>.Equals(t)
                Some {
                    Property = prop
                    Kind = if DBRefTypeHelpers.isFileRefManyType t then Many else Single
                    TargetType = if isFile then typeof<SoloDatabase.Types.SoloDBFileHeader> else typeof<SoloDatabase.Types.SoloDBDirectoryHeader>
                    TargetTable = if isFile then "SoloDBFileHeader" else "SoloDBDirectoryHeader"
                })
    struct(documents, filesystem)

let private getReferenceSpecs ownerType =
    let properties = getReferenceMembers ownerType
    TypeMetadataCache.Discover(ownerType,
        (fun metadata ->
            match metadata.ReferenceSpecs with
            | ValueSome operation ->
                let struct(source,value) = operation.Value
                if obj.ReferenceEquals(source,properties) then ValueSome { Generated = operation.Generated; Value = value } else ValueNone
            | ValueNone -> ValueNone),
        (fun operation metadata ->
            match metadata.Relations with
            | ValueSome relations when obj.ReferenceEquals(relations.Value,properties) ->
                { metadata with ReferenceSpecs = ValueSome { Generated = operation.Generated; Value = struct(properties,operation.Value) } }
            | _ -> metadata),
        Func<_>(fun () -> buildReferenceSpecs ownerType properties))

let internal getRelationSpecs ownerType =
    let struct(documents, _) = getReferenceSpecs ownerType
    documents

let internal getFileSystemReferenceSpecs ownerType =
    let struct(_, filesystem) = getReferenceSpecs ownerType
    filesystem

let internal hasReferenceProperties ownerType =
    let struct(documents, filesystem) = getReferenceSpecs ownerType
    documents.Length <> 0 || filesystem.Length <> 0

let internal hasManyBackReference (ownerType: Type) (targetType: Type) =
    getRelationSpecs targetType
    |> Array.exists (fun (_, kind, candidateTargetType, _, _, _, _, _) ->
        kind = Many && (if isNull candidateTargetType then isNull ownerType else candidateTargetType.Equals(ownerType)))

let private sharedManyCollisionMessage (ownerType: Type) (ownerTable: string) (linkTable: string) (propNames: string array) =
    let props = String.Join(",", propNames)
    $"Error: contradictory shared-many topology detected for '{ownerType.FullName}'.\nReason: Multiple DBRefMany properties ({props}) on collection '{ownerTable}' resolve to the same link table '{linkTable}'.\nFix: ensure each DBRefMany relation resolves to a distinct link table or redesign the relation topology."

let private validateSharedManyContradictions (ownerType: Type) (ownerTable: string) (descriptors: RelationDescriptor array) =
    descriptors
    |> Array.filter (fun d -> d.Kind = Many)
    |> Array.groupBy (fun d -> d.LinkTable)
    |> Array.iter (fun (linkTable, grp) ->
        if grp.Length > 1 then
            let propNames = grp |> Array.map (fun d -> d.Property.Name) |> Array.sort
            raise (InvalidOperationException(sharedManyCollisionMessage ownerType ownerTable linkTable propNames)))

let private resolveRelationDescriptors (tx: RelationTxContext) (ownerType: Type) =
    let ownerTable = formatName tx.OwnerTable
    let descriptors =
        getRelationSpecs ownerType
        |> Array.map (fun (prop, kind, targetType, typedIdType, onDelete, onOwnerDelete, isUnique, orderBy) ->
        let targetTable = resolveTargetCollectionName tx.Connection ownerTable prop.Name targetType
        let defaultRelName = relationName ownerTable prop.Name
        let canonicalManyName = canonicalManyRelationName ownerTable targetTable
        let isMutualMany = kind = Many && hasManyBackReference ownerType targetType
        let proposedRelName = defaultRelName
        let relName =
            match tryGetStoredRelationName tx.Connection ownerTable prop.Name with
            | Some stored when not (String.IsNullOrWhiteSpace stored) -> formatName stored
            | _ -> proposedRelName

        let canonicalSharedLinkTable = linkTableFromRelationName canonicalManyName
        let existingCanonicalTableIsManyKind =
            sqliteTableExistsByName tx.Connection canonicalSharedLinkTable
            && (let storedKind =
                    let mutable statement = tx.Connection.OpenStatement("SELECT RefKind FROM SoloDBRelation WHERE Name = @name LIMIT 1;")
                    try
                        statement.BindText(1, canonicalManyName)
                        let kind = if statement.Step() then statement.Row.GetString(0) else null
                        statement.Dispose()
                        kind
                    with error -> statement.Abort(error); reraise()
                storedKind = "Many")
        let isSharedMany =
            kind = Many
            && (isMutualMany || existingCanonicalTableIsManyKind)
        let ownerUsesSourceColumn =
            if isSharedMany then
                sharedManyOwnerUsesSourceColumn ownerTable targetTable
            else
                true
        let linkSourceTable, linkTargetTable =
            if ownerUsesSourceColumn then ownerTable, targetTable
            else targetTable, ownerTable

        {
            OwnerTable = ownerTable
            OwnerType = ownerType
            Property = prop
            PropertyPath = prop.Name
            Kind = kind
            TargetType = targetType
            TargetTable = targetTable
            LinkSourceTable = linkSourceTable
            LinkTargetTable = linkTargetTable
            OwnerUsesSourceColumn = ownerUsesSourceColumn
            RelationName = relName
            LinkTable = if isSharedMany then canonicalSharedLinkTable else linkTableFromRelationName relName
            OnDelete = onDelete
            OnOwnerDelete = onOwnerDelete
            IsUnique = isUnique
            OrderBy = orderBy
            TypedIdType = typedIdType
            TargetSoloIdProperty = tryGetSoloIdProperty targetType
        })

    validateSharedManyContradictions ownerType ownerTable descriptors
    descriptors

let internal buildRelationDescriptors (tx: RelationTxContext) (ownerType: Type) =
    if not tx.Preparation.AllowReuse then resolveRelationDescriptors tx ownerType
    else
        let entry = tx.Preparation.Entry(tx.Connection, formatName tx.OwnerTable, ownerType)
        match entry.Descriptors with
        | ValueSome descriptors -> descriptors
        | ValueNone ->
            let descriptors = resolveRelationDescriptors tx ownerType
            entry.Descriptors <- ValueSome descriptors
            descriptors
