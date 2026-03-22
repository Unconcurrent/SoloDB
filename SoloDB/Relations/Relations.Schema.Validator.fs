module internal SoloDatabase.RelationsSchemaValidator

open System
open System.Reflection
open System.Collections.Generic
open Microsoft.FSharp.Reflection
open Microsoft.Data.Sqlite
open SoloDatabase
open SoloDatabase.Attributes
open SoloDatabase.Utils
open SQLiteTools
open RelationsTypes
open RelationsSchemaBuilder

let internal tryGetSoloIdProperty (targetType: Type) =
    let props =
        targetType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
        |> Array.filter (fun p -> not (isNull (p.GetCustomAttribute<SoloId>(true))))
    match props with
    | [||] -> ValueNone
    | [| p |] -> ValueSome p
    | _ ->
        raise (InvalidOperationException(
            $"Error: Invalid [SoloId] declaration on '{targetType.FullName}'.\nReason: Multiple [SoloId] properties are not supported.\nFix: Keep exactly one [SoloId] property on relation targets."))

let rec private containsNestedRelationRefType (t: Type) =
    if isNull t then false
    elif DBRefTypeHelpers.isAnyRelationRefType t then true
    elif t.IsArray then
        containsNestedRelationRefType (t.GetElementType())
    elif t.IsGenericType then
        t.GetGenericArguments() |> Array.exists containsNestedRelationRefType
    else
        false

let private isFSharpUnionType (t: Type) =
    not (isNull t) && FSharpType.IsUnion(t, true)

let private containsUnionHostedRelationRefType (rootType: Type) =
    let visited = HashSet<Type>()

    let rec scan (t: Type) =
        if isNull t then false
        elif DBRefTypeHelpers.isOptionWrappedRelationRefType t || DBRefTypeHelpers.isAnyRelationRefType t then true
        elif t = typeof<string> || t.IsPrimitive || t.IsEnum
             || t = typeof<decimal> || t = typeof<Guid>
             || t = typeof<DateTime> || t = typeof<DateTimeOffset>
             || t = typeof<DateOnly> || t = typeof<TimeOnly>
             || t = typeof<TimeSpan> then false
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

    isFSharpUnionType rootType && scan rootType

let private validateRelationTargetType (ownerType: Type) (prop: PropertyInfo) (targetType: Type) =
    if isFSharpUnionType targetType then
        raise (InvalidOperationException(
            $"Error: Invalid relation target on {ownerType.FullName}.{prop.Name}.\nReason: Target type '{targetType.FullName}' is an F# discriminated union. DBRef/DBRefMany targets require stable property paths for relation tracking, but DU cases serialize as tagged JSON values.\nFix: Use a record or class type instead."))
    elif targetType.IsInterface then
        raise (InvalidOperationException(
            $"Error: Invalid relation target on {ownerType.FullName}.{prop.Name}.\nReason: Target type '{targetType.FullName}' is an interface.\nFix: Use a concrete class with a writable int64 Id property."))
    elif targetType.IsAbstract && not (JsonFunctions.mustIncludeTypeInformationInSerializationFn targetType) then
        raise (InvalidOperationException(
            $"Error: Invalid relation target on {ownerType.FullName}.{prop.Name}.\nReason: Target type '{targetType.FullName}' is abstract without [Polymorphic] attribute.\nFix: Add [Polymorphic] to the abstract base class, or use a concrete class with a writable int64 Id property."))
    else
        getWritableInt64IdPropertyOrThrow targetType |> ignore

let private buildRelationSpecs (ownerType: Type) =
    if containsUnionHostedRelationRefType ownerType then
        raise (InvalidOperationException(
            $"Error: Invalid relation declaration on '{ownerType.FullName}'.\nReason: DBRef/DBRefMany properties inside F# discriminated unions are not supported. DU cases serialize as tagged JSON values without stable property paths for relation tracking.\nFix: Use a record or class type to hold relation properties."))

    ownerType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
    |> Array.choose (fun prop ->
        if not prop.CanRead then
            None
        else
            let propType = prop.PropertyType
            if DBRefTypeHelpers.isOptionWrappedRelationRefType propType then
                raise (InvalidOperationException(
                    $"Error: Invalid relation property '{ownerType.FullName}.{prop.Name}'.\nReason: Option-wrapped DBRef/DBRefMany is not supported.\nFix: Use DBRef<T>/DBRef<TTarget,'TId>/DBRefMany<T>/DBRefMany<TTarget,'TId> directly (non-option)."))
            elif DBRefTypeHelpers.isAnyRelationRefType propType then
                if not prop.CanWrite then
                    raise (InvalidOperationException(
                        $"Error: Invalid relation property '{ownerType.FullName}.{prop.Name}'.\nReason: Relation properties must be writable.\nFix: Add a public setter, or remove relation type from this property."))
                else
                    let generic = propType.GetGenericTypeDefinition()
                    let args = propType.GetGenericArguments()
                    let targetType = args.[0]
                    let typedIdType =
                        if DBRefTypeHelpers.isDBRefTypedDefinition generic || DBRefTypeHelpers.isDBRefManyTypedDefinition generic then ValueSome args.[1]
                        else ValueNone
                    let attr = prop.GetCustomAttribute<SoloRefAttribute>(true)
                    let onDelete = if isNull attr then DeletePolicy.Restrict else attr.OnDelete
                    let onOwnerDelete = if isNull attr then DeletePolicy.Deletion else attr.OnOwnerDelete
                    let isUnique = not (isNull attr) && attr.Unique
                    let kind = if DBRefTypeHelpers.isDBRefManyDefinition generic then Many else Single

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
                        | ValueSome soloIdProp when soloIdProp.PropertyType <> idType ->
                            raise (InvalidOperationException(
                                $"Error: Invalid typed relation on {ownerType.FullName}.{prop.Name}.\nReason: DBRef id type '{idType.FullName}' does not match target [SoloId] type '{soloIdProp.PropertyType.FullName}'.\nFix: Align DBRef<'TTarget,'TId> with target [SoloId] property type."))
                        | ValueSome _ -> ()
                    | ValueNone -> ()

                    let orderBy = if isNull attr then DBRefOrder.Undefined else attr.OrderBy
                    Some (prop, kind, targetType, typedIdType, onDelete, onOwnerDelete, isUnique, orderBy)
            elif containsNestedRelationRefType propType then
                raise (InvalidOperationException(
                    $"Error: Invalid relation property '{ownerType.FullName}.{prop.Name}'.\nReason: Container-wrapped relation-like shapes are not supported.\nFix: Use direct DBRef<T>/DBRef<TTarget,'TId>/DBRefMany<T>/DBRefMany<TTarget,'TId> property types."))
            else
                None)

let internal getRelationSpecs (ownerType: Type) =
    relationSpecsCache.GetOrAdd(ownerType, Func<_, _>(buildRelationSpecs))

let internal hasManyBackReference (ownerType: Type) (targetType: Type) =
    getRelationSpecs targetType
    |> Array.exists (fun (_, kind, candidateTargetType, _, _, _, _, _) ->
        kind = Many && candidateTargetType = ownerType)

let private br04Message (ownerType: Type) (ownerTable: string) (linkTable: string) (propNames: string array) =
    let props = String.Join(",", propNames)
    $"Error: contradictory shared-many topology detected for '{ownerType.FullName}'.\nReason: Multiple DBRefMany properties ({props}) on collection '{ownerTable}' resolve to the same link table '{linkTable}'.\nFix: ensure each DBRefMany relation resolves to a distinct link table or redesign the relation topology."

let private validateSharedManyContradictions (ownerType: Type) (ownerTable: string) (descriptors: RelationDescriptor array) =
    descriptors
    |> Array.filter (fun d -> d.Kind = Many)
    |> Array.groupBy (fun d -> d.LinkTable)
    |> Array.iter (fun (linkTable, grp) ->
        if grp.Length > 1 then
            let propNames = grp |> Array.map (fun d -> d.Property.Name) |> Array.sort
            raise (InvalidOperationException(br04Message ownerType ownerTable linkTable propNames)))

let private validateConflictingPolicies (ownerType: Type) (descriptors: RelationDescriptor array) =
    descriptors
    |> Array.groupBy (fun d -> d.Kind, d.TargetType)
    |> Array.iter (fun ((kind, targetType), grp) ->
        if grp.Length > 1 then
            let variants =
                grp
                |> Array.map (fun d -> d.OnDelete, d.OnOwnerDelete, d.IsUnique)
                |> Array.distinct
            if variants.Length > 1 then
                let propNames =
                    grp
                    |> Array.map (fun d -> d.Property.Name)
                    |> Array.sort
                let propNames = String.Join(", ", propNames)
                let kindName = if kind = Single then "DBRef" else "DBRefMany"
                raise (InvalidOperationException(
                    $"Error: Conflicting relation policies detected on '{ownerType.FullName}'.\nReason: {kindName} properties ({propNames}) targeting '{targetType.FullName}' do not agree on delete-policy settings.\nFix: Use consistent OnDelete/OnOwnerDelete/Unique settings for relations to the same target type."))
        )

let private validateCrossKindConflictingPolicies (ownerType: Type) (descriptors: RelationDescriptor array) =
    descriptors
    |> Array.groupBy (fun d -> d.TargetType)
    |> Array.iter (fun (targetType, grp) ->
        if grp.Length > 1 then
            let kinds =
                grp
                |> Array.map _.Kind
                |> Array.distinct
            if kinds.Length > 1 then
                let variants =
                    grp
                    |> Array.map (fun d -> d.OnDelete, d.OnOwnerDelete, d.IsUnique)
                    |> Array.distinct
                if variants.Length > 1 then
                    let propNames =
                        grp
                        |> Array.map (fun d -> d.Property.Name)
                        |> Array.sort
                    let propNames = String.Join(", ", propNames)
                    raise (InvalidOperationException(
                        $"Error: Conflicting relation policies detected on '{ownerType.FullName}'.\nReason: Mixed DBRef/DBRefMany properties ({propNames}) targeting '{targetType.FullName}' do not agree on delete-policy settings.\nFix: Use consistent OnDelete/OnOwnerDelete/Unique settings for relations to the same target type."))
        )

let internal buildRelationDescriptors (tx: RelationTxContext) (ownerType: Type) =
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
                    tx.Connection.QueryFirstOrDefault<string>(
                        "SELECT RefKind FROM SoloDBRelation WHERE Name = @name LIMIT 1;",
                        {| name = canonicalManyName |})
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
    validateConflictingPolicies ownerType descriptors
    validateCrossKindConflictingPolicies ownerType descriptors
    descriptors
