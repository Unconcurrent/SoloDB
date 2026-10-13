namespace SoloDatabase

open System
open System.Reflection
open NativeSQLiteTools
open SoloDatabase.RelationsTypes
open SoloDatabase.RelationsSchemaBuilder
open SoloDatabase.RelationsSchemaValidator
open SoloDatabase.RelationsSchemaLinkTableDDL

module internal HydrationSqlMetadata =
    /// Counts descriptor constructions. "Reflected once per type" is a standing contract, so the
    /// counter is maintained diagnostic infrastructure rather than a temporary test seam. It is
    /// allocation-free and costs one predictable branch when disabled.
    module internal DescriptorInstrumentation =
        let mutable private enabled = 0
        let internal SetEnabled (value: bool) = System.Threading.Volatile.Write(&enabled, if value then 1 else 0)
        let internal IsEnabled () = System.Threading.Volatile.Read(&enabled) = 1
        let mutable private constructions = 0
        let internal Increment () =
            if System.Threading.Volatile.Read(&enabled) = 1 then
                System.Threading.Interlocked.Increment(&constructions) |> ignore
        let internal Count () = System.Threading.Volatile.Read(&constructions)
        let internal Reset () = System.Threading.Volatile.Write(&constructions, 0)

    let private buildDescriptor (t: Type) (properties: RelationMember[]) : RelationHydrationDescriptor =
        DescriptorInstrumentation.Increment()
        let singles = properties |> Array.filter(fun property -> DBRefTypeHelpers.isDBRefType property.ReferenceType || DBRefTypeHelpers.isFileRefType property.ReferenceType)
        let manyProps = properties |> Array.filter(fun property -> DBRefTypeHelpers.isDBRefManyType property.ReferenceType)
        let hasFileMany = properties |> Array.exists(fun property -> DBRefTypeHelpers.isFileRefManyType property.ReferenceType)

        // Single references and document-Many references share name-keyed hydration paths.
        // Check both categories together: a hidden property of the other kind collides
        // just as a second property of the same kind.
        Array.append singles manyProps
        |> Array.groupBy (fun p -> p.Name)
        |> Array.iter (fun (name, group) ->
            if group.Length > 1 then
                raise (InvalidOperationException(
                    sprintf "Error: Ambiguous relation property '%s.%s'.\nReason: %d relation properties on this type share that name, so name-keyed hydration and Include/Exclude paths cannot select one.\nFix: Rename the hiding property, or remove the redeclaration." t.FullName name group.Length)))

        let manyEntries =
            manyProps
            |> Array.map (fun p ->
                let targetType = DBRefTypeHelpers.referenceTargetType p.ReferenceType
                { Property = p
                  TargetType = targetType
                  TrackerGetter = p.Read
                  TrackerSetter = p.Write
                  TrackerCtor = RelationsAccessorCache.compiledDefaultCtor p.ReferenceType
                  TargetIdWriter = EntityIdentity.compiledInt64IdWriter targetType })

        RelationHydrationDescriptor(
            singles,
            manyEntries,
            { HasAny = singles.Length > 0 || manyEntries.Length > 0 || hasFileMany
              HasSingle = singles.Length > 0
              HasMany = manyEntries.Length > 0 || hasFileMany })

    let internal getRelationDescriptor (t: Type) : RelationHydrationDescriptor =
        let properties = getReferenceMembers t
        let descriptor = TypeMetadataCache.Discover(t,
            (fun metadata ->
                match metadata.Hydration with
                | ValueSome operation ->
                    let struct(source,value) = operation.Value
                    if obj.ReferenceEquals(source,properties) then ValueSome { Generated = operation.Generated; Value = value } else ValueNone
                | ValueNone -> ValueNone),
            (fun operation metadata ->
                match metadata.Relations with
                | ValueSome relations when obj.ReferenceEquals(relations.Value,properties) ->
                    { metadata with Hydration = ValueSome { Generated = operation.Generated; Value = struct(properties,operation.Value) } }
                | _ -> metadata),
            Func<Lazy<RelationHydrationDescriptor>>(fun () -> Lazy<RelationHydrationDescriptor>(fun () -> buildDescriptor t properties)))
        descriptor.Value

    let internal getRelationShape (t: Type) : RelationHydrationShape =
        (getRelationDescriptor t).Shape

    let internal hasRelationProperties (t: Type) =
        (getRelationShape t).HasAny

    let internal tableExists (connection: NativeSqliteConnection) (tableName: string) =
        let mutable statement = connection.OpenStatement("SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name)")
        try
            statement.BindText(1, tableName)
            statement.Step() |> ignore
            let exists = statement.Row.GetBoolean(0)
            statement.Dispose()
            exists
        with error -> statement.Abort(error); reraise()
