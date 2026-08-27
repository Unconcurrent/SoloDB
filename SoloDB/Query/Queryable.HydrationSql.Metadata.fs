namespace SoloDatabase

open System
open System.Reflection
open Microsoft.Data.Sqlite
open SQLiteTools
open SoloDatabase.RelationsTypes
open SoloDatabase.RelationsSchemaBuilder
open SoloDatabase.RelationsSchemaValidator
open SoloDatabase.RelationsSchemaLinkTableDDL

module internal HydrationSqlMetadata =
    [<Struct>]
    type internal RelationShapeInfo = {
        HasAny: bool
        HasSingle: bool
        HasMany: bool
    }

    /// A DBRefMany property plus everything materialization needs for it, resolved once from the
    /// accessor cache instead of on every populated owner.
    type internal ManyRelationEntry = {
        Property: PropertyInfo
        TargetType: Type
        TrackerGetter: Func<obj, obj>
        TrackerSetter: Action<obj, obj>
        TrackerCtor: Func<obj>
        TargetIdWriter: Action<obj, int64>
    }

    /// The relation surface of one owner type, reflected once.
    ///
    /// Order is the sequence returned by GetProperties(Public ||| Instance) for that type,
    /// filtered, with no sort and no regroup. Emitted hydration paths follow this sequence, so
    /// re-ordering here would change generated SQL.
    type internal RelationDescriptor = {
        SingleProperties: PropertyInfo array
        ManyRelations: ManyRelationEntry array
        Shape: RelationShapeInfo
    }

    /// Counts descriptor constructions. "Reflected once per type" is a standing contract, so the
    /// counter is maintained diagnostic infrastructure rather than a temporary test seam. It is
    /// allocation-free and costs one predictable branch when disabled.
    module internal DescriptorInstrumentation =
        let mutable internal Enabled = false
        let mutable private constructions = 0
        let internal Increment () = if Enabled then System.Threading.Interlocked.Increment(&constructions) |> ignore
        let internal Count () = System.Threading.Volatile.Read(&constructions)
        let internal Reset () = System.Threading.Volatile.Write(&constructions, 0)

    let private buildDescriptor (t: Type) : RelationDescriptor =
        DescriptorInstrumentation.Increment()
        // Inherited public instance properties are part of this surface and are preserved.
        let all = t.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
        let singles = all |> Array.filter (fun p -> DBRefTypeHelpers.isDBRefType p.PropertyType)
        let manyProps = all |> Array.filter (fun p -> DBRefTypeHelpers.isDBRefManyType p.PropertyType)

        // A derived type that hides a same-named relation property makes name-keyed hydration
        // paths and include/exclude ambiguous. Fail closed rather than binding whichever
        // reflected property happens to come first.
        let assertNoAmbiguousNames (props: PropertyInfo array) (kindLabel: string) =
            props
            |> Array.groupBy (fun p -> p.Name)
            |> Array.iter (fun (name, group) ->
                if group.Length > 1 then
                    raise (InvalidOperationException(
                        sprintf "Error: Ambiguous %s relation property '%s.%s'.\nReason: More than one property with that name is visible on the type, so name-keyed hydration and Include/Exclude paths cannot select one.\nFix: Rename the hiding property, or remove the redeclaration." kindLabel t.FullName name)))
        assertNoAmbiguousNames singles "single"
        assertNoAmbiguousNames manyProps "many"

        let manyEntries =
            manyProps
            |> Array.map (fun p ->
                let targetType = (Utils.GenericTypeArgCache.Get p.PropertyType).[0]
                { Property = p
                  TargetType = targetType
                  TrackerGetter = RelationsAccessorCache.compiledPropGetter p
                  TrackerSetter = RelationsAccessorCache.compiledPropSetter p
                  TrackerCtor = RelationsAccessorCache.compiledDefaultCtor p.PropertyType
                  TargetIdWriter = RelationsAccessorCache.compiledInt64IdWriter targetType })

        { SingleProperties = singles
          ManyRelations = manyEntries
          Shape =
            { HasAny = singles.Length > 0 || manyEntries.Length > 0
              HasSingle = singles.Length > 0
              HasMany = manyEntries.Length > 0 } }

    // Lazy with ExecutionAndPublication: GetOrAdd may run a factory more than once during a race,
    // so the Lazy, not the dictionary, is what guarantees a single construction and one shared
    // immutable descriptor for every caller.
    let internal relationDescriptorCache =
        System.Collections.Concurrent.ConcurrentDictionary<Type, Lazy<RelationDescriptor>>()

    let internal getRelationDescriptor (t: Type) : RelationDescriptor =
        relationDescriptorCache.GetOrAdd(
            t,
            Func<Type, Lazy<RelationDescriptor>>(fun t ->
                Lazy<RelationDescriptor>((fun () -> buildDescriptor t), System.Threading.LazyThreadSafetyMode.ExecutionAndPublication))
        ).Value

    let internal getRelationShape (t: Type) : RelationShapeInfo =
        (getRelationDescriptor t).Shape

    let internal hasRelationProperties (t: Type) =
        (getRelationShape t).HasAny

    let internal tableExists (connection: SqliteConnection) (tableName: string) =
        connection.QueryFirst<int64>(
            "SELECT CASE WHEN EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name) THEN 1 ELSE 0 END",
            {| name = tableName |}) = 1L
