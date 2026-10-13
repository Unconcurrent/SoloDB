namespace SoloDatabase

open SoloDatabase.JsonSerializator
open System.Linq.Expressions
open System
open System.Collections.Generic
open System.Collections
open SoloDatabase.Types
open System.IO
open System.Text
open NativeSQLiteTools
open JsonFunctions
open FileStorage
open Connections
open Utils
open System.Runtime.CompilerServices
open System.Reflection
open System.Data
open System.Globalization
open System.Linq
open SoloDatabase.Attributes

[<AbstractClass; Sealed>]
type internal CustomIdRunner =

    /// Insert / cascade-write entry. IsEmpty gate, then GenerateId + SetId. Idempotent on
    /// already-set ids.
    static member RunIfEmpty<'T when 'T :> obj> (item: 'T, collection: ISoloDBCollection<'T>) : unit =
        match CustomTypeId<'T>.Get() with
        | Some x ->
            let boxedItem = box item
            let oldId = x.GetId boxedItem
            match x.Generator with
            | :? SoloDatabase.Attributes.IIdGenerator as generator ->
                if generator.IsEmpty oldId then
                    let id = generator.GenerateId (box collection) boxedItem
                    x.SetId id boxedItem
            | :? SoloDatabase.Attributes.IIdGenerator<'T> as generator ->
                if generator.IsEmpty oldId then
                    let id = generator.GenerateId collection item
                    x.SetId id boxedItem
            | other -> raise (System.InvalidOperationException(
                sprintf "Error: Invalid Id generator type.\nReason: Type '%s' is not supported.\nFix: Use a supported Id generator or configure a custom Id strategy." (other.GetType().ToString())))
        | None -> ()


    static let runOpenMethod = lazy (
        typeof<CustomIdRunner>.GetMethods(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Static)
        |> Array.tryFind (fun m -> m.Name = "RunCascadeIfEmpty" && m.IsGenericMethodDefinition)
        |> Option.defaultWith (fun () ->
            raise (System.InvalidOperationException("CustomIdRunner.RunCascadeIfEmpty<'T> not found via reflection — internal SoloDB defect."))))

    static member private RunCascadeIfEmpty<'T when 'T :> obj> (item: obj, collectionName: string, tx: RelationsTypes.RelationTxContext) =
        match CustomTypeId<'T>.Get(), tx.CollectionFactory with
        | None, _ -> ()
        | Some _, Some factory ->
            let collection = factory.CreateCollection<'T>(collectionName)
            CustomIdRunner.RunIfEmpty(unbox<'T> item, collection)
        | Some _, None ->
            raise (InvalidOperationException("A cascade ID generator requires a transactional collection factory."))

    /// Typed registration can be emitted directly; runtime discovery constructs the
    /// identical callable once, with no reflective invocation on cascade writes.
    static member Register<'T when 'T :> obj>() =
        let run = Action<obj,string,RelationsTypes.RelationTxContext>(fun item name tx -> CustomIdRunner.RunCascadeIfEmpty<'T>(item,name,tx))
        TypeMetadataCache.Register(typeof<'T>, fun metadata -> { metadata with CascadeIdentity = MetadataValue.generated (run :> Delegate) })

    static member RunBoxedIfEmpty (targetType: Type, item: obj, collectionName: string, tx: RelationsTypes.RelationTxContext) : unit =
        let run = TypeMetadataCache.Discover(targetType, (fun metadata -> metadata.CascadeIdentity),
            (fun operation metadata -> { metadata with CascadeIdentity = ValueSome operation }), Func<Delegate>(fun () ->
                if not (MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "cascade identity execution" targetType
                runOpenMethod.Value.MakeGenericMethod(targetType).CreateDelegate(typeof<Action<obj,string,RelationsTypes.RelationTxContext>>)))
                :?> Action<obj,string,RelationsTypes.RelationTxContext>
        run.Invoke(item, collectionName, tx)



/// Roots typed cascade execution for generated original model identities.
[<AbstractClass; Sealed>]
type internal CollectionMetadata private () =
    static member Register<'T when 'T :> obj>() = CustomIdRunner.Register<'T>()
