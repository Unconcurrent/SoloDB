namespace SoloDatabase

open System
open System.Collections.Generic

/// Populates Many trackers from deferred hydration payloads after owner materialization.
module internal HydrationManyPopulator =
    open SoloDatabase.JsonSerializator
    open SoloDatabase.RelationsTypes

    let populateFromHydrationJson (ownerType: Type) (ownerEntities: (int64 * obj) array) (hydrationMap: Dictionary<int64, string>) =
        // The descriptor supplies cached tracker access, construction and target identity operations.
        let descriptor = HydrationSqlMetadata.getRelationDescriptor ownerType

        if descriptor.ManyCount = 0 then ()
        else

        for (ownerId, ownerObj) in ownerEntities do
            match hydrationMap.TryGetValue(ownerId) with
            | false, _ -> ()
            | true, hydrationJsonStr ->
                let mutable hydration = JsonReadContext(JsonLexer.fromUtf16Span(hydrationJsonStr.AsSpan()),{ Start = 0; Length = hydrationJsonStr.Length })
                JsonReadCursor.scan &hydration
                if not(isNull hydration.Properties) then
                    for manyIndex in 0 .. descriptor.ManyCount - 1 do
                        let relation = descriptor.Many manyIndex
                        let prop = relation.Property
                        let mutable array = JsonReadContext(hydration.Cursor,hydration.Range)
                        if JsonReadCursor.tryProperty &hydration prop.Name &array then
                            let items = ResizeArray<int64 * obj>()
                            JsonReadCursor.scan &array
                            if not(isNull array.Elements) then
                                for range in array.Elements do
                                    let mutable element = JsonReadCursor.child &array range
                                    JsonReadCursor.scan &element
                                    if not(isNull element.Properties) then
                                        let mutable identity = JsonReadContext(element.Cursor,element.Range)
                                        let mutable value = JsonReadContext(element.Cursor,element.Range)
                                        if JsonReadCursor.tryProperty &element "Id" &identity && JsonReadCursor.tryProperty &element "Value" &value then
                                            let id = JsonValue.ReadRawPhysicalId<Utf16Reader> &identity
                                            let target = JsonValue.ReadRawRuntime(&value,relation.TargetType)
                                            relation.TargetIdWriter.Invoke(target,id)
                                            items.Add(id,target)
                            let tracker = relation.TrackerGetter.Invoke(ownerObj)
                            match tracker with
                            | :? IDBRefManyInternal as references -> references.SetLoadedBoxed (items |> Seq.map snd) (items |> Seq.map fst)
                            | null ->
                                let instance = relation.TrackerCtor.Invoke()
                                relation.TrackerSetter.Invoke(ownerObj,instance)
                                (instance :?> IDBRefManyInternal).SetLoadedBoxed (items |> Seq.map snd) (items |> Seq.map fst)
                            | _ -> ()
