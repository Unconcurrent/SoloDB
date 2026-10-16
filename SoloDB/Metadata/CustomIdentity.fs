namespace SoloDatabase

open System
open SoloDatabase
open SoloDatabase.Attributes


[<AbstractClass; Sealed>]
type internal CustomTypeId<'t> =
    static member internal Value =
        match CustomIdMetadata.GetMembers typeof<'t> with
        | [||] -> None
        | members ->
            let memberValue = members.[0]
            let generator = memberValue.Generator
            match generator with
            | :? IIdGenerator | :? IIdGenerator<'t> -> ()
            | _ -> raise (InvalidOperationException($"Generator type for Id property ({memberValue.Name}) does not implement the IIdGenerator or IIdGenerator<'T> interface."))
            Some {|
                Generator = generator
                SetId = fun id owner -> memberValue.Write.Invoke(owner,id)
                GetId = fun owner -> memberValue.Read.Invoke owner
                IdType = memberValue.IdType
                Member = memberValue
            |}

    /// Preserves the actionable generator diagnostic when a dependency fails initialization.
    static member internal Get () : _ =
        try
            CustomTypeId<'t>.Value
        with
        | :? System.TypeInitializationException as tie when not (Utils.isNull tie.InnerException) ->
            raise tie.InnerException