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
            let generated = match TypeMetadataCache.Get(typeof<'t>).CustomIdentity with ValueSome operations -> operations.Generated | _ -> false
            if generated && (typeof<bool>.Equals(memberValue.IdType) || typeof<nativeint>.Equals(memberValue.IdType) || typeof<unativeint>.Equals(memberValue.IdType)) then
                raise (InvalidOperationException($"Error: Invalid [<SoloId>] type on '{typeof<'t>.FullName}.{memberValue.Name}'.\nReason: '{memberValue.IdType.Name}' has a value space too small or too platform-dependent to serve as a stable identity key (bool: 2 values; IntPtr/UIntPtr: width depends on runtime).\nFix: Use a wider type — int64, Guid, string, or a custom struct."))
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