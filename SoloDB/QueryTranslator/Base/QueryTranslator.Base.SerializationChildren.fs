namespace SoloDatabase

open System
open Utils
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseHelpers

/// Original CLR, stored-label and positional identities select one writer child.
module internal QuerySerializationChildren =
    [<Struct>]
    type internal ChildSelection = {
        ValueType: Type
        Ordinal: int voption
        StoredLabel: obj
        ClrName: string voption
        Sequence: bool
        Rest: bool
    }

    let private child valueType ordinal label =
        ValueSome { ValueType = valueType; Ordinal = ordinal; StoredLabel = label; ClrName = ValueNone; Sequence = false; Rest = false }

    /// Resolve only the requested declared child, including its original access identity.
    let internal selectNamedChild count nameAt typeAt access =
        let name, requested =
            match access with
            | ClrMember(name, target) -> name, ValueSome target
            | StoredLabel(:? string as name) -> name, ValueNone
            | _ -> null, ValueNone
        let mutable result = ValueNone
        let mutable index = 0
        while result.IsNone && not(isNull name) && index < count do
            if nameAt index = name then
                let target: Type = typeAt index
                if requested |> ValueOption.forall (fun requested -> target.Equals requested) then
                    result <- child target (ValueSome index) (box name)
            index <- index + 1
        match result, requested with
        | ValueNone, ValueSome target -> child target ValueNone (box name)
        | _ -> result

    let internal selectPositionChild parent count typeAt access =
        let position index =
            if index >= 0 && index < count then child (typeAt index) (ValueSome index) (box index) else ValueNone
        match access with
        | StoredLabel(:? int as index) -> position index
        | ClrMember("Rest", target) when isTuple parent ->
            ValueSome { ValueType = target; Ordinal = ValueNone; StoredLabel = box "Rest"; ClrName = ValueSome "Rest"; Sequence = false; Rest = true }
        | ClrMember(name, target) when isTuple parent ->
            match JsonSerializator.JsonValue.SerializationShape parent with
            | JsonSerializator.JsonSerializationShape.ArrayMembers members ->
                members |> Array.tryFindIndex (fun memberInfo -> memberInfo.ClrName = name && memberInfo.ValueType.Equals target)
                |> ValueOption.ofOption |> ValueOption.bind position
            | _ -> ValueNone
        | _ -> ValueNone

    /// Member identity selection is shared by produced and actual-runtime writer inputs.
    let internal selectChild parent shape access =
        match shape, access with
        | JsonSerializator.JsonSerializationShape.Uninterpreted, StoredLabel label -> child typeof<JsonSerializator.JsonValue> ValueNone label
        | JsonSerializator.JsonSerializationShape.Uninterpreted, ClrMember(name, _) -> child typeof<JsonSerializator.JsonValue> ValueNone (box name)
        | JsonSerializator.JsonSerializationShape.ObjectMembers members, _ ->
            members |> Array.tryFindIndex (fun memberInfo ->
                match access with
                | ClrMember(name, valueType) -> memberInfo.ClrName = name && memberInfo.ValueType.Equals valueType
                | StoredLabel(:? string as name) -> memberInfo.StoredName = name
                | _ -> false)
            |> ValueOption.ofOption
            |> ValueOption.bind (fun index -> child members[index].ValueType (ValueSome index) (box members[index].StoredName)
                                             |> ValueOption.map (fun selected -> { selected with ClrName = ValueSome members[index].ClrName }))
        | JsonSerializator.JsonSerializationShape.DeclaredMembers members, _ ->
            selectNamedChild members.Length (fun index -> let struct(name, _) = members[index] in name)
                (fun index -> let struct(_, target) = members[index] in target) access
        | JsonSerializator.JsonSerializationShape.ArrayMembers members, ClrMember("Rest", _) when isTuple parent ->
            selectPositionChild parent members.Length (fun index -> members[index].ValueType) access
        | JsonSerializator.JsonSerializationShape.ArrayMembers members, ClrMember(name, valueType) ->
            members |> Array.tryFindIndex (fun memberInfo -> memberInfo.ClrName = name && memberInfo.ValueType.Equals valueType)
            |> ValueOption.ofOption
            |> ValueOption.bind (fun index -> child members[index].ValueType (ValueSome index) (box index))
        | JsonSerializator.JsonSerializationShape.DeclaredArrayMembers members, _ ->
            selectPositionChild parent members.Length (fun index -> members[index]) access
        | JsonSerializator.JsonSerializationShape.ArrayMembers members, StoredLabel(:? int as index) when index >= 0 && index < members.Length -> child members[index].ValueType (ValueSome index) (box index)
        | JsonSerializator.JsonSerializationShape.GroupingValues(key, _), ClrMember("Key", _) -> child key (ValueSome 0) (box "Key")
        | JsonSerializator.JsonSerializationShape.GroupingValues(key, _), StoredLabel(:? string as name) when name = "Key" -> child key (ValueSome 0) (box "Key")
        | JsonSerializator.JsonSerializationShape.GroupingValues(_, item), StoredLabel(:? string as name) when name = "Items" ->
            ValueSome { ValueType = item.MakeArrayType(); Ordinal = ValueSome 1; StoredLabel = box "Items"; ClrName = ValueNone; Sequence = true; Rest = false }
        | JsonSerializator.JsonSerializationShape.SequenceValues element, StoredLabel label
        | JsonSerializator.JsonSerializationShape.DictionaryValues element, StoredLabel label -> child element (ValueSome 0) label
        | _ -> ValueNone
