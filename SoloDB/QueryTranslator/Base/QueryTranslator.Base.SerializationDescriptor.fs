namespace SoloDatabase

open System
open SoloDatabase.QueryTranslatorSerialization
open SoloDatabase.QuerySerializationChildren
open SoloDatabase.QuerySerializationSql

/// Consumes authored writer descriptors at the existing typed callback boundaries.
module internal QuerySerializationDescriptor =
    /// The private operand parser delegates member identity to the same shape owner.
    let internal resolveInputDescriptor descriptor =
        let list (value: JsonSerializator.JsonValue) = JsonSerializator.JsonValue.ArrayItems value
        let text = function
            | JsonSerializator.JsonValue.String value -> value
            | JsonSerializator.JsonValue.Null -> null
            | _ -> raise(FormatException("The writer input requires a text metadata value."))
        let kind = function
            | JsonSerializator.JsonValue.Number value -> int value
            | _ -> raise(FormatException("The writer input requires an authored metadata operation."))
        let actualType declared header =
            let fields = list header
            JsonSerializator.JsonValue.ResolveWriterType(declared, kind fields[0] <> 0, fields[1])
        let rec resolve descriptor =
            let node = list descriptor
            match kind node[0] with
            | 0 ->
                let declared = TypeMetadata.nameToType(text node[1])
                struct(actualType declared node[2], ValueNone)
            | 3 -> resolve node[1]
            | 2 ->
                let struct(target, _) = resolve node[1]
                struct(target, ValueSome node[2])
            | 1 ->
                let struct(parent, produced) = resolve node[1]
                let step = list node[2]
                let access =
                    match kind step[0] with
                    | 0 -> StoredLabel(match step[1] with
                                       | JsonSerializator.JsonValue.Number index -> box(int index)
                                       | value -> box(text value))
                    | 1 -> ClrMember(text step[1], TypeMetadata.nameToType(text step[2]))
                    | _ -> raise(FormatException("The writer input requires an authored member or position."))
                let selected, children =
                    match produced with
                    | ValueSome(JsonSerializator.JsonValue.List fields) ->
                        match kind fields[0] with
                        | 0 ->
                            let entries = list fields[1]
                            let entry index = list entries[index]
                            selectNamedChild entries.Count (fun index -> text (entry index).[0])
                                (fun index -> TypeMetadata.nameToType(text (entry index).[1])) access,
                            ValueSome(entries, 2)
                        | 1 ->
                            let entries = list fields[1]
                            selectPositionChild parent entries.Count
                                (fun index -> TypeMetadata.nameToType(text (list entries[index]).[0])) access,
                            ValueSome(entries, 1)
                        | 2 ->
                            let target = TypeMetadata.nameToType(text fields[1])
                            selectChild parent (JsonSerializator.JsonSerializationShape.SequenceValues target) access,
                            ValueSome(list (JsonSerializator.JsonValue.List [|JsonSerializator.JsonValue.List fields|]), 2)
                        | _ -> raise(FormatException("The writer input requires an authored child layout."))
                    | _ -> selectChild parent (JsonSerializator.JsonValue.SerializedValueFacts(Utils.optionalPayloadType parent)) access, ValueNone
                let struct(target, next) =
                    match selected, children with
                    | ValueSome selected, ValueSome(entries, _) when selected.Rest && entries.Count > 7 ->
                        let fields = JsonSerializator.JsonValue.List
                                        [|JsonSerializator.JsonValue.Number 1M
                                          JsonSerializator.JsonValue.List [|for index in 7 .. entries.Count - 1 -> entries[index]|]|]
                        struct(selected.ValueType, ValueSome fields)
                    | ValueSome selected, ValueSome(entries, childPosition) when selected.Ordinal.IsSome ->
                        let struct(target, next) = resolve (list entries[selected.Ordinal.Value]).[childPosition]
                        if selected.Sequence then struct(selected.ValueType, ValueNone) else struct(target, next)
                    | ValueSome selected, _ -> struct(selected.ValueType, ValueNone)
                    | _ -> struct(typeof<JsonSerializator.JsonValue>, ValueNone)
                let target =
                    match JsonSerializator.JsonValue.SerializedValueFacts(Utils.optionalPayloadType target) with
                    | JsonSerializator.JsonSerializationShape.Uninterpreted -> target
                    | _ -> actualType (Utils.optionalPayloadType target) step[3]
                struct(target, next)
            | _ -> raise(FormatException("The writer input requires an authored metadata node."))
        let struct(target, _) = resolve descriptor
        target
