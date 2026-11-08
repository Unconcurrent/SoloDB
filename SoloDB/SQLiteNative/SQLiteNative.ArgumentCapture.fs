namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic

/// The borrowed statement owns one bounded capture buffer. No getter runs during later statement binding.
[<Sealed>]
type internal NativeArgumentCapture() =
    let mutable layout = NativeArgumentSources.empty
    let mutable values: obj[] = Array.empty
    let mutable count = 0
    let dynamicNames = ResizeArray<string>()
    let ensure capacity =
        if values.Length < capacity then
            let expanded = Array.zeroCreate (max capacity (max 4 (values.Length * 2)))
            Array.Copy(values, expanded, count)
            values <- expanded
    let sameNames (names: ResizeArray<string>) =
        let previous = layout.Names
        let mutable same = previous.Length = names.Count
        let mutable i = 0
        while same && i < previous.Length do
            same <- previous.[i] = names.[i]
            i <- i + 1
        same
    let add name value =
        if String.IsNullOrEmpty(name) then invalidArg "parameters" "SQL argument names must be nonempty."
        ensure (count + 1)
        dynamicNames.Add(name)
        values.[count] <- value
        count <- count + 1
    member _.Layout = layout
    member _.Value(index: int) = values.[index]
    member _.Count = count
    member _.Clear() =
        Array.Clear(values, 0, count)
        count <- 0
    member this.Capture(source: obj) =
        this.Clear()
        dynamicNames.Clear()
        try
            match source with
            | null -> layout <- NativeArgumentSources.empty
            | :? ParameterValues as packet ->
                for pair in packet.Constants do add pair.Key pair.Value
                for i = 0 to packet.Names.Length - 1 do add packet.Names.[i] packet.Values.[packet.Offset + i]
                if not (sameNames dynamicNames) then layout <- NativeArgumentLayout(dynamicNames.ToArray())
            | :? SqlArguments as packet ->
                for i = 0 to packet.Names.Length - 1 do add packet.Names.[i] packet.Values.[i]
                if not (sameNames dynamicNames) then layout <- NativeArgumentLayout(dynamicNames.ToArray())
            | :? IDictionary<string, obj> as dictionary ->
                for KeyValue(name, value) in dictionary do add name value
                if not (sameNames dynamicNames) then layout <- NativeArgumentLayout(dynamicNames.ToArray())
            | :? IDictionary as dictionary ->
                for entry in dictionary do
                    let pair = unbox<DictionaryEntry> entry
                    add (pair.Key :?> string) pair.Value
                if not (sameNames dynamicNames) then layout <- NativeArgumentLayout(dynamicNames.ToArray())
            | source ->
                let struct (descriptor, capture) = NativeArgumentSources.forType(source.GetType())
                layout <- descriptor
                ensure descriptor.Names.Length
                count <- descriptor.Names.Length
                capture.Invoke(source, values)
        with _ -> this.Clear(); reraise()

    interface INativeArgumentBinding with
        member _.Layout = layout
        member _.Bind(target) =
            for i = 0 to count - 1 do
                let ordinal = target.Ordinal(i, target.Session)
                if ordinal > 0 then target.Check(NativeParameterBinding.bind target.Statement ordinal values.[i])
        member this.Clear() = this.Clear()
