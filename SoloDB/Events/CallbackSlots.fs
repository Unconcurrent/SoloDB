namespace SoloDatabase

open System

/// Serialized callbacks retain a separate value for each active nesting level.
type internal CallbackSlots<'T>(create: unit -> 'T) =
    let slots = ResizeArray<'T>()
    let mutable depth = 0

    member _.Borrow() =
        if depth = slots.Count then slots.Add(create())
        let value = slots.[depth]
        depth <- depth + 1
        value

    member _.Return() =
        if depth <= 0 then raise (InvalidOperationException("Callback slot underflow."))
        depth <- depth - 1
