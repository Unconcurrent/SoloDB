namespace SoloDatabase

open System.Collections.Generic

/// Captures are ordered by their owning producer, independently of SQL dependencies.
type internal QueryValueBindingGroup = {
    Entries: ResizeArray<Choice<int, QueryValueBindingGroup>>
}

type internal QueryValueBindingOrder() =
    let root = { Entries = ResizeArray() }
    let mutable current = root
    member _.ReserveProjection() =
        let group = { Entries = ResizeArray() }
        current.Entries.Add(Choice2Of2 group)
        group
    member _.Within(group, action: unit -> 'Result) : 'Result =
        let previous = current
        current <- group
        try action()
        finally current <- previous
    member _.Add(index) = current.Entries.Add(Choice1Of2 index)
    member _.Indices() =
        let indices = ResizeArray<int>()
        let rec collect group =
            for entry in group.Entries do
                match entry with
                | Choice1Of2 index -> indices.Add index
                | Choice2Of2 child -> collect child
        collect root
        indices.ToArray()
    member _.Clear() =
        root.Entries.Clear()
        current <- root
