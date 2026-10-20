namespace SoloDatabase

open System
open System.Collections.Generic

/// A sufficient finite-height proof over original member signatures, never closed-instance sampling.
module internal QueryStructuralClosure =
    type private Rule = { Fields: ModelTypeTemplate[]; Exposed: bool[]; FirstSlot: int }

    let finite (resolve: Type -> ModelTypeTemplate[] voption) (seed: ModelTypeTemplate) =
        let indices = Dictionary<Type,int>()
        let rules = ResizeArray<Rule>()
        let mutable slots = 0
        let mutable known = true
        let mutable changed = true
        let rule owner =
            match indices.TryGetValue owner with
            | true,index -> rules[index]
            | _ ->
                let fields = match resolve owner with ValueSome fields -> fields | ValueNone -> known <- false; [||]
                let count = if owner.IsGenericType then owner.GetGenericArguments().Length else 0
                let result = { Fields = fields; Exposed = Array.zeroCreate count; FirstSlot = slots }
                indices.Add(owner,rules.Count)
                rules.Add result
                slots <- slots + count
                changed <- true
                result
        let rec expose template accept =
            match template with
            | UnavailableType -> known <- false
            | FormalType index -> accept index
            | ElementType _ -> known <- false
            | NamedType(owner,arguments) ->
                let current = rule owner
                for index = 0 to current.Exposed.Length - 1 do
                    if current.Exposed[index] then expose arguments[index] accept
        // Only heads and exposed argument constructors become component owners.
        // This also discovers closed constant heads that have no formal occurrences.
        while known && changed do
            changed <- false
            expose seed ignore
            let mutable index = 0
            while known && index < rules.Count do
                let current = rules[index]
                for field in current.Fields do
                    expose field (fun slot ->
                        if not current.Exposed[slot] then
                            current.Exposed[slot] <- true
                            changed <- true)
                index <- index + 1
        if not known then false else
        let edges = Array.init slots (fun _ -> HashSet<struct(int * bool)>())
        let rec occurrences source destination nested template =
            match template with
            | UnavailableType -> known <- false
            | FormalType index -> edges[source + index].Add(struct(destination,nested)) |> ignore
            | NamedType(_,arguments) ->
                for argument in arguments do occurrences source destination true argument
            | ElementType(_,element) -> occurrences source destination true element
        let rec flow source template =
            match template with
            | NamedType(owner,arguments) ->
                let current = rules[indices[owner]]
                // All arguments contribute to closed identity, including unexposed slots.
                for index = 0 to arguments.Length - 1 do
                    occurrences source (current.FirstSlot + index) false arguments[index]
                    if current.Exposed[index] then flow source arguments[index]
            | _ -> ()
        for current in rules do
            for field in current.Fields do flow current.FirstSlot field
        if not known then false else
        let numbers = Array.create slots -1
        let low = Array.zeroCreate slots
        let components = Array.create slots -1
        let active = Array.zeroCreate slots
        let pending = Stack<int>()
        let mutable number = 0
        let mutable componentIndex = 0
        let rec connect vertex =
            numbers[vertex] <- number
            low[vertex] <- number
            number <- number + 1
            pending.Push vertex
            active[vertex] <- true
            for struct(target,_) in edges[vertex] do
                if numbers[target] = -1 then
                    connect target
                    low[vertex] <- min low[vertex] low[target]
                elif active[target] then low[vertex] <- min low[vertex] numbers[target]
            if low[vertex] = numbers[vertex] then
                let mutable finished = false
                while not finished do
                    let target = pending.Pop()
                    active[target] <- false
                    components[target] <- componentIndex
                    finished <- target = vertex
                componentIndex <- componentIndex + 1
        for vertex = 0 to slots - 1 do
            if numbers[vertex] = -1 then connect vertex
        let mutable bounded = true
        for vertex = 0 to slots - 1 do
            for struct(target,positive) in edges[vertex] do
                if positive && components[vertex] = components[target] then bounded <- false
        // With no positive SCC, constructor height grows only along an acyclic
        // condensation path. Seeds and constant fragments have finite height;
        // projection creates no constructors. A finite alphabet at bounded height
        // therefore gives finitely many closed Types. A positive SCC is unknown.
        bounded
