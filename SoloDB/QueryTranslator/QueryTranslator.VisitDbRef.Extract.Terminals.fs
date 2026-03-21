namespace SoloDatabase

open System.Linq.Expressions
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.DBRefManyExtractorHelpers

module internal DBRefManyExtractTerminals =
    type RecognizedTerminal =
        {
            Terminal: Terminal
            Source: Expression
            CountPredicate: Expression option
        }

    let tryRecognizeTerminal (mce: MethodCallExpression) : RecognizedTerminal option =
        let source = getSource mce
        if isNull source then
            None
        else
            let mutable countPredicate: Expression option = None
            let terminalOpt =
                match mce.Method.Name with
                | "Any" ->
                    match getArg mce with
                    | Some pred -> Some (Terminal.Any(Some pred))
                    | None -> Some Terminal.Exists
                | "All" ->
                    match getArg mce with
                    | Some pred -> Some (Terminal.All pred)
                    | None -> Some Terminal.Exists
                | "Count" | "LongCount" ->
                    let t = if mce.Method.Name = "Count" then Terminal.Count else Terminal.LongCount
                    countPredicate <- getArg mce
                    Some t
                | "Sum" ->
                    match getArg mce with
                    | Some sel -> Some (Terminal.Sum sel)
                    | None -> Some Terminal.SumProjected
                | "Min" ->
                    match getArg mce with
                    | Some sel -> Some (Terminal.Min sel)
                    | None -> Some Terminal.MinProjected
                | "Max" ->
                    match getArg mce with
                    | Some sel -> Some (Terminal.Max sel)
                    | None -> Some Terminal.MaxProjected
                | "Average" ->
                    match getArg mce with
                    | Some sel -> Some (Terminal.Average sel)
                    | None -> Some Terminal.AverageProjected
                | "Contains" -> getArg mce |> Option.map Terminal.Contains
                | "Select" -> getArg mce |> Option.map Terminal.Select
                | "First" -> Some (Terminal.First(getArg mce))
                | "FirstOrDefault" -> Some (Terminal.FirstOrDefault(getArg mce))
                | "Last" -> Some (Terminal.Last(getArg mce))
                | "LastOrDefault" -> Some (Terminal.LastOrDefault(getArg mce))
                | "Single" -> Some (Terminal.Single(getArg mce))
                | "SingleOrDefault" -> Some (Terminal.SingleOrDefault(getArg mce))
                | "ElementAt" -> getArg mce |> Option.map Terminal.ElementAt
                | "ElementAtOrDefault" -> getArg mce |> Option.map Terminal.ElementAtOrDefault
                | "MinBy" -> getArg mce |> Option.map Terminal.MinBy
                | "MaxBy" -> getArg mce |> Option.map Terminal.MaxBy
                | "DistinctBy" -> getArg mce |> Option.map Terminal.DistinctBy
                | "Order" | "OrderDescending" | "UnionBy" | "IntersectBy" | "ExceptBy" | "DefaultIfEmpty" | "Cast" ->
                    let identityLambda = mkIdentityLambdaForDbRefMany mce
                    Some (Terminal.Select(identityLambda :> Expression))
                | "CountBy" -> getArg mce |> Option.map Terminal.CountBy
                | _ -> None

            terminalOpt
            |> Option.map (fun terminal ->
                let source =
                    match mce.Method.Name with
                    | "DistinctBy" | "Order" | "OrderDescending" | "UnionBy" | "IntersectBy" | "ExceptBy" | "DefaultIfEmpty" | "Cast" ->
                        mce :> Expression
                    | _ -> source
                {
                    Terminal = terminal
                    Source = source
                    CountPredicate = countPredicate
                })
