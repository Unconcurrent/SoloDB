namespace SoloDatabase

open System
open System.Linq.Expressions
open SqlDu.Engine.C1.Spec
open DBRefTypeHelpers
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorVisitPost

/// Unified extraction: walks a DBRefMany expression tree from terminal inward,
/// collecting ALL operators into a QueryDescriptor regardless of order.
module internal DBRefManyExtractor =

    /// Extract the source expression from a MethodCallExpression.
    let private getSource (mce: MethodCallExpression) =
        if not (isNull mce.Object) then mce.Object
        elif mce.Arguments.Count > 0 then mce.Arguments.[0]
        else null

    /// Extract the first argument (predicate/selector/value) from a MethodCallExpression.
    let private getArg (mce: MethodCallExpression) =
        if not (isNull mce.Object) then
            if mce.Arguments.Count >= 1 then Some mce.Arguments.[0] else None
        elif mce.Arguments.Count >= 2 then Some mce.Arguments.[1]
        else None

    /// Check if an expression is a DBRefMany type or wraps one via OfType/OrderBy/Where etc.
    let rec private isDBRefManyChain (expr: Expression) : bool =
        let e = unwrapConvert expr
        if isDBRefManyType e.Type then true
        else
            match e with
            | :? MethodCallExpression as mc ->
                let src = getSource mc
                not (isNull src) && isDBRefManyChain src
            | _ -> false

    /// Try to extract a terminal + operator chain from an expression.
    /// Returns None if the expression is not a DBRefMany chain.
    let tryExtract (expr: Expression) : QueryDescriptor voption =
        // First, identify the terminal and peel it.
        // Pre-peel Distinct, ToList, ToArray from the outermost expression.
        let mutable outerDistinct = false
        let rec preProcess (e: Expression) =
            match e with
            | :? MethodCallExpression as mc when mc.Method.Name = "Distinct" ->
                outerDistinct <- true
                let src = getSource mc
                if isNull src then e else preProcess src
            | :? MethodCallExpression as mc when mc.Method.Name = "ToList" || mc.Method.Name = "ToArray" ->
                let src = getSource mc
                if isNull src then e else preProcess src
            | _ -> e
        let expr = preProcess expr

        match expr with
        | :? MethodCallExpression as mce ->
            let source = getSource mce
            if isNull source then ValueNone
            else

            // Determine terminal type.
            let terminalOpt =
                match mce.Method.Name with
                | "Any" ->
                    match getArg mce with
                    | Some pred -> Some (Terminal.Any(Some pred))
                    | None -> Some Terminal.Exists
                | "All" ->
                    match getArg mce with
                    | Some pred -> Some (Terminal.All pred)
                    | None -> Some Terminal.Exists // All() without pred = trivially true
                | "Count" | "LongCount" ->
                    let t = if mce.Method.Name = "Count" then Terminal.Count else Terminal.LongCount
                    Some t
                | "Sum" -> getArg mce |> Option.map Terminal.Sum
                | "Min" -> getArg mce |> Option.map Terminal.Min
                | "Max" -> getArg mce |> Option.map Terminal.Max
                | "Average" -> getArg mce |> Option.map Terminal.Average
                | "Contains" -> getArg mce |> Option.map Terminal.Contains
                | "Select" ->
                    // Select as terminal: materializes DBRefMany to json_group_array.
                    getArg mce |> Option.map Terminal.Select
                | _ -> None

            match terminalOpt with
            | None -> ValueNone
            | Some terminal ->

            // Check if the source chain involves DBRefMany.
            if not (isDBRefManyChain source) then ValueNone
            else

            // Walk the source chain inward, collecting operators.
            let mutable wheres = ResizeArray<Expression>()
            let mutable sortKeys = ResizeArray<Expression * SortDirection>()
            let mutable limit: Expression option = None
            let mutable offset: Expression option = None
            let mutable takeWhileInfo: (LambdaExpression * bool) option = None
            let mutable groupByKey: LambdaExpression option = None
            let mutable distinct = false
            let mutable selectProj: LambdaExpression option = None
            let mutable setOp: SetOperation option = None
            let mutable ofTypeName: string option = None
            let mutable groupByHaving: Expression option = None

            let rec walkChain (e: Expression) : Expression =
                let e = unwrapConvert e
                match e with
                | :? MethodCallExpression as mc ->
                    let src = getSource mc
                    let arg = getArg mc

                    match mc.Method.Name with
                    | "Where" ->
                        match arg with
                        | Some pred -> wheres.Add(pred)
                        | None -> ()
                        walkChain src

                    | "OrderBy" | "OrderByDescending" ->
                        let dir = if mc.Method.Name = "OrderBy" then SortDirection.Asc else SortDirection.Desc
                        match arg with
                        | Some key ->
                            // OrderBy is the primary sort key. Since we walk outermost-first,
                            // ThenBy keys are already in the list. Insert OrderBy at position 0.
                            sortKeys.Insert(0, (key, dir))
                        | None -> ()
                        walkChain src

                    | "ThenBy" | "ThenByDescending" ->
                        let dir = if mc.Method.Name = "ThenBy" then SortDirection.Asc else SortDirection.Desc
                        match arg with
                        | Some key -> sortKeys.Add(key, dir)
                        | None -> ()
                        walkChain src

                    | "Take" ->
                        limit <- arg
                        walkChain src

                    | "Skip" ->
                        offset <- arg
                        walkChain src

                    | "TakeWhile" | "SkipWhile" ->
                        match arg with
                        | Some pred ->
                            match tryExtractLambdaExpression pred with
                            | ValueSome lambda ->
                                takeWhileInfo <- Some (lambda, mc.Method.Name = "TakeWhile")
                            | ValueNone -> ()
                        | None -> ()
                        walkChain src

                    | "Select" ->
                        match arg with
                        | Some proj ->
                            match tryExtractLambdaExpression proj with
                            | ValueSome lambda -> selectProj <- Some lambda
                            | ValueNone -> ()
                        | None -> ()
                        walkChain src

                    | "Distinct" ->
                        distinct <- true
                        walkChain src

                    | "GroupBy" ->
                        match arg with
                        | Some key ->
                            match tryExtractLambdaExpression key with
                            | ValueSome lambda -> groupByKey <- Some lambda
                            | ValueNone -> ()
                        | None -> ()
                        walkChain src

                    | "OfType" ->
                        let genericArgs = mc.Method.GetGenericArguments()
                        if genericArgs.Length = 1 then
                            match Utils.typeToName genericArgs.[0] with
                            | Some tn -> ofTypeName <- Some tn
                            | None -> ()
                        walkChain src

                    | "Intersect" ->
                        match arg with
                        | Some rightSrc -> setOp <- Some (SetOperation.Intersect rightSrc)
                        | None -> ()
                        walkChain src

                    | "Except" ->
                        match arg with
                        | Some rightSrc -> setOp <- Some (SetOperation.Except rightSrc)
                        | None -> ()
                        walkChain src

                    | "Union" ->
                        match arg with
                        | Some rightSrc -> setOp <- Some (SetOperation.Union rightSrc)
                        | None -> ()
                        walkChain src

                    | "Concat" ->
                        match arg with
                        | Some rightSrc -> setOp <- Some (SetOperation.Concat rightSrc)
                        | None -> ()
                        walkChain src

                    | "ToList" | "ToArray" ->
                        // Identity passthrough (L11).
                        walkChain src

                    | _ ->
                        // Unknown operator — stop walking, return current expression as source.
                        e
                | _ ->
                    // Not a method call — this should be the DBRefMany source (member access).
                    e

            let innerSource = walkChain source

            // Handle GroupBy terminal: extract the HAVING predicate from Any/All.
            let finalTerminal, finalGroupByHaving =
                match groupByKey with
                | Some _ ->
                    match terminal with
                    | Terminal.Any(Some pred) -> Terminal.Exists, Some pred
                    | Terminal.All pred -> Terminal.All pred, Some pred  // Keep All for negation in builder
                    | _ -> terminal, None
                | None -> terminal, None

            // If Select is the terminal, capture its projection.
            let finalSelectProj =
                match finalTerminal with
                | Terminal.Select projExpr when selectProj.IsNone ->
                    match tryExtractLambdaExpression projExpr with
                    | ValueSome lambda -> Some lambda
                    | ValueNone -> selectProj
                | _ -> selectProj

            ValueSome {
                Source = innerSource
                OfTypeName = ofTypeName
                WherePredicates = wheres |> Seq.toList
                SortKeys = sortKeys |> Seq.toList
                Limit = limit
                Offset = offset
                TakeWhileInfo = takeWhileInfo
                GroupByKey = groupByKey
                Distinct = distinct || outerDistinct
                SelectProjection = finalSelectProj
                SetOp = setOp
                Terminal = finalTerminal
                GroupByHavingPredicate = finalGroupByHaving
            }

        // Non-MethodCall expressions (MemberExpression for .Count property, etc.)
        | :? MemberExpression as me when me.Member.Name = "Count" && not (isNull me.Expression) ->
            let inner = unwrapConvert me.Expression
            if isDBRefManyType inner.Type then
                ValueSome {
                    Source = inner
                    OfTypeName = None; WherePredicates = []; SortKeys = []
                    Limit = None; Offset = None; TakeWhileInfo = None
                    GroupByKey = None; Distinct = false; SelectProjection = None
                    SetOp = None; Terminal = Terminal.Count
                    GroupByHavingPredicate = None
                }
            else ValueNone

        | _ -> ValueNone
