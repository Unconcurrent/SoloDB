namespace SoloDatabase

open System
open System.Linq.Expressions
open SqlDu.Engine.C1.Spec
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.ChainExpr
open SoloDatabase.ChainState


/// Turns a finalised extraction state into a query descriptor, including where a Count
/// predicate belongs and how a CountBy source is normalised.
module internal ChainDescriptorBuild =
    let normalizeCountBySourceCore (terminal: Terminal) (source: Expression) : Expression * LambdaExpression option =
        match terminal, source with
        | (Terminal.Count | Terminal.LongCount), (:? MethodCallExpression as srcMc) when srcMc.Method.Name = "CountBy" ->
            match getArg srcMc, getSource srcMc with
            | Some keyExpr, src when not (isNull src) ->
                match tryExtractLambdaExpression keyExpr with
                | ValueSome keyLambda -> src, Some keyLambda
                | ValueNone -> source, None
            | _ -> source, None
        | (Terminal.Any _, _) | (Terminal.All _, _) | (Terminal.Sum _, _) | (Terminal.SumProjected, _)
        | (Terminal.Min _, _) | (Terminal.MinProjected, _) | (Terminal.Max _, _) | (Terminal.MaxProjected, _)
        | (Terminal.Average _, _) | (Terminal.AverageProjected, _) | (Terminal.Select _, _) | (Terminal.Contains _, _)
        | (Terminal.Exists, _) | (Terminal.First _, _) | (Terminal.FirstOrDefault _, _)
        | (Terminal.Last _, _) | (Terminal.LastOrDefault _, _) | (Terminal.Single _, _) | (Terminal.SingleOrDefault _, _)
        | (Terminal.MinBy _, _) | (Terminal.MaxBy _, _) | (Terminal.DistinctBy _, _)
        | (Terminal.ElementAt _, _) | (Terminal.ElementAtOrDefault _, _) | (Terminal.CountBy _, _) -> source, None
        | (Terminal.Count, _) | (Terminal.LongCount, _) -> source, None

    let normalizeCountBySource (terminal: Terminal) (source: Expression) (state: ExtractionState) : Expression =
        let result, groupByKeyOpt = normalizeCountBySourceCore terminal source
        match groupByKeyOpt with Some k -> state.GroupByKey <- Some k | None -> ()
        result

    /// Place a count predicate into the appropriate where collection based on boundary state.
    let placeCountPredicateCore
        (seenBoundary: bool) (addWhere: Expression -> unit) (addPostBoundWhere: Expression -> unit)
        (countPredicate: Expression option) =
        match countPredicate with
        | Some pred when seenBoundary -> addPostBoundWhere pred
        | Some pred -> addWhere pred
        | None -> ()

    let placeCountPredicate (state: ExtractionState) (countPredicate: Expression option) =
        placeCountPredicateCore state.SeenBoundary state.Wheres.Add state.PostBoundWheres.Add countPredicate

    /// Centralised descriptor construction from a finalised ExtractionState.
    /// Callers declare only the intentional differences via parameters:
    ///   source                   — the chain's outer source expression
    ///   terminal                 — the resolved Terminal value
    ///   outerDistinct            — OR'd into Distinct alongside state.Distinct
    ///   selectManyInnerLambda    — caller override (None for non-SelectMany contexts)
    /// Every other field is read directly from state.
    let buildDescriptorFromState
        (source: Expression)
        (terminal: Terminal)
        (outerDistinct: bool)
        (selectManyInnerLambda: LambdaExpression option)
        (state: ExtractionState) : QueryDescriptor =
        {
            Source = source
            OfTypeName = state.OfTypeName
            CastTypeName = state.CastTypeName
            WherePredicates = state.Wheres |> Seq.toList
            SortKeys = state.SortKeys |> Seq.toList
            Limit = state.Limit
            Offset = state.Offset
            PostBoundWherePredicates = state.PostBoundWheres |> Seq.toList
            PostBoundSortKeys = state.PostBoundSortKeys |> Seq.toList
            PostBoundLimit = state.PostBoundLimit
            PostBoundOffset = state.PostBoundOffset
            TakeWhileInfo = state.TakeWhileInfo
            PostBoundTakeWhileInfo = state.PostBoundTakeWhileInfo
            GroupByKey = state.GroupByKey
            Distinct = state.Distinct || outerDistinct
            SelectProjection = state.SelectProjection
            SetOps = state.SetOps |> Seq.toList
            Terminal = terminal
            GroupByHavingPredicate = state.GroupByHaving
            DefaultIfEmpty = state.DefaultIfEmpty
            PostSelectDefaultIfEmpty = state.PostSelectDefaultIfEmpty
            SelectManyInnerLambda = selectManyInnerLambda
        }

    /// Evaluate a Take/Skip bound expression once at extract time and clamp
    /// the resulting value to the non-negative int64 range. Single source of
    /// truth for the dynamic-bound evaluation policy used by both GroupBy
    /// and GroupJoin chain extractors; callers wrap the int64 in their own
    /// emit-time SqlExpr literal as needed.
