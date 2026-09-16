namespace SoloDatabase
open System
open System.Collections
open System.Collections.Generic
open System.Linq.Expressions
open System.Threading
open Utils
open SoloDatabase
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.SqlModel
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.ChainExpr
open SoloDatabase.ChainPolicy
open SoloDatabase.ChainState
open SoloDatabase.ChainWalk
open SoloDatabase.GroupJoinRuntimeTypes
open SoloDatabase.ChainDescriptorBuild

/// GroupJoin's chain-extraction policy and descriptor recognition.
///
/// The policy value lives here rather than in a shared module: GroupJoin rejects unsupported
/// OfType and admits two TakeWhile boundaries, where GroupBy does neither. Merging the two
/// configurations would widen or narrow what each accepts.
module internal GroupJoinExtract =
    let extractorConfig =
        {
            EnsureOfTypeSupported = DBRefManyHelpers.ensureOfTypeSupported
            MultipleTakeSkipBoundariesMessage =
                "Error: Multiple Take/Skip boundaries in GroupJoin chain are not supported.\n" +
                "Reason: The current descriptor admits one semantic pagination boundary.\n" +
                "Fix: Keep at most one Take or Skip inside the GroupJoin group chain."
            TooManyTakeWhileBoundariesMessage =
                "Error: More than two TakeWhile/SkipWhile boundaries are not supported in GroupJoin chain.\n" +
                "Reason: The current descriptor supports one inner and one outer boundary.\n" +
                "Fix: Simplify the GroupJoin group chain or move additional windowing after AsEnumerable()."
        }

    let tryExtractGroupQueryDescriptor (rt: GroupJoinRuntime) (expr: Expression) : QueryDescriptor option =
        let expr, outerDistinct, _outerMaterialize = preprocessRoot expr
        match expr with
        | :? MethodCallExpression as mce ->
            match tryRecognizeTerminal mce with
            | None -> None
            | Some recognized ->
                let isGroupRoot (e: Expression) =
                    match unwrapConvert e with
                    | :? ParameterExpression as p -> Object.ReferenceEquals(p, rt.GroupParam)
                    | _ -> false
                if not (isRootedChain unwrapConvert isGroupRoot recognized.Source) then None
                else
                    let state = createState ()
                    let source = normalizeCountBySource recognized.Terminal recognized.Source state
                    let innerSource = walkChain extractorConfig state source
                    finalizeState state
                    placeCountPredicate state recognized.CountPredicate
                    match unwrapConvert innerSource with
                    | :? ParameterExpression as p when Object.ReferenceEquals(p, rt.GroupParam) ->
                        Some (buildDescriptorFromState
                                (Expression.Constant(null) :> Expression)
                                recognized.Terminal
                                outerDistinct
                                state.SelectManyLambda
                                state)
                    | _ -> None
        | _ -> None

    /// Extract a non-terminal source chain (g.Where().OrderBy().Select()) as a QueryDescriptor.
    /// Uses shared walkChain for non-terminal group source chains.
    let tryExtractGroupSourceDescriptor (rt: GroupJoinRuntime) (expr: Expression) : QueryDescriptor option =
        let isGroupRoot (e: Expression) =
            match unwrapConvert e with
            | :? ParameterExpression as p -> Object.ReferenceEquals(p, rt.GroupParam)
            | _ -> false
        if not (isRootedChain unwrapConvert isGroupRoot expr) then None
        else
            let state = createState ()
            let innerSource = walkChain extractorConfig state expr
            finalizeState state
            match unwrapConvert innerSource with
            | :? ParameterExpression as p when Object.ReferenceEquals(p, rt.GroupParam) ->
                // Fail-closed guard: reject operators that shared walkChain admits
                // but GroupJoin rowset builders do not consume.
                if state.OfTypeName.IsSome
                   || state.CastTypeName.IsSome
                   || state.TakeWhileInfo.IsSome
                   || state.PostBoundTakeWhileInfo.IsSome
                   || state.GroupByKey.IsSome
                   || state.GroupByHaving.IsSome
                   || not (state.SetOps |> Seq.isEmpty)
                   || state.SelectManyLambda.IsSome
                   || not (state.PostBoundWheres |> Seq.isEmpty)
                   || not (state.PostBoundSortKeys |> Seq.isEmpty)
                   || state.PostBoundLimit.IsSome
                   || state.PostBoundOffset.IsSome then
                    None
                else
                // Intentional zero-out shape — does NOT use buildDescriptorFromState
                // because the helper propagates 8 of these fields from state, and
                // this extractor explicitly nulls them as a fail-closed guard. A
                // zero-out-subset parameter on the helper would obscure the intent.
                Some {
                    Source = Expression.Constant(null) :> Expression
                    OfTypeName = None
                    CastTypeName = None
                    WherePredicates = state.Wheres |> Seq.toList
                    SortKeys = state.SortKeys |> Seq.toList
                    Limit = state.Limit
                    Offset = state.Offset
                    PostBoundWherePredicates = state.PostBoundWheres |> Seq.toList
                    PostBoundSortKeys = state.PostBoundSortKeys |> Seq.toList
                    PostBoundLimit = state.PostBoundLimit
                    PostBoundOffset = state.PostBoundOffset
                    TakeWhileInfo = None
                    PostBoundTakeWhileInfo = None
                    GroupByKey = None
                    Distinct = state.Distinct
                    SelectProjection = state.SelectProjection
                    SetOps = []
                    Terminal = Terminal.Count
                    GroupByHavingPredicate = None
                    DefaultIfEmpty = state.DefaultIfEmpty
                    PostSelectDefaultIfEmpty = state.PostSelectDefaultIfEmpty
                    SelectManyInnerLambda = None
                }
            | _ -> None

    let tryGetGroupChainDescriptor (rt: GroupJoinRuntime) (expr: Expression) : QueryDescriptor option =
        match tryExtractGroupQueryDescriptor rt expr with
        | Some desc when hasQueryDescriptorChainOps desc -> Some desc
        | _ ->
        match tryExtractGroupSourceDescriptor rt expr with
        | Some desc when hasQueryDescriptorChainOps desc -> Some desc
        | _ -> None

    let tryExtractGroupTerminalChain (rt: GroupJoinRuntime) (expr: Expression) : (QueryDescriptor * Terminal) option =
        let expr, outerDistinct, _outerMaterialize = preprocessRoot expr
        match expr with
        | :? MethodCallExpression as mce ->
            match tryRecognizeTerminal mce with
            | None -> None
            | Some recognized ->
                let isGroupRoot (e: Expression) =
                    match unwrapConvert e with
                    | :? ParameterExpression as p -> Object.ReferenceEquals(p, rt.GroupParam)
                    | _ -> false
                if not (isRootedChain unwrapConvert isGroupRoot recognized.Source) then None
                else
                    let state = createState ()
                    let source = normalizeCountBySource recognized.Terminal recognized.Source state
                    let innerSource = walkChain extractorConfig state source
                    finalizeState state
                    placeCountPredicate state recognized.CountPredicate
                    let desc =
                        buildDescriptorFromState innerSource recognized.Terminal outerDistinct None state
                    match unwrapConvert innerSource with
                    | :? ParameterExpression as p when Object.ReferenceEquals(p, rt.GroupParam) ->
                        Some (desc, recognized.Terminal)
                    | _ -> None
        | _ -> None
