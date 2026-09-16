namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Linq.Expressions
open System.Threading
open SoloDatabase.SqlModel
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryableHelperJoin
open SoloDatabase.ChainDescriptorBuild
open SoloDatabase.ChainPolicy
open SoloDatabase.ChainState
open SoloDatabase.ChainExpr
open SoloDatabase.ChainWalk

/// GroupBy chained-expression support — correlated subqueries for
/// group-item chains that go beyond simple aggregate whitelist.
/// Uses shared Terminal DU + walkChain extraction with per-context GroupBy building.

/// GroupBy's chain-extraction policy and terminal recognition.
///
/// The policy value lives here rather than in a shared module because GroupBy and GroupJoin
/// deliberately admit different operators and report different messages; merging them would
/// widen or narrow what each accepts.
module internal GroupByExtract =
    let extractorConfig : ExtractorConfig = {
        EnsureOfTypeSupported = fun _ -> ()
        MultipleTakeSkipBoundariesMessage =
            "Error: Multiple Take/Skip boundaries in GroupBy chain are not supported.\nFix: Simplify the chain or call AsEnumerable() before the GroupBy."
        TooManyTakeWhileBoundariesMessage =
            "Error: Too many TakeWhile/SkipWhile boundaries in GroupBy chain are not supported.\nFix: Simplify the chain or call AsEnumerable() before the GroupBy."
    }

    let buildDescriptor (source: Expression) (outerDistinct: bool) (state: ExtractionState) (terminal: Terminal) : QueryDescriptor =
        buildDescriptorFromState source terminal outerDistinct None state

    let tryExtractGroupByQueryDescriptor (groupParam: ParameterExpression) (expr: Expression) : QueryDescriptor option =
        let expr, outerDistinct, _ = preprocessRoot expr
        let isGroupRoot (e: Expression) =
            match unwrapConvert e with
            | :? ParameterExpression as p -> Object.ReferenceEquals(p, groupParam)
            | _ -> false
        if not (isRootedChain unwrapConvert isGroupRoot expr) then
            None
        else
            let state = createState ()
            let _innerSource = walkChain extractorConfig state expr
            finalizeState state
            Some (buildDescriptor expr outerDistinct state (Terminal.Select expr))

    /// Try to extract a chained group-item expression as a QueryDescriptor + Terminal.
    let tryExtractGroupByTerminalChain (groupParam: ParameterExpression) (expr: Expression) : (QueryDescriptor * Terminal) option =
        let expr, outerDistinct, _ = preprocessRoot expr
        match expr with
        | :? MethodCallExpression as mce ->
            match tryRecognizeTerminal mce with
            | None -> None
            | Some recognized ->
                let isGroupRoot (e: Expression) =
                    match unwrapConvert e with
                    | :? ParameterExpression as p -> Object.ReferenceEquals(p, groupParam)
                    | _ -> false
                if not (isRootedChain unwrapConvert isGroupRoot recognized.Source) then None
                else
                    let state = createState ()
                    let source = normalizeCountBySource recognized.Terminal recognized.Source state
                    let _innerSource = walkChain extractorConfig state source
                    finalizeState state
                    placeCountPredicate state recognized.CountPredicate
                    Some (buildDescriptor expr outerDistinct state recognized.Terminal, recognized.Terminal)
        | _ -> None

