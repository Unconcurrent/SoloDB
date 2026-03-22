namespace SoloDatabase

open System
open System.Linq.Expressions
open DBRefTypeHelpers
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.DBRefManyExtractorHelpers
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.SharedDescriptorExtract

module internal DBRefManyExtractor =
    let private extractorConfig =
        {
            EnsureOfTypeSupported = DBRefManyHelpers.ensureOfTypeSupported
            MultipleTakeSkipBoundariesMessage = DBRefManyHelpers.multipleTakeSkipBoundariesMessage
            TooManyTakeWhileBoundariesMessage =
                "Error: More than two TakeWhile/SkipWhile boundaries are not supported on DBRefMany.\n" +
                "Reason: The relation translator currently supports one inner and one outer window boundary.\n" +
                "Fix: Simplify the query or move additional windowing after AsEnumerable()."
        }

    let tryExtract (expr: Expression) : QueryDescriptor voption =
        let expr, outerDistinct, outerMaterialize = preprocessRoot expr

        match expr with
        | :? MethodCallExpression as mce ->
            match tryRecognizeTerminal mce with
            | None -> ValueNone
            | Some recognized ->
                if not (isDBRefManyChain unwrapConvert isDBRefManyType recognized.Source) then ValueNone
                else
                    let state : SharedDescriptorExtract.ExtractionState = createState ()
                    let source = normalizeCountBySource recognized.Terminal recognized.Source state
                    let innerSource = walkChain extractorConfig state source
                    finalizeState state
                    placeCountPredicate state recognized.CountPredicate

                    let finalTerminal, finalGroupByHaving =
                        match state.GroupByKey with
                        | Some _ ->
                            match recognized.Terminal with
                            | Terminal.Any(Some pred) -> Terminal.Exists, Some pred
                            | Terminal.All pred -> Terminal.All pred, Some pred
                            | _ -> recognized.Terminal, None
                        | None -> recognized.Terminal, None

                    let finalSelectProj =
                        match finalTerminal with
                        | Terminal.Select projExpr when state.SelectProjection.IsNone ->
                            match tryExtractLambdaExpression projExpr with
                            | ValueSome lambda -> Some lambda
                            | ValueNone -> state.SelectProjection
                        | _ -> state.SelectProjection

                    ValueSome(
                        buildDescriptor
                            innerSource
                            state.OfTypeName
                            state.CastTypeName
                            state.Wheres
                            state.SortKeys
                            state.Limit
                            state.Offset
                            state.PostBoundWheres
                            state.PostBoundSortKeys
                            state.PostBoundLimit
                            state.PostBoundOffset
                            state.TakeWhileInfo
                            state.PostBoundTakeWhileInfo
                            state.GroupByKey
                            (state.Distinct || outerDistinct)
                            finalSelectProj
                            (state.SetOps |> Seq.tryHead)
                            (state.SetOps |> Seq.toList)
                            finalTerminal
                            finalGroupByHaving
                            state.DefaultIfEmpty
                            state.PostSelectDefaultIfEmpty)

        // Non-MethodCall expressions (MemberExpression for .Count property, etc.)
        | :? MemberExpression as me when me.Member.Name = "Count" && not (isNull me.Expression) ->
            tryBuildCountPropertyDescriptor unwrapConvert isDBRefManyType me

        | :? MemberExpression as me when outerMaterialize ->
            let memberExpr = unwrapConvert (me :> Expression)
            tryBuildMaterializeDescriptor isDBRefManyType memberExpr

        | _ -> ValueNone
