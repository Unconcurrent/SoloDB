namespace SoloDatabase

open System.Collections.Generic
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorSerialization

/// Publishes operands in their owning grouped SELECT before scalar composition.
type internal QueryGroupCompletion(alias: string, nextName: unit -> string) =
    let projections = ResizeArray<Projection>()
    let rowJoins = ResizeArray<JoinEdge>()
    let selectorJoins = ResizeArray<JoinEdge>()

    member _.Alias = alias
    member _.RowJoins = rowJoins
    member _.SelectorJoins = selectorJoins

    /// Actual group producers supply both their value and their original input.
    member _.Bind(produced: QueryProducedValue) =
        match produced with
        | DirectValue(value, input) ->
            let name = "_group" + nextName()
            projections.Add { Alias = Some name; Expr = value }
            let companions, published = publishInput (fun () -> "_input" + nextName()) input
            projections.AddRange companions
            let value = SqlExpr.Column(Some alias, name)
            let published = scopeSerializationInput (Some alias) OuterLayer
                                { published with Source = ValueSome(jsonToken value); Carrier = input.Carrier }
            DirectValue(value, published)
        | PackedValue _ -> invalidOp "A group-stage operand must be supplied by its actual grouped producer."

    /// Deferred scalar work runs over completed group rows and their companions.
    member _.Publish(grouped: SelectCore, completed: SelectCore, produced) =
        let grouped =
            { grouped with
                Projections = ProjectionSetOps.ofList(ProjectionSetOps.toList grouped.Projections @ List.ofSeq projections)
                Joins = grouped.Joins @ QueryTranslatorReferenceJoin.preparedJoinClauses rowJoins }
        let source = { Ctes = []; Body = SingleSelect grouped }
        let completed =
            { completed with
                Source = Some(DerivedTable(source, alias))
                Joins = completed.Joins @ QueryTranslatorReferenceJoin.preparedJoinClauses selectorJoins }
        QueryProduced.publishCore nextName completed produced false
