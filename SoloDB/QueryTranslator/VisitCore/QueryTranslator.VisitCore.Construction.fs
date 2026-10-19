namespace SoloDatabase

open System
open SoloDatabase.Metadata.Generation
open System.Collections
open System.Collections.Generic
open System.Collections.ObjectModel
open System.Linq.Expressions
open System.Reflection
open System.Runtime.InteropServices
open System.Text
open JsonFunctions
open Utils
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorBase
open SoloDatabase.SqlModel
open SoloDatabase.JsonSerializator.JsonEqualitySupport
open SoloDatabase.QueryTranslatorSerialization
open SoloDatabase.QuerySerializationSql

/// Construction combines the results of its actual component translations.
module internal QueryTranslatorVisitCoreConstruction =
    type private TuplePart = Element of QueryProducedValue | Rest of Type * QueryProducedValue

    let private membersInOrder (visitProduced: Expression -> QueryBuilder -> QueryProducedValue) target (names: string array) (expressions: Expression array) (slots: int array voption) (qb: QueryBuilder) =
        let fieldQb = { qb with InsideJsonObjectProjection = true }
        let components = expressions |> Array.map (fun expression -> visitProduced expression fieldQb)
        let memberTypes =
            match slots with
            | ValueSome slots -> slots |> Array.map (fun index -> expressions[index].Type)
            | ValueNone -> expressions |> Array.map _.Type
        QueryProduced.construct qb.SourceContext.NextAlias (fun components ->
            let components =
                match slots with
                | ValueSome slots -> slots |> Array.map (fun index -> QueryProduced.embedded components[index])
                | ValueNone -> components |> Array.map QueryProduced.embedded
            let values = components |> Array.mapi (fun index (value, _) -> struct(names[index], value))
            let input = QuerySerializationInput.Members(target, names, memberTypes, components |> Array.map snd)
            DirectValue(SqlExpr.JsonObjectExpr(values |> Array.map (fun struct(name, value) -> name, value) |> Array.toList), input)) components

    let internal members visitProduced target names expressions qb =
        membersInOrder visitProduced target names expressions ValueNone qb

    let internal orderedMembers visitProduced target names expressions slots qb =
        membersInOrder visitProduced target names expressions (ValueSome slots) qb

    let internal tuple (visitProduced: Expression -> QueryBuilder -> QueryProducedValue) (expression: Expression) (qb: QueryBuilder) =
        let rec parts expression =
            tupleElements parts (fun argument -> [Rest(argument.Type, visitProduced argument qb)])
                (fun argument -> Element(visitProduced argument qb)) expression
        let parts = parts expression |> List.toArray
        let components = parts |> Array.map (function Element value | Rest(_, value) -> value)
        QueryProduced.construct qb.SourceContext.NextAlias (fun components ->
            let leaves =
                [|for index in 0 .. parts.Length - 1 do
                    let value, input = components[index]
                    match parts[index] with
                    | Element _ -> yield value, input
                    | Rest(target, _) ->
                        match JsonSerializator.JsonValue.SerializationShape target with
                        | JsonSerializator.JsonSerializationShape.ArrayMembers members ->
                            for position in 0 .. members.Length - 1 do
                                let struct(child, _) = serializationAccess input (StoredLabel(box position))
                                yield tupleLeafToken value position, child
                        | _ -> raise(NotSupportedException("A tuple Rest operand requires its declared array layout."))|]
            let leaves = leaves |> Array.map QueryProduced.embedded
            DirectValue(SqlExpr.JsonArrayExpr(leaves |> Array.map fst |> Array.toList),
                        QuerySerializationInput.Positions(expression.Type, leaves |> Array.map snd))) components
