namespace SoloDatabase

open System
open System.Collections.Generic
open SoloDatabase.SqlModel

/// Nested defaults retain their stored-query representation without running model code.
module internal QueryDefault =
    let private scalar variables comparison target =
        let struct(value, writer) = StoredValueParameter.allocateProduced comparison variables target (QueryValueMetadata.Default target)
        QueryProduced.typed writer value

    /// A consumed scalar default uses the same comparison writer as an authored scalar.
    let comparisonValue variables target = scalar variables true target

    let value (variables: Dictionary<string, obj>) (target: Type) =
        let absent = QueryProduced.typed target (SqlExpr.Literal SqlLiteral.Null)
        if not target.IsValueType || not(isNull(TypeMetadata.nullableUnderlying target)) then absent else
        match absent.Input.Facts with
        | JsonSerializator.JsonSerializationShape.Scalar ->
            scalar variables false target
        | _ -> DirectValue(SqlExpr.Literal SqlLiteral.Null, { absent.Input with StructuralDefault = true })

    /// The parent is already bound; a selected null child is not an absent parent.
    let memberResult createDefault target value (input: QuerySerializationInput) (selected: QueryProducedValue) =
        if not input.StructuralDefault then selected else
        QueryProduced.choose target
            (SqlExpr.Unary(UnaryOperator.IsNull, QueryTranslatorBaseTypes.nullCarrier input.Carrier value))
            (createDefault target) selected
