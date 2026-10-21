namespace SoloDatabase

open System
open SoloDatabase.SqlModel

/// Publishes authored writer and physical occurrence descriptors through SQL.
module internal QuerySerializationSql =
    let internal absentWriterHeader = SqlExpr.JsonArrayExpr [SqlExpr.Literal(SqlLiteral.Integer 0L); SqlExpr.Literal SqlLiteral.Null]

    /// The descriptor uses ordinary SQL arrays and their existing escaping.
    let internal serializationOperand (operand: QuerySerializationOperand) =
        Array.fold (fun descriptor (step: QuerySerializationStep) ->
            SqlExpr.JsonArrayExpr [SqlExpr.Literal(SqlLiteral.Integer 1L); descriptor;
                                  SqlExpr.JsonArrayExpr [step.Kind; step.Label; step.MemberType; step.ActualType]])
            operand.Descriptor operand.Accesses

    let internal writerHeader source =
        let header = SqlExpr.Binary(source, BinaryOperator.JsonText, SqlExpr.Literal(SqlLiteral.String "$.$type"))
        SqlExpr.JsonArrayExpr [SqlExpr.Unary(UnaryOperator.IsNotNull, header); SqlExpr.FunctionCall("jsonb", [header])]

    let internal rootDescriptor (input: QuerySerializationInput) =
        let declared = Utils.optionalPayloadType input.OriginalType
        TypeMetadata.registerType declared
        let actual =
            match input.Source, input.Facts with
            | ValueSome source, JsonSerializator.JsonSerializationShape.ObjectMembers _
            | ValueSome source, JsonSerializator.JsonSerializationShape.DeclaredMembers _
            | ValueSome source, JsonSerializator.JsonSerializationShape.Dynamic
                when input.Shape.IsNone && not(TypeMetadata.isSealed declared) ->
                writerHeader source
            | _ -> absentWriterHeader
        SqlExpr.JsonArrayExpr [SqlExpr.Literal(SqlLiteral.Integer 0L);
                              SqlExpr.Literal(SqlLiteral.String declared.FullName); actual]

    /// Produced children are framed with the value instead of being inferred later.
    let rec internal writerDescriptor (input: QuerySerializationInput) =
        let origin = input.Operand |> ValueOption.map serializationOperand |> ValueOption.defaultWith (fun () -> rootDescriptor input)
        let writer =
            if input.Operand.IsSome || input.Children.Length = 0 then origin
            else
                let names (childInputs: QuerySerializationInput array) (fields: (string * Type) array) =
                    SqlExpr.JsonArrayExpr [SqlExpr.Literal(SqlLiteral.Integer 0L);
                        SqlExpr.JsonArrayExpr(fields |> Array.mapi (fun index (name, declared) ->
                            TypeMetadata.registerType declared
                            SqlExpr.JsonArrayExpr [SqlExpr.Literal(SqlLiteral.String name);
                                                  SqlExpr.Literal(SqlLiteral.String declared.FullName);
                                                  inputDescriptor childInputs[index]]) |> Array.toList)]
                let children =
                    match input.Facts with
                    | JsonSerializator.JsonSerializationShape.DeclaredMembers members ->
                        names input.Children (members |> Array.map (fun struct(name, declared) -> name, declared))
                    | JsonSerializator.JsonSerializationShape.ArrayMembers members ->
                        positionsDescriptor (Array.map (fun (memberInfo: JsonSerializator.JsonStoredMember) -> memberInfo.ValueType) members) input.Children
                    | JsonSerializator.JsonSerializationShape.DeclaredArrayMembers members -> positionsDescriptor members input.Children
                    | JsonSerializator.JsonSerializationShape.SequenceValues element
                    | JsonSerializator.JsonSerializationShape.DictionaryValues element ->
                        TypeMetadata.registerType element
                        SqlExpr.JsonArrayExpr [SqlExpr.Literal(SqlLiteral.Integer 2L);
                                              SqlExpr.Literal(SqlLiteral.String element.FullName); inputDescriptor input.Children[0]]
                    | JsonSerializator.JsonSerializationShape.GroupingValues(key, item) ->
                        let items = QuerySerializationInput.Sequence(item.MakeArrayType(), input.Children[1])
                        names [|input.Children[0]; items|] [|"Key", key; "Items", item.MakeArrayType()|]
                    | _ -> SqlExpr.Literal SqlLiteral.Null
                SqlExpr.JsonArrayExpr [SqlExpr.Literal(SqlLiteral.Integer 2L); origin; children]
        writer

    and internal inputDescriptor (input: QuerySerializationInput) =
        let writer = writerDescriptor input
        if input.PhysicalOwner.IsNone && input.ReferenceOwner.IsNone && input.OriginalOrderOperand.IsNone then writer
        else
            let physical = input.PhysicalOwner |> ValueOption.map _.Descriptor |> ValueOption.defaultValue (SqlExpr.Literal SqlLiteral.Null)
            let reference = input.ReferenceOwner |> ValueOption.map _.Descriptor |> ValueOption.defaultValue (SqlExpr.Literal SqlLiteral.Null)
            let fields = [SqlExpr.Literal(SqlLiteral.Integer 3L); writer; physical; reference]
            match input.OriginalOrderOperand with
            | ValueNone -> SqlExpr.JsonArrayExpr fields
            | ValueSome units -> SqlExpr.JsonArrayExpr (fields @ [SqlExpr.JsonArrayExpr [units.Data.Value; units.Path]])

    and private positionsDescriptor declared children =
        SqlExpr.JsonArrayExpr [SqlExpr.Literal(SqlLiteral.Integer 1L);
            SqlExpr.JsonArrayExpr(Array.mapi (fun index child ->
                TypeMetadata.registerType declared[index]
                SqlExpr.JsonArrayExpr [SqlExpr.Literal(SqlLiteral.String declared[index].FullName); inputDescriptor child]) children |> Array.toList)]

    let internal isConstantDescriptor descriptor =
        SqlExpr.fold (fun constant expression ->
            constant && (match expression with
                         | SqlExpr.Column _ | SqlExpr.Parameter _ | SqlExpr.ScalarSubquery _ | SqlExpr.Exists _ | SqlExpr.InSubquery _ -> false
                         | _ -> true)) true descriptor

    /// Constant element facts describe the sequence once; variable facts retain each row.
    let internal sequenceDescriptor target (element: QuerySerializationInput) =
        let input = QuerySerializationInput.Sequence(target, element)
        let descriptor = inputDescriptor element
        if isConstantDescriptor descriptor then inputDescriptor input
        else
            let declared = ModelMetadata.SequenceElement target
            TypeMetadata.registerType declared
            let items = SqlExpr.FunctionCall("jsonb_group_array", [SqlExpr.JsonArrayExpr
                            [SqlExpr.Literal(SqlLiteral.String declared.FullName); descriptor]])
            SqlExpr.JsonArrayExpr [SqlExpr.Literal(SqlLiteral.Integer 2L); rootDescriptor input;
                                  SqlExpr.JsonArrayExpr [SqlExpr.Literal(SqlLiteral.Integer 1L); items]]
