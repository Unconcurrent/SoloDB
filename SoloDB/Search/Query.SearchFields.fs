namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.SqlModel

/// Fixed member paths use the same child/writer facts as ordinary query access.
module internal QuerySearchFields =
    let path (selector: LambdaExpression) =
        let rec walk expression =
            match QueryTranslatorBaseHelpers.unwrapStructCopy expression with
            | :? MemberExpression as memberValue ->
                let parent, path, _ = walk memberValue.Expression
                if DBRefTypeHelpers.isAnyRelationRefType parent.OriginalType then
                    raise(NotSupportedException("Search fields cannot traverse references."))
                let struct(child, segments) = QueryTranslatorSerialization.serializationAccess parent (ClrMember(memberValue.Member.Name,memberValue.Type))
                if child.OriginalType <> memberValue.Type then
                    raise(NotSupportedException("The selected search member is not stored by its serializer."))
                child,path @ segments,memberValue.Member.Name
            | :? ParameterExpression as parameter when Object.ReferenceEquals(parameter,selector.Parameters.[0]) ->
                QuerySerializationInput.ForType(parameter.Type),[],""
            | _ -> raise(NotSupportedException("Search fields must be fixed stored member paths."))
        if not(selector.ReturnType.Equals(typeof<string>)) then
            raise(NotSupportedException("A search field must be a string; arrays and nontext definitions are unsupported."))
        let _,segments,leaf = walk selector.Body
        if segments.IsEmpty then invalidArg "selector" "Select a stored string member."
        JsonPathOps.text segments,leaf
