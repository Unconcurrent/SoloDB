namespace SoloDatabase

open System
open System.Linq.Expressions

/// Shared admission for captured public values and representation-neutral wrappers.
module internal QueryTranslatorExpressionAdmission =
    let internal isByRefLikeType (target: Type) =
#if NETSTANDARD2_1 || NET6_0_OR_GREATER
        target.IsByRefLike
#else
        target.IsValueType && target.CustomAttributes |> Seq.exists (fun attribute -> attribute.AttributeType.FullName = "System.Runtime.CompilerServices.IsByRefLikeAttribute")
#endif

    let internal isConstantParameter (expression: Expression) =
        expression.NodeType <> ExpressionType.Lambda && expression.NodeType <> ExpressionType.Quote
        && not(isByRefLikeType expression.Type)
        && QueryTranslatorBaseHelpers.isFullyConstant expression
        && (match expression with :? NewExpression as created when DateTimeFunctions.isDateTimeLikeType created.Type -> false | _ -> true)
        && (match expression with :? ConstantExpression as constant when isNull constant.Value -> false | _ -> true)
