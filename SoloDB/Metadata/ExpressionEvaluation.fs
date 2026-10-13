namespace SoloDatabase

open System
open System.Linq.Expressions
open System.Collections.Generic

module internal ExpressionEvaluation =
    /// Evaluates a closed value through the same direct leaves as invocation binding.
    let internal evaluateExpr<'O> (e: Expression) =
        match e with
        | :? ConstantExpression as ce ->
            ce.Value :?> 'O
        | _ ->
            let run = QueryValuePlan.bind (Dictionary<ParameterExpression, int>()) (fun _ -> None) e
            unbox<'O> (run [||])
