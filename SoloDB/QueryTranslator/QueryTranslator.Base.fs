namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Linq.Expressions
open System.Reflection
open System.Runtime.InteropServices
open System.Text
open JsonFunctions
open Utils
open QueryTranslatorBaseTypes
open QueryTranslatorBaseHelpers
open SqlDu.Engine.C1.Spec

module internal QueryTranslatorBase =
    type MemberAccess = QueryTranslatorBaseTypes.MemberAccess
    type UpdateManyRelationTransform = QueryTranslatorBaseTypes.UpdateManyRelationTransform
    type QueryBuilder = QueryTranslatorBaseTypes.QueryBuilder

    let appendVariable = QueryTranslatorBaseTypes.appendVariable
    let isPrimitiveSQLiteType = QueryTranslatorBaseTypes.isPrimitiveSQLiteType
    let escapeSQLiteString = QueryTranslatorBaseTypes.escapeSQLiteString

    let mathFunctionTransformation = QueryTranslatorBaseHelpers.mathFunctionTransformation
    let evaluateExpr<'O> = QueryTranslatorBaseHelpers.evaluateExpr<'O>
    let isRootParameter = QueryTranslatorBaseHelpers.isRootParameter
    let isFullyConstant = QueryTranslatorBaseHelpers.isFullyConstant
    let isAnyConstant = QueryTranslatorBaseHelpers.isAnyConstant
    let inlineLambdaInvocation = QueryTranslatorBaseHelpers.inlineLambdaInvocation
    let inline compareKnownJsonDu (qb: QueryBuilder) (targetExpr: SqlExpr) (targetType: Type) (knownObject: obj) : SqlExpr =
        QueryTranslatorBaseHelpers.compareKnownJsonDu qb targetExpr targetType knownObject

    [<return: Struct>]
    let internal (|OfShape0|_|) (_retType: ('any1 -> 'T) | null) (_objType: ('any2 -> 'O) | null) (name: string) (m: MethodCallExpression) =
        if m.Method.Name = name && (isNull _retType || typeof<'T>.IsAssignableFrom m.Type) then
            let ret =
                if not (isNull m.Object) then
                    // Instance method: target is m.Object
                    ValueSome struct (m.Object, m.Arguments, 0)
                elif m.Arguments.Count > 0 then
                    // Static/Extension method: target is first arg
                    ValueSome struct (m.Arguments.[0], m.Arguments, 1)
                else
                    ValueNone

            let ret =
                match ret with
                | ValueSome struct (o, _, _) when isNull _objType || typeof<'O>.IsAssignableFrom o.Type -> ret
                | _ -> ValueNone

            match ret with
            | ValueSome struct (o, _, _) -> ValueSome o
            | _ -> ValueNone
        else
            ValueNone

    [<return: Struct>]
    let internal (|OfShape1|_|) (_retType: ('any1 -> 'T) | null) (_objType: ('any2 -> 'O) | null) (name: string) (_argType: (unit -> 'A) | null) (m: MethodCallExpression) =
        if m.Method.Name = name && (isNull _retType || typeof<'T>.IsAssignableFrom m.Type) then
            let ret =
                if not (isNull m.Object) then
                    // Instance method: target is m.Object, args are m.Arguments
                    ValueSome struct (m.Object, m.Arguments, 0)
                elif m.Arguments.Count > 0 then
                    // Static/Extension method: target is first arg, rest are args
                    ValueSome struct (m.Arguments.[0], m.Arguments, 1)
                else
                    ValueNone

            let ret =
                match ret with
                | ValueSome struct (o, _, _) when isNull _objType || typeof<'O>.IsAssignableFrom o.Type -> ret
                | _ -> ValueNone

            let ret =
                match ret with
                | ValueSome struct (o, args, argsIndex) when args.Count > argsIndex && (isNull _argType || typeof<'A>.IsAssignableFrom args.[argsIndex].Type) ->
                        ValueSome struct (o, args.[argsIndex])
                | _ -> ValueNone

            ret
        else
            ValueNone

    [<return: Struct>]
    let internal (|OfShape2|_|) (_retType: ('any1 -> 'T) | null) (_objType: ('any2 -> 'O) | null) (name: string) (_arg1Type: (unit -> 'A) | null) (_arg2Type: (unit -> 'B) | null) (m: MethodCallExpression) =
        if m.Method.Name = name && (isNull _retType || typeof<'T>.IsAssignableFrom m.Type) then
            let ret =
                if not (isNull m.Object) then
                    ValueSome struct (m.Object, m.Arguments, 0)
                elif m.Arguments.Count > 0 then
                    ValueSome struct (m.Arguments.[0], m.Arguments, 1)
                else
                    ValueNone

            let ret =
                match ret with
                | ValueSome struct (o, _, _) when isNull _objType || typeof<'O>.IsAssignableFrom o.Type -> ret
                | _ -> ValueNone

            match ret with
            | ValueSome struct (o, args, argsIndex) when args.Count > argsIndex + 1
                && (isNull _arg1Type || typeof<'A>.IsAssignableFrom args.[argsIndex].Type)
                && (isNull _arg2Type || typeof<'B>.IsAssignableFrom args.[argsIndex + 1].Type) ->
                    ValueSome struct (o, args.[argsIndex], args.[argsIndex + 1])
            | _ -> ValueNone
        else
            ValueNone

    [<return: Struct>]
    let internal (|OfShape3|_|) (_retType: ('any1 -> 'T) | null) (_objType: ('any2 -> 'O) | null) (name: string) (_arg1Type: (unit -> 'A) | null) (_arg2Type: (unit -> 'B) | null) (_arg3Type: (unit -> 'C) | null) (m: MethodCallExpression) =
        if m.Method.Name = name && (isNull _retType || typeof<'T>.IsAssignableFrom m.Type) then
            let ret =
                if not (isNull m.Object) then
                    ValueSome struct (m.Object, m.Arguments, 0)
                elif m.Arguments.Count > 0 then
                    ValueSome struct (m.Arguments.[0], m.Arguments, 1)
                else
                    ValueNone

            let ret =
                match ret with
                | ValueSome struct (o, _, _) when isNull _objType || typeof<'O>.IsAssignableFrom o.Type -> ret
                | _ -> ValueNone

            match ret with
            | ValueSome struct (o, args, argsIndex) when args.Count > argsIndex + 2
                && (isNull _arg1Type || typeof<'A>.IsAssignableFrom args.[argsIndex].Type)
                && (isNull _arg2Type || typeof<'B>.IsAssignableFrom args.[argsIndex + 1].Type)
                && (isNull _arg3Type || typeof<'C>.IsAssignableFrom args.[argsIndex + 2].Type) ->
                    ValueSome struct (o, args.[argsIndex], args.[argsIndex + 1], args.[argsIndex + 2])
            | _ -> ValueNone
        else
            ValueNone

    // You cannot use the generic's "<" or ">" chars inside match's case
    let inline internal OfIEnum () = Unchecked.defaultof<IEnumerable>
    let inline internal OfString () = Unchecked.defaultof<string>
    let inline internal OfType () = Unchecked.defaultof<Type>
    let inline internal OfPropInfo () = Unchecked.defaultof<PropertyInfo>
    let inline internal OfValueType () = Unchecked.defaultof<ValueType>
    let inline internal OfStringComparison () = Unchecked.defaultof<StringComparison>

    /// Returns true if the StringComparison is case-insensitive.
    let inline internal isIgnoreCase (comparison: StringComparison) =
        match comparison with
        | StringComparison.OrdinalIgnoreCase
        | StringComparison.CurrentCultureIgnoreCase
        | StringComparison.InvariantCultureIgnoreCase -> true
        | _ -> false

    /// <summary>
    /// A list of functions to handle unknown expression types.
    /// Handlers are called when the main translator encounters an expression it doesn't recognize.
    /// They must not modify the QueryBuilder or expression if they return false (indicating not handled).
    /// Handlers are evaluated in reverse order (last added, first called).
    /// </summary>
    let unknownExpressionHandler = List<Func<QueryBuilder, Expression, bool>>(seq {
        Func<QueryBuilder, Expression, bool>(fun _qb exp -> raise<bool> (ArgumentException (sprintf "Unhandled expression type: '%O'" exp.NodeType)))
    })

    /// <summary>
    /// A list of functions called before the main translator attempts to translate an expression.
    /// This allows for overriding default translation behavior.
    /// Handlers must not modify the QueryBuilder or expression if they return false (indicating not handled).
    /// Handlers are evaluated in reverse order (last added, first called).
    /// </summary>
    let preExpressionHandler = List<Func<QueryBuilder, Expression, bool>>(seq {
        // No operation example.
        Func<QueryBuilder, Expression, bool>(fun _qb _exp -> false)
    })

    /// <summary>
    /// Executes a list of handlers for a given expression until one of them returns true.
    /// </summary>
    /// <param name="handler">The list of handler functions.</param>
    /// <param name="qb">The query builder.</param>
    /// <param name="exp">The expression to handle.</param>
    /// <returns>True if a handler processed the expression, otherwise false.</returns>
    let internal runHandler (handler: List<Func<QueryBuilder, Expression, bool>>) (qb: QueryBuilder) (exp: Expression) =
        let mutable index = handler.Count - 1
        let mutable handled = false
        while index >= 0 && not handled do
            handled <- handler.[index].Invoke(qb, exp)
            index <- index - 1
        handled

    // Legacy string-builder visitor helpers removed:
    // visitNestedArrayPredicateHelper, buildJsonPathFromMemberExpression, formatMemberAccessPath,
    // tryHandleCollectionOrGroupingMemberAccess, tryHandleRootParameterMemberAccess, emitFallbackMemberAccess.
    // All callers now use the DU visitor path (visitDu + SqlDuMinimalEmit.emitExpr).
