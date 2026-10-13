namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions

/// Roots closed query operations over the existing provider and executor.
[<AbstractClass; Sealed>]
type internal QueryMetadata private () =
    /// Registers a scalar result for a collection source.
    static member RegisterResult<'Source, 'Result>() =
        QueryProviderMetadata<SoloDBCollectionQueryProvider<'Source>>.RegisterResult(
            typeof<'Result>, Func<IQueryProvider, Expression, obj>(fun provider expression ->
                box (provider.Execute<'Result>(expression))))

    /// Registers a sequence element and both generic and non-generic query routes.
    static member RegisterSequence<'Source, 'Element>() =
        ModelMetadata.RegisterSequence(typeof<IQueryable<'Element>>, typeof<'Element>)
        ModelMetadata.RegisterSequence(typeof<IOrderedQueryable<'Element>>, typeof<'Element>)
        ModelMetadata.RegisterSequence(typeof<IEnumerable<'Element>>, typeof<'Element>)
        ModelMetadata.RegisterSequence(typeof<IOrderedEnumerable<'Element>>, typeof<'Element>)
        TypeMetadata.registerArguments typeof<IQueryable<'Element>> [|typeof<'Element>|]
        TypeMetadata.registerArguments typeof<IOrderedQueryable<'Element>> [|typeof<'Element>|]
        TypeMetadata.registerArguments typeof<IEnumerable<'Element>> [|typeof<'Element>|]
        QueryProviderMetadata<SoloDBCollectionQueryProvider<'Source>>.RegisterQuery(
            typeof<'Element>, Func<IQueryProvider, Expression, IQueryable>(fun provider expression ->
                provider.CreateQuery<'Element>(expression) :> IQueryable))
        QueryMetadata.RegisterResult<'Source, IEnumerable<'Element>>()
        QueryProviderMetadata<SoloDBCollectionQueryProvider<'Source>>.RegisterSequence(
            typeof<'Element>, typeof<IEnumerable<'Element>>, Func<SoloDBCollectionQueryProvider<'Source>, string, obj, QueryableTranslationCore.BatchLoadPlan voption, obj>(
                fun provider sql arguments hydration -> box (provider.ExecuteEnumetable<'Element> sql arguments hydration)))
    /// Registers an actual retained sequence part and its invocation argument type.
    static member RegisterCompiledSequence<'Source, 'Args, 'Element>() =
        CompiledQueryMetadata<'Source, 'Args>.RegisterSequence(typeof<'Element>,
            Func<ISoloDBCollection<'Source>, Expression, ParameterExpression array, ParameterExpression, Expression array, Func<'Args, obj>>(
                fun source body parameters argument arguments ->
                    let run = CompiledQueries.compileExpression<'Source, 'Args, 'Element> source body parameters argument arguments
                    Func<'Args, obj>(fun args -> box (run args))))
        QueryMetadata.RegisterSequence<'Source, 'Element>()

    /// Registers an actual retained scalar part and its invocation argument type.
    static member RegisterCompiledResult<'Source, 'Args, 'Result>() =
        CompiledQueryMetadata<'Source, 'Args>.RegisterResult(typeof<'Result>,
            Func<ISoloDBCollection<'Source>, Expression, ParameterExpression array, ParameterExpression, Expression array, Func<'Args, obj>>(
                fun source body parameters argument arguments ->
                    let run = CompiledQueries.scalarExpression<'Source, 'Args, 'Result> source body parameters argument arguments
                    Func<'Args, obj>(fun args -> box (run args))))
        QueryMetadata.RegisterResult<'Source, 'Result>()

