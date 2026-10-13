namespace SoloDatabase

open System
open System.Reflection
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions

/// Runtime-result dispatch belongs to the provider's closed source type.
[<AbstractClass; Sealed>]
type internal QueryProviderMetadata<'Provider> private () =
    static member RegisterQuery(elementType: Type, create) =
        let operation = { Generated = true; Value = create }
        TypeMetadataCache.Register(typeof<'Provider>, fun metadata -> { metadata with ProviderQueries = MetadataMap.set elementType operation metadata.ProviderQueries })
    static member RegisterResult(resultType: Type, execute) =
        let operation = { Generated = true; Value = execute }
        TypeMetadataCache.Register(typeof<'Provider>, fun metadata -> { metadata with ProviderResults = MetadataMap.set resultType operation metadata.ProviderResults })
    static member RegisterSequence(elementType: Type, resultType: Type, execute: Func<'Provider,string,obj,QueryableTranslationCore.BatchLoadPlan voption,obj>) =
        TypeMetadataCache.Register(typeof<'Provider>, fun metadata ->
            { metadata with
                SequenceResults = MetadataMap.set elementType { Generated = true; Value = resultType } metadata.SequenceResults
                ProviderSequences = MetadataMap.set elementType { Generated = true; Value = execute :> Delegate } metadata.ProviderSequences })
    static member SequenceResultType(elementType: Type) =
        TypeMetadataCache.Discover(typeof<'Provider>, (fun metadata -> MetadataMap.tryFind elementType metadata.SequenceResults),
            (fun operation metadata -> { metadata with SequenceResults = MetadataMap.set elementType operation metadata.SequenceResults }),
            Func<Type>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "query sequence result" elementType
                typedefof<IEnumerable<_>>.MakeGenericType(elementType)))
    static member CreateQuery(elementType: Type) =
        TypeMetadataCache.Discover(typeof<'Provider>, (fun metadata -> MetadataMap.tryFind elementType metadata.ProviderQueries),
            (fun operation metadata -> { metadata with ProviderQueries = MetadataMap.set elementType operation metadata.ProviderQueries }),
            Func<_>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "provider query" elementType
                typeof<'Provider>.GetMethod("CreateTypedQuery", BindingFlags.NonPublic ||| BindingFlags.Static)
                    .MakeGenericMethod(elementType)
                    .CreateDelegate(typeof<Func<IQueryProvider,Expression,IQueryable>>)
                :?> Func<IQueryProvider,Expression,IQueryable>))
    static member ExecuteResult(resultType: Type) =
        TypeMetadataCache.Discover(typeof<'Provider>, (fun metadata -> MetadataMap.tryFind resultType metadata.ProviderResults),
            (fun operation metadata -> { metadata with ProviderResults = MetadataMap.set resultType operation metadata.ProviderResults }),
            Func<_>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "provider result" resultType
                typeof<'Provider>.GetMethod("ExecuteTypedResult", BindingFlags.NonPublic ||| BindingFlags.Static)
                    .MakeGenericMethod(resultType)
                    .CreateDelegate(typeof<Func<IQueryProvider,Expression,obj>>)
                :?> Func<IQueryProvider,Expression,obj>))
    static member ExecuteSequence(elementType: Type) =
        TypeMetadataCache.Discover(typeof<'Provider>, (fun metadata -> MetadataMap.tryFind elementType metadata.ProviderSequences),
            (fun operation metadata -> { metadata with ProviderSequences = MetadataMap.set elementType operation metadata.ProviderSequences }),
            Func<Delegate>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "provider sequence" elementType
                typeof<'Provider>.GetMethod("ExecuteEnumetable", BindingFlags.NonPublic ||| BindingFlags.Instance)
                    .MakeGenericMethod(elementType)
                    .CreateDelegate(typeof<Func<'Provider,string,obj,QueryableTranslationCore.BatchLoadPlan voption,obj>>)))
        :?> Func<'Provider,string,obj,QueryableTranslationCore.BatchLoadPlan voption,obj>
