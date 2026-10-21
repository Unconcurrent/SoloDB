namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq.Expressions

type internal CompiledQueryFactory<'Source> =
    Func<ISoloDBCollection<'Source>,Expression,ParameterExpression array,ParameterExpression,Expression array,IDictionary<ParameterExpression,int>,Func<obj[],obj>>

/// Closed operations for retained SQL parts of a compiled client expression.
[<AbstractClass; Sealed>]
type internal CompiledQueryMetadata<'Source, 'Args> private () =
    static member private Adapt(create: Func<ISoloDBCollection<'Source>,Expression,ParameterExpression array,ParameterExpression,Expression array,Func<'Args,obj>>) =
        CompiledQueryFactory<'Source>(fun source body parameters argument arguments scope ->
            if scope.Count <> 1 then MetadataRuntime.missing "compiled query lexical frame" typeof<'Args>
            let run = create.Invoke(source,body,parameters,argument,arguments)
            Func<obj[],obj>(fun values -> run.Invoke(unbox<'Args>(QueryStorage.value values[0]))))
    static member RegisterSequence(elementType: Type, create: Func<ISoloDBCollection<'Source>,Expression,ParameterExpression array,ParameterExpression,Expression array,Func<'Args,obj>>) =
        CompiledQueryMetadata<'Source,'Args>.RegisterScopedSequence(elementType,CompiledQueryMetadata<'Source,'Args>.Adapt create)
    static member RegisterResult(resultType: Type, create: Func<ISoloDBCollection<'Source>,Expression,ParameterExpression array,ParameterExpression,Expression array,Func<'Args,obj>>) =
        CompiledQueryMetadata<'Source,'Args>.RegisterScopedResult(resultType,CompiledQueryMetadata<'Source,'Args>.Adapt create)
    static member RegisterScopedSequence(elementType: Type, create: CompiledQueryFactory<'Source>) =
        TypeMetadataCache.Register(typeof<'Source>, fun metadata -> { metadata with CompiledSequences = MetadataMap.set (struct(typeof<'Args>,elementType)) { Generated = true; Value = create :> Delegate } metadata.CompiledSequences })
    static member RegisterScopedResult(resultType: Type, create: CompiledQueryFactory<'Source>) =
        TypeMetadataCache.Register(typeof<'Source>, fun metadata -> { metadata with CompiledResults = MetadataMap.set (struct(typeof<'Args>,resultType)) { Generated = true; Value = create :> Delegate } metadata.CompiledResults })
    static member Operation(sequence, resultType: Type) =
        let key = struct(typeof<'Args>, resultType)
        let operation =
            TypeMetadataCache.Discover(typeof<'Source>,
                (fun metadata -> MetadataMap.tryFind key (if sequence then metadata.CompiledSequences else metadata.CompiledResults)),
                (fun operation metadata ->
                    if sequence then { metadata with CompiledSequences = MetadataMap.set key operation metadata.CompiledSequences }
                    else { metadata with CompiledResults = MetadataMap.set key operation metadata.CompiledResults }),
                Func<Delegate>(fun () ->
                    if not (MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "compiled query result" resultType
                    let owner = typeof<CompiledQueryMetadata<'Source, 'Args>>.Assembly.GetType("SoloDatabase.CompiledQueries", true)
                    let name = if sequence then "createSequenceDispatcher" else "createResultDispatcher"
                    owner.GetMethod(name, System.Reflection.BindingFlags.Public ||| System.Reflection.BindingFlags.NonPublic ||| System.Reflection.BindingFlags.Static)
                        .MakeGenericMethod(typeof<'Source>, typeof<'Args>, resultType).Invoke(null, Array.empty) :?> Delegate))
        operation :?> CompiledQueryFactory<'Source>

/// Typed argument carriers publish their bound access operations once.
module internal CompiledQueryArguments =
    // Compile owns these argument carriers and supplies their typed operations.
    let pairArguments<'A, 'B>() =
        let owner = typeof<struct ('A * 'B)>
        QueryValueMetadata.RegisterReader(owner, "Item1", true, typeof<'A>, Func<obj,obj>(fun value -> let struct(a, _) = unbox<struct ('A * 'B)> value in box a))
        QueryValueMetadata.RegisterReader(owner, "Item2", true, typeof<'B>, Func<obj,obj>(fun value -> let struct(_, b) = unbox<struct ('A * 'B)> value in box b))
        let argument = Expression.Parameter(owner, "args")
        let values =
            [| BoundValueAccessExpression(argument, typeof<'A>, QueryValueMetadata.Reader(owner, "Item1", true, typeof<'A>), "Item1") :> Expression
               BoundValueAccessExpression(argument, typeof<'B>, QueryValueMetadata.Reader(owner, "Item2", true, typeof<'B>), "Item2") :> Expression |]
        struct(argument, values)

    let tripleArguments<'A, 'B, 'C>() =
        let owner = typeof<struct ('A * 'B * 'C)>
        QueryValueMetadata.RegisterReader(owner, "Item1", true, typeof<'A>, Func<obj,obj>(fun value -> let struct(a, _, _) = unbox<struct ('A * 'B * 'C)> value in box a))
        QueryValueMetadata.RegisterReader(owner, "Item2", true, typeof<'B>, Func<obj,obj>(fun value -> let struct(_, b, _) = unbox<struct ('A * 'B * 'C)> value in box b))
        QueryValueMetadata.RegisterReader(owner, "Item3", true, typeof<'C>, Func<obj,obj>(fun value -> let struct(_, _, c) = unbox<struct ('A * 'B * 'C)> value in box c))
        let argument = Expression.Parameter(owner, "args")
        let values =
            [| BoundValueAccessExpression(argument, typeof<'A>, QueryValueMetadata.Reader(owner, "Item1", true, typeof<'A>), "Item1") :> Expression
               BoundValueAccessExpression(argument, typeof<'B>, QueryValueMetadata.Reader(owner, "Item2", true, typeof<'B>), "Item2") :> Expression
               BoundValueAccessExpression(argument, typeof<'C>, QueryValueMetadata.Reader(owner, "Item3", true, typeof<'C>), "Item3") :> Expression |]
        struct(argument, values)
