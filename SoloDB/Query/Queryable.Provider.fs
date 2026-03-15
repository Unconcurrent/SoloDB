namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open System.Text
open System.Runtime.CompilerServices
open Microsoft.Data.Sqlite
open SQLiteTools
open Utils
open JsonFunctions
open Connections
open SoloDatabase
open SoloDatabase.JsonSerializator
open SoloDatabase.RelationsTypes
open SoloDatabase.QueryTranslatorBaseTypes
open SqlDu.Engine.C1.Spec


/// <summary>
/// An internal interface defining the contract for a SoloDB query provider.
/// </summary>
type internal ISoloDBCollectionQueryProvider =
    /// <summary>Gets the source collection object.</summary>
    abstract member Source: obj
    /// <summary>Gets additional data passed from the parent database instance.</summary>
    abstract member AdditionalData: obj

module internal QueryableTranslation =
    let startFilterTranslationWithConnection (metadataConnection: SqliteConnection) (source: ISoloDBCollection<'T>) (expression: Expression) =
        let query, variables, _ = QueryableTranslationCore.startTranslationWithConnection metadataConnection source expression
        query, variables

/// <summary>
/// The internal implementation of <c>IQueryProvider</c> for SoloDB collections.
/// </summary>
/// <param name="source">The source collection.</param>
/// <param name="data">Additional data, such as cache clearing functions.</param>
type internal SoloDBCollectionQueryProvider<'T>(source: ISoloDBCollection<'T>, data: obj) =
    static let enumerableDispatchCache = System.Collections.Concurrent.ConcurrentDictionary<Type, MethodInfo>()

    interface ISoloDBCollectionQueryProvider with
        override this.Source = source
        override this.AdditionalData = data

    interface SoloDBQueryProvider
    member internal this.ExecuteEnumetable<'Elem> (query: string) (par: obj) (batchCtxObj: obj) : IEnumerable<'Elem> =
        let batchCtx = batchCtxObj :?> QueryableTranslationCore.BatchLoadContext voption
        seq {
            use connection = source.GetInternalConnection()
            match batchCtx with
            | ValueSome ctx when ctx.HasSingleRelations || ctx.HasManyRelations ->
                // Buffer all results, then batch-load relation properties.
                let buffer = ResizeArray<int64 * 'Elem>()
                for row in connection.Query<Types.DbObjectRow>(query, par) do
                    let entity = JsonFunctions.fromSQLite<'Elem> row
                    buffer.Add(row.Id.Value, entity)

                if buffer.Count > 0 then
                    // Select projections can change result element type (for example to string),
                    // so batch loading must run only for actual owner entity objects.
                    let ownerEntities =
                        buffer
                        |> Seq.choose (fun (id, e) ->
                            let boxed = box e
                            if isNull boxed then None
                            elif ctx.OwnerType.IsInstanceOfType boxed then Some(id, boxed)
                            else None)
                        |> Seq.toArray

                    if ownerEntities.Length > 0 then
                        if ctx.HasSingleRelations then
                            Relations.withRelationSqliteWrap "query-batch-load" "ExecuteEnumerable.batchLoadDBRefProperties" (fun () ->
                                Relations.batchLoadDBRefProperties connection ctx.OwnerTable ctx.OwnerType ctx.ExcludedPaths ctx.IncludedPaths ownerEntities
                            )

                        if ctx.HasManyRelations then
                            Relations.withRelationSqliteWrap "query-batch-load" "ExecuteEnumerable.batchLoadDBRefManyProperties" (fun () ->
                                Relations.batchLoadDBRefManyProperties connection ctx.OwnerTable ctx.OwnerType ctx.ExcludedPaths ctx.IncludedPaths ownerEntities source.InTransaction
                            )

                        Relations.captureRelationVersionForEntities connection ctx.OwnerTable ownerEntities

                for (_id, entity) in buffer do
                    yield entity
            | _ ->
                for row in connection.Query<Types.DbObjectRow>(query, par) do
                    yield JsonFunctions.fromSQLite<'Elem> row
        }

    interface IQueryProvider with
        member this.CreateQuery<'TResult>(expression: Expression) : IQueryable<'TResult> =
            SoloDBCollectionQueryable<'T, 'TResult>(this, expression)

        member this.CreateQuery(expression: Expression) : IQueryable =
            let elementType = expression.Type.GetGenericArguments().[0]
            let queryableType = typedefof<SoloDBCollectionQueryable<_,_>>.MakeGenericType(elementType)
            Activator.CreateInstance(queryableType, source, this, expression) :?> IQueryable

        member this.Execute(expression: Expression) : obj =
            (this :> IQueryProvider).Execute<IEnumerable<'T>>(expression)

        member this.Execute<'TResult>(expression: Expression) : 'TResult =
            let query, variables, batchCtx = QueryableTranslationCore.startTranslation source expression

            #if DEBUG
            if System.Diagnostics.Debugger.IsAttached then
                printfn "%s" query
            #endif

            let inline batchLoadSingle (connection: SqliteConnection) (row: Types.DbObjectRow) (entity: 'TResult) =
                match batchCtx with
                | ValueSome ctx when (ctx.HasSingleRelations || ctx.HasManyRelations) && not (isNull (box entity)) && row.Id.HasValue && ctx.OwnerType.IsAssignableFrom(typeof<'TResult>) ->
                    if ctx.HasSingleRelations then
                        Relations.withRelationSqliteWrap "query-batch-load" "ExecuteScalar.batchLoadDBRefProperties" (fun () ->
                            Relations.batchLoadDBRefProperties connection ctx.OwnerTable ctx.OwnerType ctx.ExcludedPaths ctx.IncludedPaths [| (row.Id.Value, box entity) |]
                        )
                    if ctx.HasManyRelations then
                        Relations.withRelationSqliteWrap "query-batch-load" "ExecuteScalar.batchLoadDBRefManyProperties" (fun () ->
                            Relations.batchLoadDBRefManyProperties connection ctx.OwnerTable ctx.OwnerType ctx.ExcludedPaths ctx.IncludedPaths [| (row.Id.Value, box entity) |] source.InTransaction
                        )
                    Relations.captureRelationVersionForEntities connection ctx.OwnerTable [| (row.Id.Value, box entity) |]
                | _ -> ()
                entity

            try
                match typeof<'TResult> with
                | t when t.IsGenericType && typeof<IEnumerable<'T>>.Equals typeof<'TResult> ->
                    let result = this.ExecuteEnumetable<'T> query variables (box batchCtx)
                    result :> obj :?> 'TResult
                | t when t.IsGenericType && typedefof<IEnumerable<_>>.Equals typedefof<'TResult> ->
                    let elemType = (GenericTypeArgCache.Get t).[0]
                    let m : MethodInfo =
                        enumerableDispatchCache.GetOrAdd(elemType, Func<Type, MethodInfo>(fun et ->
                            typeof<SoloDBCollectionQueryProvider<'T>>
                                .GetMethod(nameof(this.ExecuteEnumetable), BindingFlags.NonPublic ||| BindingFlags.Instance)
                                .MakeGenericMethod(et)
                        ))
                    m.Invoke(this, [|query; variables; box batchCtx|]) :?> 'TResult
                    // When is explain query plan.
                | t when t = typeof<string> && QueryableTranslationCore.isAggregateExplainQuery expression ->
                    use connection = source.GetInternalConnection()
                    let result = connection.Query<{|detail: string|}>(query, variables) |> Seq.toList
                    let plan = result |> List.map(_.detail) |> String.concat ";\n"
                    plan :> obj :?> 'TResult
                | t when t = typeof<string> && QueryableTranslationCore.isGetGeneratedSQLQuery expression ->
                    query :> obj :?> 'TResult
                | _other ->
                    use connection = source.GetInternalConnection()
                    let methodName =
                        match expression with
                        | :? MethodCallExpression as mce -> mce.Method.Name
                        | _ -> "Execute"
                    let getTerminalDefaultValue() =
                        let isExpressionLikeArgument (expr: Expression) =
                            match expr with
                            | :? LambdaExpression -> true
                            | _ -> typeof<Expression>.IsAssignableFrom expr.Type
                        let defaultExprOpt =
                            match expression with
                            | :? MethodCallExpression as mce ->
                                let args = Array.init (mce.Arguments.Count - 1) (fun i -> mce.Arguments.[i + 1])
                                match mce.Method.Name, args with
                                | ("FirstOrDefault" | "SingleOrDefault"), [| defaultValue |]
                                    when not (isExpressionLikeArgument defaultValue) ->
                                    Some defaultValue
                                | ("FirstOrDefault" | "SingleOrDefault"), [| predicate; defaultValue |]
                                    when isExpressionLikeArgument predicate ->
                                    Some defaultValue
                                | _ ->
                                    None
                            | _ ->
                                None
                        match defaultExprOpt with
                        | Some defaultExpr -> QueryTranslator.evaluateExpr<'TResult> defaultExpr
                        | None -> Unchecked.defaultof<'TResult>

                    // Add Single, First, and the OrDefault Variant here.
                    let query = connection.Query<Types.DbObjectRow>(query, variables)

                    match methodName with
                    | "Single" ->
                        let row = query.Single()
                        let entity = JsonFunctions.fromSQLite<'TResult> row
                        batchLoadSingle connection row entity
                    | "SingleOrDefault" ->
                        // Emulate query.SupportedLinqMethods.SingleOrDefault()
                        use enumerator = query.GetEnumerator()
                        match enumerator.MoveNext() with
                        | false -> getTerminalDefaultValue()
                        | true ->
                            let prevElement = enumerator.Current
                            match enumerator.MoveNext() with
                            | true -> raise (InvalidOperationException("Sequence contains more than one element"))
                            | false ->
                                // Only one element was found, return it.
                                let entity = JsonFunctions.fromSQLite<'TResult> prevElement
                                batchLoadSingle connection prevElement entity

                    | "First" ->
                        let row = query.First()
                        let entity = JsonFunctions.fromSQLite<'TResult> row
                        batchLoadSingle connection row entity
                    | "FirstOrDefault" ->
                        match query |> Seq.tryHead with
                        | None -> getTerminalDefaultValue()
                        | Some row ->
                            let entity = JsonFunctions.fromSQLite<'TResult> row
                            batchLoadSingle connection row entity

                    | "MinBy"
                    | "MaxBy" ->
                        match query |> Seq.tryHead with
                        | Some row ->
                            let entity = JsonFunctions.fromSQLite<'TResult> row
                            batchLoadSingle connection row entity
                        | None when typeof<'TResult>.IsValueType && isNull (Nullable.GetUnderlyingType(typeof<'TResult>)) ->
                            raise (InvalidOperationException("Sequence contains no elements"))
                        | None ->
                            Unchecked.defaultof<'TResult>

                    | methodName when methodName.EndsWith("OrDefault", StringComparison.Ordinal) ->
                        match query |> Seq.tryHead with
                        | None -> getTerminalDefaultValue()
                        | Some row ->
                            let entity = JsonFunctions.fromSQLite<'TResult> row
                            batchLoadSingle connection row entity
                    | _ ->
                        match query |> Seq.tryHead with
                        | None -> raise (InvalidOperationException("Sequence contains no elements"))
                        | Some row ->
                            let entity = JsonFunctions.fromSQLite<'TResult> row
                            batchLoadSingle connection row entity

            finally
                ()
            
/// <summary>
/// The internal implementation of <c>IQueryable</c> and <c>IOrderedQueryable</c> for SoloDB collections.
/// </summary>
/// <param name="provider">The query provider that created this queryable.</param>
/// <param name="expression">The expression tree representing the query.</param>
and internal SoloDBCollectionQueryable<'I, 'T>(provider: IQueryProvider, expression: Expression) =
    interface IOrderedQueryable<'T>

    interface IQueryable<'T> with
        member _.Provider = provider
        member _.Expression = expression
        member _.ElementType = typeof<'T>

    interface IEnumerable<'T> with
        member this.GetEnumerator() =
            let items = (provider.Execute<IEnumerable<'T>>(expression))
            items.GetEnumerator()

    interface IEnumerable with
        member this.GetEnumerator() =
            (this :> IEnumerable<'T>).GetEnumerator() :> IEnumerator

/// <summary>
/// A private, sealed class representing the root of a query, which is always a collection.
/// </summary>
/// <param name="c">The source collection.</param>
and [<Sealed>] private RootQueryable<'T>(c: ISoloDBCollection<'T>) =
    member val Source = c

    interface IRootQueryable with
        override this.SourceTableName = this.Source.Name

    interface IQueryable<'T> with
        member _.Provider = null
        member _.Expression = Expression.Constant(c)
        member _.ElementType = typeof<'T>

    interface IEnumerable<'T> with
        member this.GetEnumerator() =
            Enumerable.Empty<'T>().GetEnumerator()

    interface IEnumerable with
        member this.GetEnumerator() =
            Enumerable.Empty<'T>().GetEnumerator()
