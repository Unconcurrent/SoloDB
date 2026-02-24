namespace SoloDatabase.MongoDB

open System.Linq.Expressions
open System
open System.Collections.Generic

/// <summary>
/// A fluent builder for creating LINQ filter expressions.
/// </summary>
/// <typeparam name="T">The type of the document to filter.</typeparam>
type FilterDefinitionBuilder<'T> () =
    /// <summary>
    /// Internal list of accumulated filter expressions.
    /// </summary>
    let mutable filters = []

    /// <summary>
    /// Gets an empty filter that matches all documents.
    /// </summary>
    member val Empty = Expression.Lambda<Func<'T, bool>>(Expression.Constant(true), Expression.Parameter(typeof<'T>, "x"))

    /// <summary>
    /// Adds an equality filter (field == value).
    /// </summary>
    member this.Eq<'TField>(field: Expression<Func<'T, 'TField>>, value: 'TField) : FilterDefinitionBuilder<'T> =
        let parameter = field.Parameters.[0]
        let body = Expression.Equal(field.Body, Expression.Constant(value, typeof<'TField>))
        let lambda = Expression.Lambda<Func<'T, bool>>(body, parameter)
        filters <- lambda :: filters
        this

    /// <summary>
    /// Adds a greater-than filter (field > value).
    /// </summary>
    member this.Gt<'TField when 'TField :> IComparable>(field: Expression<Func<'T, 'TField>>, value: 'TField) : FilterDefinitionBuilder<'T> =
        let parameter = field.Parameters.[0]
        let body = Expression.GreaterThan(field.Body, Expression.Constant(value, typeof<'TField>))
        let lambda = Expression.Lambda<Func<'T, bool>>(body, parameter)
        filters <- lambda :: filters
        this

    /// <summary>
    /// Adds a less-than filter (field &lt; value).
    /// </summary>
    member this.Lt<'TField when 'TField :> IComparable>(field: Expression<Func<'T, 'TField>>, value: 'TField) : FilterDefinitionBuilder<'T> =
        let parameter = field.Parameters.[0]
        let body = Expression.LessThan(field.Body, Expression.Constant(value, typeof<'TField>))
        let lambda = Expression.Lambda<Func<'T, bool>>(body, parameter)
        filters <- lambda :: filters
        this

    /// <summary>
    /// Adds an "in" filter, which checks if a field's value is in a given set of values.
    /// </summary>
    member this.In<'TField>(field: Expression<Func<'T, 'TField>>, values: IEnumerable<'TField>) : FilterDefinitionBuilder<'T> =
        let parameter = field.Parameters.[0]
        let expressions =
            values
            |> Seq.map (fun value ->
                Expression.Equal(field.Body, Expression.Constant(value, typeof<'TField>))
            )
            |> Seq.toList

        let combinedBody =
            expressions
            |> List.reduce (fun acc expr -> Expression.OrElse(acc, expr))

        let lambda = Expression.Lambda<Func<'T, bool>>(combinedBody, parameter)
        filters <- lambda :: filters
        this

    /// <summary>
    /// Adds an equality filter using a string field name.
    /// </summary>
    member this.Eq<'TField>(field: string, value: 'TField) : FilterDefinitionBuilder<'T> =
        this.Eq<'TField>(Helper.getPropertyExpression (field), value)

    /// <summary>
    /// Adds a greater-than filter using a string field name.
    /// </summary>
    member this.Gt<'TField when 'TField :> IComparable>(field: string, value: 'TField) : FilterDefinitionBuilder<'T> =
        this.Gt<'TField>(Helper.getPropertyExpression (field), value)

    /// <summary>
    /// Adds a less-than filter using a string field name.
    /// </summary>
    member this.Lt<'TField when 'TField :> IComparable>(field: string, value: 'TField) : FilterDefinitionBuilder<'T> =
        this.Lt<'TField>(Helper.getPropertyExpression (field), value)

    /// <summary>
    /// Adds an "in" filter using a string field name.
    /// </summary>
    member this.In<'TField>(field: string, values: IEnumerable<'TField>) : FilterDefinitionBuilder<'T> =
        this.In<'TField>(Helper.getPropertyExpression (field), values)

    /// <summary>
    /// Combines all registered filters into a single LINQ expression using 'AndAlso'.
    /// </summary>
    member this.Build() : Expression<Func<'T, bool>> =
        if filters.IsEmpty then
            Expression.Lambda<Func<'T, bool>>(Expression.Constant(true), Expression.Parameter(typeof<'T>, "x"))
        else
            let parameter = Expression.Parameter(typeof<'T>, "x")
            let combined =
                filters
                |> List.reduce (fun acc filter ->
                    Expression.AndAlso(acc.Body, Expression.Invoke(filter, parameter))
                    |> Expression.Lambda<Func<'T, bool>>)
            combined

    /// <summary>
    /// Allows the builder to be implicitly converted to its resulting expression.
    /// </summary>
    static member op_Implicit(builder: FilterDefinitionBuilder<'T>) : Expression<Func<'T, bool>> =
        builder.Build()

/// <summary>
/// A fluent builder for creating query expressions. This is a wrapper around <see cref="FilterDefinitionBuilder{T}"/>.
/// </summary>
/// <typeparam name="T">The type of the document to query.</typeparam>
type QueryDefinitionBuilder<'T>() =
    /// <summary>
    /// The underlying filter builder instance.
    /// </summary>
    let filterBuilder = new FilterDefinitionBuilder<'T>()

    /// <summary>
    /// Gets an empty query that matches all documents.
    /// </summary>
    member this.Empty = filterBuilder.Empty

    /// <summary>
    /// Adds an equality query condition (field == value).
    /// </summary>
    member this.EQ<'TField>(field: Expression<Func<'T, 'TField>>, value: 'TField) : QueryDefinitionBuilder<'T> =
        filterBuilder.Eq<'TField>(field, value) |> ignore
        this

    /// <summary>
    /// Adds a greater-than query condition (field > value).
    /// </summary>
    member this.GT<'TField when 'TField :> IComparable>(field: Expression<Func<'T, 'TField>>, value: 'TField) : QueryDefinitionBuilder<'T> =
        filterBuilder.Gt<'TField>(field, value) |> ignore
        this

    /// <summary>
    /// Adds a less-than query condition (field &lt; value).
    /// </summary>
    member this.LT<'TField when 'TField :> IComparable>(field: Expression<Func<'T, 'TField>>, value: 'TField) : QueryDefinitionBuilder<'T> =
        filterBuilder.Lt<'TField>(field, value) |> ignore
        this

    /// <summary>
    /// Adds an "in" query condition.
    /// </summary>
    member this.IN<'TField>(field: Expression<Func<'T, 'TField>>, values: IEnumerable<'TField>) : QueryDefinitionBuilder<'T> =
        filterBuilder.In<'TField>(field, values) |> ignore
        this

    /// <summary>
    /// Adds an equality query condition using a string field name.
    /// </summary>
    member this.EQ<'TField>(field: string, value: 'TField) : QueryDefinitionBuilder<'T> =
        filterBuilder.Eq<'TField>(field, value) |> ignore
        this

    /// <summary>
    /// Adds a greater-than query condition using a string field name.
    /// </summary>
    member this.GT<'TField when 'TField :> IComparable>(field: string, value: 'TField) : QueryDefinitionBuilder<'T> =
        filterBuilder.Gt<'TField>(field, value) |> ignore
        this

    /// <summary>
    /// Adds a less-than query condition using a string field name.
    /// </summary>
    member this.LT<'TField when 'TField :> IComparable>(field: string, value: 'TField) : QueryDefinitionBuilder<'T> =
        filterBuilder.Lt<'TField>(field, value) |> ignore
        this

    /// <summary>
    /// Adds an "in" query condition using a string field name.
    /// </summary>
    member this.IN<'TField>(field: string, values: IEnumerable<'TField>) : QueryDefinitionBuilder<'T> =
        filterBuilder.In<'TField>(field, values) |> ignore
        this

    /// <summary>
    /// Combines all query conditions into a single LINQ expression.
    /// </summary>
    member this.Build() : Expression<Func<'T, bool>> =
        filterBuilder.Build()

    /// <summary>
    /// Allows the builder to be implicitly converted to its resulting expression.
    /// </summary>
    static member op_Implicit(builder: QueryDefinitionBuilder<'T>) : Expression<Func<'T, bool>> =
        builder.Build()

/// <summary>
/// Provides static access to filter and query builders, mimicking the MongoDB driver's `Builders` class.
/// </summary>
[<Sealed>]
type Builders<'T> =
    /// <summary>
    /// Gets a new <see cref="FilterDefinitionBuilder{T}"/>.
    /// </summary>
    static member Filter with get() = FilterDefinitionBuilder<'T> ()

    /// <summary>
    /// Gets a new <see cref="QueryDefinitionBuilder{T}"/>.
    /// </summary>
    static member Query with get() = QueryDefinitionBuilder<'T> ()
