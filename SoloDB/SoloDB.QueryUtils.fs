namespace SoloDatabase

open System.Linq
open Utils

/// <summary>
/// Module containing query utility functions extracted from SoloDB static members.
/// </summary>
module internal QueryUtils =

    /// <summary>
    /// Analyzes the provided LINQ query and returns the query plan that SQLite would use to execute it.
    /// </summary>
    /// <remarks>Calling this function will clear the in-memory cache of prepared SQL commands.</remarks>
    /// <param name="query">The LINQ query to analyze.</param>
    /// <returns>A string describing the query plan.</returns>
    let explainQueryPlan (query: IQueryable<'T>) =
        match query.Provider with
        | :? ISoloDBCollectionQueryProvider as p ->
            match p.AdditionalData with
            | :? SoloDBToCollectionData as data -> data.ClearCacheFunction()
            | _ -> ()
        | _ -> ()
        // This is a hack, I do not think that it is possible to add new IQueryable methods directly.
        query.Aggregate(QueryPlan.ExplainQueryPlanReference, (fun _a _b -> ""))

    /// <summary>
    /// Translates the provided LINQ query into its corresponding SQL statement.
    /// </summary>
    /// <param name="query">The LINQ query to translate.</param>
    /// <returns>The generated SQL string.</returns>
    let getSQL (query: IQueryable<'T>) =
        query.Aggregate(QueryPlan.GetGeneratedSQLReference, (fun _a _b -> ""))
