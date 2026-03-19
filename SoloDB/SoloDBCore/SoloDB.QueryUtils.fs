namespace SoloDatabase

open System.Linq

/// <summary>
/// Module containing query utility functions extracted from SoloDB static members.
/// Uses direct provider type check instead of Aggregate sentinel hack.
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
            p.GetExplainQueryPlan(query.Expression)
        | _ -> "Query provider is not a SoloDB provider — cannot explain query plan."

    /// <summary>
    /// Translates the provided LINQ query into its corresponding SQL statement.
    /// </summary>
    /// <param name="query">The LINQ query to translate.</param>
    /// <returns>The generated SQL string.</returns>
    let getSQL (query: IQueryable<'T>) =
        match query.Provider with
        | :? ISoloDBCollectionQueryProvider as p ->
            p.TranslateToSQL(query.Expression)
        | _ -> "Query provider is not a SoloDB provider — cannot translate to SQL."
