namespace SoloDatabase

/// Row-dependent auxiliary arguments use the same validation as acquired parameters.
module internal SearchFunctions =
    let register (connection: NativeSqliteConnection) =
        connection.RegisterManagedScalar("SoloDB_SearchMarker",2,
            RawSqliteFunctions.RawScalarFunc(fun context arguments ->
                let value = if arguments.[0].IsNull then null else arguments.[0].CopyToString()
                context.SetText(SearchLiteral.Marker(value,arguments.[1].CopyToString()))),
            RawSqliteFunctions.SqliteFunctionFlags.Deterministic)
        connection.RegisterManagedScalar("SoloDB_SearchTokens",1,
            RawSqliteFunctions.RawScalarFunc(fun context arguments ->
                context.SetInt32(SearchLiteral.Tokens(arguments.[0].GetInt32()))),
            RawSqliteFunctions.SqliteFunctionFlags.Deterministic)
