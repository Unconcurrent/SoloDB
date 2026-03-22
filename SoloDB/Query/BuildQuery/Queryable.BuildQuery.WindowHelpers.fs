namespace SoloDatabase

open SqlDu.Engine.C1.Spec

module internal QueryableBuildQueryWindowHelpers =
    let rowNumberExpr (partitionBy: SqlExpr list) (orderBy: (SqlExpr * SortDirection) list) =
        SqlExpr.WindowCall({
            Kind = WindowFunctionKind.RowNumber
            Arguments = []
            PartitionBy = partitionBy
            OrderBy = orderBy
        })

    let rowNumberOver (orderBy: (SqlExpr * SortDirection) list) =
        rowNumberExpr [] orderBy

    let rowNumberByKey (keyExpr: SqlExpr) (orderBy: (SqlExpr * SortDirection) list) =
        rowNumberExpr [keyExpr] orderBy
