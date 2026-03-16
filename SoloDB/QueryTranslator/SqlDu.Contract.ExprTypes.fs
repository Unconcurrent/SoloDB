namespace SqlDu.Engine.C1.Spec
type SelectComposition =
    | SingleSelect
    | UnionAll

type SqlExpr =
    | Column of sourceAlias: string option * column: string
    | Literal of SqlLiteral
    | Parameter of name: string
    | JsonExtractExpr of sourceAlias: string option * column: string * path: JsonPath
    | JsonRootExtract of sourceAlias: string option * column: string
    | JsonSetExpr of target: SqlExpr * assignments: (JsonPath * SqlExpr) list
    | JsonArrayExpr of elements: SqlExpr list
    | JsonObjectExpr of properties: (string * SqlExpr) list
    | FunctionCall of name: string * arguments: SqlExpr list
    | AggregateCall of kind: AggregateKind * argument: SqlExpr option * distinct: bool * separator: SqlExpr option
    | WindowCall of WindowSpec
    | Unary of UnaryOperator * SqlExpr
    | Binary of SqlExpr * BinaryOperator * SqlExpr
    | Between of SqlExpr * lower: SqlExpr * upper: SqlExpr
    | InList of SqlExpr * head: SqlExpr * tail: SqlExpr list
    | InSubquery of SqlExpr * SqlSelect
    | Cast of SqlExpr * sqlType: string
    | Coalesce of head: SqlExpr * tail: SqlExpr list
    | Exists of SqlSelect
    | ScalarSubquery of SqlSelect
    | CaseExpr of firstBranch: (SqlExpr * SqlExpr) * restBranches: (SqlExpr * SqlExpr) list * elseExpr: SqlExpr option
    | UpdateFragment of path: SqlExpr * value: SqlExpr
and WindowSpec = {
    Kind: WindowFunctionKind
    Arguments: SqlExpr list
    PartitionBy: SqlExpr list
    OrderBy: (SqlExpr * SortDirection) list
}
and TableSource =
    | BaseTable of table: string * alias: string option
    | DerivedTable of query: SqlSelect * alias: string
    | FromJsonEach of valueExpr: SqlExpr * alias: string option
and Projection = {
    Alias: string option
    Expr: SqlExpr
}
and JoinShape =
    | CrossJoin of source: TableSource
    | ConditionedJoin of kind: JoinKind * source: TableSource * onExpr: SqlExpr
and OrderBy = {
    Expr: SqlExpr
    Direction: SortDirection
}
and ProjectionSet =
    | AllColumns
    | Explicit of head: Projection * tail: Projection list
and SelectCore = {
    Source: TableSource option
    Joins: JoinShape list
    Projections: ProjectionSet
    Where: SqlExpr option
    GroupBy: SqlExpr list
    Having: SqlExpr option
    OrderBy: OrderBy list
    Limit: SqlExpr option
    Offset: SqlExpr option
    Distinct: bool
}
and SelectBody =
    | SingleSelect of SelectCore
    | UnionAllSelect of head: SelectCore * tail: SelectCore list
and CteBinding = {
    Name: string
    Query: SqlSelect
}
and SqlSelect = {
    Ctes: CteBinding list
    Body: SelectBody
}

module internal ProjectionSetOps =
    let toList =
        function
        | AllColumns -> []
        | Explicit(head, tail) -> head :: tail

    let ofList =
        function
        | [] -> AllColumns
        | head :: tail -> Explicit(head, tail)

    let isAllColumns =
        function
        | AllColumns -> true
        | Explicit _ -> false

    let map f projections =
        match projections with
        | AllColumns -> AllColumns
        | Explicit(head, tail) -> Explicit(f head, tail |> List.map f)
