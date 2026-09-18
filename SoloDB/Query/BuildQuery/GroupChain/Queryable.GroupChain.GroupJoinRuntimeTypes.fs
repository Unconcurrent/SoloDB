namespace SoloDatabase
open System
open System.Collections
open System.Collections.Generic
open System.Linq.Expressions
open System.Threading
open Utils
open SoloDatabase
open SoloDatabase.SqlModel

/// Source correlation and expression-binding dependencies for grouped joins.
/// Bare element access retains its specialized scalar projection over that source.
module internal GroupJoinRuntimeTypes =
    type GroupJoinElementKind =
        | FirstLike of orDefault: bool
        | LastLike of orDefault: bool
        | SingleLike of orDefault: bool
        | ElementAtLike of indexExpr: Expression * orDefault: bool
    type GroupJoinElementCall =
        { Call: MethodCallExpression
          Kind: GroupJoinElementKind }
    type GroupJoinRuntime =
        { InnerCtx: QueryContext
          InnerRootTable: string
          InnerSelect: SqlSelect
          OuterAlias: string
          OuterParam: ParameterExpression
          GroupParam: ParameterExpression
          OuterKeyExpr: SqlExpr
          InnerKeySelector: LambdaExpression
          Vars: Dictionary<string, obj>
          TranslateJoinExpr: QueryContext -> string -> Dictionary<string, obj> -> ParameterExpression option -> Expression -> SqlExpr
          MaterializeDiscoveredJoins: ResizeArray<JoinEdge> -> string option -> Collections.Generic.HashSet<string> option -> JoinShape list
          TryTranslateDbRefValueIdKey: ParameterExpression -> string -> Expression -> SqlExpr option
          ReplaceExpression: Expression -> Expression -> Expression -> Expression
          TranslateOuterExpr: Expression -> SqlExpr }
