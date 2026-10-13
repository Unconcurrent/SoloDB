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
          InnerSelect: SqlSelect
          GroupParam: ParameterExpression
          OuterKeyExpr: SqlExpr
          InnerParameter: ParameterExpression
          InnerKeyName: string
          Vars: Dictionary<string, obj>
          ReplaceExpression: Expression -> Expression -> Expression -> Expression }
