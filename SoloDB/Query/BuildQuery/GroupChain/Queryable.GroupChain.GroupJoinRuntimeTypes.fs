namespace SoloDatabase
open System
open System.Collections
open System.Collections.Generic
open System.Linq.Expressions
open System.Threading
open Utils
open SoloDatabase
open SqlDu.Engine.C1.Spec
open SoloDatabase.DBRefManyDescriptor

/// The values a GroupJoin chain build carries, and whether a descriptor has chain operations at
/// all. Both the chain emitter and the element builder need these, so they are owned here rather
/// than by whichever of the two happens to be compiled first.
module internal GroupJoinRuntimeTypes =
    type GroupJoinElementKind =
        | FirstLike of orDefault: bool
        | LastLike of orDefault: bool
        | SingleLike of orDefault: bool
        | ElementAtLike of indexExpr: Expression * orDefault: bool
    type GroupJoinElementCall =
        { Call: MethodCallExpression
          Kind: GroupJoinElementKind
          Chain: QueryDescriptor }
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
    let hasQueryDescriptorChainOps (desc: QueryDescriptor) =
        not desc.WherePredicates.IsEmpty
        || not desc.SortKeys.IsEmpty
        || desc.Offset.IsSome
        || desc.Limit.IsSome
        || desc.SelectProjection.IsSome
        || desc.Distinct
        || desc.DefaultIfEmpty.IsSome
        || desc.TakeWhileInfo.IsSome
        || desc.GroupByKey.IsSome
        || not desc.SetOps.IsEmpty
