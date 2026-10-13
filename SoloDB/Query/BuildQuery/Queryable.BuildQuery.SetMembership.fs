namespace SoloDatabase

open System
open System.Linq.Expressions
open System.Collections.Generic
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryableHelperBase
open SoloDatabase.QueryableHelperJoin
open SoloDatabase.QueryableHelperPreprocess
open SoloDatabase.QueryableHelperState
open SoloDatabase.QueryableLayerBuild

/// Default set membership retains the completed left representative and independent right input.
module internal QueryableBuildQuerySetMembership =
    let apply<'T> (sourceCtx: QueryContext) (statements: ResizeArray<SQLSubquery>)
        (translateQueryFn: QueryContext -> Dictionary<string, obj> -> Expression -> TranslatedQuery)
        (m: {| Value: SupportedLinqMethods; OriginalMethod: System.Reflection.MethodInfo; Expressions: Expression array |}) =
        let isExcept = m.Value = SupportedLinqMethods.Except || m.Value = SupportedLinqMethods.ExceptBy
        let byKey = m.Value = SupportedLinqMethods.ExceptBy || m.Value = SupportedLinqMethods.IntersectBy
        let argumentCount = if byKey then 2 else 1
        if m.Expressions.Length <> argumentCount then
            raise (NotSupportedException("Custom set comparers cannot be translated to SQL. Use the default comparer or call AsEnumerable() first."))
        let elementType = m.OriginalMethod.GetGenericArguments().[if byKey then 1 else 0]
        addComplexFinal statements (fun ctx ->
            let rhs = readSoloDBQueryable<'T> m.Expressions[0]
            let right = translateQueryFn sourceCtx ctx.Vars rhs
            let leftInput = QueryTranslatorSerialization.scopeSerializationInput (Some "set_left") OuterLayer ctx.Input
            let rows = QueryProduced.completedRows sourceCtx.NextAlias ctx.Inner ctx.Input
            let fields = [{ Alias = Some "Id"; Expr = QueryDefaultEquality.outputIdentity (m.OriginalMethod.GetGenericArguments()[0]) leftInput (SqlExpr.Column(Some "set_left", "Id")) }
                          { Alias = Some "Value"; Expr = SqlExpr.Column(Some "set_left", "Value") }
                          { Alias = Some "_tree_ordinal"; Expr = SqlExpr.Column(Some "set_left", "_tree_ordinal") }]
                         @ QueryTranslatorSerialization.companionProjections (Some "set_left") ctx.Input
            let keyName = "_set_key" + sourceCtx.NextAlias()
            let inputCore = mkCore (fields @ [{ Alias = Some keyName; Expr = SqlExpr.Literal SqlLiteral.Null }]) (Some(DerivedTable(rows, "set_left")))
            let struct(prepared, keyInput) =
                if byKey then
                    QueryTranslator.publishSelector { sourceCtx with SerializationInput = ctx.Input }
                        "set_left" m.Expressions[1] ctx.Vars keyName inputCore
                else
                    QueryProduced.publishSlot sourceCtx.NextAlias keyName inputCore
                        (DirectValue(rootValue elementType OuterLayer (Some "set_left") "Value", leftInput)) false
            let alias = "set_prepared"
            let keyInput = QueryTranslatorSerialization.scopeSerializationInput (Some alias) OuterLayer keyInput
            let leftKey = QueryDefaultEquality.key sourceCtx.NextAlias elementType (SqlExpr.Column(Some alias, keyName)) keyInput
            let rightInput = QueryTranslatorSerialization.scopeSerializationInput (Some "set_right") OuterLayer right.SerializationInput
            let rightKey = QueryDefaultEquality.key sourceCtx.NextAlias elementType (rootValue elementType OuterLayer (Some "set_right") "Value") rightInput
            let matches = QueryDefaultEquality.membership leftKey rightKey
                              (mkCore [{ Alias = Some "Value"; Expr = rightKey.Value }] (Some(DerivedTable(right.Query, "set_right"))))
            let predicate = if isExcept then SqlExpr.Unary(UnaryOperator.Not, matches) else matches
            let fields = fields |> List.map (fun field -> { field with Expr = SqlExpr.Column(Some alias, field.Alias.Value) })
            let result = QueryDefaultEquality.firstRows sourceCtx.NextAlias
                             { mkCore fields (Some(DerivedTable(prepared, alias))) with Where = Some predicate }
                             leftKey [SqlExpr.Column(Some alias, "_tree_ordinal"), SortDirection.Asc]
            { Query = result; SerializationInput = ctx.Input; KeyInput = ValueNone })
