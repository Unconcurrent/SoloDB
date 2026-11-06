namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseTypes

/// Query-only text methods consume the selected occurrence's search context.
module internal QuerySearchAuxiliary =
    let isMethod (method: Reflection.MethodInfo) =
        not(isNull method.DeclaringType) && method.DeclaringType.IsGenericType &&
        method.DeclaringType.GetGenericTypeDefinition().Equals(typedefof<SearchHit<_>>) &&
        (method.Name = "Text" || method.Name = "Snippet")
    let translate visitProduced (visitDu: Expression -> QueryBuilder -> SqlExpr) (call: MethodCallExpression) (builder: QueryBuilder) =
        let rec lambda (expression: Expression) =
            match expression with
            | :? UnaryExpression as quote when quote.NodeType = ExpressionType.Quote -> lambda quote.Operand
            | :? LambdaExpression as value -> value
            | expression ->
                QueryQuotationSyntax.tryFromExpression expression
                |> ValueOption.defaultWith(fun () -> ExpressionEvaluation.evaluateExpr<LambdaExpression> expression)
        let path,_ = QuerySearchFields.path(lambda call.Arguments.[0])
        let produced = visitProduced call.Object builder
        let markers =
            if call.Method.Name <> "Snippet" then Array.empty else
            [|visitDu call.Arguments.[1] builder
              visitDu call.Arguments.[2] builder
              visitDu call.Arguments.[3] builder|]
        QueryProduced.consume builder.SourceContext.NextAlias (fun _ input ->
            let owner = input.SearchOwner |> ValueOption.defaultWith(fun () ->
                invalidOp "Text and Snippet require a row from Search, inside a query projection.")
            if not owner.Complete then
                invalidOp "Text and Snippet require original query context for every possible hit; materialized or constructed hits do not retain it."
            let field position =
                match owner.Direct with
                | ValueSome direct when owner.Sources.Length = 1 ->
                    match position with
                    | 0 -> Literal(SqlLiteral.String owner.Sources.[0].Key)
                    | 1 -> direct.Owner.Identity
                    | _ -> owner.Sources.[0].Match
                | _ -> FunctionCall("jsonb_extract",[owner.Descriptor;Literal(SqlLiteral.String("$[" + string position + "]"))])
            let row = field 1
            let auxiliary (source: QuerySearchSource) =
                let index = source.Index
                let column = index.Definition.Fields |> Array.tryFindIndex(fun field -> field.Path = path)
                             |> Option.defaultWith(fun () -> invalidArg "selector" $"This field is not declared in search index '{index.Name}'.")
                let table = index.Table
                let rowInvariant = SqlExpr.fold (fun constant node -> constant && (match node with Column _ | ScalarSubquery _ | Exists _ | InSubquery _ -> false | _ -> true)) true source.Match
                let query = if rowInvariant then source.Match else field 2
                let nativeText = Column(Some table,index.Definition.Fields.[column].Alias)
                let text =
                    if call.Method.Name = "Text" then nativeText
                    else FunctionCall("snippet",[Column(None,table);Literal(SqlLiteral.Integer(int64 column));
                                                  markers.[0];markers.[1];Literal(SqlLiteral.String "...");markers.[2]])
                let matching = FunctionCall("match",[query;Column(None,table)])
                let identity = Binary(Column(Some table,"rowid"),BinaryOperator.Eq,row)
                let sourceTable, value, predicate =
                    if call.Method.Name = "Text" && index.Definition.Storage = SearchTextStorage.Shared then
                        let alias = "search_text_" + builder.SourceContext.NextAlias()
                        let value = FunctionCall("jsonb_extract",[Column(Some alias,"Value");Literal(SqlLiteral.String path)])
                        let present = Binary(FunctionCall("json_type",[Column(Some alias,"Value");Literal(SqlLiteral.String path)]),BinaryOperator.Eq,Literal(SqlLiteral.String "text"))
                        BaseTable(index.Collection,Some alias),CaseExpr((present,value),[],Some(Literal(SqlLiteral.String ""))),Binary(Column(Some alias,"Id"),BinaryOperator.Eq,row)
                    else
                        BaseTable(table,None),text,(if call.Method.Name = "Text" then identity else Binary(identity,BinaryOperator.And,matching))
                ScalarSubquery { Ctes = []; Body = SingleSelect {
                    Source = Some sourceTable; Joins = []; Projections = Explicit({ Alias = None; Expr = value },[])
                    Where = Some predicate; GroupBy = []; Having = None; OrderBy = []
                    Limit = Some(Literal(SqlLiteral.Integer 1L)); Offset = None; Distinct = false } }
            let sources = owner.Sources |> Array.map(fun source ->
                Binary(field 0,BinaryOperator.Eq,Literal(SqlLiteral.String source.Key)),auxiliary source)
            if sources.Length = 1 then snd sources.[0]
            elif sources.Length = 0 then invalidOp "A search projection has no original source."
            else CaseExpr(sources.[0],sources |> Array.skip 1 |> Array.toList,Some(Literal SqlLiteral.Null))) produced
