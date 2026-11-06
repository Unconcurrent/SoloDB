namespace SoloDatabase

open System
open SoloDatabase.SqlModel

/// Bounded search arms select identities and caller keys before fetching outputs.
module internal QuerySearchPage =
    let private select core = { Ctes = []; Body = SingleSelect core }
    let private integer value = Literal(SqlLiteral.Integer value)
    let private normalize = FlattenTransform.normalizeExprQuoting
    let private qualifier name =
        match normalize(Column(Some name,"Id")) with Column(Some name,_) -> name | _ -> name
    let private fromSource name (value: string option) = value.IsNone || value = Some(qualifier name)
    let rec private conjunction = function
        | Binary(left,BinaryOperator.And,right) -> conjunction left @ conjunction right
        | expression -> [expression]
    let private matching table = function
        | FunctionCall(name,[text;Column(None,column)]) when name.Equals("match",StringComparison.OrdinalIgnoreCase) && column = table -> Some text
        | _ -> None
    let private rowConstant expression =
        match expression with Literal _ | Parameter _ -> true | _ -> false
    let private equal left right expression =
        match expression with
        | Binary(a,BinaryOperator.Eq,b) -> (a = left && b = right) || (a = right && b = left)
        | _ -> false

    let private fieldText (index: SearchIndexDescriptor) alias path =
        let value = Column(Some alias,"Value")
        let path = Literal(SqlLiteral.String path)
        CaseExpr((Binary(FunctionCall("json_type",[value;path]),BinaryOperator.Eq,Literal(SqlLiteral.String "text")),
                  FunctionCall("jsonb_extract",[value;path])),[],Some(Literal(SqlLiteral.String "")))

    /// Unwrap only the auxiliary query produced for this exact cursor and row.
    let private auxiliary (index: SearchIndexDescriptor) source text expression =
        match expression with
        | ScalarSubquery { Ctes = []; Body = SingleSelect core }
            when core.Joins.IsEmpty && core.GroupBy.IsEmpty && core.Having.IsNone &&
                 not core.Distinct && core.OrderBy.IsEmpty && core.Offset.IsNone &&
                 core.Limit = Some(integer 1L) ->
            match core.Source,ProjectionSetOps.toList core.Projections,core.Where with
            | Some(BaseTable(table,alias)),[projection],Some predicate ->
                let terms = conjunction(normalize predicate)
                let local = qualifier(defaultArg alias table)
                let id = Column(Some(qualifier source),"Id")
                if table = index.Table && terms.Length = 2 &&
                   (terms |> List.exists(equal (Column(Some local,"rowid")) id)) &&
                   (terms |> List.exists(fun term -> matching index.Table term = Some text)) then
                    match projection.Expr with
                    | FunctionCall("snippet",[Column(None,table);column;opening;closing;ellipsis;tokens])
                        when table = index.Table && ([column;opening;closing;ellipsis;tokens] |> List.forall rowConstant) ->
                        Some projection.Expr
                    | _ -> None
                elif table = index.Table && terms.Length = 1 &&
                     (terms |> List.exists(equal (Column(Some local,"rowid")) id)) then
                    match normalize projection.Expr with
                    | Column(Some alias,name) when alias = local && (index.Definition.Fields |> Array.exists(fun field -> field.Alias = name)) ->
                        Some(Column(Some index.Table,name))
                    | _ -> None
                elif table = index.Collection && terms.Length = 1 &&
                     (terms |> List.exists(equal (Column(Some local,"Id")) id)) then
                    index.Definition.Fields |> Array.tryPick(fun field ->
                        if normalize projection.Expr = fieldText index local field.Path then
                            Some(fieldText index (qualifier source) field.Path)
                        else None)
                else None
            | _ -> None
        | _ -> None

    let rec private page movable nextAlias (indexes: SearchIndexDescriptor list) (core: SelectCore) =
        if core.Limit.IsNone || core.OrderBy.IsEmpty || core.Distinct ||
           not core.GroupBy.IsEmpty || core.Having.IsSome then core else
        match core.Source,core.Joins with
        | Some(DerivedTable({ Ctes = []; Body = SingleSelect inner },alias)),[]
            when inner.Limit.IsNone && inner.Offset.IsNone && not inner.Distinct &&
                 inner.GroupBy.IsEmpty && inner.Having.IsNone ->
            match inner.Source,inner.Joins with
            | Some(BaseTable(table,tableAlias)),[ConditionedJoin(JoinKind.Inner,BaseTable(fts,None),condition)] ->
                let source = qualifier(defaultArg tableAlias table)
                let known = indexes |> List.exists(fun index -> index.Collection = table && index.Table = fts)
                let rowId = Column(Some source,"Id")
                let rec root expression =
                    match normalize expression with
                    | Literal _ | Parameter _ -> true
                    | Column(Some owner,("Id" | "Value")) when owner = source -> true
                    | Column(Some owner,"rank") when owner = fts -> true
                    | JsonArrayExpr values -> List.forall root values
                    | JsonObjectExpr values -> List.forall(snd >> root) values
                    | Unary((UnaryOperator.IsNull | UnaryOperator.Neg),value) -> root value
                    | CaseExpr(first,rest,otherwise) ->
                        List.forall(fun (test,value) -> root test && root value) (first::rest) && Option.forall root otherwise
                    | FunctionCall("jsonb",[Column(Some owner,"Value")]) when owner = source -> true
                    | FunctionCall("jsonb_set",[value;Literal(SqlLiteral.String "$.Id");identity]) -> root value && normalize identity = rowId
                    | _ -> false
                let text = inner.Where |> Option.bind(normalize >> matching fts)
                if not known || text.IsNone || not(rowConstant text.Value) ||
                   not(equal rowId (Column(Some fts,"rowid")) (normalize condition)) ||
                   not(ProjectionSetOps.toList inner.Projections |> List.forall(fun projection -> root projection.Expr)) then core else
                let aliases = AliasRewrite.buildProjectionAliasMap inner
                let compose = function Column _ | JsonExtractExpr _ | JsonRootExtract _ | FunctionCall _ -> true | _ -> false
                let mutable valid = true
                SqlExprCombinators.mapCoreExpressions (fun expression ->
                    if SqlExpr.exists(function
                        | JsonExtractExpr(Some owner,column,_) when qualifier owner = qualifier alias ->
                            aliases |> Map.tryFind column |> Option.exists(compose >> not)
                        | _ -> false) expression then valid <- false
                    expression) id core |> ignore
                if not valid then core else
                // Substitute only this proved, unbounded native search producer.
                // The general optimizer's expression and correlation fences stand.
                let rewritten = SqlExprCombinators.mapCoreExpressions (AliasRewrite.rewriteScopedAliasExpr aliases alias) id core
                let predicate = match rewritten.Where with
                                | None -> inner.Where
                                | Some outer -> Some(Binary(outer,BinaryOperator.And,inner.Where.Value))
                let expanded = { rewritten with Source = inner.Source; Joins = inner.Joins; Where = predicate }
                let planned = page movable nextAlias indexes expanded
                if planned.Limit.IsNone then planned else core
            | _ -> core
        | Some(BaseTable(table,alias)),[ConditionedJoin(JoinKind.Inner,BaseTable(fts,None),condition)] ->
            match indexes |> List.tryFind(fun index -> index.Table = fts && index.Collection = table) with
            | None -> core
            | Some index ->
                let source = qualifier(defaultArg alias table)
                let id = Column(Some source,"Id")
                let join = normalize condition
                let text = core.Where |> Option.bind(fun predicate ->
                    conjunction(normalize predicate) |> List.tryPick(matching fts))
                match text with
                | Some text when rowConstant text && equal id (Column(Some fts,"rowid")) join ->
                    let rec safe expression =
                        let expression = normalize expression
                        if movable table source core expression then true else
                        match expression with
                        | Column(Some alias,("rowid" | "rank")) when alias = fts -> true
                        | Column(Some alias,name) when alias = fts ->
                            index.Definition.Fields |> Array.exists(fun field -> field.Alias = name)
                        | Column(None,name) when name = fts -> true
                        | JsonExtractExpr(alias,"Value",path) when fromSource source alias ->
                            index.Definition.Fields |> Array.exists(fun field -> field.Path = JsonPathOps.text(JsonPathOps.toList path))
                        | JsonArrayExpr fields | RowValueExpr fields -> fields |> List.forall safe
                        | JsonObjectExpr fields -> fields |> List.forall(snd >> safe)
                        | Unary(_,value) -> safe value
                        | Binary(left,operation,right) when
                            (match operation with
                             | BinaryOperator.Eq | BinaryOperator.Ne | BinaryOperator.Lt | BinaryOperator.Le
                             | BinaryOperator.Gt | BinaryOperator.Ge | BinaryOperator.And | BinaryOperator.Or
                             | BinaryOperator.Is | BinaryOperator.IsNot -> true | _ -> false) -> safe left && safe right
                        | Binary(left,BinaryOperator.JsonText,Literal(SqlLiteral.String _)) -> safe left
                        | CaseExpr(first,rest,otherwise) ->
                            ((first::rest) |> List.forall(fun (test,value) -> safe test && safe value)) &&
                            (otherwise |> Option.forall safe)
                        | Coalesce(first,rest) -> (first::rest) |> List.forall safe
                        | FunctionCall("snippet",values) -> values |> List.forall safe
                        | FunctionCall("jsonb_set",[value;Literal(SqlLiteral.String "$.Id");identity]) ->
                            safe value && normalize identity = id
                        | FunctionCall(("json" | "jsonb" | "json_array" | "jsonb_array" | "json_object" | "jsonb_object" | "json_quote"),values) ->
                            values |> List.forall safe
                        | FunctionCall("printf",Literal(SqlLiteral.String "%!.17g")::values) -> values |> List.forall safe
                        | FunctionCall(("json_type" | "jsonb_extract" | "json_extract"),[value;Literal(SqlLiteral.String path)]) ->
                            match normalize value with
                            | Column(alias,"Value") when fromSource source alias ->
                                index.Definition.Fields |> Array.exists(fun field -> field.Path = path)
                            | _ -> safe value
                        | _ -> false
                    let expose expression =
                        expression |> SqlExpr.map(fun node ->
                            auxiliary index source text node |> Option.defaultValue node)
                    let projections = core.Projections |> ProjectionSetOps.map(fun projection ->
                        { projection with Expr = expose projection.Expr })
                    let values = ProjectionSetOps.toList projections
                    if values.IsEmpty || not(values |> List.forall(fun projection -> safe projection.Expr)) ||
                       not(core.OrderBy |> List.forall(fun order ->
                           not(SqlExpr.exists(function AggregateCall _ | WindowCall _ | ScalarSubquery _ | Exists _ | InSubquery _ -> true | _ -> false) order.Expr))) then core else
                    let expensive = values |> List.exists(fun projection ->
                        SqlExpr.exists(function
                            | Column(alias,"Value") when fromSource source alias -> true
                            | JsonExtractExpr(alias,"Value",_) | JsonRootExtract(alias,"Value") when fromSource source alias -> true
                            | FunctionCall("snippet",_) -> true
                            | Column(Some alias,name) when alias = fts ->
                                index.Definition.Fields |> Array.exists(fun field -> field.Alias = name)
                            | _ -> false) projection.Expr)
                    if not expensive then core else
                    let pageAlias = nextAlias()
                    let orderName i = "_search_order_" + string i
                    let keys = core.OrderBy |> List.mapi(fun i order ->
                        { Alias = Some(orderName i); Expr = order.Expr })
                    let rank = Column(Some fts,"rank")
                    let needsRank = values |> List.exists(fun projection ->
                        SqlExpr.exists(fun node -> normalize node = rank) projection.Expr)
                    let rankKey =
                        if not needsRank then None else
                        core.OrderBy |> List.tryFindIndex(fun order -> normalize order.Expr = rank)
                        |> Option.map orderName |> Option.orElse(Some "_search_rank")
                    let rankField =
                        match rankKey with
                        | Some "_search_rank" -> [{Alias = rankKey;Expr = rank}]
                        | _ -> []
                    let ids = { core with Projections = ProjectionSetOps.ofList({Alias = Some "_search_id";Expr = id}::keys @ rankField) }
                    let selectedId = Column(Some pageAlias,"_search_id")
                    let rewrite expression =
                        expression |> SqlExpr.map(fun node ->
                            let isId = match normalize node with Column(alias,"Id") -> fromSource source alias | _ -> false
                            if isId then selectedId else
                            if needsRank && normalize node = rank then Column(Some pageAlias,rankKey.Value) else
                            match core.OrderBy |> List.tryFindIndex(fun order -> normalize order.Expr = normalize node) with
                            | Some i -> Column(Some pageAlias,orderName i)
                            | None -> node)
                    let result = projections |> ProjectionSetOps.map(fun projection ->
                        { Alias = AliasRewrite.projectionAlias projection; Expr = rewrite projection.Expr })
                    let expressions = result |> ProjectionSetOps.toList |> List.map _.Expr
                    let usesDocument = expressions |> List.exists(SqlExpr.exists(function
                        | Column(alias,"Value") | JsonExtractExpr(alias,"Value",_) | JsonRootExtract(alias,"Value") when fromSource source alias -> true
                        | _ -> false))
                    let usesFts = expressions |> List.exists(SqlExpr.exists(function
                        | Column(Some alias,_) when qualifier alias = fts -> true
                        | Column(None,name) when name = fts -> true
                        | _ -> false))
                    let needsMatch = expressions |> List.exists(SqlExpr.exists(function
                        | FunctionCall("snippet",_) -> true
                        | Column(Some alias,"rank") when qualifier alias = fts -> true
                        | _ -> false))
                    let joins = [
                        if usesDocument then
                            CrossJoin(BaseTable(table,alias))
                        if usesFts then
                            CrossJoin(BaseTable(fts,None)) ]
                    // Output fetches depend on the completed page, never on a second
                    // traversal of all MATCH candidates. Candidate joins remain reorderable.
                    let predicates = [
                        if usesDocument then Binary(id,BinaryOperator.Eq,selectedId)
                        if usesFts then Binary(Column(Some fts,"rowid"),BinaryOperator.Eq,selectedId)
                        if usesFts && needsMatch then FunctionCall("match",[text;Column(None,fts)]) ]
                    { core with Source = Some(DerivedTable(select ids,pageAlias)); Joins = joins
                                Projections = result
                                Where = if predicates.IsEmpty then None else Some(List.reduce (fun left right -> Binary(left,BinaryOperator.And,right)) predicates)
                                Limit = None; Offset = None
                                OrderBy = core.OrderBy |> List.mapi(fun i order -> { order with Expr = Column(Some pageAlias,orderName i) }) }
                | _ -> core
        | _ -> core

    let plan movable nextAlias (indexes: SearchIndexDescriptor list) query =
        if List.isEmpty indexes then query else
        let rec expression node = SqlExprCombinators.mapChildren expression selectQuery node
        and core value =
            let mapped = SqlExprCombinators.mapCoreExpressions expression selectQuery value
            page movable nextAlias indexes mapped
        and selectQuery query =
            SqlExprCombinators.mapSelectParts (SqlExprCombinators.mapCores core) selectQuery query
        selectQuery query
