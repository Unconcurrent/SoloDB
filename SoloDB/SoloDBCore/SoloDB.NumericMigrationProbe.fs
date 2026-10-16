namespace SoloDatabase

open System
open System.Collections.Generic
open NumericMigrationPlan
open NumericMigrationSql
open SoloDatabase.JsonSerializator

/// Each branch samples one declared field before its representation is classified.
module internal NumericMigrationProbe =
    type private RuntimeField = {
        Names: HashSet<string>
        Types: ResizeArray<Type>
        mutable HasValue: bool
    }

    let private parameter (parameters: Dictionary<string, obj>) value =
        let name = "@probe" + string parameters.Count
        parameters.Add(name, value)
        name

    let private excludeTypes parameters (declared: Type voption) (names: string[]) actual =
        if names.Length = 0 then ""
        else
            let fallback = parameter parameters (box declared.Value.FullName)
            let names = names |> Array.map (box >> parameter parameters) |> String.concat ","
            "coalesce(" + actual + "," + fallback + ") NOT IN (" + names + ")"

    let private direct table plan parameters (declared: Type voption) state excluded =
        let runtime = declared.IsSome
        let joins = ResizeArray<string>()
        let predicates = ResizeArray<string>()
        let mutable parent: string option = None
        let mutable path = "$"

        let pathSql (value: string) =
            match parent with
            | None -> parameter parameters (box value)
            | Some origin -> origin + " || " + parameter parameters (box (value.Substring(1)))

        for selection in plan.Paths[state] do
            match selection with
            | Member name -> path <- JsonEqualitySupport.appendMember path name
            | Index index -> path <- path + "[" + string index + "]"
            | SameValue -> ()
            | CaseField(tag, name) ->
                let tagPath = pathSql (JsonEqualitySupport.appendMember path "Tag")
                predicates.Add("json_extract(d.Value," + tagPath + ")=" + parameter parameters (box tag))
                path <- JsonEqualitySupport.appendMember path name
            | ActualType name ->
                let typePath = pathSql (JsonEqualitySupport.appendMember path "$type")
                predicates.Add("json_extract(d.Value," + typePath + ")=" + parameter parameters (box name))
            | Elements | Values ->
                let sourcePath = pathSql path
                let alias = "j" + string joins.Count
                let kind = if selection = Elements then "array" else "object"
                joins.Add(" JOIN json_each(d.Value," + sourcePath + ") " + alias +
                          " ON json_type(d.Value," + sourcePath + ")='" + kind + "'")
                parent <- Some(alias + ".fullkey")
                path <- "$"

        let selectedPath = pathSql path
        let kind = "json_type(d.Value," + selectedPath + ")"
        let actual =
            if runtime then
                predicates.Add(kind + "='object'")
                "json_extract(d.Value," + pathSql (JsonEqualitySupport.appendMember path "$type") + ")"
            else
                predicates.Add(kind + "<>'null'")
                "NULL"

        let sample = if runtime then "json_array(" + actual + ")" else kind
        let exclude = excludeTypes parameters declared excluded actual
        if exclude <> "" then predicates.Add(exclude)
        "SELECT " + sample + " FROM " + quoted table + " d" + String.Concat(joins) +
        " WHERE " + String.Join(" AND ", predicates) + " LIMIT 1"

    let private recursive table plan parameters (declared: Type voption) state excluded =
        let runtime = declared.IsSome
        let actual = "json_extract(d.Value,w.path||'.\"$type\"')"
        let selected =
            if runtime then "json_array(" + actual + ")"
            else "w.type"
        let predicate = if runtime then "w.type='object'" else "w.type<>'null'"
        let exclude = excludeTypes parameters declared excluded actual
        let predicate = if exclude = "" then predicate else predicate + " AND " + exclude
        let sample =
            "(WITH RECURSIVE " + walk table plan true +
            " SELECT " + selected + " FROM walk w WHERE w.state=" + string state +
            " AND " + predicate + " LIMIT 1)"
        "SELECT sample FROM (SELECT " + sample +
        " AS sample FROM " + quoted table + " d) WHERE sample IS NOT NULL LIMIT 1"

    let private query table plan (fields: Dictionary<Selection list, RuntimeField>) runtime =
        let graph =
            if plan.Recursive then correlated table plan
            else { Prefix = ""; Parameters = Dictionary<string, obj>() }

        let branch (declared: Type voption) state excluded =
            let runtime = declared.IsSome
            let sql =
                if plan.Recursive then recursive table plan graph.Parameters declared state excluded
                else direct table plan graph.Parameters declared state excluded
            "(" + string state + "," + (if runtime then "1" else "0") + ",(" + sql + "))"

        let fieldAt path =
            match fields.TryGetValue path with
            | true, field -> field
            | _ ->
                let field = { Names = HashSet<string>(StringComparer.Ordinal)
                              Types = ResizeArray<Type>(); HasValue = false }
                fields.Add(path, field)
                field

        for struct (state, _) in plan.Dynamic do
            fieldAt plan.Paths[state] |> ignore

        let branches = [|
            if runtime then
                for struct (state, declared) in plan.Dynamic do
                    let field = fieldAt plan.Paths[state]
                    if field.Names.Count = 0 || not field.HasValue then
                        yield branch (ValueSome declared) state (Seq.toArray field.Names)
            else
                for struct (state, _) in plan.Leaves do
                    yield branch ValueNone state [||]
        |]

        if branches.Length = 0 then ValueNone
        else
            let sql =
                graph.Prefix +
                "SELECT column1,CASE WHEN column2 THEN 'runtime' ELSE column3 END," +
                "CASE WHEN column2 THEN json_extract(column3,'$[0]') ELSE NULL END FROM (VALUES " + String.Join(",", branches) +
                ") WHERE column3 IS NOT NULL"
            ValueSome { graph with Prefix = sql }

    let sample (connection: NativeSqliteConnection) table target =
        let fields = Dictionary<Selection list, RuntimeField>()
        let resolve (declared: Type) path = seq {
            match fields.TryGetValue path with
            | true, field -> yield! field.Types
            | _ -> ()
        }

        let rec within prefix path =
            match prefix, path with
            | [], _ -> true
            | head :: tail, next :: rest when head = next -> within tail rest
            | _ -> false

        let mutable plan = NumericMigrationPlan.create target resolve
        let mutable more = true
        let mutable result = ValueNone

        while more && result.IsNone do
            more <- false
            let runtimeTypes = ResizeArray<struct (int * string option)>()

            // Complete numerical samples before deciding whether another runtime
            // shape is needed. A present value ends that field's type search.
            for phase = 0 to 1 do
                if result.IsNone then
                    match query table plan fields (phase = 1) with
                    | ValueNone -> ()
                    | ValueSome query ->
                        let mutable statement = connection.OpenStatement(query.Prefix)
                        try
                            try
                                statement.BindValues(query.Parameters)
                                while result.IsNone && statement.Step() do
                                    let state = statement.Row.GetInt32(0)
                                    let kind = statement.Row.GetString(1)
                                    if kind <> "runtime" then
                                        for KeyValue(path, field) in fields do
                                            if within path plan.Paths[state] then field.HasValue <- true
                                        if kind <> "text" then result <- ValueSome kind
                                    else
                                        let name =
                                            if statement.Row.IsDBNull(2) then None
                                            else Some(statement.Row.GetString(2))
                                        runtimeTypes.Add(struct (state, name))
                            with error ->
                                statement.Abort(error)
                                reraise()
                        finally
                            statement.Dispose()

            if result.IsNone then
                for struct (state, name) in runtimeTypes do
                    let struct (_, declared) =
                        plan.Dynamic |> Array.find (fun struct (index, _) -> index = state)
                    let field = fields[plan.Paths[state]]
                    let name = defaultArg name declared.FullName
                    if field.Names.Add(name) then
                        let actual = if name = declared.FullName then declared else TypeMetadata.nameToType name
                        if not (isNull actual) && not (declared.Equals actual)
                           && declared.IsAssignableFrom actual then
                            field.Types.Add(actual)
                            more <- true
                        elif not field.HasValue then
                            more <- true

            if more then plan <- NumericMigrationPlan.create target resolve

        result
