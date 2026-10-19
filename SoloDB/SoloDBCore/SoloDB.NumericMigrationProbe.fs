namespace SoloDatabase

open System
open System.Collections.Generic
open NumericMigrationPlan
open NumericMigrationSql
open SoloDatabase.JsonSerializator

/// Select one present numerical value before classifying the collection representation.
module internal NumericMigrationProbe =
    type private RuntimeField = {
        Names: HashSet<string>
        Types: ResizeArray<Type>
    }

    let private parameter (parameters: Dictionary<string, obj>) value =
        let name = "@probe" + string parameters.Count
        parameters.Add(name, value)
        name

    let private query table plan (fields: Dictionary<Selection list, RuntimeField>) =
        if plan.Leaves.Length = 0 && plan.Dynamic.Length = 0 then ValueNone
        else
            let graph = correlated table plan
            let actual = "json_extract(d.Value,w.path||'.\"$type\"')"
            let runtime = ResizeArray<string>()
            for struct (state, declared) in plan.Dynamic do
                let path = plan.Paths[state]
                let field =
                    match fields.TryGetValue path with
                    | true, field -> field
                    | _ ->
                        let names = HashSet<string>(StringComparer.Ordinal)
                        names.Add(declared.FullName) |> ignore
                        let field = { Names = names; Types = ResizeArray<Type>() }
                        fields.Add(path, field)
                        field
                let fallback = parameter graph.Parameters (box declared.FullName)
                let names = field.Names |> Seq.map (box >> parameter graph.Parameters) |> String.concat ","
                runtime.Add("(w.state=" + string state + " AND coalesce(" + actual + "," + fallback +
                            ") NOT IN (" + names + "))")

            let numerical = "w.state IN (SELECT state FROM leaves)"
            let candidate =
                if runtime.Count = 0 then numerical
                else numerical + " OR (w.type='object' AND (" + String.Join(" OR ", runtime) + "))"
            let sample =
                "(WITH RECURSIVE " + walk table plan true +
                " SELECT json_array(w.state,CASE WHEN " + numerical + " THEN w.type ELSE 'runtime' END," +
                actual + ") FROM walk w WHERE w.type<>'null' AND (" + candidate + ") LIMIT 1)"
            let sql =
                graph.Prefix +
                "SELECT json_extract(sample,'$[0]'),json_extract(sample,'$[1]'),json_extract(sample,'$[2]') " +
                "FROM (SELECT " + sample + " AS sample FROM " + quoted table +
                " d) WHERE sample IS NOT NULL LIMIT 1"
            ValueSome { graph with Prefix = sql }

    let sample (connection: NativeSqliteConnection) table target =
        let fields = Dictionary<Selection list, RuntimeField>()
        let resolve (_: Type) path = seq {
            match fields.TryGetValue path with
            | true, field -> yield! field.Types
            | _ -> ()
        }
        let mutable plan = NumericMigrationPlan.create target resolve
        let mutable searching = true
        let mutable result = ValueNone
        while searching do
            match query table plan fields with
            | ValueNone -> searching <- false
            | ValueSome query ->
                let mutable statement = connection.OpenStatement(query.Prefix)
                let mutable runtimeType = ValueNone
                try
                    try
                        statement.BindValues(query.Parameters)
                        if statement.Step() then
                            let state = statement.Row.GetInt32(0)
                            let kind = statement.Row.GetString(1)
                            if kind = "runtime" then
                                runtimeType <- ValueSome(struct (state, statement.Row.GetString(2)))
                            else
                                result <- ValueSome kind
                                searching <- false
                        else searching <- false
                    with error ->
                        statement.Abort(error)
                        reraise()
                finally
                    statement.Dispose()

                match runtimeType with
                | ValueSome(struct (state, name)) ->
                    let struct (_, declared) =
                        plan.Dynamic |> Array.find (fun struct (index, _) -> index = state)
                    let field = fields[plan.Paths[state]]
                    field.Names.Add(name) |> ignore
                    let actual = TypeMetadata.nameToType name
                    if not (isNull actual) && declared.IsAssignableFrom actual then
                        field.Types.Add(actual)
                        plan <- NumericMigrationPlan.create target resolve
                | ValueNone -> ()
        result
