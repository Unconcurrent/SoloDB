namespace SoloDatabase

open System
open System.Collections.Generic
open NumericMigrationPlan
open NumericMigrationSql
open SoloDatabase.JsonSerializator

/// Select the first present numerical value independently for each serialized field.
module internal NumericMigrationProbe =
    type private RuntimeField = {
        Names: HashSet<string>
        Types: ResizeArray<Type>
    }

    let private parameter (parameters: Dictionary<string, obj>) value =
        let name = "@probe" + string parameters.Count
        parameters.Add(name, value)
        name

    let private runtimeField (fields: Dictionary<Selection list, RuntimeField>) path (declared: Type) =
        match fields.TryGetValue path with
        | true, field -> field
        | _ ->
            let names = HashSet<string>(StringComparer.Ordinal)
            names.Add(declared.FullName) |> ignore
            let field = { Names = names; Types = ResizeArray<Type>() }
            fields.Add(path, field)
            field

    let private query table plan (fields: Dictionary<Selection list, RuntimeField>) =
        let graph = document table plan
        let actual = "json_extract(d.Value,w.path||'.\"$type\"')"
        let runtime = ResizeArray<string>()
        for struct (state, declared) in plan.Dynamic do
            let field = runtimeField fields plan.Paths[state] declared
            let fallback = parameter graph.Parameters (box declared.FullName)
            let names = field.Names |> Seq.map (box >> parameter graph.Parameters) |> String.concat ","
            runtime.Add("(w.state=" + string state + " AND coalesce(" + actual + "," + fallback +
                        ") NOT IN (" + names + "))")
        let numerical = "w.state IN (SELECT state FROM leaves)"
        let candidate =
            if runtime.Count = 0 then numerical
            else numerical + " OR (w.type='object' AND (" + String.Join(" OR ", runtime) + "))"
        let sql =
            graph.Prefix +
            " SELECT w.state,CASE WHEN " + numerical + " THEN w.type ELSE 'runtime' END," + actual +
            " FROM walk w JOIN " + quoted table + " d ON d.Id=w.rowid " +
            "WHERE w.type<>'null' AND (" + candidate + ")"
        { graph with Prefix = sql }

    let sample (connection: NativeSqliteConnection) table target =
        let fields = Dictionary<Selection list, RuntimeField>()
        let answered = HashSet<Selection list>()
        let resolve (_: Type) path = seq {
            match fields.TryGetValue path with
            | true, field -> yield! field.Types
            | _ -> ()
        }
        let mutable plan = NumericMigrationPlan.create target resolve
        let mutable parents = Array.empty<ResizeArray<int>>
        let mutable known = Array.empty<bool>
        let mutable numerical = Array.empty<bool>
        let mutable declared = Array.empty<Type>
        let mutable pending = Array.empty<bool>
        let mutable pendingDirty = true
        let mutable queryDirty = true
        let mutable remaining = 0
        let mutable undiscovered = false

        let ancestors states =
            let reached = Array.zeroCreate plan.Paths.Length
            let queue = Queue<int>()
            let add state =
                if not reached[state] then
                    reached[state] <- true
                    queue.Enqueue(state)
            for state in states do add state
            while queue.Count <> 0 do
                for parent in parents[queue.Dequeue()] do add parent
            reached

        let prepare () =
            parents <- Array.init plan.Paths.Length (fun _ -> ResizeArray<int>())
            for edge in plan.Edges do parents[edge.Target].Add(edge.Source)
            numerical <- Array.zeroCreate plan.Paths.Length
            remaining <- 0
            for struct (state, _) in plan.Leaves do
                numerical[state] <- true
                if not (answered.Contains plan.Paths[state]) then remaining <- remaining + 1
            known <- ancestors (plan.Leaves |> Seq.map (fun struct (state, _) -> state))
            declared <- Array.zeroCreate plan.Paths.Length
            undiscovered <- false
            for struct (state, target) in plan.Dynamic do
                declared[state] <- target
                if not known[state] then undiscovered <- true
            pendingDirty <- true
            queryDirty <- true

        let pendingStates () =
            if pendingDirty then
                pending <- ancestors (plan.Leaves |> Seq.choose (fun struct (state, _) ->
                    if answered.Contains plan.Paths[state] then None else Some state))
                pendingDirty <- false
            pending

        let relevant state = not known[state] || (pendingStates ())[state]
        let required () = remaining <> 0 || undiscovered
        let buildQuery () =
            let needed = ancestors (seq {
                for struct (state, _) in plan.Leaves do
                    if not (answered.Contains plan.Paths[state]) then yield state
                for struct (state, _) in plan.Dynamic do
                    if relevant state then yield state
            })
            let selected = { plan with
                                Edges = plan.Edges |> Array.filter (fun edge -> needed[edge.Target])
                                Leaves = plan.Leaves |> Array.filter (fun struct (state, _) ->
                                    not (answered.Contains plan.Paths[state]))
                                Dynamic = plan.Dynamic |> Array.filter (fun struct (state, _) -> relevant state) }
            query table selected fields

        prepare ()
        let mutable result = ValueNone
        if required () then
            let mutable documents = connection.OpenStatement("SELECT Id FROM " + quoted table + " ORDER BY Id")
            try
                try
                    let mutable searching = true
                    let mutable currentQuery = ValueNone
                    while searching && required () && documents.Step() do
                        let id = documents.Row.GetInt64(0)
                        let mutable restart = true
                        while searching && required () && restart do
                            restart <- false
                            if queryDirty then
                                currentQuery <- ValueSome(buildQuery ())
                                queryDirty <- false
                            let query = currentQuery.Value
                            query.Parameters["@document"] <- box id
                            let mutable statement = connection.OpenStatement(query.Prefix)
                            try
                                try
                                    statement.BindValues(query.Parameters)
                                    while searching && required () && not restart && statement.Step() do
                                        let state = statement.Row.GetInt32(0)
                                        if numerical[state] then
                                            if answered.Add(plan.Paths[state]) then
                                                let kind = statement.Row.GetString(1)
                                                result <- ValueSome kind
                                                if kind = "text" then
                                                    remaining <- remaining - 1
                                                    pendingDirty <- true
                                                    queryDirty <- true
                                                else searching <- false
                                        elif relevant state then
                                            let name = statement.Row.GetString(2)
                                            let field = fields[plan.Paths[state]]
                                            if field.Names.Add(name) then
                                                let actual = TypeMetadata.nameToType name
                                                if not (isNull actual) && declared[state].IsAssignableFrom actual then
                                                    field.Types.Add(actual)
                                                    restart <- true
                                with error ->
                                    statement.Abort(error)
                                    reraise()
                            finally
                                statement.Dispose()
                            if restart then
                                plan <- NumericMigrationPlan.create target resolve
                                prepare ()
                with error ->
                    documents.Abort(error)
                    reraise()
            finally
                documents.Dispose()
        result
