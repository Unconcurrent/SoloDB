namespace SoloDatabase

open System
open System.Collections.Generic
open System.Text
open SoloDatabase.Metadata.Generation
open SoloDatabase.NativeSQLiteTools
open NumericMigrationPlan

/// Set-based traversal uses declared value roles and the original JSON leaf text.
module internal NumericMigration =
    type private Query = {
        Prefix: string
        Parameters: Dictionary<string, obj>
    }

    let private quoted (name: string) = "\"" + name.Replace("\"","\"\"") + "\""

    let private query table plan =
        let parameters = Dictionary<string, obj>()
        let text = StringBuilder("WITH RECURSIVE edges(source,target,selection,label,tag) AS (")

        if plan.Edges.Length = 0 then
            text.Append("SELECT 0,0,0,NULL,NULL WHERE 0") |> ignore
        else
            text.Append("VALUES ") |> ignore
            for index = 0 to plan.Edges.Length - 1 do
                if index <> 0 then text.Append(',') |> ignore

                let edge = plan.Edges[index]
                let selection, label, tag =
                    match edge.Selection with
                    | Member name -> 1, Some name, None
                    | Index index -> 2, None, Some index
                    | Elements -> 3, None, None
                    | Values -> 4, None, None
                    | CaseField(tag, name) -> 5, Some name, Some tag
                    | SameValue -> 6, None, None
                    | ActualType name -> 7, Some name, None

                text.Append('(').Append(edge.Source).Append(',')
                    .Append(edge.Target).Append(',').Append(selection).Append(',') |> ignore

                match label with
                | Some name ->
                    let parameter = "@edge" + string index
                    parameters.Add(parameter, box name)
                    text.Append(parameter) |> ignore
                | None -> text.Append("NULL") |> ignore

                let tagSql = match tag with Some value -> string value | None -> "NULL"
                text.Append(',').Append(tagSql).Append(')') |> ignore

        text.Append("), leaves(state,kind) AS (") |> ignore
        if plan.Leaves.Length = 0 then
            text.Append("SELECT 0,0 WHERE 0") |> ignore
        else
            text.Append("VALUES ") |> ignore
            for index = 0 to plan.Leaves.Length - 1 do
                if index <> 0 then text.Append(',') |> ignore
                let struct (state, kind) = plan.Leaves[index]
                text.Append('(').Append(state).Append(',').Append(int kind).Append(')') |> ignore

        text.Append("), walk(rowid,state,path,type) AS (") |> ignore
        text.Append("SELECT Id,").Append(plan.Root).Append(",'$',json_type(Value) FROM ").Append(quoted table) |> ignore
        text.Append(" UNION SELECT w.rowid,e.target,j.fullkey,j.type FROM walk w JOIN edges e ON e.source=w.state JOIN ")
            .Append(quoted table).Append(" d ON d.Id=w.rowid JOIN json_each(d.Value,w.path) j WHERE ") |> ignore
        text.Append("(w.type='object' AND ((e.selection=1 AND j.key=e.label) OR e.selection=4 OR ")
            .Append("(e.selection=5 AND j.key=e.label AND json_extract(d.Value,w.path||'.Tag')=e.tag))) OR ")
            .Append("(w.type='array' AND ((e.selection=2 AND j.key=e.tag) OR e.selection=3))") |> ignore
        text.Append(" UNION SELECT w.rowid,e.target,w.path,w.type FROM walk w JOIN edges e ON e.source=w.state JOIN ")
            .Append(quoted table).Append(" d ON d.Id=w.rowid WHERE w.type<>'null' AND ")
            .Append("(e.selection=6 OR (e.selection=7 AND w.type='object' AND ")
            .Append("json_extract(d.Value,w.path||'.\"$type\"')=e.label))) ") |> ignore

        { Prefix = text.ToString(); Parameters = parameters }

    let private prepare connection table target =
        let actual = Dictionary<Type, HashSet<Type>>()
        let mutable plan = NumericMigrationPlan.create target actual
        let mutable more = plan.Dynamic.Length <> 0

        while more do
            more <- false
            let statements = query table plan
            let states = String.Join(",", plan.Dynamic |> Array.map (fun struct (state, _) -> string state))
            let sql =
                statements.Prefix +
                " SELECT DISTINCT w.state,json_extract(d.Value,w.path||'.\"$type\"') FROM walk w JOIN " +
                quoted table + " d ON d.Id=w.rowid WHERE w.state IN (" + states + ") AND w.type='object' AND " +
                "json_type(d.Value,w.path||'.\"$type\"')='text'"

            let mutable statement = (connection: NativeSqliteConnection).OpenStatement(sql)
            let found = ResizeArray<struct (int * string)>()
            try
                try
                    statement.BindValues(statements.Parameters)
                    while statement.Step() do
                        found.Add(struct (statement.Row.GetInt32(0), statement.Row.GetString(1)))
                with error ->
                    statement.Abort(error)
                    reraise()
            finally
                statement.Dispose()

            for struct (state, name) in found do
                let declared =
                    plan.Dynamic
                    |> Array.find (fun struct (index, _) -> index = state)
                    |> fun struct (_, target) -> target

                let resolved = TypeMetadata.nameToType name
                if not (isNull resolved) && not (declared.Equals resolved) && declared.IsAssignableFrom resolved then
                    let types =
                        match actual.TryGetValue declared with
                        | true, types -> types
                        | _ ->
                            let types = HashSet<Type>()
                            actual.Add(declared, types)
                            types

                    if types.Add resolved then more <- true

            if more then plan <- NumericMigrationPlan.create target actual
        plan

    let sample connection table target =
        let plan = prepare connection table target
        if plan.Leaves.Length = 0 then ValueNone
        else
            let statements = query table plan
            let sql =
                statements.Prefix + " SELECT w.type FROM walk w JOIN leaves l ON l.state=w.state " +
                "WHERE w.type IS NOT NULL AND w.type<>'null' ORDER BY w.type='text' LIMIT 1"

            let mutable statement = connection.OpenStatement(sql)
            try
                try
                    statement.BindValues(statements.Parameters)
                    if statement.Step() then ValueSome(statement.Row.GetString(0)) else ValueNone
                with error ->
                    statement.Abort(error)
                    reraise()
            finally
                statement.Dispose()

    let requireCurrent connection table target =
        let plan = prepare connection table target
        if plan.Leaves.Length <> 0 then
            let statements = query table plan
            let sql =
                statements.Prefix + " SELECT 1 FROM walk w JOIN leaves l ON l.state=w.state " +
                "WHERE w.type IS NOT NULL AND w.type NOT IN ('null','text') LIMIT 1"

            let mutable statement = connection.OpenStatement(sql)
            try
                try
                    statement.BindValues(statements.Parameters)
                    if statement.Step() then
                        raise(InvalidOperationException("A numerical collection must finish migration before it can be opened from an event handler."))
                with error ->
                    statement.Abort(error)
                    reraise()
            finally
                statement.Dispose()

    let migrate connection table target =
        let plan = prepare connection table target
        if plan.Leaves.Length <> 0 then
            let statements = query table plan
            let sql =
                statements.Prefix +
                ", matches AS (SELECT w.rowid,w.path,MIN(l.kind) kind FROM walk w JOIN leaves l ON l.state=w.state " +
                "WHERE w.type IS NOT NULL AND w.type<>'null' GROUP BY w.rowid,w.path), " +
                "changes AS (SELECT rowid,path,kind,row_number() OVER(PARTITION BY rowid ORDER BY path) ordinal FROM matches), " +
                "patched(rowid,ordinal,value) AS (SELECT d.Id,0,d.Value FROM " + quoted table +
                " d WHERE EXISTS(SELECT 1 FROM changes c WHERE c.rowid=d.Id) UNION ALL " +
                "SELECT p.rowid,c.ordinal,jsonb_set(p.value,c.path,NUMERIC_NORMALIZE(c.kind,d.Value -> c.path)) " +
                "FROM patched p JOIN changes c ON c.rowid=p.rowid AND c.ordinal=p.ordinal+1 JOIN " + quoted table +
                " d ON d.Id=p.rowid) UPDATE " + quoted table +
                " SET Value=(SELECT p.value FROM patched p WHERE p.rowid=Id ORDER BY p.ordinal DESC LIMIT 1) " +
                "WHERE Id IN (SELECT rowid FROM changes)"

            let mutable statement = (connection: NativeSqliteConnection).OpenStatement(sql)
            try
                try
                    statement.BindValues(statements.Parameters)
                    while statement.Step() do ()
                with error ->
                    statement.Abort(error)
                    reraise()
            finally
                statement.Dispose()

    let legacy = function
        | ValueNone | ValueSome "text" -> false
        | ValueSome "integer" | ValueSome "real" | ValueSome "object" -> true
        | _ -> raise(FormatException("A stored numerical value must be a number or an encoded numerical key."))
