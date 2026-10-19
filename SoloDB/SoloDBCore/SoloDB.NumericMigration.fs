namespace SoloDatabase

open System
open System.Collections.Generic
open NumericMigrationPlan
open NumericMigrationSql

/// Set-based traversal uses declared value roles and the original JSON leaf text.
module internal NumericMigration =
    let private prepare connection table target =
        let actual = Dictionary<Type, HashSet<Type>>()
        let actualTypes declared _ =
            match actual.TryGetValue declared with
            | true, types -> types :> Type seq
            | _ -> Seq.empty

        let mutable plan = NumericMigrationPlan.create target actualTypes
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

            if more then plan <- NumericMigrationPlan.create target actualTypes
        plan

    let sample connection table target =
        NumericMigrationProbe.sample connection table target

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
                "changes AS (SELECT rowid,path,kind,row_number() OVER change_order ordinal," +
                "lead(0,1,1) OVER change_order finished FROM matches WINDOW change_order AS (PARTITION BY rowid ORDER BY path)), " +
                "patched(rowid,ordinal,value,finished) AS (SELECT d.Id,0,d.Value,0 FROM changes c JOIN " + quoted table +
                " d ON d.Id=c.rowid WHERE c.ordinal=1 UNION ALL " +
                "SELECT p.rowid,c.ordinal,jsonb_set(p.value,c.path,NUMERIC_NORMALIZE(c.kind,d.Value -> c.path)),c.finished " +
                "FROM patched p JOIN changes c ON c.rowid=p.rowid AND c.ordinal=p.ordinal+1 JOIN " + quoted table +
                " d ON d.Id=p.rowid) UPDATE " + quoted table + " AS target " +
                "SET Value=p.value FROM patched p WHERE p.finished=1 AND target.Id=p.rowid"

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
