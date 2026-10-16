namespace SoloDatabase

open System.Collections.Generic
open System.Text
open NumericMigrationPlan

/// Shared SQL graph text for complete rewrites and document-local recursive probes.
module internal NumericMigrationSql =
    type Query = {
        Prefix: string
        Parameters: Dictionary<string, obj>
    }

    let quoted (name: string) = "\"" + name.Replace("\"","\"\"") + "\""

    let private definitions plan =
        let parameters = Dictionary<string, obj>()
        let text = StringBuilder("edges(source,target,selection,label,tag) AS (")

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

        text.Append(')') |> ignore
        { Prefix = text.ToString(); Parameters = parameters }

    let walk table plan correlated =
        let text = StringBuilder("walk(rowid,state,path,type) AS (")
        let rowId, value, source, lookup =
            if correlated then
                "d.Id", "d.Value", "", ""
            else
                "Id", "Value", " FROM " + quoted table,
                " JOIN " + quoted table + " d ON d.Id=w.rowid"

        text.Append("SELECT ").Append(rowId).Append(',').Append(plan.Root)
            .Append(",'$',json_type(").Append(value).Append(')').Append(source) |> ignore
        text.Append(" UNION SELECT w.rowid,e.target,j.fullkey,j.type FROM walk w ")
            .Append("JOIN edges e ON e.source=w.state").Append(lookup)
            .Append(" JOIN json_each(d.Value,w.path) j WHERE ") |> ignore
        text.Append("(w.type='object' AND ((e.selection=1 AND j.key=e.label) OR e.selection=4 OR ")
            .Append("(e.selection=5 AND j.key=e.label AND json_extract(d.Value,w.path||'.Tag')=e.tag))) OR ")
            .Append("(w.type='array' AND ((e.selection=2 AND j.key=e.tag) OR e.selection=3))") |> ignore
        text.Append(" UNION SELECT w.rowid,e.target,w.path,w.type FROM walk w ")
            .Append("JOIN edges e ON e.source=w.state").Append(lookup)
            .Append(" WHERE w.type<>'null' AND ")
            .Append("(e.selection=6 OR (e.selection=7 AND w.type='object' AND ")
            .Append("json_extract(d.Value,w.path||'.\"$type\"')=e.label))) ") |> ignore
        text.ToString()

    let query table plan =
        let graph = definitions plan
        { graph with Prefix = "WITH RECURSIVE " + graph.Prefix + ", " + walk table plan false }

    let correlated table plan =
        let graph = definitions plan
        { graph with Prefix = "WITH RECURSIVE " + graph.Prefix + " " }
