namespace SoloDatabase

open System
open System.Linq
open System.Linq.Expressions

[<Sealed>]
type SearchHit<'T>(item: 'T, score: double) =
    member _.Item = item
    member _.Score = score
    member _.Text(field: Expression<Func<'T,string>>) : string =
        ignore field
        invalidOp "Text belongs in a database query projection; it cannot run on a materialized hit."
    member _.Snippet(field: Expression<Func<'T,string>>, opening: string, closing: string, tokens: int) : string =
        ignore field; ignore opening; ignore closing; ignore tokens
        invalidOp "Snippet belongs in a database query projection; it cannot run on a materialized hit."

type SearchQueryKind =
    | AllWords = 0
    | Phrase = 1
    | Prefix = 2
    | TypeAhead = 3
    | LiteralChunks = 4
    | Fts5Syntax = 5

[<Sealed>]
type SearchQuery private (kind: SearchQueryKind, text: string) =
    do
        if isNull text then nullArg "text"
        if kind <> SearchQueryKind.AllWords && kind <> SearchQueryKind.TypeAhead && text.IndexOf('\000') >= 0 then
            invalidArg "text" "This query kind cannot represent NUL."
    member _.Kind = kind
    member _.Text = text
    static member AllWords(text) = SearchQuery(SearchQueryKind.AllWords,text)
    static member Phrase(text) = SearchQuery(SearchQueryKind.Phrase,text)
    static member Prefix(text) = SearchQuery(SearchQueryKind.Prefix,text)
    static member TypeAhead(text) = SearchQuery(SearchQueryKind.TypeAhead,text)
    static member LiteralChunks(text) = SearchQuery(SearchQueryKind.LiteralChunks,text)
    static member Fts5Syntax(text) = SearchQuery(SearchQueryKind.Fts5Syntax,text)

type internal SearchLiteral =
    static let metadata = lazy (
        QueryValueMetadata.RegisterCall<string>(typeof<SearchLiteral>, "AllWords", [||], [|typeof<string>|],
            Func<obj,obj[],string>(fun _ values -> SearchLiteral.AllWords(unbox values.[0])))
        QueryValueMetadata.RegisterCall<string>(typeof<SearchLiteral>, "Format", [||], [|typeof<SearchQuery>|],
            Func<obj,obj[],string>(fun _ values -> SearchLiteral.Format(unbox values.[0])))
        QueryValueMetadata.RegisterCall<string>(typeof<SearchLiteral>, "Marker", [||], [|typeof<string>;typeof<string>|],
            Func<obj,obj[],string>(fun _ values -> SearchLiteral.Marker(unbox values.[0],unbox values.[1])))
        QueryValueMetadata.RegisterCall<int>(typeof<SearchLiteral>, "Tokens", [||], [|typeof<int>|],
            Func<obj,obj[],int>(fun _ values -> SearchLiteral.Tokens(unbox values.[0]))))
    static member EnsureMetadata() = metadata.Force()
    static member Marker(value: string, name: string) =
        if isNull value then nullArg name
        value
    static member Tokens(value: int) =
        if value < 1 || value > 64 then invalidArg "tokens" "Snippet token counts must be between 1 and 64."
        value
    static member AllWords(text: string) =
        if isNull text then nullArg "text"
        let chunks = text.Replace('\000',' ').Split((null: char[]), StringSplitOptions.RemoveEmptyEntries)
        if chunks.Length = 0 then "\"\"" else
        chunks |> Array.map (fun text -> "\"" + text.Replace("\"","\"\"") + "\"") |> String.concat " "
    static member Format(query: SearchQuery) =
        if isNull (box query) then nullArg "query"
        let quote (text: string) = "\"" + text.Replace("\"","\"\"") + "\""
        match query.Kind with
        | SearchQueryKind.AllWords -> SearchLiteral.AllWords query.Text
        | SearchQueryKind.Phrase -> quote query.Text
        | SearchQueryKind.Prefix -> if query.Text.Length = 0 then "\"\"" else quote query.Text + "*"
        | SearchQueryKind.LiteralChunks -> SearchLiteral.AllWords query.Text
        | SearchQueryKind.TypeAhead ->
            let chunks = query.Text.Replace('\000',' ').Split((null: char[]),StringSplitOptions.RemoveEmptyEntries)
            chunks |> Array.mapi (fun i text -> quote text + (if i = chunks.Length-1 then "*" else "")) |> String.concat " "
            |> fun text -> if text.Length = 0 then "\"\"" else text
        | SearchQueryKind.Fts5Syntax -> query.Text
        | _ -> invalidArg "query" "Unknown search query kind."

type internal SearchSql =
    static member Score(_item: obj, _index: string) : double = invalidOp "Search SQL scope is required."
    static member Rank(_item: obj) : double = invalidOp "Search SQL scope is required."
    static member Identity(_item: obj) : int64 = invalidOp "Search SQL scope is required."

type internal SearchMarker =
    static member Query<'T>(_source: IQueryable<'T>, _index: string, _match: string, _selector: Expression<Func<'T,SearchHit<'T>>>) : IQueryable<SearchHit<'T>> =
        invalidOp "Search markers are translated by the query provider."

type internal ISearchOwner =
    abstract Owner: obj
    abstract InTransaction: bool
    abstract Database: string
    abstract RootExpression: Expression
    abstract Build: Expression * Expression * Expression -> Expression
