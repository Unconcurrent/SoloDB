namespace SoloDatabase

open System
open System.Collections.Generic
open System.Text

/// Optional proof for a single ordinary source. SQLite still parses and executes all SQL.
module internal NativeBlobIdentity =
    type private Token =
        | Name of string * quoted: bool
        | Literal
        | Integer
        | Parameter
        | Symbol of string
        | End
    [<Struct>]
    type Projection = { Column: string voption; Label: string }
    type Query = { Bindings: Binding list; Input: Input; Projection: Projection[] }
    and Binding = { Name: string; Columns: string[]; Query: Query }
    and Input = Named of schema: string * name: string | Derived of Query
    // SQLite identifier folding is ASCII only. Unicode case equivalence can
    // otherwise mistake a view or a different column for the physical source.
    let equalIdentifier (left: string) (right: string) =
        if Object.ReferenceEquals(left, right) then true
        elif isNull left || isNull right || left.Length <> right.Length then false
        else
            let fold value = if value >= 'A' && value <= 'Z' then char (int value + 32) else value
            let mutable equal = true
            let mutable index = 0
            while equal && index < left.Length do
                equal <- fold left.[index] = fold right.[index]
                index <- index + 1
            equal
    let private equal left right = equalIdentifier left right
    let private lex (sql: string) =
        let tokens = ResizeArray<Token>()
        let mutable i = 0
        let mutable valid = true
        let add token =
            if tokens.Count >= 2048 then valid <- false else tokens.Add(token)
        let quoted delimiter closing identifier =
            i <- i + 1
            let value = StringBuilder()
            let mutable closed = false
            while valid && not closed && i < sql.Length do
                if sql.[i] = closing then
                    if delimiter <> '[' && i + 1 < sql.Length && sql.[i + 1] = closing then
                        value.Append(closing) |> ignore
                        i <- i + 2
                    else closed <- true; i <- i + 1
                else value.Append(sql.[i]) |> ignore; i <- i + 1
            if not closed then valid <- false
            elif identifier then add (Name(value.ToString(), true))
            else add Literal
        while valid && i < sql.Length do
            let c = sql.[i]
            if c = ' ' || (c >= '\t' && c <= '\r') then i <- i + 1
            elif c = '-' && i + 1 < sql.Length && sql.[i+1] = '-' then
                i <- i + 2
                while i < sql.Length && sql.[i] <> '\n' do i <- i + 1
            elif c = '/' && i + 1 < sql.Length && sql.[i+1] = '*' then
                i <- i + 2
                let finish = sql.IndexOf("*/", i, StringComparison.Ordinal)
                if finish < 0 then valid <- false else i <- finish + 2
            elif c = '\'' then quoted c c false
            elif c = '"' || c = '`' then quoted c c true
            elif c = '[' then quoted c ']' true
            elif (c = 'x' || c = 'X') && i + 1 < sql.Length && sql.[i+1] = '\'' then
                i <- i + 1
                quoted '\'' '\'' false
            elif (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c = '_' || c >= '\u0080' then
                let start = i
                i <- i + 1
                while i < sql.Length && ((sql.[i] >= 'a' && sql.[i] <= 'z') || (sql.[i] >= 'A' && sql.[i] <= 'Z') || (sql.[i] >= '0' && sql.[i] <= '9') || sql.[i] = '_' || sql.[i] = '$' || sql.[i] >= '\u0080') do i <- i + 1
                add (Name(sql.Substring(start, i-start), false))
            elif Char.IsDigit(c) then
                let start = i
                while i < sql.Length && Char.IsDigit(sql.[i]) do i <- i + 1
                let mutable integer = true
                if i < sql.Length && sql.[i] = '.' then
                    integer <- false; i <- i + 1
                    while i < sql.Length && Char.IsDigit(sql.[i]) do i <- i + 1
                if i < sql.Length && (sql.[i] = 'e' || sql.[i] = 'E') then
                    integer <- false; i <- i + 1
                    if i < sql.Length && (sql.[i] = '+' || sql.[i] = '-') then i <- i + 1
                    let exponent = i
                    while i < sql.Length && Char.IsDigit(sql.[i]) do i <- i + 1
                    if exponent = i then valid <- false
                if i = start then valid <- false else add (if integer then Integer else Literal)
            elif c = '?' || c = '@' || c = ':' || c = '$' then
                i <- i + 1
                let start = i
                if c = '?' then
                    while i < sql.Length && Char.IsDigit(sql.[i]) do i <- i + 1
                else
                    while i < sql.Length && (Char.IsLetterOrDigit(sql.[i]) || sql.[i] = '_') do i <- i + 1
                    if i = start then valid <- false
                add Parameter
            else
                let mutable width = 1
                if i + 1 < sql.Length then
                    let pair = sql.Substring(i, 2)
                    if pair = "<=" || pair = ">=" || pair = "<>" || pair = "!=" || pair = "==" then width <- 2
                add (Symbol(sql.Substring(i,width)))
                i <- i + width
        if valid then tokens.Add(End); ValueSome(tokens.ToArray()) else ValueNone

    let private reserved (name: string) =
        match name with
        | null -> true
        | _ when name |> Seq.exists (fun character -> character > '\u007f') -> false
        | _ ->
            match name.ToUpperInvariant() with
            | "SELECT" | "FROM" | "WHERE" | "ORDER" | "BY" | "LIMIT" | "OFFSET" | "GROUP" | "HAVING"
            | "JOIN" | "LEFT" | "RIGHT" | "INNER" | "OUTER" | "CROSS" | "NATURAL" | "FULL" | "ON" | "USING"
            | "UNION" | "INTERSECT" | "EXCEPT" | "WINDOW" | "AS" | "DISTINCT" | "ALL" | "AND" | "OR" | "NOT"
            | "IS" | "IN" | "NULL" | "TRUE" | "FALSE" | "ASC" | "DESC" | "COLLATE" | "INDEXED"
            | "WITH" | "RECURSIVE" | "MATERIALIZED" -> true
            | _ -> false

    /// Recognition is optional and consumes the complete statement. Unknown syntax
    /// never changes SQLite execution; it only declines automatic physical identity.
    let tryQuery (sql: string) =
        match lex sql with
        | ValueNone -> ValueNone
        | ValueSome tokens ->
            let mutable i = 0
            let mutable valid = true
            let token () = tokens.[min i (tokens.Length-1)]
            let advance () = if i < tokens.Length-1 then i <- i + 1 else valid <- false
            let keyword word = match token() with Name(name,false) when equal name word -> true | _ -> false
            let accept word = if keyword word then advance(); true else false
            let symbol text = match token() with Symbol name when name = text -> true | _ -> false
            let take text = if symbol text then advance(); true else false
            let require accepted = if not accepted then valid <- false
            let name () =
                match token() with
                | Name(value, quoted) when quoted || not (reserved value) -> advance(); value
                | _ -> valid <- false; ""
            let columns () =
                if take "(" then
                    let names = ResizeArray<string>()
                    names.Add(name())
                    while valid && take "," do names.Add(name())
                    require (take ")")
                    names.ToArray()
                else Array.empty
            let rec query depth =
                if depth > 32 then valid <- false
                let bindings = ResizeArray<Binding>()
                if accept "WITH" then
                    if accept "RECURSIVE" then valid <- false
                    let binding () =
                        let label = name()
                        let labels = columns()
                        require (accept "AS")
                        // Materialization does not change the forwarded row identity.
                        if not (accept "MATERIALIZED") && accept "NOT" then require (accept "MATERIALIZED")
                        require (take "(")
                        let body = if valid then query (depth+1) else Unchecked.defaultof<Query>
                        require (take ")")
                        if bindings |> Seq.exists (fun prior -> equal prior.Name label) then valid <- false
                        bindings.Add({Name=label;Columns=labels;Query=body})
                    binding()
                    while valid && take "," do binding()
                let references = ResizeArray<string[]>()
                let projection = ResizeArray<Projection>()
                let column wildcard =
                    let parts = ResizeArray<string>()
                    let mutable star = false
                    if wildcard && take "*" then parts.Add("*"); star <- true
                    else
                        parts.Add(name())
                        while valid && take "." do
                            if wildcard && take "*" then parts.Add("*"); star <- true else parts.Add(name())
                            if star && symbol "." then valid <- false
                            if parts.Count > 3 then valid <- false
                    let result = parts.ToArray()
                    references.Add(result)
                    if star then ValueNone else ValueSome result.[result.Length - 1]
                let alias fallback =
                    if accept "AS" then name()
                    else match token() with Name(value, quoted) when quoted || not (reserved value) -> name() | _ -> fallback
                let literal integerOnly =
                    if not (take "+") then take "-" |> ignore
                    match token() with
                    | Integer | Parameter -> advance(); true
                    | Literal when not integerOnly -> advance(); true
                    | Name(value,false) when not integerOnly && (equal value "NULL" || equal value "TRUE" || equal value "FALSE") -> advance(); true
                    | _ -> false
                let atom () = if not (literal false) then column false |> ignore
                let rec predicate depth =
                    if depth > 32 then valid <- false
                    elif accept "NOT" then predicate (depth+1)
                    elif take "(" then disjunction (depth+1); require (take ")")
                    else
                        atom()
                        match token() with
                        | Symbol(value) when value = "=" || value = "==" || value = "!=" || value = "<>" || value = "<" || value = ">" || value = "<=" || value = ">=" -> advance(); atom()
                        | _ when accept "IS" -> accept "NOT" |> ignore; require (accept "NULL")
                        | _ ->
                            let negated = accept "NOT"
                            if accept "IN" then
                                require (take "(")
                                require (literal false)
                                while valid && take "," do require (literal false)
                                require (take ")")
                            elif negated then valid <- false
                and conjunction depth =
                    predicate depth
                    while valid && accept "AND" do predicate depth
                and disjunction depth =
                    conjunction depth
                    while valid && accept "OR" do conjunction depth
                require (accept "SELECT")
                let projected () =
                    let item = column true
                    let label = match item with ValueSome value -> value | _ -> null
                    let label = alias label
                    if item.IsNone && not (isNull label) then valid <- false
                    projection.Add({Column=item;Label=label})
                projected()
                while valid && take "," do projected()
                require (accept "FROM")
                let input, schema, table =
                    if take "(" then
                        let body = if valid then query (depth+1) else Unchecked.defaultof<Query>
                        require (take ")")
                        Derived body, null, null
                    else
                        let first = name()
                        let schema, table = if take "." then first, name() else null, first
                        Named(schema,table), schema, table
                let tableAlias = alias table
                if accept "WHERE" then disjunction 0
                if accept "ORDER" then
                    require (accept "BY")
                    let ordered () =
                        column false |> ignore
                        if not (accept "ASC") then accept "DESC" |> ignore
                    ordered()
                    while valid && take "," do ordered()
                if accept "LIMIT" then
                    require (literal true)
                    if accept "OFFSET" || take "," then require (literal true)
                for reference in references do
                    if reference.Length = 2 && (isNull tableAlias || not (equal reference.[0] tableAlias)) then valid <- false
                    elif reference.Length = 3 && (isNull schema || not (equal reference.[0] schema && equal reference.[1] table)) then valid <- false
                {Bindings=List.ofSeq bindings;Input=input;Projection=projection.ToArray()}
            // A catalog view definition has the same one query owner as user SQL.
            if accept "CREATE" then
                (accept "TEMP" || accept "TEMPORARY") |> ignore
                require (accept "VIEW")
                if accept "IF" then require (accept "NOT"); require (accept "EXISTS")
                name() |> ignore
                if take "." then name() |> ignore
                columns() |> ignore
                require (accept "AS")
            let result = query 0
            take ";" |> ignore
            require (token() = End)
            if valid then ValueSome result else ValueNone
