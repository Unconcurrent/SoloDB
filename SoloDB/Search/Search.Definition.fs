namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq.Expressions

module internal SearchDefinitionResolution =
    let fieldPath collection (selector: LambdaExpression) =
        QuerySearchFields.path selector
    let resolve collection (builder: SearchIndexBuilder<'T>) =
        let aliases = HashSet<string>(StringComparer.Ordinal)
        let fields = builder.Fields |> Array.map (fun field ->
            let path, leaf = fieldPath collection field.Selector
            let alias = if isNull field.Alias then leaf else field.Alias
            if alias.IndexOf('\000') >= 0 ||
               SearchDefinition.identifier alias = "rank" || SearchDefinition.identifier alias = "rowid" then
                invalidArg "name" "A search alias cannot be rank, rowid or contain NUL."
            if not(aliases.Add(SearchDefinition.identifier alias)) then invalidArg "name" "Search aliases must be distinct native identifiers."
            { Path = path; Alias = alias; Weight = field.Weight })
        if fields.Length = 0 then invalidArg "configure" "Declare at least one search field."
        if builder.Storage <> SearchTextStorage.Shared && builder.Storage <> SearchTextStorage.Copied then
            invalidArg "Storage" "Unknown search text storage."
        if String.IsNullOrWhiteSpace builder.Tokenizer || builder.Tokenizer.IndexOf('\000') >= 0 then
            invalidArg "Tokenizer" "A tokenizer definition is required."
        let prefix = builder.PrefixLengths |> Array.distinct |> Array.sort
        if prefix |> Array.exists (fun length -> length <= 0) then invalidArg "PrefixLengths" "Prefix lengths must be positive."
        { Fields = fields; Storage = builder.Storage; Tokenizer = builder.Tokenizer; Prefix = prefix }
