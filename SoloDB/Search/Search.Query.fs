namespace SoloDatabase

open System
open System.Linq
open System.Linq.Expressions
open System.Reflection

/// One producer for ordinary and compiled collection search expressions.
module internal SearchQueryBuilding =
    let build<'T> (source: Expression) (name: Expression) (query: Expression) =
        SearchLiteral.EnsureMetadata()
        let item = Expression.Parameter(typeof<'T>,"item")
        let hit = typeof<SearchHit<'T>>
        let flags = BindingFlags.Static ||| BindingFlags.NonPublic ||| BindingFlags.Public
        let score = Expression.Call(typeof<SearchSql>.GetMethod("Score",flags),Expression.Convert(item,typeof<obj>),Expression.Constant(""))
        let constructor = hit.GetConstructor([|typeof<'T>;typeof<double>|])
        let selector = Expression.Lambda<Func<'T,SearchHit<'T>>>(Expression.New(constructor,[|item :> Expression;score :> Expression|],
            [|hit.GetProperty("Item") :> MemberInfo;hit.GetProperty("Score") :> MemberInfo|]),item)
        let marker = typeof<SearchMarker>.GetMethod("Query",flags).MakeGenericMethod(typeof<'T>)
        let literal = typeof<SearchLiteral>.GetMethod((if query.Type.Equals(typeof<string>) then "AllWords" else "Format"),flags)
        let query = Expression.Call(marker,source,name,Expression.Call(literal,query),Expression.Quote(selector))
        let row = Expression.Parameter(hit,"hit")
        let rank = Expression.Lambda<Func<SearchHit<'T>,double>>(Expression.Call(typeof<SearchSql>.GetMethod("Rank",flags),Expression.Convert(row,typeof<obj>)),row)
        let order = Expression.Call(typeof<Queryable>,"OrderBy",[|hit;typeof<double>|],query,Expression.Quote(rank))
        let identity = Expression.Lambda<Func<SearchHit<'T>,int64>>(Expression.Call(typeof<SearchSql>.GetMethod("Identity",flags),Expression.Convert(row,typeof<obj>)),row)
        Expression.Call(typeof<Queryable>,"ThenBy",[|hit;typeof<int64>|],order,Expression.Quote(identity)) :> Expression
