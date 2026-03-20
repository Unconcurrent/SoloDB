namespace SoloDatabase

open System
open System.Collections.Generic
open System.Data
open Microsoft.FSharp.Reflection
open Utils
open SQLiteToolsParams

module internal SQLiteToolsMapperQuery =
    let internal defaultOf<'T> () : 'T =
        let t = typeof<'T>
        if isTuple t then
            let elementTypes = GenericTypeArgCache.Get t
            let defaults = elementTypes |> Array.map (fun et ->
                if et.IsValueType then Activator.CreateInstance(et)
                else null)
            FSharpValue.MakeTuple(defaults, t) :?> 'T
        else
            Unchecked.defaultof<'T>

    let internal queryCommandWith<'T> (map: IDataReader -> int -> IDictionary<string, int> -> 'T) (command: IDbCommand) (nullableCachedDict: Dictionary<string, int>) = seq {
        use reader = command.ExecuteReader()
        let dict =
            if isNull nullableCachedDict then
                Dictionary<string, int>(reader.FieldCount)
            else
                nullableCachedDict

        if dict.Count = 0 then
            for i in 0..(reader.FieldCount - 1) do
                dict.[reader.GetName(i)] <- i

        while reader.Read() do
            yield map reader 0 dict
    }

    let internal queryInnerWith<'T> (map: IDataReader -> int -> IDictionary<string, int> -> 'T) this (sql: string) (parameters: obj) = seq {
        use command = createCommand this sql parameters
        yield! queryCommandWith<'T> map command null
    }
