module SQLiteTools

open Microsoft.Data.Sqlite
open System
open System.Reflection
open System.Collections
open System.Collections.Generic
open SoloDatabase


type SQLiteTypeMapper = 
    abstract member Type: Type
    abstract member Read: reader: SqliteDataReader -> index: int -> obj
    abstract member Write: this: obj -> ValueTuple<obj, int>

let private customMappers = [| 
    {
        new SQLiteTypeMapper with
            member this.Type = typeof<DateTimeOffset>

            member this.Read reader index =
                DateTimeOffset.FromUnixTimeMilliseconds (reader.GetInt64 index)

            member _.Write this =
                let this = this :?> DateTimeOffset

                struct (this.ToUnixTimeMilliseconds(), sizeof<int64>)
    }
|]

let private addParameter (command: SqliteCommand) (key: string) (value: obj) =
    let struct (value, size) = 
        if value = null then 
            struct (null, -1)
        else
        let valType = value.GetType()
        match Array.tryFind (fun (m: SQLiteTypeMapper) -> m.Type = valType) customMappers with
        | Some mapper -> 
            mapper.Write value
        | None -> 
            if valType.Name.StartsWith "Nullable`" then
                if valType.GetProperty("HasValue").GetValue value = true then
                    struct (valType.GetProperty("Value").GetValue value, -1)
                else struct (null, -1)
            else
                struct (value, -1)

    let par = command.CreateParameter()
    par.ParameterName <- key
    par.Value <- value

    if size > 0 then
        par.Size <- size

    command.Parameters.Add par |> ignore

let private createCommand(this: SqliteConnection)(sql: string)(parameters: obj option) =
    let command = this.CreateCommand()
    command.CommandText <- sql
    match parameters with
    | Some parameters when parameters <> null ->        
        match parameters with
        | :? IDictionary as dict ->
            for key in dict.Keys do
                let value = dict.[key]
                let key = key :?> string
                addParameter command key value

        | parameters ->
        for p in parameters.GetType().GetProperties() do
            if p.CanRead then
                addParameter command p.Name (p.GetValue parameters)
    | _ -> ()

    command

let rec private mapToType<'T> (reader: SqliteDataReader) (startIndex: int) (columns: IDictionary<string, int>) : 'T =
    let readColumn (name) (propType) =
        match columns.TryGetValue(name) with
        | true, index ->
            match Array.tryFind (fun (m: SQLiteTypeMapper) -> m.Type = propType) customMappers with
            | Some mapper -> 
                if reader.IsDBNull index then null
                else mapper.Read(reader)(index)
            | None -> match reader.GetValue(index) with
                        | :? DBNull -> null
                        | value -> value
        | false, _ ->
            if propType.IsClass && propType <> typeof<string> then
                mapToType (reader) (startIndex) columns
            else null

    let targetType = typeof<'T>

    if targetType = typeof<obj> then
        let jsonObj = JsonSerializator.JsonValue.New()
        for key in columns.Keys do
            jsonObj.JS(key) <- reader.GetValue(columns.[key])

        jsonObj.ToObject<obj>() :?> 'T
    else
    // Check if the target type itself is a custom mapper type
    match Array.tryFind (fun (m: SQLiteTypeMapper) -> m.Type = targetType) customMappers with
    | Some mapper -> 
        mapper.Read(reader)(startIndex) :?> 'T
    | None ->
        match targetType with
        | t when t = typeof<int8> -> reader.GetByte(startIndex) :> obj :?> 'T
        | t when t = typeof<uint8> -> reader.GetByte(startIndex) :> obj :?> 'T
        | t when t = typeof<int16> -> reader.GetInt16(startIndex) :> obj :?> 'T
        | t when t = typeof<uint16> -> reader.GetInt32(startIndex) |> uint16 :> obj :?> 'T
        | t when t = typeof<int32> -> reader.GetInt32(startIndex) :> obj :?> 'T
        | t when t = typeof<uint32> -> reader.GetInt64(startIndex) |> uint32 :> obj :?> 'T
        | t when t = typeof<int64> -> reader.GetInt64(startIndex) :> obj :?> 'T
        | t when t = typeof<uint64> -> reader.GetInt64(startIndex) |> uint64 :> obj :?> 'T
        | t when t = typeof<float> -> reader.GetDouble(startIndex) |> float :> obj :?> 'T
        | t when t = typeof<double> -> reader.GetDouble(startIndex) :> obj :?> 'T
        | t when t = typeof<decimal> -> reader.GetDecimal(startIndex) :> obj :?> 'T
        | t when t = typeof<string> -> reader.GetString(startIndex) :> obj :?> 'T
        | t when t = typeof<bool> -> reader.GetBoolean(startIndex) :> obj :?> 'T
        | t when t = typeof<byte array> -> reader.GetValue(startIndex) :?> 'T
        | t when t = typeof<Guid> -> reader.GetGuid(startIndex) :> obj :?> 'T
        | t when t = typeof<DateTime> -> reader.GetDateTime(startIndex) :> obj :?> 'T
        | _ ->             
            let props = targetType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)

            // If the type is a F# record/union/etc then to use the more complete Json Serializer.
            let jsonMode = props |> Seq.sumBy(fun p -> if p.CanWrite then 1 else 0) = 0

            let instance: obj = 
                if jsonMode 
                then JsonSerializator.JsonValue.New()
                else Utils.initEmpty targetType

            for prop in props do
                if prop.CanWrite || jsonMode then
                    let propType = 
                        if prop.PropertyType.Name.StartsWith "Nullable`" then
                            Nullable.GetUnderlyingType prop.PropertyType
                        else
                            prop.PropertyType

                    let value = readColumn prop.Name propType                    

                    if value <> null then
                        let valueType = value.GetType()
                        let value =
                            if valueType <> prop.PropertyType then
                                if prop.PropertyType.Name.StartsWith "Nullable`" then
                                    Activator.CreateInstance(prop.PropertyType, value)
                                else
                                    Convert.ChangeType(value, prop.PropertyType)
                            else
                                value

                        if jsonMode then
                            (instance :?> JsonSerializator.JsonValue).[prop.Name] <- JsonSerializator.JsonValue.Serialize value
                        else
                            prop.SetValue(instance, value)

            if jsonMode then
                let jsonObj = (instance :?> JsonSerializator.JsonValue)
                jsonObj.ToObject<'T>()
            else
                instance :?> 'T


let private queryInner<'T> this (sql: string)(parameters: obj option) = seq {
       use command = createCommand this sql parameters
       use reader = command.ExecuteReader()
       let dict = Dictionary<string, int>()

       for i in 0..(reader.FieldCount - 1) do
           dict.Add(reader.GetName(i), i)

       while reader.Read() do
           yield mapToType<'T> reader 0 dict
   }
    

type SqliteConnection with
    member this.Execute(sql: string, ?parameters: obj) =
        use command = createCommand this sql parameters
        command.ExecuteNonQuery()

    member this.Query<'T>(sql: string, ?parameters: obj) =
        queryInner<'T> this sql parameters

    member this.QueryFirst<'T>(sql: string, ?parameters: obj) = 
        queryInner<'T> this sql parameters |> Seq.head

    member this.QueryFirstOrDefault<'T>(sql: string, ?parameters: obj) = 
        match queryInner<'T> this sql parameters |> Seq.tryHead with
        | Some x -> x
        | None -> Unchecked.defaultof<'T>

    member this.Query<'T1, 'T2, 'TReturn>(sql: string, map: ('T1 -> 'T2 -> 'TReturn), parameters: obj, splitOn: string) = seq {
        use command = createCommand this sql (Some parameters)
        use reader = command.ExecuteReader()

        let dict = Dictionary<string, int>()

        for i in 0..(reader.FieldCount - 1) do
           dict.Add(reader.GetName(i), i)

        let splitIndex = reader.GetOrdinal(splitOn)

        while reader.Read() do
            let t1 = mapToType<'T1> reader 0 dict
            let t2 = 
                if reader.IsDBNull(splitIndex) then Unchecked.defaultof<'T2>
                else mapToType<'T2> reader splitIndex dict

            yield map t1 t2
    }

// For C# usage.
let execute (this: SqliteConnection) sql (parameters: obj) = this.Execute(sql, parameters)
let query<'T> (this: SqliteConnection) sql (parameters: obj) = this.Query<'T>(sql, parameters)
let queryFirst<'T> (this: SqliteConnection) sql (parameters: obj) = this.QueryFirst<'T>(sql, parameters)
let queryFirstOrDefault<'T> (this: SqliteConnection) sql (parameters: obj) = this.QueryFirstOrDefault<'T>(sql, parameters)