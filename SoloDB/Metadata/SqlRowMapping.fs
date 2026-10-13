namespace SoloDatabase

open System
open System.Reflection
open System.Collections.Generic
open System.Runtime.Serialization
open Microsoft.FSharp.Reflection
open SoloDatabase.JsonSerializator
open System.Reflection.Emit
open SoloDatabase.Metadata.CodecGeneration
open Utils
open SQLiteToolsRowConversions
open SoloDatabase.QueryableGroupByAliases

/// Dynamic mapping is resolved once for the statement's actual column layout.
/// Typed and generated callers can instead supply a SqlRow mapper directly.
module internal SQLiteToolsMapper =
    let internal defaultOf<'T> () : 'T = SQLiteToolsMapperQuery.defaultOf<'T>()

    let private textOrNull (row: SqlRow) ordinal =
        if ordinal >= 0 && not (row.IsDBNull ordinal) then row.GetString ordinal else null

    let private columnReader (typ: Type) ordinal =
        let method = DynamicMethod("ReadMappedColumn",typeof<obj>,[|typeof<SqlRow>|],typeof<SqlRowValue>.Module,true)
        let il = RuntimeCodecIL(method.GetILGenerator())
        if typeof<NativeArray.NativeArray>.Equals(typ) || typeof<byte[]>.Equals(typ) then
            il.EmitInt(OpCodes.Ldarga,0)
            il.EmitInt(OpCodes.Ldc_I4,ordinal)
            let name, flags =
                if typeof<NativeArray.NativeArray>.Equals(typ) then "GetNativeArray",BindingFlags.Instance ||| BindingFlags.NonPublic
                else "GetValue",BindingFlags.Instance ||| BindingFlags.Public
            il.EmitMethod(OpCodes.Call,typeof<SqlRow>.GetMethod(name,flags))
            if typeof<byte[]>.Equals(typ) then il.EmitType(OpCodes.Isinst,typ)
        else
            il.Emit OpCodes.Ldarg_0
            il.EmitInt(OpCodes.Ldc_I4,ordinal)
            let operation = SqlReadArguments.operation typ
            let read = typeof<SqlRowValue>.GetMethod(operation.Method,BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Static)
            il.EmitMethod(OpCodes.Call,read.MakeGenericMethod(SqlReadArguments.resolve typ operation.Arguments))
        if typ.IsValueType then il.EmitType(OpCodes.Box,typ)
        il.Emit OpCodes.Ret
        method.CreateDelegate(typeof<Func<SqlRow,obj>>) :?> Func<SqlRow,obj>

    let private buildShape<'T> (columns: NativeColumnLayout) start finish =
        let t = typeof<'T>
        let ordinal name = columns.Ordinal(name,start,finish)
        let rec tuple (tupleType: Type) (next: int ref) =
            let elements = TypeMetadata.GenericTypeArgCache.Get tupleType
            let readers = elements |> Array.mapi(fun position typ ->
                if position = 7 && elements.Length = 8 && isTuple typ then tuple typ next
                else
                    let index = next.Value
                    next.Value <- index + 1
                    if index >= finish then
                        raise(InvalidOperationException($"Tuple arity mismatch: tuple type '{t.Name}' requires more than {finish - start} columns from startIndex {start}."))
                    let read = columnReader typ index
                    let nullable = not(isNull(TypeMetadata.nullableUnderlying typ))
                    let defaultValue = lazy(QueryValueMetadata.Default typ)
                    Func<SqlRow,obj>(fun row ->
                        if nullable || not(row.IsDBNull index) then read.Invoke row
                        elif typ.IsValueType then
                            raise(InvalidOperationException($"DB null in non-nullable tuple element {index - start} of type '{typ.Name}'."))
                        else defaultValue.Value))
            let create = QueryValueMetadata.Constructor(tupleType,Array.toList elements)
            Func<SqlRow,obj>(fun row -> create.Invoke(readers |> Array.map(fun read -> read.Invoke row)))
        if isTuple t then
            let read = tuple t (ref start)
            Func<SqlRow,'T>(fun row -> unbox<'T>(read.Invoke row))
        else
            let model = JsonModelDiscovery.Model t
            match TypeMetadata.recordFields t with
            | ValueSome names ->
                let fields = names |> Array.map(fun name -> model.Members |> Array.find(fun memberValue -> memberValue.Name = name && memberValue.Field.IsNone))
                let reads = fields |> Array.map(fun field ->
                    let index = ordinal field.Name
                    let read = if index < 0 then null else columnReader field.Type index
                    let defaultValue = lazy(QueryValueMetadata.Default field.Type)
                    Func<SqlRow,obj>(fun row -> if index >= 0 && not(row.IsDBNull index) then read.Invoke row else defaultValue.Value))
                let create = QueryValueMetadata.Constructor(t,fields |> Array.map _.Type |> Array.toList)
                Func<SqlRow,'T>(fun row -> unbox<'T>(create.Invoke(reads |> Array.map(fun read -> read.Invoke row))))
            | ValueNone ->
                let properties = t.GetProperties() |> Array.filter _.CanWrite |> Array.map(fun value -> value :> MemberInfo)
                let members =
                    if t.IsValueType && properties.Length = 0 then
                        t.GetFields(BindingFlags.NonPublic ||| BindingFlags.Instance) |> Array.filter(fun field -> not field.IsPrivate) |> Array.map(fun field -> field :> MemberInfo)
                    else properties
                let create =
                    let constructor = Option.ofObj(t.GetConstructor(Type.EmptyTypes))
                    if constructor.IsSome || t.IsValueType then QueryMethodDiscovery.defaultConstruction t constructor
                    else Func<obj>(fun () -> FormatterServices.GetSafeUninitializedObject t)
                let writes = members |> Array.choose(fun memberInfo ->
                    let index = ordinal (memberInfo.Name.TrimEnd('@'))
                    if index < 0 then None else
                    let typ = match memberInfo with :? PropertyInfo as property -> property.PropertyType | :? FieldInfo as field -> field.FieldType | _ -> invalidOp "Unsupported mapped member."
                    Some(struct(index,columnReader typ index,QueryMethodDiscovery.setter memberInfo)))
                Func<SqlRow,'T>(fun row ->
                    let result = create.Invoke()
                    for struct(index,read,write) in writes do
                        if not(row.IsDBNull index) then write.Invoke(result,read.Invoke row)
                    unbox<'T> result)

    let private build<'T> (columns: NativeColumnLayout) start finish =
        let t = typeof<'T>
        let ordinal name = columns.Ordinal(name,start,finish)
        match t with
        | _ when typeof<Types.DbObjectRow>.Equals(t) ->
            let id = ordinal "Id"
            let value = ordinal "ValueJSON"
            let hydration = ordinal "HydrationJSON"
            Func<SqlRow, 'T>(fun row ->
                let document: Types.DbObjectRow = {
                    Id = if id >= 0 && not (row.IsDBNull id) then Nullable(row.GetInt64 id) else Nullable()
                    ValueJSON = textOrNull row value
                    HydrationJSON = textOrNull row hydration }
                unbox<'T> (box document))
        | _ when typeof<byte[]>.Equals(t) -> Func<SqlRow, 'T>(fun row -> unbox<'T> (row.GetValue(start)))
        | _ when typeof<NativeArray.NativeArray>.Equals(t) -> Func<SqlRow, 'T>(fun row -> unbox<'T> (box (row.GetNativeArray(start))))
        | _ when typeof<obj>.Equals(t) || typeof<JsonValue>.Equals(t) ->
            let segment = Dictionary<string, int>(finish - start, StringComparer.Ordinal)
            for i = start to finish - 1 do segment.[columns.Names.[i]] <- i
            let named = segment |> Seq.toArray
            Func<SqlRow, 'T>(fun row ->
                if typeof<obj>.Equals(t) && named.Length = 1 then
                    let scalar = if row.IsDBNull(start) then null else row.GetValue(start)
                    JsonValue.ParseInto<'T>(JsonValue.WriteText<obj>(scalar,false,false))
                elif typeof<JsonValue>.Equals(t) && named.Length = 1 && named.[0].Key.StartsWith(jsonObjectFn) then
                    let json = if row.CanBorrowUtf16 then JsonValue.Parse(row.TextUtf16(start)) else JsonValue.Parse(row.GetString(start))
                    unbox<'T> (box json)
                elif typeof<JsonValue>.Equals(t) then
                    let result = JsonValue.Object(Dictionary(named.Length))
                    for KeyValue(name,i) in named do result.[name] <- JsonValue.Serialize<obj>(row.GetValue(i))
                    unbox<'T>(box result)
                else
                    let values = seq { for KeyValue(name,i) in named do yield KeyValuePair(name,row.GetValue(i)) }
                    JsonValue.ParseInto<'T>(JsonValue.WriteProperties values))
        | _ ->
            match tryBindScalar<'T> start with
            | ValueSome mapper -> mapper
            | ValueNone -> buildShape<'T> columns start finish

    let internal bindRange<'T> (columns: NativeColumnLayout) start finish =
        let key = struct (typeof<'T>, start, finish)
        let binder = SqlRowMapping<'T>.CurrentBinder
        let cached =
            match columns.Mappings.TryGetValue key with
            | true, value ->
                let struct(previous, mapper) = unbox<struct (Delegate * Func<SqlRow,'T>)> value
                if obj.ReferenceEquals(previous, binder) then ValueSome mapper else ValueNone
            | _ -> ValueNone
        match cached with
        | ValueSome mapper -> mapper
        | ValueNone ->
            let mapper =
                if not (isNull binder) then
                    let mapper = binder.Invoke(SqlRowShape(columns,start,finish))
                    if isNull mapper then raise (InvalidOperationException("The typed row factory returned no mapper."))
                    mapper
                elif not (MetadataRuntime.supportsDynamicCode()) &&
                     not (typeof<'T>.Equals(typeof<Types.DbObjectRow>)) && not (typeof<'T>.Equals(typeof<byte[]>)) &&
                     not (typeof<'T>.Equals(typeof<NativeArray.NativeArray>)) && not (typeof<'T>.Equals(typeof<obj>)) && not (typeof<'T>.Equals(typeof<JsonValue>)) then
                    MetadataRuntime.missing "SQL row mapping" typeof<'T>
                else build<'T> columns start finish
            columns.Mappings.[key] <- box (struct (binder :> Delegate,mapper))
            mapper

    let internal bind<'T> (columns: NativeColumnLayout) start = bindRange<'T> columns start columns.Count
