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
open SoloDatabase.QueryableGroupByAliases

/// Dynamic mapping is resolved once for the statement's actual column layout.
/// Typed and generated callers can instead supply a SqlRow mapper directly.
module internal SQLiteToolsMapper =
    let internal defaultOf<'T> () : 'T = SQLiteToolsMapperQuery.defaultOf<'T>()

    let private textOrNull (row: SqlRow) ordinal =
        if ordinal >= 0 && not (row.IsDBNull ordinal) then row.GetString ordinal else null

    let private api =
        let flags = BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Static
        let read (typ: Type) =
            match Metadata.Generation.TypeSupport.SqlMappedReader typ.FullName with
            | Some name -> typeof<SqlRowValue>.GetMethod(name,flags) :> MethodBase
            | None ->
                let operation = SqlReadArguments.operation typ
                typeof<SqlRowValue>.GetMethod(operation.Method,flags).MakeGenericMethod(SqlReadArguments.resolve typ operation.Arguments) :> MethodBase
        { IsNull = typeof<SqlRow>.GetMethod("IsDBNull") :> MethodBase
          Read = Func<Type,MethodBase>(read)
          Uninitialized = Func<Type,MethodBase>(fun typ -> typeof<JsonMetadata>.GetMethod("Uninitialized",flags).MakeGenericMethod(typ) :> MethodBase)
          ValueType = Func<Type,bool>(fun typ -> typ.IsValueType)
          Nullable = Func<Type,bool>(fun typ -> not(isNull(TypeMetadata.nullableUnderlying typ)))
          Name = Func<Type,string>(fun typ -> typ.Name)
          TupleNull = typeof<SqlRowShape>.GetMethod("TupleNull",flags) :> MethodBase }

    let private buildShape<'T> (columns: NativeColumnLayout) start finish =
        let typ = typeof<'T>
        let method = DynamicMethod("ReadSqlRow",typ,[|typeof<int[]>;typeof<SqlRow>|],typ.Module,true)
        let il = RuntimeCodecIL(method.GetILGenerator())
        let ordinal = Action<int>(fun index ->
            il.Emit OpCodes.Ldarg_0
            il.EmitInt(OpCodes.Ldc_I4,index)
            il.Emit OpCodes.Ldelem_I4)
        let ordinals =
            if Metadata.Generation.TypeSupport.ScalarRow((Metadata.Generation.TypeSupport.Runtime typ).Shape,typ.FullName) then
                SqlRowEmitter.scalarRow il api typ ordinal
                [|start|]
            elif isTuple typ then
                let model = RuntimeTupleFacts.model typ
                let count = (CodecTupleEmitter.leaves model).Length
                SqlRowShape(columns,start,finish).RequireTupleColumns(typ.Name,count)
                SqlRowEmitter.tupleRow il api model ordinal
                Array.init count (fun index -> start + index)
            else
                let model = JsonModelDiscovery.Model typ
                let fields = TypeMetadata.recordFields typ |> ValueOption.toOption
                let properties, fallback =
                    match fields with
                    | Some _ -> model.Members,[||]
                    | None ->
                        let properties = typ.GetProperties() |> Array.filter _.CanWrite
                        let present (memberInfo: MemberInfo) = columns.Ordinal(Metadata.Generation.TypeSupport.SqlColumnName(false,memberInfo.Name),start,finish) >= 0
                        let selected = properties |> Array.filter present
                                       |> Array.map(fun property -> QueryMethodDiscovery.memberFacts property true)
                        let fallback =
                            if Metadata.Generation.TypeSupport.SqlRowFieldFallback(typ.IsValueType,false,properties.Length) then
                                typ.GetFields(BindingFlags.NonPublic ||| BindingFlags.Instance)
                                |> Array.filter(fun field -> not field.IsPrivate && present field)
                                |> Array.map(fun field -> QueryMethodDiscovery.memberFacts field true)
                            else [||]
                        selected,fallback
                let members = SqlRowEmitter.members fields properties fallback
                let constructor = Option.ofObj(typ.GetConstructor(Type.EmptyTypes)) |> Option.map(fun value -> value :> MethodBase)
                let ordinals = members |> Array.map(fun memberValue ->
                    columns.Ordinal(Metadata.Generation.TypeSupport.SqlColumnName(fields.IsSome,memberValue.Name),start,finish))
                SqlRowEmitter.objectRow il api model members fields.IsSome constructor ordinal
                    (Func<int,bool>(fun index -> ordinals[index] >= 0))
                ordinals
        method.CreateDelegate(typeof<Func<SqlRow,'T>>,ordinals) :?> Func<SqlRow,'T>

    let private tryBuiltin<'T> (columns: NativeColumnLayout) start finish =
        let t = typeof<'T>
        let ordinal name = columns.Ordinal(name,start,finish)
        match t with
        | _ when typeof<Types.DbObjectRow>.Equals(t) ->
            let id = ordinal "Id"
            let value = ordinal "ValueJSON"
            let hydration = ordinal "HydrationJSON"
            ValueSome(Func<SqlRow, 'T>(fun row ->
                let document: Types.DbObjectRow = {
                    Id = if id >= 0 && not (row.IsDBNull id) then Nullable(row.GetInt64 id) else Nullable()
                    ValueJSON = textOrNull row value
                    HydrationJSON = textOrNull row hydration }
                unbox<'T> (box document)))
        | _ when typeof<byte[]>.Equals(t) -> ValueSome(Func<SqlRow, 'T>(fun row -> unbox<'T> (row.GetValue(start))))
        | _ when typeof<NativeArray.NativeArray>.Equals(t) -> ValueSome(Func<SqlRow, 'T>(fun row -> unbox<'T> (box (row.GetNativeArray(start)))))
        | _ when typeof<Dictionary<string, obj>>.Equals(t) ->
            let named = columns.NamedColumns(start,finish)
            ValueSome(Func<SqlRow, 'T>(fun row ->
                let result = Dictionary<string, obj>(named.Count,StringComparer.Ordinal)
                for KeyValue(name,i) in named do result.[name] <- row.GetValue(i)
                unbox<'T> (box result)))
        | _ when typeof<obj>.Equals(t) || typeof<JsonValue>.Equals(t) ->
            let named = columns.NamedColumns(start,finish)
            let singleton =
                if named.Count = 1 then
                    use mutable entries = named.GetEnumerator()
                    entries.MoveNext() |> ignore
                    entries.Current
                else Unchecked.defaultof<KeyValuePair<string,int>>
            ValueSome(Func<SqlRow, 'T>(fun row ->
                if typeof<obj>.Equals(t) && named.Count = 1 then
                    let scalar = if row.IsDBNull(singleton.Value) then null else row.GetValue(singleton.Value)
                    JsonValue.ParseInto<'T>(JsonValue.WriteText<obj>(scalar,false,false))
                elif typeof<JsonValue>.Equals(t) && named.Count = 1 && singleton.Key.StartsWith(jsonObjectFn) then
                    let json = if row.CanBorrowUtf16 then JsonValue.Parse(row.TextUtf16(singleton.Value)) else JsonValue.Parse(row.GetString(singleton.Value))
                    unbox<'T> (box json)
                elif typeof<JsonValue>.Equals(t) then
                    let result = JsonValue.Object(Dictionary(named.Count))
                    for KeyValue(name,i) in named do result.[name] <- JsonValue.Serialize<obj>(row.GetValue(i))
                    unbox<'T>(box result)
                else
                    let values = seq { for KeyValue(name,i) in named do yield KeyValuePair(name,row.GetValue(i)) }
                    JsonValue.ParseInto<'T>(JsonValue.WriteProperties values)))
        | _ -> ValueNone

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
                else
                    match tryBuiltin<'T> columns start finish with
                    | ValueSome mapper -> mapper
                    | ValueNone when not (MetadataRuntime.supportsDynamicCode()) -> MetadataRuntime.missing "SQL row mapping" typeof<'T>
                    | ValueNone -> buildShape<'T> columns start finish
            columns.Mappings.[key] <- box (struct (binder :> Delegate,mapper))
            mapper

    let internal bind<'T> (columns: NativeColumnLayout) start = bindRange<'T> columns start columns.Count
