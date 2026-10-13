namespace SoloDatabase.Data.Sqlite

open System
open System.Collections
open System.Collections.Generic
open System.Data
open System.Data.Common
open SoloDatabase

/// SQLite storage choices for values with an alternative representation.
type SqliteType =
    | Integer = 1
    | Real = 2
    | Text = 3
    | Blob = 4

type SqliteOpenMode =
    | ReadWriteCreate = 0
    | ReadWrite = 1
    | ReadOnly = 2
    | Memory = 3

type SqliteCacheMode =
    | Default = 0
    | Private = 1
    | Shared = 2

[<Struct>]
type internal AdoParameterSlot = { Value: obj; Size: int; SqliteType: int }

[<AllowNullLiteral>]
type internal IAdoParameterOwner =
    abstract InvalidateLayout: unit -> unit

/// A standard input parameter for the external ADO connection adapter.
[<Sealed; AllowNullLiteral>]
type SqliteParameter() as this =
    inherit DbParameter()
    let mutable name = ""
    let mutable value: obj = null
    let mutable declaredType = DbType.String
    let mutable sqliteType = ValueNone
    let mutable size = ValueNone
    let mutable owner: IAdoParameterOwner = null
    let mutable ownerReferences = 0
    let mutable otherOwners: Dictionary<IAdoParameterOwner, int> = null
    let mutable source = ""
    let mutable sourceVersion = DataRowVersion.Current
    let mutable nullable = false
    let mutable nullMapping = false
    let mutable precision = 0uy
    let mutable scale = 0uy
    new(name: string, value: obj) as this = new SqliteParameter() then this.ParameterName <- name; this.Value <- value
    new(name: string, ``type``: SqliteType) as this = new SqliteParameter() then this.ParameterName <- name; this.SqliteType <- ``type``
    new(name: string, ``type``: SqliteType, size: int) as this = new SqliteParameter(name, ``type``) then this.Size <- size
    new(name: string, ``type``: SqliteType, size: int, sourceColumn: string) as this = new SqliteParameter(name, ``type``, size) then this.SourceColumn <- sourceColumn
    member _.SqliteType
        with get() = match sqliteType with ValueSome value -> value | _ -> enum<SqliteType>(NativeParameterBinding.inferRawType value)
        and set value = sqliteType <- ValueSome value
    member _.ResetSqliteType() = declaredType <- DbType.String; sqliteType <- ValueNone
    member internal _.Attach(collection: IAdoParameterOwner) =
        if isNull owner then owner <- collection; ownerReferences <- 1
        elif Object.ReferenceEquals(owner, collection) then ownerReferences <- ownerReferences + 1
        else
            if isNull otherOwners then otherOwners <- Dictionary<IAdoParameterOwner, int>()
            match otherOwners.TryGetValue(collection) with
            | true, count -> otherOwners.[collection] <- count + 1
            | _ -> otherOwners.Add(collection, 1)
    member internal _.Detach(collection: IAdoParameterOwner) =
        if Object.ReferenceEquals(owner, collection) then
            ownerReferences <- ownerReferences - 1
            if ownerReferences = 0 then
                owner <- null
                if not (isNull otherOwners) && otherOwners.Count > 0 then
                    let mutable iterator = otherOwners.GetEnumerator()
                    let next = try iterator.MoveNext() |> ignore; iterator.Current finally iterator.Dispose()
                    owner <- next.Key
                    ownerReferences <- next.Value
                    otherOwners.Remove(next.Key) |> ignore
        elif not (isNull otherOwners) then
            match otherOwners.TryGetValue(collection) with
            | true, 1 -> otherOwners.Remove(collection) |> ignore
            | true, count -> otherOwners.[collection] <- count - 1
            | _ -> ()
        if not (isNull otherOwners) && otherOwners.Count = 0 then otherOwners <- null
    member internal _.Capture() =
        { AdoParameterSlot.Value = value; Size = (match size with ValueSome value -> value | _ -> -1); SqliteType = (match sqliteType with ValueSome value -> int value | _ -> 0) }
    override _.ParameterName
        with get() = name
        and set value =
            let value = if isNull value then "" else value
            if value <> name then
                name <- value
                if not (isNull owner) then owner.InvalidateLayout()
                if not (isNull otherOwners) then
                    for pair in otherOwners do pair.Key.InvalidateLayout()
    override _.Value with get() = value and set v = value <- v
    override _.DbType with get() = declaredType and set value = declaredType <- value
    override _.ResetDbType() = this.ResetSqliteType()
    override _.Size
        with get() =
            match size, value with
            | ValueSome size, _ -> size
            | _, (:? string as value) -> value.Length
            | _, (:? (byte array) as value) -> value.Length
            | _ -> 0
        and set v =
            if v < -1 then raise (ArgumentOutOfRangeException("value"))
            size <- ValueSome v
    override _.Direction
        with get() = ParameterDirection.Input
        and set v = if v <> ParameterDirection.Input then raise (ArgumentException("SQLite parameters support input only.", "value"))
    override _.IsNullable with get() = nullable and set v = nullable <- v
    override _.SourceColumn with get() = source and set v = source <- if isNull v then "" else v
    override _.SourceVersion with get() = sourceVersion and set v = sourceVersion <- v
    override _.SourceColumnNullMapping with get() = nullMapping and set v = nullMapping <- v
    override _.Precision with get() = precision and set v = precision <- v
    override _.Scale with get() = scale and set v = scale <- v

/// Standard mutable parameter storage; execution captures its current values once.
[<Sealed>]
type SqliteParameterCollection internal () as this =
    inherit DbParameterCollection()
    let items = ResizeArray<SqliteParameter>()
    let gate = obj()
    let mutable plan: SqlParameterPlan<SqliteParameterCollection, AdoParameterSlot[]> = Unchecked.defaultof<_>
    let parameter (value: obj) =
        match value with
        | :? SqliteParameter as value -> value
        | _ -> invalidArg "value" "An SqliteParameter is required."
    let indexOfName name =
        let mutable result = -1
        let mutable index = 0
        while index < items.Count && result < 0 do
            if String.Equals(items.[index].ParameterName, name, StringComparison.Ordinal) then result <- index
            index <- index + 1
        result
    let requireName name =
        let index = indexOfName name
        if index < 0 then raise (IndexOutOfRangeException($"Parameter '{name}' was not found."))
        index
    member internal _.Capture(previous: AdoParameterSlot[]) =
        let values = if isNull previous || previous.Length <> items.Count then Array.zeroCreate items.Count else previous
        for i = 0 to items.Count - 1 do values.[i] <- items.[i].Capture()
        values
    member internal _.Plan =
        if isNull (box plan) then
            for parameter in items do
                if String.IsNullOrEmpty(parameter.ParameterName) then raise (InvalidOperationException("ParameterName must be set."))
            plan <- SqlParameterPlan<SqliteParameterCollection, AdoParameterSlot[]>(
                Array.init items.Count (fun i -> items.[i].ParameterName),
                Func<_, _, _>(fun source previous -> source.Capture(previous)),
                Action<_, _>(fun writer values ->
                    for i = 0 to values.Length - 1 do writer.Raw(i, values.[i].Value, values.[i].Size, values.[i].SqliteType)),
                Action<_>(fun values -> if not (isNull values) then Array.Clear(values, 0, values.Length)), true)
        plan
    member this.Add(value: SqliteParameter) = this.Add(value :> obj) |> ignore; value
    member this.Add(parameterName: string, ``type``: SqliteType) = this.Add(SqliteParameter(parameterName, ``type``))
    member this.Add(parameterName: string, ``type``: SqliteType, size: int) = this.Add(SqliteParameter(parameterName, ``type``, size))
    member this.Add(parameterName: string, ``type``: SqliteType, size: int, sourceColumn: string) = this.Add(SqliteParameter(parameterName, ``type``, size, sourceColumn))
    member this.AddRange(values: IEnumerable<SqliteParameter>) =
        if isNull values then nullArg "collection"
        for parameter in values do this.Add(parameter) |> ignore
    member _.Contains(parameter: SqliteParameter) = items.Contains(parameter)
    member _.CopyTo(parameters: SqliteParameter[], index: int) = items.CopyTo(parameters, index)
    member _.IndexOf(parameter: SqliteParameter) = items.IndexOf(parameter)
    member this.Insert(index: int, parameter: SqliteParameter) = this.Insert(index, parameter :> obj)
    member this.Remove(parameter: SqliteParameter) = this.Remove(parameter :> obj)
    member _.AddWithValue(parameterName: string, value: obj) =
        let result = SqliteParameter(ParameterName = parameterName, Value = value)
        this.Add(result) |> ignore
        result
    member _.Item
        with get(index: int) = items.[index]
        and set(index: int) (value: SqliteParameter) = this.SetParameter(index, value)
    member _.Item
        with get(name: string) = items.[requireName name]
        and set(name: string) (value: SqliteParameter) = this.SetParameter(name, value)
    override _.Count = items.Count
    override _.SyncRoot = gate
    override _.IsFixedSize = false
    override _.IsReadOnly = false
    override _.IsSynchronized = false
    override _.GetEnumerator() = (items :> IEnumerable).GetEnumerator()
    override _.CopyTo(array: Array, index: int) = (items :> ICollection).CopyTo(array, index)
    override _.Contains(value: obj) = match value with :? SqliteParameter as p -> items.Contains p | _ -> false
    override _.Contains(value: string) = indexOfName value >= 0
    override _.IndexOf(value: obj) = match value with :? SqliteParameter as p -> items.IndexOf p | _ -> -1
    override _.IndexOf(parameterName: string) = indexOfName parameterName
    override _.Add(value: obj) =
        let p = parameter value
        p.Attach(this)
        items.Add p
        plan <- Unchecked.defaultof<_>
        items.Count - 1
    override _.AddRange(values: Array) =
        if isNull values then nullArg "values"
        for value in values do this.Add(value) |> ignore
    override _.Clear() =
        for p in items do p.Detach(this)
        items.Clear()
        plan <- Unchecked.defaultof<_>
    override _.Insert(index: int, value: obj) =
        if index < 0 || index > items.Count then raise (ArgumentOutOfRangeException("index"))
        let p = parameter value
        p.Attach(this)
        items.Insert(index, p)
        plan <- Unchecked.defaultof<_>
    override _.Remove(value: obj) =
        let index = this.IndexOf(value)
        if index >= 0 then this.RemoveAt(index)
    override _.RemoveAt(index: int) =
        let p = items.[index]
        items.RemoveAt(index)
        p.Detach(this)
        plan <- Unchecked.defaultof<_>
    override _.RemoveAt(parameterName: string) = this.RemoveAt(requireName parameterName)
    override _.GetParameter(index: int) = items.[index] :> DbParameter
    override _.GetParameter(name: string) = items.[requireName name] :> DbParameter
    override _.SetParameter(index: int, value: DbParameter) =
        let p = parameter value
        if not (Object.ReferenceEquals(p, items.[index])) then
            p.Attach(this)
            items.[index].Detach(this)
            items.[index] <- p
            plan <- Unchecked.defaultof<_>
    override _.SetParameter(name: string, value: DbParameter) = this.SetParameter(requireName name, value)
    interface IAdoParameterOwner with
        member _.InvalidateLayout() = plan <- Unchecked.defaultof<_>
