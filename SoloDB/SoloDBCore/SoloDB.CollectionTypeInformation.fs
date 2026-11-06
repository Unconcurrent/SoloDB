namespace SoloDatabase

open System
open System.Collections.Generic
open SoloDatabase.Connections
open SoloDatabase.JsonSerializator
open SoloDatabase.Metadata.CodecGeneration

/// Complete model snapshots share the collection row and its transaction lifetime.
module internal CollectionTypeInformation =
    let private hasColumn (connection: NativeSqliteConnection) =
        let mutable statement = connection.OpenStatement("SELECT EXISTS(SELECT 1 FROM pragma_table_info('SoloDBCollections') WHERE name='TypeInformation')")
        try
            statement.StepMaintenance() |> ignore
            let found = statement.Row.GetBoolean(0)
            statement.Dispose()
            found
        with error -> statement.Abort(error); reraise()

    let ensureColumn (source: NativeConnection) =
        let required =
            use connection = source.Get()
            not connection.IsReadOnly && not(hasColumn connection)
        if required then
            source.WithTransaction(fun connection ->
                if not(hasColumn connection) then
                    let mutable statement = connection.OpenStatement("ALTER TABLE SoloDBCollections ADD COLUMN TypeInformation BLOB")
                    try statement.StepMaintenance() |> ignore; statement.Dispose()
                    with error -> statement.Abort(error); reraise())

    let private fields = function Object fields -> fields | _ -> invalidOp "A recorded type description must be an object."
    let private items = function List values -> values | _ -> invalidOp "Recorded type histories must be arrays."
    let private field name value = (fields value)[name]
    let private key value = struct(field "For" value,field "Identity" value)
    let private shape value = field "Shape" value
    let private alternatives value = items(field "Alternatives" value)
    let private sameDeclaration left right = field "Contract" left = field "Contract" right
    let private contains proposed saved =
        let saved = alternatives saved
        alternatives proposed |> Seq.forall(fun value ->
            saved |> Seq.exists(fun candidate -> key value = key candidate && field "Contract" value = field "Contract" candidate))
    let private compatible proposed saved =
        let saved = alternatives saved
        alternatives proposed |> Seq.forall(fun value ->
            let matches = saved |> Seq.filter(fun candidate -> key value = key candidate) |> Seq.toArray
            matches.Length = 0 || matches |> Array.exists(fun candidate -> field "Contract" value = field "Contract" candidate))

    let private read (connection: NativeSqliteConnection) name =
        let mutable statement = connection.OpenStatement("SELECT json(TypeInformation) FROM SoloDBCollections WHERE Name=@name")
        try
            statement.BindText(1,name)
            let result =
                if statement.StepMaintenance() && not(statement.Row.IsDBNull(0)) then
                    ResizeArray<JsonValue>(items(JsonValue.Parse(statement.Row.GetString(0))))
                else ResizeArray<JsonValue>()
            statement.Dispose()
            result
        with error -> statement.Abort(error); reraise()

    // Equal encodings prove equal values; different encodings say nothing.
    // Short-circuit the common positive case, then compare decoded JSON. The
    // structural fallback below also handles equivalent escaped/object forms.
    let private present = "CASE WHEN jsonb_extract(value,'$.Shape')=@shape THEN 1 ELSE json_extract(value,'$.Shape')=json(@shape) END"
    let private equalContract left right =
        "CASE WHEN jsonb_extract(" + left + ")=jsonb_extract(" + right + ") THEN 1 ELSE json_extract(" + left + ")=json_extract(" + right + ") END"
    let private alternative value =
        "json_extract(" + value + ",'$.For'),json_extract(" + value + ",'$.Identity'),json_extract(" + value + ",'$.Contract')"
    let private lookupSql iterator =
        "SELECT EXISTS(SELECT 1 FROM " + iterator + "(TypeInformation) AS saved WHERE (" + present + ") OR (" +
        equalContract "saved.value,'$.Shape.Contract'" "@shape,'$.Contract'" +
        " AND NOT EXISTS(SELECT " + alternative "proposed.value" + " FROM " + iterator +
        "(@shape,'$.Alternatives') AS proposed EXCEPT SELECT " + alternative "candidate.value" + " FROM " +
        iterator + "(saved.value,'$.Shape.Alternatives') AS candidate))) FROM SoloDBCollections WHERE Name=@name LIMIT 1"
    // JSONB iteration is optional, not a higher engine admission floor.
    let private lookupText = lookupSql "json_each"
    let private lookupBinary = lookupSql "jsonb_each"
    let private encoded (connection: NativeSqliteConnection) (description: CollectionTypeDescription) =
        let saved = Threading.Volatile.Read(&description.EncodedShape)
        if not(isNull saved) then saved else
        let mutable statement = connection.OpenStatement("SELECT jsonb(@shape)")
        try
            statement.BindText(1,description.Shape)
            statement.StepMaintenance() |> ignore
            let bytes = statement.Row.GetBlob(0)
            statement.Dispose()
            let previous = Threading.Interlocked.CompareExchange(&description.EncodedShape,bytes,null)
            if isNull previous then bytes else previous
        with error -> statement.Abort(error); reraise()

    let lookup (connection: NativeSqliteConnection) name (description: CollectionTypeDescription) =
        let sql = if SQLitePCL.raw.sqlite3_libversion_number() >= 3051000 then lookupBinary else lookupText
        let bytes = encoded connection description
        let mutable statement = connection.OpenStatement(sql)
        let struct(exists,recorded) =
            try
                statement.BindBlob(1,bytes,bytes.Length)
                statement.BindText(2,name)
                let result = if statement.StepMaintenance() then struct(true,statement.Row.GetBoolean(0)) else struct(false,false)
                statement.Dispose()
                result
            with error -> statement.Abort(error); reraise()
        if not exists || recorded then struct(exists,recorded)
        else
            // Alternate legal JSON encodings and partial discovery sets take a
            // bounded read-only semantic check through the existing JSON owner.
            let proposed = JsonValue.Parse description.Shape
            struct(true,read connection name |> Seq.exists(fun entry ->
                let saved = shape entry
                sameDeclaration proposed saved && contains proposed saved))

    let private copy value = Dictionary<string,JsonValue>(fields value,StringComparer.Ordinal)
    let private pairs entry =
        let values = alternatives(shape entry)
        let labels = items(field "Alternatives" (field "Labels" entry))
        Array.init values.Count (fun index -> struct(values[index],labels[index]))
    let private withAlternatives root (values: struct(JsonValue * JsonValue)[]) =
        let ordered = values |> Array.sortWith(fun struct(a,_) struct(b,_) ->
            let struct(owner,identity) = key a
            let struct(other,target) = key b
            let compare = String.CompareOrdinal(owner.ToJsonString(),other.ToJsonString())
            if compare <> 0 then compare else
            let compare = String.CompareOrdinal(identity.ToJsonString(),target.ToJsonString())
            if compare <> 0 then compare else String.CompareOrdinal((field "Contract" a).ToJsonString(),(field "Contract" b).ToJsonString()))
        let result,meaning,labels = copy root,copy(shape root),copy(field "Labels" root)
        meaning["Alternatives"] <- List(ordered |> Array.map(fun struct(value,_) -> value))
        labels["Alternatives"] <- List(ordered |> Array.map(fun struct(_,value) -> value))
        result["Shape"] <- Object meaning
        result["Labels"] <- Object labels
        Object result

    let private merge (connection: NativeSqliteConnection) name (description: CollectionTypeDescription) =
        let history = read connection name
        let proposed = JsonValue.Parse description.History
        let meaning = shape proposed
        let newest predicate = history |> Seq.mapi(fun index value -> index,value) |> Seq.rev |> Seq.tryFind(fun (_,value) -> predicate(shape value))
        match newest(fun saved -> sameDeclaration meaning saved && contains meaning saved) with
        | Some _ -> false
        | None ->
            let proposals = pairs proposed
            // Knowledge discovered by an older live model also belongs to
            // compatible newer snapshots. Existing concrete revisions stay put.
            for index = 0 to history.Count-1 do
                let saved = history[index]
                if sameDeclaration meaning (shape saved) then
                    let existing = pairs saved
                    let added = proposals |> Array.filter(fun struct(value,_) ->
                        existing |> Array.forall(fun struct(candidate,_) -> key value <> key candidate))
                    if added.Length > 0 then history[index] <- withAlternatives saved (Array.append existing added)
            let candidate = newest(fun saved -> sameDeclaration meaning saved && compatible meaning saved)
            match candidate with
            | Some(index,saved) ->
                let existing = pairs saved
                let added = proposals |> Array.filter(fun struct(value,_) ->
                    existing |> Array.forall(fun struct(candidate,_) -> key value <> key candidate))
                history[index] <- withAlternatives saved (Array.append existing added)
            | None ->
                let previous = newest(sameDeclaration meaning)
                let retained =
                    match previous with
                    | Some(_,saved) -> pairs saved |> Array.filter(fun struct(value,_) ->
                        proposals |> Array.forall(fun struct(candidate,_) -> key value <> key candidate))
                    | None -> [||]
                history.Add(withAlternatives proposed (Array.append retained proposals))
                if history.Count > 10 then history.RemoveAt(0)
            let mutable statement = connection.OpenStatement("UPDATE SoloDBCollections SET TypeInformation=jsonb(@history) WHERE Name=@name")
            try
                statement.BindText(1,(List history).ToJsonString())
                statement.BindText(2,name)
                statement.StepMaintenance() |> ignore
                statement.Dispose()
                true
            with error -> statement.Abort(error); reraise()

    /// Every acquisition entrance checks presence before a transactional merge.
    /// The result invalidates invocation-local reuse only when metadata changed.
    let recordChanged (connection: NativeSqliteConnection) name description =
        if connection.IsReadOnly then false else
        let struct(_,recorded) = lookup connection name description
        not recorded && merge connection name description

    let record (connection: NativeSqliteConnection) name description =
        recordChanged connection name description |> ignore

    let tryRead (connection: NativeSqliteConnection) name =
        if not(hasColumn connection) then ValueNone
        else
            let history = read connection name
            if history.Count = 0 then ValueNone else ValueSome((List history).ToJsonString())
