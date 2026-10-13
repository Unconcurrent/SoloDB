namespace SoloDatabase

open System
open System.Collections.Generic
open System.Numerics
open SoloDatabase.JsonSerializator
open SoloDatabase.Metadata.Generation

/// Closed serialized facts form a graph; recursive members retain their original back-edges.
module internal NumericMigrationPlan =
    type Selection =
        | Member of string
        | Index of int
        | Elements
        | Values
        | SameValue
        | ActualType of string
        | CaseField of int * string

    type Edge = { Source: int; Target: int; Selection: Selection }
    type Plan = { Root: int; Leaves: struct(int * NumericKind)[]; Edges: Edge[]; Dynamic: struct(int * Type)[] }

    let create (rootType: Type) (actualTypes: Dictionary<Type,HashSet<Type>>) =
        let states=Dictionary<Type,int>()
        let leaves=ResizeArray<struct(int * NumericKind)>()
        let edges=ResizeArray<Edge>()
        let dynamic=ResizeArray<struct(int * Type)>()
        let mutable nextState=0
        let allocate() =
            let state=nextState
            nextState<-state+1
            state
        let rec prepare (target: Type) =
            match states.TryGetValue target with
            | true,state -> state
            | _ ->
                let state=allocate()
                states.Add(target,state)
                if typeof<decimal>.Equals(target) then leaves.Add(struct(state,NumericKind.Decimal))
                elif typeof<BigInteger>.Equals(target) then leaves.Add(struct(state,NumericKind.ArbitraryInteger))
                else
                    let shape=JsonValue.SerializedValueFacts target
                    let child selection valueType = edges.Add({Source=state;Target=prepare valueType;Selection=selection})
                    let members fields = for struct(name,valueType) in fields do child (Member name) valueType
                    match shape with
                    | (JsonSerializationShape.Scalar | JsonSerializationShape.ReferenceIdentity _) as scalar ->
                        let identity =
                            match scalar with
                            | JsonSerializationShape.ReferenceIdentity identity -> ValueSome identity
                            | _ ->
                                TypeMetadataCache.Read(target,fun metadata -> metadata.ReferenceType)
                                |> ValueOption.bind (fun reference ->
                                    if not reference.Value.Many && not(isNull reference.Value.Identity) then ValueSome reference.Value.Identity else ValueNone)
                        identity |> ValueOption.iter (child (Member "$dbrefTypedId"))
                    | JsonSerializationShape.NullableValue payload -> child SameValue payload
                    | JsonSerializationShape.SequenceValues element -> child Elements element
                    | JsonSerializationShape.DictionaryValues value -> child Values value
                    | JsonSerializationShape.GroupingValues(key,element) ->
                        child (Member "Key") key
                        child Elements element
                        let items=allocate()
                        edges.Add({Source=state;Target=items;Selection=Member "Items"})
                        edges.Add({Source=items;Target=prepare element;Selection=Elements})
                    | JsonSerializationShape.ArrayMembers fields ->
                        for index=0 to fields.Length-1 do child (Index index) fields[index].ValueType
                    | JsonSerializationShape.DeclaredArrayMembers fields ->
                        for index=0 to fields.Length-1 do child (Index index) fields[index]
                    | JsonSerializationShape.ObjectMembers fields ->
                        members(fields |> Array.map(fun field -> struct(field.StoredName,field.ValueType)))
                    | JsonSerializationShape.DeclaredMembers fields ->
                        match TypeMetadataCache.Read(target,fun metadata -> metadata.UnionStorage) with
                        | ValueSome _ -> members fields
                        | _ when not target.IsAbstract -> members fields
                        | _ -> ()
                    | JsonSerializationShape.Dynamic | JsonSerializationShape.Uninterpreted -> ()
                    let modelShape =
                        match shape with
                        | JsonSerializationShape.ObjectMembers _ | JsonSerializationShape.DeclaredMembers _ | JsonSerializationShape.Dynamic -> true
                        | _ -> false
                    match (if modelShape then TypeMetadataCache.Read(target,fun metadata -> metadata.UnionStorage) else ValueNone) with
                    | ValueSome cases ->
                        let struct(hasTag,tags,names,types)=cases.Value
                        if hasTag then
                            for index=0 to tags.Length-1 do
                                for field=0 to names[index].Length-1 do
                                    child (CaseField(tags[index],names[index][field])) types.[index].[field]
                    | ValueNone -> ()
                    if modelShape && not target.IsValueType && not target.IsSealed then
                        dynamic.Add(struct(state,target))
                        match actualTypes.TryGetValue target with
                        | true,types -> for actual in types do child (ActualType actual.FullName) actual
                        | _ -> ()
                state
        let root=prepare rootType
        {Root=root;Leaves=leaves.ToArray();Edges=edges.ToArray();Dynamic=dynamic.ToArray()}
