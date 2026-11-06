namespace SoloDatabase

open System

/// Original member signatures retain formal slots independently of closed argument values.
type internal ModelTypeTemplate =
    | UnavailableType
    | FormalType of int
    | NamedType of Type * ModelTypeTemplate[]
    | ElementType of int * ModelTypeTemplate

namespace SoloDatabase.Metadata.CodecGeneration

open System
open System.Collections.Generic
open System.Text
open SoloDatabase
open SoloDatabase.Metadata.Generation
open SoloDatabase.JsonSerializator

/// Original descriptive facts; no value access or executable codec is needed.
type internal CodecDescriptionFacts<'T> = {
    Definition: string
    Scope: string
    GenericDefinition: 'T option
    GenericArguments: 'T[]
    Templates: struct(string * ModelTypeTemplate)[]
    CaseTemplates: ModelTypeTemplate[][]
    UnionRoot: 'T option
    FullName: string
    Identity: string
    Abstract: bool
    Sealed: bool
    RequiresTypeInformation: bool
    NonGenericSequence: bool
    Record: bool
    Decision: TypeSupportDecision
    Arguments: 'T[]
    Members: struct(string * 'T * bool)[]
    Cases: struct(string * int * struct(string * 'T)[])[]
    Enum: struct(string * string)[]
    EnumUnderlying: 'T option
    Flags: bool
    PhysicalId: bool
    CustomId: string option
    Alternatives: 'T[]
    ByteSequence: bool
}

type internal CollectionTypeDescription = {
    History: string
    Contract: string
    Alternatives: string
    Shape: string
    mutable EncodedShape: byte[]
}

/// Shared projection for runtime discovery and compiler-supplied descriptions.
module internal CodecDescription =
    let private quote (value: string) =
        let b = StringBuilder().Append('"')
        JsonStringLiteral.appendStringBuilder value b
        b.Append('"').ToString()
    let private array (values: string[]) = "[" + String.Join(",",values) + "]"
    let private objectValue values =
        "{" + (values |> Array.map(fun (name,value) -> quote name + ":" + value) |> fun fields -> String.Join(",",fields)) + "}"
    let private flag value = if value then "true" else "false"

    let private encoding definition =
        match definition with
        | "System.String" -> "string"
        | "System.Boolean" -> "boolean"
        | "System.Guid" -> "Guid D string"
        | "System.Byte[]" -> "base64 string"
        | "System.Char" -> "unsigned UTF16 code unit"
        | "System.DateTime" -> "DateTime.ToBinary signed integer"
        | "System.DateTimeOffset" -> "Unix milliseconds; original offset is not stored"
        | "System.DateOnly" -> "day number"
        | "System.TimeSpan" | "System.TimeOnly" -> "TotalMilliseconds truncated to signed Int64"
        | "System.Decimal" -> "exact decimal numeric key string"
        | "System.Numerics.BigInteger" -> "exact integer numeric key string"
        | _ ->
            match TypeSupport.Numeric definition with
            | Some shape ->
                let width = if shape.Native then string (IntPtr.Size * 8) else string shape.Width
                (if shape.Kind = NumericKind.Binary then "binary floating point " else "integer ") + width +
                (if shape.Signed then " signed" else " unsigned")
            | None -> "dynamic JSON"

    let assemblyScope (target: Type) =
        let identity = target.Assembly.GetName()
        identity.Version <- null
        identity.FullName

    let rec typeIdentity (target: Type) =
        if target.IsArray then typeIdentity(target.GetElementType()) + "[" + String(',',target.GetArrayRank()-1) + "]"
        elif target.IsGenericType then
            target.GetGenericTypeDefinition().FullName + "[" + String.Join(",",target.GetGenericArguments() |> Array.map typeIdentity) + "], " + assemblyScope target
        else target.FullName + ", " + assemblyScope target

    let describe (facts: Func<Type,CodecDescriptionFacts<Type>>) (root: Type) =
        let projected = HashSet<Type>()
        let build root =
            let labels = ResizeArray<string * string>()
            let active = Dictionary<Type,int>()
            let definitions = Dictionary<Type,int>()
            let mutable next = 0
            let rec signature = function
                | UnavailableType -> objectValue [|"Type",quote "Unknown"|]
                | FormalType position -> objectValue [|"Parameter",string position|]
                | NamedType(target,arguments) -> objectValue [|"Type",quote target.FullName; "Assembly",quote (assemblyScope target); "Arguments",arguments |> Array.map signature |> array|]
                | ElementType(rank,element) -> objectValue [|"ArrayRank",string rank;"Element",signature element|]
            let rec node target =
                match active.TryGetValue target with
                | true,index -> objectValue [|"Reference",string index|]
                | _ ->
                    projected.Add target |> ignore
                    let f = facts.Invoke target
                    match f.GenericDefinition |> Option.bind(fun definition ->
                        match definitions.TryGetValue definition with true,index -> Some index | _ -> None) with
                    | Some index ->
                        let values = ResizeArray<string * string>()
                        values.Add("GenericReference",string index)
                        values.Add("Arguments",f.GenericArguments |> Array.map node |> array)
                        objectValue(values.ToArray())
                    | None ->
                        let index = next
                        next <- next+1
                        active.Add(target,index)
                        match f.GenericDefinition with Some definition -> definitions[definition] <- index | None -> ()
                        let d = f.Decision
                        let values = ResizeArray<string * string>()
                        values.Add("Definition",string index)
                        labels.Add(string index,objectValue [|"FullName",quote f.FullName;"AssemblyQualifiedName",quote f.Identity|])
                        values.Add("FullName",quote f.Definition)
                        values.Add("Assembly",quote f.Scope)
                        values.Add("Abstract",flag f.Abstract)
                        let kind =
                            if f.NonGenericSequence then "Sequence"
                            elif f.UnionRoot.IsSome then "FSharpDUCase"
                            elif f.Cases.Length > 0 then "FSharpDU"
                            elif f.Record then "FSharpRecord"
                            elif d.Shape = TypeShape.Object then "POCO"
                            else string d.Shape
                        values.Add("Type",quote kind)
                        let argument position =
                            if position >= 0 && position < f.Arguments.Length then node f.Arguments[position]
                            else objectValue [|"Type",quote "Dynamic"|]
                        let fields members =
                            members |> Array.map(fun struct(name,child) -> name,node child) |> objectValue
                        match d.Shape with
                        | _ when f.ByteSequence && CodecPreparation.writer(f.Definition,d,true,f.Abstract,f.ByteSequence,f.NonGenericSequence) = RawRootOperation.Scalar ->
                            values.Add("Shape",quote "base64 string")
                        | TypeShape.Scalar ->
                            values.Add("Shape",quote(if f.ByteSequence then "base64 string" else encoding f.Definition))
                        | TypeShape.Enum ->
                            values.Add("Shape",objectValue(f.Enum |> Array.map(fun struct(name,value) -> name,quote value)))
                            values.Add("Flags",flag f.Flags)
                            values.Add("OpenValues","true")
                            match f.EnumUnderlying with Some child -> values.Add("Underlying",node child) | None -> ()
                        | TypeShape.Nullable | TypeShape.Option ->
                            values.Add("Shape",argument 0)
                            values.Add("Null",quote "null encodes no payload; missing members use the existing reader default")
                        | TypeShape.Tuple ->
                            values.Add("Shape",f.Arguments |> Array.map node |> array)
                        | shape when TypeSupport.DictionaryShape shape ->
                            values.Add("Shape",argument d.ElementArgument)
                            values.Add("Key",argument d.KeyArgument)
                            values.Add("KeyEncoding",quote "string, invariant IFormattable, or application ToString")
                        | TypeShape.NonGenericDictionary ->
                            values.Add("Shape",objectValue [|"Type",quote "Dynamic"|])
                            values.Add("KeyEncoding",quote "application string conversion")
                        | TypeShape.DynamicObject | TypeShape.BsonDocument ->
                            values.Add("Shape",objectValue [|"Type",quote "Dynamic"|])
                        | TypeShape.DocumentReference | TypeShape.TypedDocumentReference
                        | TypeShape.FileReference | TypeShape.DirectoryReference ->
                            values.Add("Shape",quote "signed Int64 physical identity or null")
                            if f.Arguments.Length > 0 then values.Add("Target",argument 0)
                            if d.IdentityArgument >= 0 then
                                values.Add("Identity",argument d.IdentityArgument)
                                values.Add("PendingIdentity",quote "$dbrefTypedId object before physical identity resolution")
                        | shape when TypeSupport.ReferenceMany shape ->
                            values.Add("Shape",[|argument 0|] |> array)
                            values.Add("Storage",quote "relation catalogue")
                        | TypeShape.Grouping | TypeShape.GroupingImplementation ->
                            values.Add("Shape",array [|argument 1|])
                        | TypeShape.Object when f.NonGenericSequence ->
                            values.Add("Shape",array [|objectValue [|"Type",quote "Dynamic"|]|])
                        | shape when d.RawKind = RawCodecKind.Sequence || shape = TypeShape.CustomSequence ->
                            let element = argument d.ElementArgument
                            if f.ByteSequence then values.Add("Shape",quote "base64 string")
                            else values.Add("Shape",array [|element|])
                        | _ ->
                            let stored = f.Members |> Array.choose(fun struct(name,child,many) -> if many then None else Some(struct(name,child)))
                            values.Add("Shape",fields stored)
                            let names = HashSet<string>(StringComparer.Ordinal)
                            if stored |> Array.exists(fun struct(name,_) -> not(names.Add name)) then
                                values.Add("Members",f.Members |> Array.mapi(fun index struct(name,child,many) ->
                                    if many then None else
                                    let memberValue = ResizeArray<string * string>()
                                    memberValue.Add("Name",quote name)
                                    memberValue.Add("Shape",node child)
                                    if index < f.Templates.Length then memberValue.Add("Signature",signature (let struct(_,value) = f.Templates[index] in value))
                                    Some(objectValue(memberValue.ToArray()))) |> Array.choose id |> array)
                        if d.Shape = TypeShape.Grouping || d.Shape = TypeShape.GroupingImplementation then
                            values.Add("KeyType",argument 0)
                            values.Add("KeyStored","false")
                        match f.UnionRoot with Some parent -> values.Add("UnionRoot",node parent) | None -> ()
                        if f.Cases.Length > 0 then
                            values.Add("Null",quote "null CLR union values encode as null; other values use the selected case members")
                            values.Add("Cases",f.Cases |> Array.map(fun struct(name,tag,members) ->
                                objectValue [|"Name",quote name;"Tag",string tag;"Fields",fields members|]) |> array)
                        let relations = f.Members |> Array.choose(fun struct(name,child,many) -> if many then Some(struct(name,child)) else None)
                        if relations.Length > 0 then values.Add("Relations",fields relations)
                        if f.PhysicalId then values.Add("PhysicalId",quote "root Id column; nested Id remains in JSON")
                        match f.CustomId with Some name -> values.Add("CustomId",quote name) | None -> ()
                        if not f.NonGenericSequence && (d.Shape = TypeShape.Object || TypeSupport.DictionaryShape d.Shape || d.Shape = TypeShape.NonGenericDictionary) &&
                           (f.RequiresTypeInformation || f.Abstract || (d.Shape = TypeShape.Object && not f.Sealed)) then
                            values.Add("Discriminator",quote "$type")
                        if f.GenericDefinition.IsSome then
                            values.Add("Arguments",f.GenericArguments |> Array.map node |> array)
                            let names = HashSet<string>(StringComparer.Ordinal)
                            if f.Templates |> Array.forall(fun struct(name,_) -> names.Add name) then
                                values.Add("MemberSignatures",f.Templates |> Array.map(fun struct(name,template) -> name,signature template) |> objectValue)
                            if f.CaseTemplates.Length > 0 then
                                values.Add("CaseSignatures",f.CaseTemplates |> Array.map(fun fields -> fields |> Array.map signature |> array) |> array)
                        elif f.GenericArguments.Length > 0 then
                            // Container parameters can include types not stored as
                            // children; retain their identity without preparing codecs.
                            values.Add("TypeArguments",f.GenericArguments |> Array.map(typeIdentity >> quote) |> array)
                        objectValue(values.ToArray())
            struct(node root,objectValue(labels.ToArray()))
        let struct(contract,rootLabels) = build root
        // Alternatives belong to the complete model snapshot, with independent
        // graph references and labels kept outside semantic shape identity.
        let alternatives = ResizeArray<struct(string * string * string * string)>()
        let recorded = HashSet<struct(string * string * string)>()
        let visited = HashSet<Type>()
        let mutable pending = projected |> Seq.toArray
        while pending.Length > 0 do
            for target in pending do
                if visited.Add target then
                    let f = facts.Invoke target
                    for alternative in f.Alternatives do
                        let struct(contract,labels) = build alternative
                        let owner,identity = typeIdentity target,typeIdentity alternative
                        if recorded.Add(struct(owner,identity,contract)) then
                            let a = facts.Invoke alternative
                            alternatives.Add(struct(owner,identity,contract,objectValue [|
                                "FullName",quote a.FullName; "AssemblyQualifiedName",quote a.Identity; "Definitions",labels |]))
            pending <- projected |> Seq.filter(fun target -> not(visited.Contains target)) |> Seq.toArray
        let ordered = alternatives.ToArray() |> Array.sortWith(fun struct(a,b,c,_) struct(x,y,z,_) ->
            let compare = String.CompareOrdinal(a,x)
            if compare <> 0 then compare else
            let compare = String.CompareOrdinal(b,y)
            if compare <> 0 then compare else String.CompareOrdinal(c,z))
        let alternatives = ordered |> Array.map(fun struct(owner,identity,contract,_) ->
            objectValue [|"For",quote owner; "Identity",quote identity; "Contract",contract|]) |> array
        let shape = objectValue [|"Contract",contract;"Alternatives",alternatives|]
        let f = facts.Invoke root
        let history = objectValue [|
            "FullName",quote f.FullName; "AssemblyQualifiedName",quote f.Identity
            "Shape",shape; "Labels",objectValue [|"Root",rootLabels;"Alternatives",ordered |> Array.map(fun struct(_,_,_,labels) -> labels) |> array|] |]
        { History = history; Contract = contract; Alternatives = alternatives; Shape = shape; EncodedShape = Unchecked.defaultof<_> }
