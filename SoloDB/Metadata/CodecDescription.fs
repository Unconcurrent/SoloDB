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

    let describe (facts: Func<'T,CodecDescriptionFacts<'T>>) (root: 'T) =
        let build labels =
            let active = Dictionary<'T,int>()
            let definitions = Dictionary<'T,int>()
            let mutable next = 0
            let rec signature = function
                | UnavailableType -> objectValue [|"Type",quote "Unknown"|]
                | FormalType position -> objectValue [|"Parameter",string position|]
                | NamedType(target,arguments) -> objectValue [|"Type",quote target.FullName; "Assembly",quote (target.Assembly.GetName().Name); "Arguments",arguments |> Array.map signature |> array|]
                | ElementType(rank,element) -> objectValue [|"ArrayRank",string rank;"Element",signature element|]
            let rec node target =
                match active.TryGetValue target with
                | true,index -> objectValue [|"Reference",string index|]
                | _ ->
                    let f = facts.Invoke target
                    match f.GenericDefinition |> Option.bind(fun definition ->
                        match definitions.TryGetValue definition with true,index -> Some index | _ -> None) with
                    | Some index ->
                        let values = ResizeArray<string * string>()
                        values.Add("GenericReference",string index)
                        values.Add("Arguments",f.GenericArguments |> Array.map node |> array)
                        if labels then
                            values.Add("FullName",quote f.FullName)
                            values.Add("AssemblyQualifiedName",quote f.Identity)
                        objectValue(values.ToArray())
                    | None ->
                        let index = next
                        next <- next+1
                        active.Add(target,index)
                        match f.GenericDefinition with Some definition -> definitions[definition] <- index | None -> ()
                        let d = f.Decision
                        let values = ResizeArray<string * string>()
                        values.Add("Definition",string index)
                        if labels then
                            values.Add("FullName",quote f.FullName)
                            values.Add("AssemblyQualifiedName",quote f.Identity)
                        values.Add("Abstract",flag f.Abstract)
                        let kind =
                            if f.NonGenericSequence then "Sequence"
                            elif f.UnionRoot.IsSome then "FSharpDUCase"
                            elif f.Cases.Length > 0 then "FSharpDU"
                            elif f.Record then "FSharpRecord"
                            elif d.Shape = TypeShape.Object then "POCO"
                            else string d.Shape
                        values.Add("Type",quote kind)
                        if not labels then
                            values.Add("ClrType",quote f.Definition)
                            values.Add("Assembly",quote f.Scope)
                        let argument position =
                            if position >= 0 && position < f.Arguments.Length then node f.Arguments[position]
                            else objectValue [|"Type",quote "Dynamic"|]
                        let fields members =
                            members |> Array.map(fun struct(name,child) -> name,node child) |> objectValue
                        match d.Shape with
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
                            values.Add("KeyType",argument 0)
                            values.Add("KeyStored","false")
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
                           (f.RequiresTypeInformation || f.Abstract || f.Alternatives.Length > 0) then
                            values.Add("Discriminator",quote "$type")
                            values.Add("Alternatives",f.Alternatives |> Array.map node |> array)
                        if f.GenericDefinition.IsSome then
                            values.Add("Arguments",f.GenericArguments |> Array.map node |> array)
                            let names = HashSet<string>(StringComparer.Ordinal)
                            if f.Templates |> Array.forall(fun struct(name,_) -> names.Add name) then
                                values.Add("MemberSignatures",f.Templates |> Array.map(fun struct(name,template) -> name,signature template) |> objectValue)
                            if f.CaseTemplates.Length > 0 then
                                values.Add("CaseSignatures",f.CaseTemplates |> Array.map(fun fields -> fields |> Array.map signature |> array) |> array)
                        objectValue(values.ToArray())
            node root
        let meaning = build false
        let description = build true
        // Exact semantic text permits a native presence query without
        // transferring all retained descriptions or conflating CLR identity labels.
        let result = description.Substring(0,description.Length-1) + ",\"ShapeKey\":" + quote meaning + "}"
        struct(result,meaning)
