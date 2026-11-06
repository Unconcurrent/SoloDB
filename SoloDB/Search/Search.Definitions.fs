namespace SoloDatabase

open System
open System.Linq.Expressions

/// Shared keeps selected text in the document; Copied also stores it in FTS5.
type SearchTextStorage =
    | Shared = 0
    | Copied = 1

type internal SearchFieldDeclaration =
    { Selector: LambdaExpression; Alias: string; Weight: double }

/// Configuration is consumed by explicit Ensure, never by collection acquisition.
[<Sealed>]
type SearchIndexBuilder<'T>() =
    let fields = ResizeArray<SearchFieldDeclaration>()
    let mutable prefixLengths = Array.empty<int>
    member val Storage = SearchTextStorage.Shared with get,set
    member val Tokenizer = "unicode61" with get,set
    member _.PrefixLengths
        with get() = Array.copy prefixLengths
        and set value =
            if isNull value then nullArg "value"
            prefixLengths <- Array.copy value
    member private this.Add(selector: LambdaExpression, weight, alias) =
        if isNull selector then nullArg "selector"
        if Double.IsNaN weight || Double.IsInfinity weight then
            invalidArg "weight" "A search field weight must be finite."
        fields.Add({ Selector = selector; Alias = alias; Weight = weight })
        this
    member this.Field<'Field>(selector: Expression<Func<'T,'Field>>, weight: double, name: string) =
        this.Add(selector,weight,name)
    member this.Field<'Field>(selector: Expression<Func<'T,'Field>>, weight: double) =
        this.Add(selector,weight,null)
    member this.Field<'Field>(selector: Expression<Func<'T,'Field>>) =
        this.Add(selector,1.0,null)
    member internal _.Fields = fields.ToArray()

type internal SearchResolvedField =
    { Path: string; Alias: string; Weight: double }

type internal SearchResolvedDefinition =
    { Fields: SearchResolvedField[]; Storage: SearchTextStorage; Tokenizer: string; Prefix: int[] }

type internal SearchIndexDescriptor =
    { Id: int64; Collection: string; Name: string; Version: int; Definition: SearchResolvedDefinition; Objects: string }
    member this.Table = "SoloDB.Search_" + string this.Id
    member this.View = this.Table + "_source"

/// A stored field and its native search column.
[<Sealed>]
type SearchFieldInfo internal (storedPath: string, alias: string, weight: double) =
    member _.StoredPath = storedPath
    member _.Alias = alias
    member _.Weight = weight

/// An immutable snapshot of a declared search index.
[<Sealed>]
type SearchIndexInfo internal (name: string, version: int, storage: SearchTextStorage, tokenizer: string,
                               fields: SearchFieldInfo[], prefixLengths: int[]) =
    let fields = Array.AsReadOnly(Array.copy fields)
    let prefixLengths = Array.AsReadOnly(Array.copy prefixLengths)
    member _.Name = name
    member _.Version = version
    member _.Storage = storage
    member _.Tokenizer = tokenizer
    member _.Fields : Collections.Generic.IReadOnlyList<SearchFieldInfo> = fields
    member _.PrefixLengths : Collections.Generic.IReadOnlyList<int> = prefixLengths

/// A diagnosed inconsistency in an explicitly verified search index.
type SearchVerificationKind =
    | MissingArtifact = 0
    | AlteredArtifact = 1
    | RankMismatch = 2
    | IntegrityFailure = 3
    | ContentMismatch = 4
    | UnownedArtifact = 5

[<Sealed>]
type SearchVerificationFinding internal (kind: SearchVerificationKind, objectName: string, description: string) =
    member _.Kind = kind
    member _.ObjectName = objectName
    member _.Description = description

/// Verification reports damage; rebuilding remains an explicit operation.
[<Sealed>]
type SearchVerification internal (name: string, findings: SearchVerificationFinding[]) =
    let findings = Array.AsReadOnly(Array.copy findings)
    member _.Name = name
    member _.IsValid = findings.Count = 0
    member _.Findings : Collections.Generic.IReadOnlyList<SearchVerificationFinding> = findings
