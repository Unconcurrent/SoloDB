namespace SoloDatabase.Metadata.Generation

open System
open SoloDatabase.Metadata.CodecGeneration

#if METADATA_TOOLING
type TypeShape =
#else
type internal TypeShape =
#endif
    | Scalar = 0
    | DynamicObject = 1
    | Object = 2
    | Array = 3
    | List = 4
    | Nullable = 5
    | Option = 6
    | Enum = 7
    | DocumentReference = 8
    | TypedDocumentReference = 9
    | ManyReference = 10
    | TypedManyReference = 11
    | Tuple = 12
    | FSharpList = 13
    | HashSet = 14
    | Queue = 15
    | Stack = 16
    | LinkedList = 17
    | Dictionary = 18
    | SortedDictionary = 19
    | DictionaryInterface = 20
    | ReadOnlyDictionaryInterface = 21
    | Enumerable = 22
    | OrderedEnumerable = 23
    | Grouping = 24
    | BsonDocument = 25
    | FileReference = 26
    | DirectoryReference = 27
    | FileMany = 28
    | DirectoryMany = 29
    | CustomDictionary = 30
    | CustomReadOnlyDictionary = 31
    | CustomSequence = 32
    | NonGenericDictionary = 33
    | GroupingImplementation = 34

#if METADATA_TOOLING
type CodecRegistrationKind =
#else
type internal CodecRegistrationKind =
#endif
    | Intrinsic = 0
    | Model = 1
    | Tuple = 2
    | Collection = 3

#if METADATA_TOOLING
type CodecArguments =
#else
type internal CodecArguments =
#endif
    | None = 0
    | Target = 1
    | Element = 2
    | Generic = 3
    | TargetAndUnderlying = 4
    | TargetAndContract = 5

#if METADATA_TOOLING
type RawCodecKind =
#else
type internal RawCodecKind =
#endif
    | None = 0
    | Intrinsic = 1
    | Sequence = 2
    | Dictionary = 3
    | Structured = 4
    | Numeric = 5

#if METADATA_TOOLING
type NumericKind =
#else
type internal NumericKind =
#endif
    | Integer = 0
    | Binary = 1
    | Decimal = 2
    | ArbitraryInteger = 3
    | Predicate = 4
    | NumericText = 5
    | Inferred = 6

#if METADATA_TOOLING
type NumericShape =
#else
type internal NumericShape =
#endif
    { Kind: NumericKind; Width: int; Signed: bool; Native: bool }

#if METADATA_TOOLING
type TypeSupportFacts =
#else
type internal TypeSupportFacts =
#endif
    { Definition: string
      ArrayRank: int
      ElementDefinition: string
      Enum: bool
      Tuple: bool
      Dictionary: bool
      ReadOnlyDictionary: bool
      NonGenericDictionary: bool
      Enumerable: bool
      Grouping: bool }

#if METADATA_TOOLING
type TypeSupportDecision =
#else
type internal TypeSupportDecision =
#endif
    { Shape: TypeShape
      Contract: string
      ElementArgument: int
      KeyArgument: int
      IdentityArgument: int
      GenericChildren: bool
      NamedChildren: string[]
      Factory: string
      Registration: string
      Arguments: CodecArguments
      RegistrationKind: CodecRegistrationKind
      ArrayWrapper: bool
      RawKind: RawCodecKind
      RawReader: string
      RawWriter: string }

#if METADATA_TOOLING
type OptionalAccess =
#else
type internal OptionalAccess =
#endif
    | Present
    | Absent
    | Payload

#if METADATA_TOOLING
type SqlTypeArgument =
#else
type internal SqlTypeArgument =
#endif
    | Target = 0
    | Element = 1
    | EnumUnderlying = 2
    | ElementEnumUnderlying = 3

#if METADATA_TOOLING
type SqlReadDecision =
#else
type internal SqlReadDecision =
#endif
    { Method: string; Arguments: SqlTypeArgument[]; NeedsClosedBinding: bool }

#if METADATA_TOOLING
type SqlRawDecision =
#else
type internal SqlRawDecision =
#endif
    { Registration: string; Arguments: SqlTypeArgument[]; Read: SqlReadDecision voption }

[<RequireQualifiedAccess>]
#if METADATA_TOOLING
type RawChildOperation =
#else
type internal RawChildOperation =
#endif
    | Member
    | StoredNumeric
    | Adapted of bool
    | Sequence of string
    | Numeric
    | Formatted
    | Primitive of string
    | Text
    | Boolean
    | Guid
    | Bytes
    | Array

#if METADATA_TOOLING
module TypeSupport =
#else
module internal TypeSupport =
#endif
    // Runtime and compiler frontends supply identity/representation facts. All support
    // precedence and operation selection is authored here, independently of their APIs.
    let private optionDefinition = "Microsoft.FSharp.Core.FSharpOption`1"
    let private nullableDefinition = "System.Nullable`1"
    let private Definition(target: System.Type) =
        if target.IsGenericType then target.GetGenericTypeDefinition().FullName else target.FullName

    let IsScalar definition =
        match definition with
        | "System.String" | "System.Int64" | "System.Int32" | "System.Int16" | "System.Byte"
        | "System.UInt64" | "System.UInt32" | "System.UInt16" | "System.SByte" | "System.Boolean"
        | "System.Half" | "System.Single" | "System.Double" | "System.Decimal" | "System.Char" | "System.Guid"
        | "System.DateTime" | "System.DateTimeOffset" | "System.TimeSpan" | "System.DateOnly" | "System.TimeOnly"
        | "System.Numerics.BigInteger" | "System.IntPtr" | "System.UIntPtr" | "SoloDatabase.JsonSerializator.JsonValue" -> true
        | _ -> false

    // This selection is consumed mechanically by ordinary factories and both IL destinations.
    let RawPrimitive definition =
        match definition with
        | "System.SByte" -> "SByte"
        | "System.Int16" -> "Int16"
        | "System.Int32" -> "Int32"
        | "System.Int64" -> "Int64"
        | "System.Byte" -> "Byte"
        | "System.UInt16" -> "UInt16"
        | "System.UInt32" -> "UInt32"
        | "System.UInt64" -> "UInt64"
        | "System.Double" -> "Double"
        | "System.Single" -> "Single"
        | "System.Half" -> "Half"
        | "System.Decimal" -> "Decimal"
        | "System.Boolean" -> "Boolean"
        | "System.Char" -> "Char"
        | "System.String" -> "String"
        | "System.Byte[]" -> "Bytes"
        | "System.Guid" -> "Guid"
        | "System.IntPtr" -> "IntPtr"
        | "System.UIntPtr" -> "UIntPtr"
        | "System.Type" -> "Type"
        | "System.DateTime" -> "DateTime"
        | "System.DateTimeOffset" -> "DateTimeOffset"
        | "System.DateOnly" -> "DateOnly"
        | "System.TimeOnly" -> "TimeOnly"
        | "System.TimeSpan" -> "TimeSpan"
        | "System.Numerics.BigInteger" -> "BigInteger"
        | _ -> ""

    let private numericDefinitions = [|
        "System.SByte", { Kind = NumericKind.Integer; Width = 8; Signed = true; Native = false }
        "System.Byte", { Kind = NumericKind.Integer; Width = 8; Signed = false; Native = false }
        "System.Int16", { Kind = NumericKind.Integer; Width = 16; Signed = true; Native = false }
        "System.UInt16", { Kind = NumericKind.Integer; Width = 16; Signed = false; Native = false }
        "System.Int32", { Kind = NumericKind.Integer; Width = 32; Signed = true; Native = false }
        "System.UInt32", { Kind = NumericKind.Integer; Width = 32; Signed = false; Native = false }
        "System.Int64", { Kind = NumericKind.Integer; Width = 64; Signed = true; Native = false }
        "System.UInt64", { Kind = NumericKind.Integer; Width = 64; Signed = false; Native = false }
        "System.IntPtr", { Kind = NumericKind.Integer; Width = 0; Signed = true; Native = true }
        "System.UIntPtr", { Kind = NumericKind.Integer; Width = 0; Signed = false; Native = true }
        "System.Half", { Kind = NumericKind.Binary; Width = 16; Signed = true; Native = false }
        "System.Single", { Kind = NumericKind.Binary; Width = 32; Signed = true; Native = false }
        "System.Double", { Kind = NumericKind.Binary; Width = 64; Signed = true; Native = false }
        "SoloDatabase.JsonSerializator.JsonSqlNumber", { Kind = NumericKind.Inferred; Width = 0; Signed = true; Native = false }
        "System.Object", { Kind = NumericKind.Inferred; Width = 0; Signed = true; Native = false }
        "System.String", { Kind = NumericKind.NumericText; Width = 0; Signed = false; Native = false }
        "System.Boolean", { Kind = NumericKind.Predicate; Width = 1; Signed = false; Native = false }
        "System.Numerics.BigInteger", { Kind = NumericKind.ArbitraryInteger; Width = 0; Signed = true; Native = false }
        "System.Decimal", { Kind = NumericKind.Decimal; Width = 96; Signed = true; Native = false }
    |]

    let private optionalNumeric = [|"System.Half"|]
    let OptionalChildren shape = if shape = TypeShape.DynamicObject then optionalNumeric else [||]
    let NumericTextAdapter definition = if definition = "System.Half" then Some "System.Single" else None

    let Numeric definition =
        numericDefinitions |> Array.tryPick(fun (name,shape) -> if name = definition then Some shape else None)

    let private numericAdapters = [|
        "System.Char", "System.UInt16", [||]
        "System.DateTime", "System.Int64", [|"System.DateTime","FromBinary",false|]
        "System.DateTimeOffset", "System.Int64", [|"System.DateTimeOffset","FromUnixTimeMilliseconds",false|]
        "System.DateOnly", "System.Int32", [|"System.DateOnly","FromDayNumber",false|]
        "System.TimeSpan", "System.Double", [|"System.TimeSpan","FromMilliseconds",false|]
        "System.TimeOnly", "System.Double", [|"System.TimeSpan","FromMilliseconds",false; "System.TimeOnly","FromTimeSpan",false|]
    |]

    let NumericCore definition =
        if (Numeric definition).IsSome then Some definition
        else numericAdapters |> Array.tryPick(fun (name,core,_) -> if name = definition then Some core else None)

    let NumericInfrastructure definition =
        match Numeric definition with Some shape -> shape.Kind = NumericKind.Inferred | None -> false

    /// Numeric infrastructure is distinct from a type's full JSON reader and writer.
    let NumericIntrinsicOperation definition =
        match definition with
        | "System.Object" -> "ObjectNumber"
        | "SoloDatabase.JsonSerializator.JsonSqlNumber" -> "SqlNumber"
        | _ -> RawPrimitive definition

    let RawNumericCore definition =
        if NumericInfrastructure definition then None else NumericCore definition

    let NumericParameterResult definition = definition = "SoloDatabase.JsonSerializator.JsonSqlNumber"

    let NumericPredicate definition = definition = "System.Boolean"
    let NumericText definition = definition = "System.String"

    let NumericFinish definition =
        numericAdapters |> Array.tryPick(fun (name,_,finish) -> if name = definition then Some finish else None)
        |> Option.defaultValue [||]

    // Output conversion has different temporal and character roles from numeric input.
    let NumericWriterCore definition =
        if definition = "System.Boolean" || definition = "System.String" || definition = "System.Object" || definition = "SoloDatabase.JsonSerializator.JsonSqlNumber" then None
        elif (Numeric definition).IsSome then Some definition
        else
            match definition with
            | "System.Char" -> Some "System.UInt16"
            | "System.DateOnly" -> Some "System.Int32"
            | "System.DateTime" | "System.DateTimeOffset" | "System.TimeSpan" | "System.TimeOnly" -> Some "System.Int64"
            | _ -> None

    let NumericWriterSteps definition =
        match definition with
        | "System.DateTime" -> [|"System.DateTime","ToBinary",true,"System.Int64"|]
        | "System.DateTimeOffset" -> [|"System.DateTimeOffset","ToUnixTimeMilliseconds",true,"System.Int64"|]
        | "System.DateOnly" -> [|"System.DateOnly","get_DayNumber",true,"System.Int32"|]
        | "System.TimeSpan" -> [|"System.TimeSpan","get_TotalMilliseconds",true,"System.Double"|]
        | "System.TimeOnly" -> [|"System.TimeOnly","ToTimeSpan",true,"System.TimeSpan"; "System.TimeSpan","get_TotalMilliseconds",true,"System.Double"|]
        | _ -> [||]

    let NumericWriterCast definition = definition = "System.TimeSpan" || definition = "System.TimeOnly"

    // Produced numeric parameters retain the intrinsic SQL conversion dependencies.
    let NumericParameterDependencies() = [|"System.Int64"; "System.Double"; "System.Decimal"; "SoloDatabase.JsonSerializator.JsonSqlNumber"|]

    let StoredNumeric definition =
        match definition with
        | "System.Decimal" -> Some(struct("PrepareDecimal","ReadDecimal"))
        | "System.Numerics.BigInteger" -> Some(struct("PrepareBigInteger","ReadBigInteger"))
        | _ -> None

    // Quoted stored numeric leaves carry exact keys; ordinary number tokens keep native conversion.
    let StoredNumericReader definition =
        (if definition = "System.Char" then None else NumericCore definition) |> Option.bind(fun core ->
            match core with
            | "System.SByte" -> Some "ReadKeySByte"
            | "System.Byte" -> Some "ReadKeyByte"
            | "System.Int16" -> Some "ReadKeyInt16"
            | "System.UInt16" -> Some "ReadKeyUInt16"
            | "System.Int32" -> Some "ReadKeyInt32"
            | "System.UInt32" -> Some "ReadKeyUInt32"
            | "System.Int64" -> Some "ReadKeyInt64"
            | "System.UInt64" -> Some "ReadKeyUInt64"
            | "System.IntPtr" -> Some "ReadKeyIntPtr"
            | "System.UIntPtr" -> Some "ReadKeyUIntPtr"
            | "System.Double" -> Some "ReadKeyDouble"
            | "System.Single" -> Some "ReadKeySingle"
            | "System.Half" -> Some "ReadKeyHalf"
            | _ -> None)

    let StoredRole name = "stored:" + name

    // Internal factory dependencies retain original return-type operands even when compiler references hide them.
    let NumericDependencies definition =
        if definition = "System.Object" then Array.append [|"System.Object"|] (numericAdapters |> Array.map(fun (name,_,_) -> name))
        else [||]

    let SupportsValuesWrapper element =
        match element with
        | "System.String" | "System.Int64" | "System.Int32" | "System.Int16" | "System.Byte"
        | "System.UInt64" | "System.UInt32" | "System.UInt16" | "System.SByte" | "System.Boolean"
        | "System.Single" | "System.Double" | "System.Decimal" | "System.Char" | "System.DateTime" | "System.TimeSpan"
        | "System.IntPtr" | "System.UIntPtr" -> true
        | _ -> false

    /// Cold construction facts for selected, validated sequence ranges.
    let SequenceConstruction name =
        match name with
        | "Array" -> struct("", "", false, false, "")
        | "Stack" -> struct("System.Collections.Generic.Stack`1", "Push", true, true, "")
        | "Queue" -> struct("System.Collections.Generic.Queue`1", "Enqueue", true, false, "")
        | "HashSet" -> struct("System.Collections.Generic.HashSet`1", "Add", false, false, "")
        | "LinkedList" -> struct("System.Collections.Generic.LinkedList`1", "AddLast", false, false, "")
        | "List" | "Elements" -> struct("System.Collections.Generic.List`1", "Add", true, false, "")
        | "Enumerable" | "OrderedEnumerable" | "FSharpList" ->
            struct("System.Collections.Generic.List`1", "Add", true, false, name)
        | _ -> invalidArg "name" "The sequence shape has no reconstruction policy."

    let SequenceBinaryReader name element =
        element = "System.Byte" && (name = "List" || name = "Enumerable" || name = "FSharpList")

    let SequenceNullMismatch name element = name = "FSharpList" && element = "System.Byte"

    let SequenceProducesResult shape =
        shape = TypeShape.Enumerable || shape = TypeShape.OrderedEnumerable ||
        shape = TypeShape.Grouping || shape = TypeShape.GroupingImplementation ||
        shape = TypeShape.FileMany || shape = TypeShape.DirectoryMany

    let ReferenceSequence shape =
        match shape with
        | TypeShape.ManyReference | TypeShape.TypedManyReference -> Some(struct(true,false,""))
        | TypeShape.FileMany -> Some(struct(false,true,"SoloDatabase.FileRef"))
        | TypeShape.DirectoryMany -> Some(struct(false,true,"SoloDatabase.DirectoryRef"))
        | _ -> None

    let OptionalQueryMembers kind =
        match kind with
        | 1 -> [| "Value" |]
        | 2 -> [| "HasValue"; "Value" |]
        | _ -> [||]

    let RawAdaptation definition isEnum =
        if isEnum then 3
        elif definition = optionDefinition then 1
        elif definition = nullableDefinition then 2
        else 0

    let ForShape shape =
        let decision factory registration arguments children kind raw reader writer contract named =
            { Shape = shape; Factory = factory; Registration = registration; Arguments = arguments
              GenericChildren = children; NamedChildren = named; RegistrationKind = kind
              ArrayWrapper = shape = TypeShape.Array; RawKind = raw; RawReader = reader; RawWriter = writer; Contract = contract
              ElementArgument = (match shape with TypeShape.Dictionary | TypeShape.SortedDictionary | TypeShape.DictionaryInterface | TypeShape.ReadOnlyDictionaryInterface | TypeShape.Grouping | TypeShape.GroupingImplementation | TypeShape.CustomDictionary | TypeShape.CustomReadOnlyDictionary -> 1 | _ -> 0)
              KeyArgument = (match shape with TypeShape.Dictionary | TypeShape.SortedDictionary | TypeShape.DictionaryInterface | TypeShape.ReadOnlyDictionaryInterface | TypeShape.Grouping | TypeShape.GroupingImplementation | TypeShape.CustomDictionary | TypeShape.CustomReadOnlyDictionary -> 0 | _ -> -1)
              IdentityArgument = (match shape with TypeShape.TypedDocumentReference | TypeShape.TypedManyReference -> 1 | _ -> -1) }
        let intrinsic name arguments children =
            decision name ("Register" + name) arguments children CodecRegistrationKind.Intrinsic RawCodecKind.Intrinsic (name + "Reader") (name + "Writer") "" [||]
        let sequence name =
            decision name ("Register" + name) CodecArguments.Generic true CodecRegistrationKind.Intrinsic RawCodecKind.Sequence "SequenceReader" "SequenceWriter" "" [||]
        let dictionary name =
            decision name ("Register" + name) CodecArguments.Generic true CodecRegistrationKind.Intrinsic RawCodecKind.Dictionary "DictionaryReader" "DictionaryWriter" "" [||]
        let model contract =
            decision "" "" CodecArguments.None false CodecRegistrationKind.Model RawCodecKind.Structured "" "" contract [||]
        match shape with
        | TypeShape.Scalar -> decision "" "RegisterScalar" CodecArguments.Target false CodecRegistrationKind.Intrinsic RawCodecKind.None "" "" "" [||]
        | TypeShape.DynamicObject -> decision "" "RegisterDynamicObject" CodecArguments.None false CodecRegistrationKind.Intrinsic RawCodecKind.None "" "" "" (numericDefinitions |> Array.map fst |> Array.filter(fun name -> name <> "System.Object" && name <> "SoloDatabase.JsonSerializator.JsonSqlNumber" && not(Array.contains name optionalNumeric)))
        | TypeShape.Enum -> intrinsic "Enum" CodecArguments.TargetAndUnderlying false
        | TypeShape.Nullable -> intrinsic "Nullable" CodecArguments.Generic true
        | TypeShape.Option -> intrinsic "Option" CodecArguments.Generic true
        | TypeShape.Array -> { sequence "Array" with Arguments = CodecArguments.Element }
        | TypeShape.List -> sequence "List"
        | TypeShape.FSharpList -> sequence "FSharpList"
        | TypeShape.HashSet -> sequence "HashSet"
        | TypeShape.Queue -> sequence "Queue"
        | TypeShape.Stack -> sequence "Stack"
        | TypeShape.LinkedList -> sequence "LinkedList"
        | TypeShape.Dictionary -> dictionary "Dictionary"
        | TypeShape.SortedDictionary -> dictionary "SortedDictionary"
        | TypeShape.DictionaryInterface -> dictionary "DictionaryInterface"
        | TypeShape.ReadOnlyDictionaryInterface -> dictionary "ReadOnlyDictionaryInterface"
        | TypeShape.DocumentReference -> { intrinsic "DocumentCodec" CodecArguments.Generic true with Registration = "RegisterDocument"; RawReader = "DocumentReader"; RawWriter = "DocumentWriter"; NamedChildren = [|"System.Decimal"|] }
        | TypeShape.TypedDocumentReference -> { intrinsic "TypedDocumentCodec" CodecArguments.Generic true with Registration = "RegisterTypedDocument"; RawReader = "TypedDocumentReader"; RawWriter = "TypedDocumentWriter"; NamedChildren = [|"System.Decimal"|] }
        | TypeShape.ManyReference -> { intrinsic "Many" CodecArguments.Generic true with RawWriter = "SequenceWriter" }
        | TypeShape.TypedManyReference -> { intrinsic "TypedMany" CodecArguments.Generic true with RawWriter = "SequenceWriter" }
        | TypeShape.Enumerable -> { sequence "Enumerable" with RegistrationKind = CodecRegistrationKind.Collection }
        | TypeShape.OrderedEnumerable -> { sequence "OrderedEnumerable" with RegistrationKind = CodecRegistrationKind.Collection }
        | TypeShape.Grouping -> { intrinsic "Grouping" CodecArguments.Generic true with RegistrationKind = CodecRegistrationKind.Collection; RawWriter = "SequenceWriter" }
        | TypeShape.GroupingImplementation -> { intrinsic "GroupingImplementation" CodecArguments.TargetAndContract false with Contract = "System.Linq.IGrouping`2"; RawWriter = "SequenceWriter" }
        | TypeShape.FileReference -> { intrinsic "FileReference" CodecArguments.None false with RegistrationKind = CodecRegistrationKind.Collection; NamedChildren = [|"SoloDatabase.Types.SoloDBFileHeader"|] }
        | TypeShape.DirectoryReference -> { intrinsic "DirectoryReference" CodecArguments.None false with RegistrationKind = CodecRegistrationKind.Collection; NamedChildren = [|"SoloDatabase.Types.SoloDBDirectoryHeader"|] }
        | TypeShape.FileMany -> { intrinsic "FileMany" CodecArguments.None false with RegistrationKind = CodecRegistrationKind.Collection; NamedChildren = [|"SoloDatabase.Types.SoloDBFileHeader"; "SoloDatabase.FileRef"|] }
        | TypeShape.DirectoryMany -> { intrinsic "DirectoryMany" CodecArguments.None false with RegistrationKind = CodecRegistrationKind.Collection; NamedChildren = [|"SoloDatabase.Types.SoloDBDirectoryHeader"; "SoloDatabase.DirectoryRef"|] }
        | TypeShape.Tuple -> { model "" with RegistrationKind = CodecRegistrationKind.Tuple; Factory = "Tuple"; GenericChildren = true }
        | TypeShape.CustomDictionary -> model "System.Collections.Generic.IDictionary`2"
        | TypeShape.CustomReadOnlyDictionary -> model "System.Collections.Generic.IReadOnlyDictionary`2"
        | TypeShape.CustomSequence -> model "System.Collections.Generic.IEnumerable`1"
        | TypeShape.NonGenericDictionary | TypeShape.BsonDocument | TypeShape.Object -> model ""
        | value -> invalidArg "shape" (string value)

    let StoredTypeName (name: string) =
        if name.Length > 0 && (uint32(int name[0]-int 'A') <= 25u || uint32(int name[0]-int 'a') <= 25u) then Some name
        else None

    let FormattedReader definition =
        match RawPrimitive definition with
        | "Guid" -> "Guid"
        | "Bytes" -> "Bytes"
        | _ -> ""

    let FiniteCaptureRoles shape =
        let marker =
            match shape with
            | TypeShape.DocumentReference | TypeShape.TypedDocumentReference | TypeShape.FileReference | TypeShape.DirectoryReference -> true
            | _ -> false
        let envelope =
            (ForShape shape).RawKind = RawCodecKind.Sequence || (ReferenceSequence shape).IsSome ||
            shape = TypeShape.Grouping || shape = TypeShape.GroupingImplementation || shape = TypeShape.CustomSequence
        marker,envelope

    let ForDefinition shape definition =
        let decision = ForShape shape
        if shape = TypeShape.Scalar && ((NumericCore definition).IsSome || definition = "System.Guid" || definition = "System.Byte[]") then
            { decision with Registration = "RegisterScalarTree"; RawKind = RawCodecKind.Numeric
                            Factory = (if (NumericTextAdapter definition).IsSome then "Numeric" else decision.Factory)
                            Arguments = (if (NumericTextAdapter definition).IsSome then CodecArguments.Target else decision.Arguments) }
        else decision

    let Select (facts: TypeSupportFacts) =
        let shape =
            match facts.Definition with
            | "SoloDatabase.FileRef" -> TypeShape.FileReference
            | "SoloDatabase.DirectoryRef" -> TypeShape.DirectoryReference
            | "SoloDatabase.FileRefMany" -> TypeShape.FileMany
            | "SoloDatabase.DirectoryRefMany" -> TypeShape.DirectoryMany
            | "SoloDatabase.MongoDB.BsonDocument" -> TypeShape.BsonDocument
            | _ when facts.Enum -> TypeShape.Enum
            | _ when IsScalar facts.Definition || (facts.ArrayRank = 1 && facts.ElementDefinition = "System.Byte") -> TypeShape.Scalar
            | "System.Object" -> TypeShape.DynamicObject
            | _ when facts.Tuple -> TypeShape.Tuple
            | _ when facts.ArrayRank = 1 -> TypeShape.Array
            | _ when facts.ArrayRank > 1 -> TypeShape.Object
            | definition when definition = nullableDefinition -> TypeShape.Nullable
            | definition when definition = optionDefinition -> TypeShape.Option
            | "System.Collections.Generic.List`1" -> TypeShape.List
            | "Microsoft.FSharp.Collections.FSharpList`1" -> TypeShape.FSharpList
            | "System.Collections.Generic.HashSet`1" -> TypeShape.HashSet
            | "System.Collections.Generic.Queue`1" -> TypeShape.Queue
            | "System.Collections.Generic.Stack`1" -> TypeShape.Stack
            | "System.Collections.Generic.LinkedList`1" -> TypeShape.LinkedList
            | "System.Collections.Generic.IEnumerable`1" -> TypeShape.Enumerable
            | "System.Linq.IOrderedEnumerable`1" -> TypeShape.OrderedEnumerable
            | "System.Linq.IGrouping`2" -> TypeShape.Grouping
            | "System.Collections.Generic.Dictionary`2" -> TypeShape.Dictionary
            | "System.Collections.Generic.SortedDictionary`2" -> TypeShape.SortedDictionary
            | "System.Collections.Generic.IDictionary`2" -> TypeShape.DictionaryInterface
            | "System.Collections.Generic.IReadOnlyDictionary`2" -> TypeShape.ReadOnlyDictionaryInterface
            | "SoloDatabase.DBRef`1" -> TypeShape.DocumentReference
            | "SoloDatabase.DBRef`2" -> TypeShape.TypedDocumentReference
            | "SoloDatabase.DBRefMany`1" -> TypeShape.ManyReference
            | "SoloDatabase.DBRefMany`2" -> TypeShape.TypedManyReference
            | _ when facts.Dictionary -> TypeShape.CustomDictionary
            | _ when facts.ReadOnlyDictionary -> TypeShape.CustomReadOnlyDictionary
            | _ when facts.NonGenericDictionary -> TypeShape.NonGenericDictionary
            | _ when facts.Grouping -> TypeShape.GroupingImplementation
            | _ when facts.Enumerable -> TypeShape.CustomSequence
            | _ -> TypeShape.Object
        ForDefinition shape facts.Definition

    // SQL native columns have a different contract from JSON. Both generated and
    // ordinary row readers use this selection, including the CLR nullable boxing rule.
    let private sqlRead methodName arguments closed =
        { Method = methodName; Arguments = arguments; NeedsClosedBinding = closed }

    let SqlReader shape underlyingShape array =
        match shape with
        | TypeShape.Enum -> sqlRead "ReadEnum" [|SqlTypeArgument.Target; SqlTypeArgument.EnumUnderlying|] false
        | TypeShape.Nullable when underlyingShape = TypeShape.Enum ->
            sqlRead "ReadNullableEnum" [|SqlTypeArgument.Element; SqlTypeArgument.ElementEnumUnderlying|] true
        | TypeShape.Nullable -> sqlRead "ReadNullable" [|SqlTypeArgument.Element|] true
        | TypeShape.Option -> sqlRead "ReadOption" [|SqlTypeArgument.Element|] true
        | TypeShape.Scalar when not array -> sqlRead "Read" [|SqlTypeArgument.Target|] false
        | _ -> sqlRead "ReadObject" [|SqlTypeArgument.Target|] false

    let ScalarRow shape =
        shape = TypeShape.Scalar || shape = TypeShape.Nullable || shape = TypeShape.Option || shape = TypeShape.Enum

    let RuntimeFacts (target: System.Type) =
        let definition = if target.IsGenericType then target.GetGenericTypeDefinition() else target
        let contract name =
            let matches (value: System.Type) = (if value.IsGenericType then value.GetGenericTypeDefinition() else value).FullName = name
            matches target || (target.GetInterfaces() |> Array.exists matches)
        { Definition = definition.FullName; ArrayRank = if target.IsArray then target.GetArrayRank() else 0
          ElementDefinition = if target.IsArray then target.GetElementType().FullName else ""
          Enum = target.IsEnum; Tuple = not target.IsArray && definition.FullName <> null && (definition.FullName.StartsWith("System.Tuple`",System.StringComparison.Ordinal) || definition.FullName.StartsWith("System.ValueTuple`",System.StringComparison.Ordinal))
          Dictionary = contract "System.Collections.Generic.IDictionary`2"; ReadOnlyDictionary = contract "System.Collections.Generic.IReadOnlyDictionary`2"
          NonGenericDictionary = contract "System.Collections.IDictionary"; Enumerable = contract "System.Collections.Generic.IEnumerable`1"; Grouping = contract "System.Linq.IGrouping`2" }

    let Runtime (target: System.Type) = Select (RuntimeFacts target)

    let IsOption(target: System.Type) = Definition target = optionDefinition

    let OptionalMember(target: System.Type, name: string) =
        let definition = Definition target
        if definition = optionDefinition || definition = "Microsoft.FSharp.Core.OptionModule" then
            match name with
            | "IsSome" | "get_IsSome" -> ValueSome OptionalAccess.Present
            | "IsNone" | "get_IsNone" -> ValueSome OptionalAccess.Absent
            | "Value" | "GetValue" -> ValueSome OptionalAccess.Payload
            | _ -> ValueNone
        elif definition = nullableDefinition then
            match name with
            | "HasValue" -> ValueSome OptionalAccess.Present
            | "Value" -> ValueSome OptionalAccess.Payload
            | _ -> ValueNone
        else ValueNone

    let SqlRawRegistration shape underlyingShape =
        match shape with
        | TypeShape.Option ->
            { Registration = "RegisterRawOption"; Arguments = [||]
              Read = ValueSome(sqlRead "ReadRawOption" [|SqlTypeArgument.Element|] true) }
        | TypeShape.Nullable when underlyingShape <> TypeShape.Enum ->
            { Registration = "RegisterRawNullable"; Arguments = [||]
              Read = ValueSome(sqlRead "ReadRawNullable" [|SqlTypeArgument.Element|] true) }
        | TypeShape.Enum ->
            { Registration = "RegisterRawEnum"; Arguments = [|SqlTypeArgument.Target; SqlTypeArgument.EnumUnderlying|]; Read = ValueNone }
        | _ -> { Registration = "RegisterRawScalar"; Arguments = [|SqlTypeArgument.Target|]; Read = ValueNone }

    let SqlScalar definition =
        match definition with
        | "System.Byte" | "System.SByte" | "System.Int16" | "System.UInt16" | "System.Int32" | "System.UInt32"
        | "System.Int64" | "System.UInt64" | "System.Single" | "System.Double" | "System.Decimal" | "System.Numerics.BigInteger" | "System.String"
        | "System.Char" | "System.Boolean" | "System.Guid" | "System.DateTime" | "System.DateTimeOffset"
        | "System.DateOnly" | "System.TimeOnly" | "System.TimeSpan" -> true
        | _ -> false

    let Collection shape =
        shape = TypeShape.CustomDictionary || shape = TypeShape.CustomReadOnlyDictionary || shape = TypeShape.CustomSequence || shape = TypeShape.NonGenericDictionary

    let RawPolymorphicFactory(shape, abstractType) =
        abstractType &&
        (Collection shape || shape = TypeShape.Enumerable || shape = TypeShape.OrderedEnumerable ||
         shape = TypeShape.Grouping || shape = TypeShape.DictionaryInterface || shape = TypeShape.ReadOnlyDictionaryInterface)

    /// Construction and insertion phases shared by dictionary instruction destinations.
    let DictionaryConstruction(shape, abstractType) =
        match shape with
        | TypeShape.Dictionary | TypeShape.DictionaryInterface | TypeShape.ReadOnlyDictionaryInterface
        | TypeShape.CustomReadOnlyDictionary -> struct("Dictionary",false,false,false)
        | TypeShape.SortedDictionary -> struct("SortedDictionary",false,false,false)
        | TypeShape.CustomDictionary when abstractType -> struct("Dictionary",false,false,false)
        | TypeShape.CustomDictionary -> struct("",true,true,false)
        | TypeShape.NonGenericDictionary -> struct((if abstractType then "Hashtable" else ""),true,false,true)
        | _ -> invalidArg "shape" "The shape has no dictionary construction policy."

    let DictionaryShape shape =
        shape = TypeShape.Dictionary || shape = TypeShape.SortedDictionary ||
        shape = TypeShape.DictionaryInterface || shape = TypeShape.ReadOnlyDictionaryInterface ||
        shape = TypeShape.CustomDictionary || shape = TypeShape.CustomReadOnlyDictionary ||
        shape = TypeShape.NonGenericDictionary

    /// Only an unsealed mutable-only declaration needs the original preferred-contract probe.
    let DictionaryReadOnlyProbe(readOnly, sealedType, nonGeneric) = not nonGeneric && not readOnly && not sealedType

    let DictionaryFormatProbe(formattable, sealedType, nullable) = not formattable && (not sealedType || nullable)

    /// Native scalar output ignores member type/identity flags; composite bodies retain their scope.
    let TupleMemberScope(primitive, adaptation) = primitive = "" && adaptation <> 3

    let RawFactoryRegistration(shape, abstractType, registration) =
        if RawPolymorphicFactory(shape,abstractType) then registration + "Readers" else registration

    /// Intrinsic closed sequence operands share the original shape classifier.
    let SequenceDefinition definition writer =
        let decision = Select {
            Definition = definition; ArrayRank = 0; ElementDefinition = ""
            Enum = false; Tuple = false; Dictionary = false; ReadOnlyDictionary = false
            NonGenericDictionary = false; Enumerable = false; Grouping = false }
        if decision.RawKind = RawCodecKind.Sequence && decision.Shape <> TypeShape.Array &&
           not(writer && RawPolymorphicFactory(decision.Shape,true)) then Some decision.Factory
        else None

    let RawChildReader(definition, isEnum, arrayRank, stored) =
        if stored && (StoredNumeric definition).IsSome then RawChildOperation.StoredNumeric
        elif RawAdaptation definition isEnum <> 0 then RawChildOperation.Adapted false
        else
            match SequenceDefinition definition false with
            | Some policy -> RawChildOperation.Sequence policy
            | None when (RawNumericCore definition).IsSome -> RawChildOperation.Numeric
            | None when FormattedReader definition <> "" -> RawChildOperation.Formatted
            | None ->
                match RawPrimitive definition with
                | "" when arrayRank = 1 -> RawChildOperation.Array
                | "" -> RawChildOperation.Member
                | name -> RawChildOperation.Primitive name

    let RawChildWriter(definition, isEnum, arrayRank, stored) =
        if stored && (StoredNumeric definition).IsSome then RawChildOperation.StoredNumeric
        elif RawAdaptation definition isEnum <> 0 then RawChildOperation.Adapted(stored && isEnum)
        else
            match SequenceDefinition definition true with
            | Some policy -> RawChildOperation.Sequence policy
            | None when (NumericWriterCore definition).IsSome -> RawChildOperation.Numeric
            | None ->
                match RawPrimitive definition with
                | "String" -> RawChildOperation.Text
                | "Boolean" -> RawChildOperation.Boolean
                | "Guid" -> RawChildOperation.Guid
                | "Bytes" -> RawChildOperation.Bytes
                | "" | "Type" when arrayRank = 1 -> RawChildOperation.Array
                | "" | "Type" -> RawChildOperation.Member
                | name -> RawChildOperation.Primitive name

    let PopulateCollection(shape, argumentCount, list, parameterless) =
        shape = TypeShape.CustomSequence && argumentCount = 1 && list && parameterless

    let internal TreeValueReader(definition, target, arguments: 'T[], helper: Func<string,'T[],'M>) : CodecReaderCall<'T,'M> option =
        match definition with
        | "System.Collections.Generic.KeyValuePair`2" ->
            Some { Result = target; Before = None; After = None
                   Call = Some(helper.Invoke("KeyValuePairDeserialize",arguments)); Cast = false; NullDefault = true }
        | _ -> None

    let internal CollectionTreeReader(shape, target, constructor: 'M option, abstractType, populate, arguments: 'T[], helper: Func<string,'T[],'M>) : CodecReaderCall<'T,'M> =
        let call name types = Some(helper.Invoke(name,types))
        let empty = { Result = target; Before = None; After = None; Call = None; Cast = false; NullDefault = shape <> TypeShape.CustomSequence }
        match shape with
        | TypeShape.CustomDictionary when abstractType -> { empty with Call = call "DictionaryDeserialize" arguments; Cast = true }
        | TypeShape.CustomDictionary ->
            if constructor.IsNone then { empty with Call = call "MissingCollectionConstructor" [|target|] }
            else { empty with Before = constructor; Call = call "CustomDictionaryDeserialize" (Array.append arguments [|target|]) }
        | TypeShape.CustomReadOnlyDictionary -> { empty with Call = call "IReadOnlyDictionaryDeserialize" arguments; Cast = true }
        | TypeShape.NonGenericDictionary when abstractType -> { empty with Call = call "NonGenericDictionaryDeserialize" [||]; Cast = true }
        | TypeShape.NonGenericDictionary ->
            if constructor.IsNone then { empty with Call = call "MissingCollectionConstructor" [|target|] }
            else { empty with Before = constructor; Call = call "CustomNonGenericDictionaryDeserialize" [|target|] }
        | TypeShape.CustomSequence when populate -> { empty with Before = constructor; Call = call "IListDeserialize" [|arguments[0];target|] }
        | TypeShape.CustomSequence -> { empty with After = constructor; Call = call "IEnumerableDeserialize" arguments; Cast = constructor.IsNone }
        | value -> invalidArg "shape" (string value)

    let internal CollectionTreeWriter(shape, target, valueType, abstractType, bytes, arguments: 'T[], helper: Func<string,'T[],'M>) : CodecWriterCall<'T,'M> =
        let name,types =
            if abstractType then "SerializeAbstract",[||]
            else match shape with
                 | TypeShape.CustomDictionary | TypeShape.CustomReadOnlyDictionary -> "SerializeDictionary",arguments
                 | TypeShape.NonGenericDictionary -> "SerializeNonGenericDictionary",[||]
                 | TypeShape.CustomSequence -> if bytes then "SerializeByteSequence",[||] else "IEnumerableSerialize",arguments
                 | value -> invalidArg "shape" (string value)
        { Input = target; ValueType = valueType; Box = valueType && (abstractType || shape <> TypeShape.CustomSequence)
          Call = helper.Invoke(name,types); Virtual = false }

    let RawCollectionReader(shape, abstractType) =
        match shape with
        | TypeShape.CustomDictionary when abstractType -> struct("DictionaryReader","DictionaryInterface")
        | TypeShape.CustomDictionary -> struct("CustomDictionaryReader","")
        | TypeShape.CustomReadOnlyDictionary -> struct("DictionaryReader","ReadOnlyDictionaryInterface")
        | TypeShape.NonGenericDictionary -> struct("NonGenericDictionaryReader","")
        | TypeShape.CustomSequence -> struct("CustomSequenceReader","")
        | _ -> struct("","")

    let RawCollectionWriter(shape, abstractType, nonGenericEnumerable) =
        if abstractType && Collection shape then "AbstractWriter"
        else match shape with
             | TypeShape.CustomDictionary | TypeShape.CustomReadOnlyDictionary -> "DictionaryWriter"
             | TypeShape.NonGenericDictionary -> "NonGenericDictionaryWriter"
             | TypeShape.CustomSequence -> "SequenceWriter"
             | _ when nonGenericEnumerable -> "NonGenericSequenceWriter"
             | _ -> ""
