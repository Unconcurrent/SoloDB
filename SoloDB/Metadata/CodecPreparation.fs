namespace SoloDatabase.Metadata.CodecGeneration

open SoloDatabase.Metadata.Generation

type internal RawRootOperation =
    | StoredNumeric = 0
    | ConvertedNumeric = 1
    | Scalar = 2
    | Tree = 3
    | Abstract = 4
    | Dynamic = 5
    | Tuple = 6
    | Bson = 7
    | Array = 8
    | Dictionary = 9
    | Sequence = 10
    | Intrinsic = 11
    | Model = 12

type internal CodecFactoryKind =
    | Model = 0 | Numeric = 1 | Tuple = 2 | ReferenceSequence = 3
    | Grouping = 4 | GroupingImplementation = 5 | Sequence = 6
    | Dictionary = 7 | Intrinsic = 8 | Collection = 9

type internal CodecTreeOperation = Model = 0 | Collection = 1 | Bson = 2 | NonGenericSequence = 3 | Abstract = 4

type internal CodecFactoryPreparation = {
    Kind: CodecFactoryKind
    Factory: string
    Registration: string
    Arguments: CodecArguments
    ArrayWrapper: bool
    ReadersOnly: bool
    Optional: int
    ArrayBodies: bool
    ConcreteReaderFactory: string
    ElementArgument: int
}

type internal CodecStoredKind =
    | Reserved = 0 | Numeric = 1 | Optional = 2 | Scalar = 3
    | Reference = 4 | ReferenceSequence = 5 | Grouping = 6 | Sequence = 7 | Body = 8 | GroupingImplementation = 9

type internal CodecStoredPreparation = {
    Kind: CodecStoredKind
    Registration: string
    Arguments: CodecArguments
    ReadersOnly: bool
    ArrayBodies: bool
    ReuseReader: bool
    ReuseWriter: bool
    ElementArgument: int
}

/// Directional publication obligations, independent of reflection or compiler operands.
module internal CodecPreparation =
    let clonePublication shape = shape = TypeShape.Object

    let argumentPublication(shape, abstractType) = shape = TypeShape.Object && not abstractType

    let treeReader shape =
        if TypeSupport.Collection shape then CodecTreeOperation.Collection
        elif shape = TypeShape.BsonDocument then CodecTreeOperation.Bson
        else CodecTreeOperation.Model

    let treeWriter(shape, nonGenericEnumerable, abstractType) =
        if abstractType then CodecTreeOperation.Abstract
        else
            match treeReader shape with
            | CodecTreeOperation.Model when nonGenericEnumerable -> CodecTreeOperation.NonGenericSequence
            | selected -> selected

    let dynamicCompanion(shape, nonGenericEnumerable) =
        shape = TypeShape.NonGenericDictionary || nonGenericEnumerable

    let factory definition (decision: TypeSupportDecision) abstractType =
        let shape = decision.Shape
        let readersOnly = TypeSupport.RawPolymorphicFactory(shape,abstractType)
        let optional = TypeSupport.RawAdaptation definition (shape = TypeShape.Enum)
        let kind =
            if decision.RawKind = RawCodecKind.Numeric then CodecFactoryKind.Numeric
            elif decision.RegistrationKind = CodecRegistrationKind.Tuple then CodecFactoryKind.Tuple
            elif (TypeSupport.ReferenceSequence shape).IsSome then CodecFactoryKind.ReferenceSequence
            elif shape = TypeShape.Grouping then CodecFactoryKind.Grouping
            elif shape = TypeShape.GroupingImplementation then CodecFactoryKind.GroupingImplementation
            elif decision.RawKind = RawCodecKind.Sequence && shape <> TypeShape.Array then CodecFactoryKind.Sequence
            elif decision.RawKind = RawCodecKind.Dictionary then CodecFactoryKind.Dictionary
            elif decision.RegistrationKind = CodecRegistrationKind.Collection then CodecFactoryKind.Collection
            elif decision.RegistrationKind = CodecRegistrationKind.Intrinsic then CodecFactoryKind.Intrinsic
            else CodecFactoryKind.Model
        { Kind = kind; Factory = decision.Factory
          Registration = TypeSupport.RawFactoryRegistration(shape,abstractType,decision.Registration) + (if optional <> 0 then "Tree" else "")
          Arguments = decision.Arguments; ArrayWrapper = decision.ArrayWrapper
          ReadersOnly = readersOnly; Optional = optional; ArrayBodies = shape = TypeShape.Array
          ConcreteReaderFactory = if kind = CodecFactoryKind.Dictionary && readersOnly then "Dictionary" else ""
          ElementArgument = decision.ElementArgument }

    let stored definition (decision: TypeSupportDecision) abstractType =
        let shape = decision.Shape
        let numeric = TypeSupport.StoredNumeric definition
        let converted = TypeSupport.StoredNumericReader definition
        let kind,registration,arguments =
            if numeric.IsSome || shape = TypeShape.DynamicObject then CodecStoredKind.Reserved,"",CodecArguments.None
            elif converted.IsSome then CodecStoredKind.Numeric,"",CodecArguments.Target
            elif TypeSupport.RawAdaptation definition (shape = TypeShape.Enum) <> 0 then CodecStoredKind.Optional,"",CodecArguments.Target
            elif TypeSupport.RawPrimitive definition <> "" || definition = "SoloDatabase.JsonSerializator.JsonValue" then
                CodecStoredKind.Scalar,"RegisterStoredScalar",CodecArguments.Target
            else
                match shape with
                | TypeShape.DocumentReference -> CodecStoredKind.Reference,"RegisterStoredDocument",CodecArguments.Generic
                | TypeShape.TypedDocumentReference -> CodecStoredKind.Reference,"RegisterStoredTypedDocument",CodecArguments.Generic
                | TypeShape.FileReference -> CodecStoredKind.Reference,"RegisterStoredFileReference",CodecArguments.None
                | TypeShape.DirectoryReference -> CodecStoredKind.Reference,"RegisterStoredDirectoryReference",CodecArguments.None
                | _ when (TypeSupport.ReferenceSequence shape).IsSome -> CodecStoredKind.ReferenceSequence,"",CodecArguments.None
                | TypeShape.Grouping -> CodecStoredKind.Grouping,"",CodecArguments.None
                | TypeShape.GroupingImplementation -> CodecStoredKind.GroupingImplementation,"",CodecArguments.TargetAndContract
                | _ when decision.RawKind = RawCodecKind.Sequence && shape <> TypeShape.Array -> CodecStoredKind.Sequence,"",CodecArguments.Generic
                | _ -> CodecStoredKind.Body,"",CodecArguments.None
        { Kind = kind; Registration = registration; Arguments = arguments
          ReadersOnly = TypeSupport.RawPolymorphicFactory(shape,abstractType); ArrayBodies = shape = TypeShape.Array
          ReuseWriter = numeric.IsNone
          ReuseReader = numeric.IsNone && converted.IsNone && shape <> TypeShape.Enum
          ElementArgument = decision.ElementArgument }

    let reader(definition, decision: TypeSupportDecision, stored) =
        if stored && (TypeSupport.StoredNumeric definition).IsSome then RawRootOperation.StoredNumeric
        elif stored && (TypeSupport.StoredNumericReader definition).IsSome then RawRootOperation.ConvertedNumeric
        elif (TypeSupport.RawNumericCore definition).IsSome || TypeSupport.RawPrimitive definition <> "" then RawRootOperation.Scalar
        elif definition = "SoloDatabase.JsonSerializator.JsonValue" then RawRootOperation.Tree
        elif decision.Shape = TypeShape.DynamicObject then RawRootOperation.Dynamic
        elif decision.Shape = TypeShape.Tuple then RawRootOperation.Tuple
        elif decision.Shape = TypeShape.BsonDocument then RawRootOperation.Bson
        elif decision.Shape = TypeShape.Array then RawRootOperation.Array
        elif TypeSupport.DictionaryShape decision.Shape then RawRootOperation.Dictionary
        elif decision.RawKind = RawCodecKind.Sequence || decision.Shape = TypeShape.CustomSequence then RawRootOperation.Sequence
        elif decision.RawKind = RawCodecKind.Intrinsic then RawRootOperation.Intrinsic
        else RawRootOperation.Model

    let writer(definition, decision: TypeSupportDecision, stored, abstractType, byteSequence, nonGenericEnumerable) =
        if stored && (TypeSupport.StoredNumeric definition).IsSome then RawRootOperation.StoredNumeric
        elif (TypeSupport.NumericWriterCore definition).IsSome || byteSequence ||
             (match TypeSupport.RawPrimitive definition with "Boolean" | "String" | "Bytes" | "Guid" -> true | _ -> false) then RawRootOperation.Scalar
        elif definition = "SoloDatabase.JsonSerializator.JsonValue" then RawRootOperation.Tree
        elif abstractType && decision.Shape <> TypeShape.DynamicObject then RawRootOperation.Abstract
        elif decision.Shape = TypeShape.DynamicObject then RawRootOperation.Dynamic
        elif decision.Shape = TypeShape.Tuple then RawRootOperation.Tuple
        elif decision.Shape = TypeShape.BsonDocument then RawRootOperation.Bson
        elif decision.Shape = TypeShape.Array then RawRootOperation.Array
        elif TypeSupport.DictionaryShape decision.Shape then RawRootOperation.Dictionary
        elif decision.RawKind = RawCodecKind.Sequence || decision.Shape = TypeShape.CustomSequence || nonGenericEnumerable then RawRootOperation.Sequence
        elif decision.RawKind = RawCodecKind.Intrinsic then RawRootOperation.Intrinsic
        else RawRootOperation.Model
