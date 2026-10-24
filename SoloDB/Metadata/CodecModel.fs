namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

/// Instruction destinations translate operands; codec policy belongs to CodecEmitter.
[<AbstractClass>]
type internal CodecIL<'Type, 'Method, 'Field>() =
    abstract InputValue: unit -> unit
    abstract InputAddress: unit -> unit
    abstract ByRefParameter: 'Method * int -> bool
    abstract ResultParameter: 'Method -> bool
    abstract PropertyStorage: 'Method -> 'Field option
    abstract Emit: OpCode -> unit
    abstract EmitInt: OpCode * int -> unit
    abstract EmitInt64: int64 -> unit
    abstract EmitFloat32: float32 -> unit
    abstract EmitFloat64: float -> unit
    abstract EmitString: string -> unit
    abstract EmitType: OpCode * 'Type -> unit
    abstract EmitMethod: OpCode * 'Method -> unit
    abstract EmitField: OpCode * 'Field -> unit
    abstract EmitLocal: OpCode * int -> unit
    abstract EmitBranch: OpCode * int -> unit
    abstract EmitSwitch: int array -> unit
    abstract Local: 'Type -> int
    abstract Label: unit -> int
    abstract Mark: int -> unit
    abstract BeginException: unit -> unit
    abstract Finally: unit -> unit
    abstract Catch: 'Type -> unit
    abstract EndException: unit -> unit

/// Original direct getters can expose writable storage without changing property identity.
module internal CodecMemberStorage =
    let allowed (directRead, isVirtual, isFinal, sealedOwner, fieldStatic, fieldReadonly, sameOwner, sameType, synchronizedGetter) =
        directRead && (not isVirtual || isFinal || sealedOwner) &&
        not fieldStatic && not fieldReadonly && sameOwner && sameType && not synchronizedGetter

/// Expands byte addressing from admitted element offsets at the target native width.
module internal CodecAddressEmitter =
    let offset (il: CodecIL<'T,'M,'F>) method elementSize byteTail =
        il.Emit OpCodes.Conv_U
        if elementSize <> 1 then
            il.EmitInt(OpCodes.Ldc_I4,elementSize); il.Emit OpCodes.Conv_U; il.Emit OpCodes.Mul
        if byteTail <> 0 then
            il.EmitInt(OpCodes.Ldc_I4,byteTail); il.Emit OpCodes.Conv_U; il.Emit OpCodes.Add
        il.EmitMethod(OpCodes.Call,method)

type internal CodecMember<'Type, 'Method, 'Field> = {
    Name: string
    Type: 'Type
    Field: 'Field option
    Getter: 'Method option
    Setter: 'Method option
    Static: bool
    Readable: bool
    Writable: bool
    Ignored: bool
    Many: bool
}

type internal CodecUnionCase<'Type, 'Method> = {
    Name: string
    Tag: int
    Factory: 'Method option
    Fields: (string * 'Type)[]
}

type internal CodecModel<'Type, 'Method, 'Field> = {
    Type: 'Type
    Name: string
    RuntimeName: string
    ValueType: bool
    Sealed: bool
    Abstract: bool
    Constructor: 'Method option
    ConstructFromMembers: bool
    ConstructorMembers: int[]
    Members: CodecMember<'Type, 'Method, 'Field>[]
    Union: bool
    Cases: CodecUnionCase<'Type, 'Method>[]
    CastUnionResult: bool
    IndexedRead: string option
    IndexedWrite: string option
}

type internal CodecApi<'Type, 'Method> = {
    Json: 'Type
    Int32: 'Type
    Dictionary: 'Type
    ObjectDictionary: 'Type
    Null: 'Method
    IsNull: 'Method
    GetType: 'Method
    TypeFromHandle: 'Method
    SerializeActual: 'Method
    ActualType: 'Type -> 'Method
    ToRuntimeObject: 'Method
    DictionaryConstructor: 'Method
    DictionaryAdd: 'Method
    NewObject: 'Method
    ObjectProperties: 'Method
    TryGetValue: 'Method
    Uninitialized: 'Type -> 'Method
    Serialize: 'Type -> 'Method
    Deserialize: 'Type -> 'Method
    ExceptionConstructor: 'Method
    ArgumentExceptionConstructor: 'Method
    InitializationExceptionConstructor: 'Method
    MissingFactoryConstructor: 'Method
    UnionCaseSelected: 'Method
    Contains: 'Method
    GetProperty: 'Method
    EnsureStack: 'Method
    RawReader: bool
}

type internal CodecTuple<'Type, 'Method, 'Field> = {
    Type: 'Type
    ValueType: bool
    Constructor: 'Method
    Members: CodecTupleMember<'Type, 'Method, 'Field>[]
}
and internal CodecTupleMember<'Type, 'Method, 'Field> = {
    Member: CodecMember<'Type, 'Method, 'Field>
    Rest: CodecTuple<'Type, 'Method, 'Field> option
}

type internal CodecReaderCall<'Type, 'Method> = {
    Result: 'Type
    Before: 'Method option
    Call: 'Method option
    After: 'Method option
    Cast: bool
    NullDefault: bool
}

type internal CodecWriterCall<'Type, 'Method> = {
    Input: 'Type
    ValueType: bool
    Box: bool
    Call: 'Method
    Virtual: bool
}


/// Cold compiler facts for original closed type assignment.
#if METADATA_TOOLING
type CodecTypeAssignment<'T> = {
#else
type internal CodecTypeAssignment<'T> = {
#endif
    Equal: 'T -> 'T -> bool
    Parents: 'T -> 'T[]
    Arguments: 'T -> 'T[]
    Definition: 'T -> 'T
    Variance: 'T -> int[]
    ValueType: 'T -> bool
    ArrayRank: 'T -> int
    Object: 'T -> bool
}

#if METADATA_TOOLING
module CodecTypeAssignment =
#else
module internal CodecTypeAssignment =
#endif
    let internal evaluate accepts facts source target =
        if facts.Equal source target || facts.Object target then true
        else
            let sourceArgs, targetArgs = facts.Arguments source, facts.Arguments target
            let rank = facts.ArrayRank source
            let same = facts.Equal (facts.Definition source) (facts.Definition target)
            let compatible =
                if rank > 0 then
                    rank = facts.ArrayRank target && sourceArgs.Length = 1 && targetArgs.Length = 1 &&
                    not(facts.ValueType sourceArgs[0] || facts.ValueType targetArgs[0]) && accepts sourceArgs[0] targetArgs[0]
                elif same && sourceArgs.Length > 0 && sourceArgs.Length = targetArgs.Length then
                    let variance = facts.Variance source
                    sourceArgs |> Array.mapi(fun index value ->
                        facts.Equal value targetArgs[index] ||
                        (index < variance.Length && not(facts.ValueType value || facts.ValueType targetArgs[index]) &&
                         (if variance[index] > 0 then accepts value targetArgs[index]
                          elif variance[index] < 0 then accepts targetArgs[index] value
                          else false))) |> Array.forall id
                else false
            compatible || (facts.Parents source |> Array.exists(fun parent -> not(facts.Equal parent source) && accepts parent target))

    let accepts facts source target =
        let rec visit source target = evaluate visit facts source target
        visit source target

type internal CodecPolymorphicApi<'T,'M> = {
    RuntimeType: 'T
    SameType: 'M
    IncludeType: 'M
    SetIncludeType: 'M
    Actual: 'M
}

/// Array emission needs only its selected child and delimiter operations.
type internal CodecArrayWriterApi<'Type,'Method> = {
    Operation: 'Type -> 'Method option
    Serialize: 'Type -> 'Method
    Literal: string -> unit
}

/// Direct text destinations retain the same member admission and typed codec calls.
type internal CodecRawWriterApi<'Type,'Method> = {
    Boolean: 'Type
    Operation: 'Type -> 'Method option
    Polymorphic: CodecPolymorphicApi<'Type,'Method>
    Literal: string -> unit
    SerializeActual: 'Method
    Property: string * int -> unit
    TypeLiteral: 'Type -> 'Method
    OmitId: 'Method
    Discard: 'Type -> 'Method
    Serialize: 'Type -> 'Method
}

/// Mechanical lexical operands for inline finite-name recognition.
type internal CodecNameApi<'Type,'Method> = {
    Int32: 'Type
    Boolean: 'Type
    SourceReference: 'Type
    PositionReference: 'Type
    Begin: 'Method
    Length: 'Method
    Position: 'Method
    Reference: 'Method
    OffsetByte: 'Method
    Word: 'Method
    Pair: 'Method
    Quoted: 'Method
    Identifier: 'Method
    Invalid: 'Method
    Unterminated: 'Method
    Unsupported: 'Method
    Colon: 'Method
    Value: 'Method
    Utf16: bool
}

/// Forward raw consumption uses the same model facts as tree conversion.
type internal CodecRawReaderApi<'Type,'Method> = {
    Boolean: 'Type
    Operation: 'Type -> 'Method option
    BeginArray: 'Method
    NextElement: 'Method
    GrowArray: 'Type -> 'Method
    TrimArray: 'Type -> 'Method
    ArrayWrapper: 'Type -> 'Method
    BeginObject: 'Method
    Start: 'Method
    Properties: CodecNameApi<'Type,'Method> option
    Skip: 'Method
    Complete: 'Method
    CheckType: 'Type -> 'Method
    Read: 'Type -> 'Method
}
