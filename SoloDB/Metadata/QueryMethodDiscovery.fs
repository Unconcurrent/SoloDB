namespace SoloDatabase

open System
open System.Reflection
open System.Reflection.Emit
open SoloDatabase.Metadata.CodecGeneration

/// Runtime facts feed the same instruction policy as compiler-established operations.
module internal QueryMethodDiscovery =
    let enumValue (owner: Type) underlying =
        let method = DynamicMethod("QueryEnumValue",typeof<obj>,[|typeof<obj>|],owner.Module,true)
        QueryOperationEmitter.enumValue (RuntimeCodecIL(method.GetILGenerator())) owner underlying
        method.CreateDelegate(typeof<Func<obj,obj>>) :?> Func<obj,obj>
    let referenceMethod (value: Type) =
        if value.IsByRef then Some(typeof<QueryReference>.GetMethod("Address",BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance).MakeGenericMethod(value.GetElementType()) :> MethodBase)
        else None
    let create (methodInfo: MethodInfo) =
        let parameters = methodInfo.GetParameters() |> Array.map _.ParameterType
        let owner = methodInfo.DeclaringType
        if methodInfo.ContainsGenericParameters || methodInfo.ReturnType.IsByRef || parameters |> Array.exists(fun parameter -> parameter.IsPointer) then
            raise (NotSupportedException($"Query method '{methodInfo.Name}' requires a generated direct operation."))
        let method = DynamicMethod("QueryCall",typeof<obj>,[|typeof<obj>;typeof<obj[]>|],methodInfo.Module,true)
        QueryOperationEmitter.call (RuntimeCodecIL(method.GetILGenerator())) typeof<QueryReference> referenceMethod (if not(isNull owner) && owner.IsValueType then Some(typeof<QueryReference>.GetMethod("Address",BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance).MakeGenericMethod(owner) :> MethodBase) else None) owner (not(isNull owner) && owner.IsValueType)
            (not(isNull owner) && not(isNull(TypeMetadata.nullableUnderlying owner))) methodInfo.IsStatic (methodInfo :> MethodBase) parameters
            methodInfo.ReturnType methodInfo.ReturnType.IsValueType (typeof<Void>.Equals(methodInfo.ReturnType)) true
        method.CreateDelegate(typeof<Func<obj,obj[],obj>>) :?> Func<obj,obj[],obj>

    let private memberFacts (memberInfo: MemberInfo) write =
        match memberInfo with
        | :? FieldInfo as field ->
            if write && (field.IsInitOnly || field.IsLiteral) then
                raise(ArgumentException("Expression must be writeable", "left"))
            { Name = field.Name; Type = field.FieldType; Field = Some field; Getter = None; Setter = None
              Static = field.IsStatic; Readable = true; Writable = not field.IsInitOnly; Ignored = false; Many = false }
        | :? PropertyInfo as property when property.GetIndexParameters().Length = 0 ->
            let method = if write then property.GetSetMethod(true) else property.GetGetMethod(true)
            if isNull method then
                if write then raise(ArgumentException("Expression must be writeable", "left"))
                else invalidArg "memberInfo" "A query property requires a getter."
            { Name = property.Name; Type = property.PropertyType; Field = None
              Getter = if write then None else Some(method :> MethodBase)
              Setter = if write then Some(method :> MethodBase) else None
              Static = method.IsStatic; Readable = not write; Writable = write; Ignored = false; Many = false }
        | _ -> invalidArg "memberInfo" "A query member requires a field or non-indexed property."

    let private readMethod (memberInfo: MemberInfo) receiverType resultType boxedReceiver boxedResult =
        let owner = memberInfo.DeclaringType
        let memberValue = memberFacts memberInfo false
        let method = DynamicMethod("QueryMember",resultType,[|receiverType|],memberInfo.Module,true)
        QueryOperationEmitter.memberRead (RuntimeCodecIL(method.GetILGenerator())) owner (not(isNull owner) && owner.IsValueType)
            (not(isNull(TypeMetadata.nullableUnderlying owner))) memberValue memberValue.Type.IsValueType boxedReceiver boxedResult
        method

    let reader (memberInfo: MemberInfo) =
        match memberInfo with
        | :? FieldInfo as field when field.IsLiteral ->
            let value = field.GetRawConstantValue()
            Func<obj,obj>(fun _ -> value)
        | _ -> (readMethod memberInfo typeof<obj> typeof<obj> true true).CreateDelegate(typeof<Func<obj,obj>>) :?> Func<obj,obj>

    let readerResult<'Value> (memberInfo: MemberInfo) =
        (readMethod memberInfo typeof<obj> typeof<'Value> true false).CreateDelegate(typeof<Func<obj,'Value>>) :?> Func<obj,'Value>

    let typedReader<'Owner,'Value> (memberInfo: MemberInfo) =
        (readMethod memberInfo typeof<'Owner> typeof<'Value> false false).CreateDelegate(typeof<Func<'Owner,'Value>>) :?> Func<'Owner,'Value>

    let private writeMethod (memberInfo: MemberInfo) receiverType valueType boxedReceiver byrefReceiver boxedValue =
        let owner = memberInfo.DeclaringType
        let memberValue = memberFacts memberInfo true
        let method = DynamicMethod("WriteQueryMember",typeof<Void>,[|receiverType;valueType|],memberInfo.Module,true)
        QueryOperationEmitter.memberWrite (RuntimeCodecIL(method.GetILGenerator())) owner owner.IsValueType memberValue boxedReceiver byrefReceiver boxedValue
        method

    let setter (memberInfo: MemberInfo) =
        (writeMethod memberInfo typeof<obj> typeof<obj> true false true).CreateDelegate(typeof<Action<obj,obj>>) :?> Action<obj,obj>

    let setterValue<'Value> (memberInfo: MemberInfo) =
        (writeMethod memberInfo typeof<obj> typeof<'Value> true false false).CreateDelegate(typeof<Action<obj,'Value>>) :?> Action<obj,'Value>

    let constructor (constructor: ConstructorInfo) =
        let owner = constructor.DeclaringType
        let parameters = constructor.GetParameters() |> Array.map _.ParameterType
        if parameters |> Array.exists(fun parameter -> parameter.IsPointer) then
            raise(NotSupportedException($"Query constructor '{owner.FullName}' requires a generated direct operation."))
        let method = DynamicMethod("QueryConstruction",typeof<obj>,[|typeof<obj[]>|],constructor.Module,true)
        QueryOperationEmitter.construction (RuntimeCodecIL(method.GetILGenerator())) typeof<QueryReference> referenceMethod owner owner.IsValueType (constructor :> MethodBase) parameters true
        method.CreateDelegate(typeof<Func<obj[],obj>>) :?> Func<obj[],obj>

    let defaultConstruction (owner: Type) (constructor: ConstructorInfo option) =
        let method = DynamicMethod("DefaultConstruction",typeof<obj>,Type.EmptyTypes,owner.Module,true)
        CodecEmitter.boxedDefaultConstruction (RuntimeCodecIL(method.GetILGenerator())) owner (not(isNull owner) && owner.IsValueType)
            (constructor |> Option.map(fun value -> value :> MethodBase))
        method.CreateDelegate(typeof<Func<obj>>) :?> Func<obj>

    let fieldAddress (field: FieldInfo) =
        let owner = field.DeclaringType
        let method = DynamicMethod("QueryFieldAddress",field.FieldType.MakeByRefType(),[|typeof<obj>;typeof<int[]>|],field.Module,true)
        let ownerAddress = if owner.IsValueType then typeof<QueryReference>.GetMethod("Address",BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance).MakeGenericMethod(owner) :> MethodBase else null
        QueryOperationEmitter.fieldAddress (RuntimeCodecIL(method.GetILGenerator())) typeof<QueryReference> owner owner.IsValueType ownerAddress field field.IsStatic
        method.CreateDelegate(typedefof<QueryAddress<_>>.MakeGenericType(field.FieldType))

    let boxAddress (owner: Type) =
        let method = DynamicMethod("QueryBoxAddress",owner.MakeByRefType(),[|typeof<obj>;typeof<int[]>|],owner.Module,true)
        QueryOperationEmitter.boxAddress (RuntimeCodecIL(method.GetILGenerator())) owner
        method.CreateDelegate(typedefof<QueryAddress<_>>.MakeGenericType(owner))

    let arrayAddress (owner: Type) =
        let element = owner.GetElementType()
        let rank = owner.GetArrayRank()
        let method = DynamicMethod("QueryArrayAddress",element.MakeByRefType(),[|typeof<obj>;typeof<int[]>|],owner.Module,true)
        let address = if rank > 1 then owner.GetMethod("Address",Array.create rank typeof<int>) :> MethodBase else null
        QueryOperationEmitter.arrayAddress (RuntimeCodecIL(method.GetILGenerator())) owner element rank address
        method.CreateDelegate(typedefof<QueryAddress<_>>.MakeGenericType(element))
