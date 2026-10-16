namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

/// Encoded metadata constants carry their actual CLR stack representation.
type internal AttributeConstant<'T> =
    | NullConstant
    | IntegerConstant of int
    | LongConstant of int64
    | SingleConstant of float32
    | DoubleConstant of float
    | StringConstant of string
    | TypeConstant of 'T
    | ArrayConstant of 'T * AttributeConstant<'T>[]
    | BoxedConstant of 'T * AttributeConstant<'T>

type internal AttributeAssignment<'T,'M,'F> = {
    Member: CodecMember<'T,'M,'F>
    Value: AttributeConstant<'T>
    Property: bool
}

type internal AttributeConstruction<'T,'M,'F> = {
    Type: 'T
    Constructor: 'M
    Arguments: AttributeConstant<'T>[]
    Assignments: AttributeAssignment<'T,'M,'F>[]
}

type internal AttributeEmitterApi<'T,'M> = {
    Object: 'T
    ObjectArray: 'T
    Exception: 'T
    GetType: 'M
    NamedError: 'M
}

/// Calls original attribute constructors and preserves encoded assignment order.
module internal AttributeConstructionEmitter =
    let rec private constant (il: CodecIL<'T,'M,'F>) getType value =
        match value with
        | NullConstant -> il.Emit OpCodes.Ldnull
        | IntegerConstant value -> il.EmitInt(OpCodes.Ldc_I4,value)
        | LongConstant value -> il.EmitInt64 value
        | SingleConstant value -> il.EmitFloat32 value
        | DoubleConstant value -> il.EmitFloat64 value
        | StringConstant value -> il.EmitString value
        | TypeConstant typ ->
            il.EmitType(OpCodes.Ldtoken,typ)
            il.EmitMethod(OpCodes.Call,getType)
        | BoxedConstant(typ,value) ->
            constant il getType value
            il.EmitType(OpCodes.Box,typ)
        | ArrayConstant(element,values) ->
            il.EmitInt(OpCodes.Ldc_I4,values.Length)
            il.EmitType(OpCodes.Newarr,element)
            for index = 0 to values.Length - 1 do
                il.Emit OpCodes.Dup
                il.EmitInt(OpCodes.Ldc_I4,index)
                constant il getType values[index]
                il.EmitType(OpCodes.Stelem,element)

    let factory (il: CodecIL<'T,'M,'F>) (api: AttributeEmitterApi<'T,'M>) attributes =
        let output = il.Local api.ObjectArray
        il.EmitInt(OpCodes.Ldc_I4,Array.length attributes)
        il.EmitType(OpCodes.Newarr,api.Object)
        il.EmitLocal(OpCodes.Stloc,output)
        for index = 0 to attributes.Length - 1 do
            let attribute = attributes[index]
            for argument in attribute.Arguments do constant il api.GetType argument
            il.EmitMethod(OpCodes.Newobj,attribute.Constructor)
            let instance = il.Local attribute.Type
            il.EmitLocal(OpCodes.Stloc,instance)
            for assignment in attribute.Assignments do
                let memberValue = assignment.Member
                il.BeginException()
                if not memberValue.Static then il.EmitLocal(OpCodes.Ldloc,instance)
                constant il api.GetType assignment.Value
                match memberValue.Field,memberValue.Setter with
                | Some field,_ -> il.EmitField((if memberValue.Static then OpCodes.Stsfld else OpCodes.Stfld),field)
                | _,Some setter -> il.EmitMethod((if memberValue.Static then OpCodes.Call else OpCodes.Callvirt),setter)
                | _ -> invalidOp "An attribute named argument requires its original field or setter."
                il.Catch api.Exception
                let error = il.Local api.Exception
                il.EmitLocal(OpCodes.Stloc,error)
                il.EmitString memberValue.Name
                il.EmitInt(OpCodes.Ldc_I4,if assignment.Property then 1 else 0)
                il.EmitLocal(OpCodes.Ldloc,error)
                il.EmitMethod(OpCodes.Call,api.NamedError)
                il.EndException()
            il.EmitLocal(OpCodes.Ldloc,output)
            il.EmitInt(OpCodes.Ldc_I4,index)
            il.EmitLocal(OpCodes.Ldloc,instance)
            il.Emit OpCodes.Stelem_Ref
        il.EmitLocal(OpCodes.Ldloc,output)
        il.Emit OpCodes.Ret
