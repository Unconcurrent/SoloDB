namespace SoloDatabase

open System

/// A managed address remains attached to its original owner, never to a copied value.
type internal QueryAddress<'Value> = delegate of obj * int[] -> byref<'Value>

[<AbstractClass; AllowNullLiteral>]
type internal QueryVariable() =
    abstract Value: obj with get,set

type internal QueryVariable<'Value>(initial: 'Value) =
    inherit QueryVariable()
    let mutable value = initial
    override _.Value with get() = box value and set next = value <- unbox next
    member _.Address: byref<'Value> = &value

[<Sealed>]
type internal QueryReference(address: Delegate, owner: obj, indexes: int[]) =
    member _.Address<'Value>(): byref<'Value> =
        &(address :?> QueryAddress<'Value>).Invoke(owner,indexes)

type internal QueryStorageOperations = {
    Variable: Func<obj,QueryVariable>
    VariableAddress: Func<obj,int[],QueryReference>
    ArrayAddress: Func<obj,int[],QueryReference>
    BindAddress: Func<Delegate,Func<obj,int[],QueryReference>>
}

module internal QueryStorage =
    let value (value: obj) =
        match value with
        | :? QueryVariable as variable -> variable.Value
        | _ -> value

    let private bind<'Value> (address: Delegate) =
        let typed = address :?> QueryAddress<'Value>
        Func<obj,int[],QueryReference>(fun owner indexes ->
            // Resolve now, so null and bounds errors precede the next argument.
            let location = &typed.Invoke(owner,indexes)
            QueryReference(typed,owner,indexes))

    let create<'Value>() =
        { Variable = Func<obj,QueryVariable>(fun value -> QueryVariable<'Value>(unbox value))
          VariableAddress = bind<'Value>(QueryAddress<'Value>(fun owner _ -> &(owner :?> QueryVariable<'Value>).Address))
          ArrayAddress = bind<'Value>(QueryAddress<'Value>(fun owner indexes -> &(owner :?> 'Value[]).[indexes.[0]]))
          BindAddress = Func<Delegate,Func<obj,int[],QueryReference>>(bind<'Value>) }
