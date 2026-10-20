namespace SoloDatabase

open System
open System.Collections.Generic
open SoloDatabase.JsonSerializator

/// One closed graph supplies original member readers and SQL component order.
module internal QueryStructuralShape =
    type Node = { Index: int; Target: Type; mutable Kind: Kind }
    and Kind =
        | ScalarNode
        | OptionalNode of Node * Lazy<Func<obj,obj>>
        | ComponentNode of Field array
    and Field = { Name: string; Node: Node; Reader: Lazy<Func<obj,obj>> }
    type Graph = { Root: Node; Nodes: Node array; Recursive: bool; Types: Dictionary<Type,Node> }

    let (|Leaf|Optional|Components|) node =
        match node.Kind with
        | ScalarNode -> Leaf node.Target
        | OptionalNode(child,_) -> Optional(node.Target,child)
        | ComponentNode fields -> Components(node.Target,fields |> Array.map(fun field -> field.Name,field.Node))

    let private reader owner name target = lazy (
        match ModelMetadata.NamedMember(owner,name) with
        | Some struct(actual,read) when actual.Equals target -> read
        | _ -> invalidOp "A structural component requires its original bound reader.")

    let memberReader graph owner name target =
        match graph.Types.TryGetValue owner with
        | true,node ->
            match node.Kind with
            | OptionalNode(child,read) when name = "Value" && child.Target.Equals target -> read.Value
            | ComponentNode fields ->
                match fields |> Array.tryFind(fun field -> field.Name = name && field.Node.Target.Equals target) with
                | Some field -> field.Reader.Value
                | _ -> invalidOp "A structural component requires its original member identity."
            | _ -> invalidOp "A structural component requires its original member identity."
        | _ -> invalidOp "A structural component requires its closed owner."

    let private tuple (t: Type) =
        if not t.IsGenericType then false else
        let definition = t.GetGenericTypeDefinition()
        definition.Assembly = typeof<Tuple<int>>.Assembly &&
        (definition.FullName.StartsWith("System.Tuple`",StringComparison.Ordinal) ||
         definition.FullName.StartsWith("System.ValueTuple`",StringComparison.Ordinal))

    let private properSubterm (child: Type) (parent: Type) =
        let pending = Stack<Type>(parent.GetGenericArguments())
        let mutable found = false
        while not found && pending.Count > 0 do
            let current = pending.Pop()
            if current.Equals child then found <- true
            else
                if current.HasElementType then pending.Push(current.GetElementType())
                for argument in current.GetGenericArguments() do pending.Push argument
        found

    type private Description = Scalar | Payload of Type | Members of (string * Type)[]

    let private intrinsic (t: Type) =
        let payload = Utils.unwrapNullableType t
        if not(payload.Equals t) then ValueSome(Payload payload)
        elif typeof<string>.Equals t || typeof<char>.Equals t || typeof<bool>.Equals t ||
             t.IsEnum || (QueryTranslatorNumeric.nativeCastType t).IsSome ||
             (QueryTranslatorNumeric.domain t).IsSome || DateTimeFunctions.isDateTimeLikeType t then ValueSome Scalar
        elif tuple t then
            let arguments = t.GetGenericArguments()
            ValueSome(Members(Array.init arguments.Length (fun index ->
                (if index = 7 then "Rest" else "Item" + string(index+1)),arguments[index])))
        else ValueNone

    let private describe t =
        match intrinsic t with
        | ValueSome description -> ValueSome description
        | ValueNone when TypeMetadata.standardComparison t ->
            TypeMetadata.recordFields t |> ValueOption.map(fun names -> Members(names |> Array.map(fun name ->
                let target = ModelMetadata.MemberType(t,name,false)
                             |> Option.orElseWith(fun () -> ModelMetadata.MemberType(t,name,true))
                             |> Option.defaultWith(fun () -> invalidOp "A structural component requires its original member type.")
                name,target)))
        | _ -> ValueNone

    let private originalMembers definition =
        match intrinsic definition with
        | ValueSome Scalar -> ValueSome [||]
        | ValueSome(Payload payload) -> ValueSome [|ModelMetadata.TypeTemplate payload|]
        | ValueSome(Members members) -> ValueSome(members |> Array.map(fun (_,target) -> ModelMetadata.TypeTemplate target))
        | ValueNone ->
            match ModelMetadata.DefinitionOwner definition with
            | ValueNone -> ValueNone
            | ValueSome owner ->
                match describe owner with
                | ValueSome(Members members) ->
                    let templates = ResizeArray<ModelTypeTemplate>(members.Length)
                    let mutable known = true
                    for name,_ in members do
                        let field = (ModelMetadata.MemberType(owner,name,false)).IsNone
                        match ModelMetadata.MemberTemplate(owner,name,field) with
                        | ValueSome template -> templates.Add template
                        | ValueNone -> known <- false
                    if known then ValueSome(templates.ToArray()) else ValueNone
                | _ -> ValueNone

    let finiteClosure root = QueryStructuralClosure.finite originalMembers (ModelMetadata.TypeTemplate root)

    let tryCreate root =
        let types = Dictionary<Type,Node>()
        let nodes = ResizeArray<Node>()
        let active = HashSet<Type>()
        let mutable recursive = false
        let finite = lazy(finiteClosure root)
        let rec build (t: Type) =
            match types.TryGetValue t with
            | true,node ->
                if active.Contains t then recursive <- true
                ValueSome node
            | _ ->
                let changing = t.IsGenericType && (active |> Seq.exists(fun owner ->
                    owner.IsGenericType && owner.GetGenericTypeDefinition().Equals(t.GetGenericTypeDefinition()) &&
                    not(properSubterm t owner)))
                if changing && not finite.Value then ValueNone else
                let node = { Index = nodes.Count; Target = t; Kind = ScalarNode }
                nodes.Add node
                types.Add(t,node)
                active.Add t |> ignore
                let result =
                    match describe t with
                    | ValueSome(Payload payload) ->
                        build payload |> ValueOption.map(fun child ->
                            node.Kind <- OptionalNode(child,reader t "Value" payload)
                            node)
                    | ValueSome Scalar -> ValueSome node
                    | ValueNone -> ValueNone
                    | ValueSome(Members members) ->
                        let fields = ResizeArray<Field>(members.Length)
                        let mutable admitted = true
                        let mutable index = 0
                        while admitted && index < members.Length do
                            let name,target = members[index]
                            match build target with
                            | ValueSome child -> fields.Add { Name = name; Node = child; Reader = reader t name target }
                            | ValueNone -> admitted <- false
                            index <- index + 1
                        if not admitted then ValueNone else
                        node.Kind <- ComponentNode(fields.ToArray())
                        ValueSome node
                active.Remove t |> ignore
                result
        match build root with
        | ValueSome node ->
            match node.Kind with
            | ComponentNode _ | OptionalNode({ Kind = ComponentNode _ },_) ->
                ValueSome { Root = node; Nodes = nodes.ToArray(); Recursive = recursive; Types = types }
            | _ -> ValueNone
        | _ -> ValueNone

    let enumValue target value =
        let underlying = TypeMetadata.enumUnderlying target
        underlying,Convert.ChangeType(value,underlying,Globalization.CultureInfo.InvariantCulture)

    [<Struct>]
    type private Preparation =
        { Node: Node; Reader: Lazy<Func<obj,obj>> voption; Value: obj; Parent: int; Ordinal: int }

    /// Invocation data is flat; only original component readers traverse the value.
    let prepare graph value =
        let pending = Stack<Preparation>()
        pending.Push { Node = graph.Root; Reader = ValueNone; Value = value; Parent = -1; Ordinal = 0 }
        let mutable destination = Span<char>.Empty
        let mutable state = JsonWriteState<char>(null,null,null)
        let mutable comma = false
        let mutable index = 0
        JsonWriteBuffer.append &destination &state '['
        while pending.Count > 0 do
            let item = pending.Pop()
            let node = item.Node
            let value = match item.Reader with ValueSome read -> read.Value.Invoke item.Value | ValueNone -> item.Value
            let current = index
            index <- index + 1
            if comma then JsonWriteBuffer.append &destination &state ','
            comma <- true
            JsonWriteBuffer.append &destination &state '['
            JsonRawNumericWriter.int64Utf16 (int64 item.Parent) &destination &state
            JsonWriteBuffer.append &destination &state ','
            JsonRawNumericWriter.int64Utf16 (int64 item.Ordinal) &destination &state
            JsonWriteBuffer.append &destination &state ','
            JsonOutputOperations.literal (Unchecked.defaultof<JsonUtf16Writer>) (if isNull value then "true" else "false") &destination &state
            match node.Kind with
            | ScalarNode ->
                JsonWriteBuffer.append &destination &state ','
                if typeof<char>.Equals node.Target then
                    JsonRawNumericWriter.int64Utf16 (int64(unbox<char> value)) &destination &state
                elif typeof<string>.Equals node.Target then
                    let encoded = if isNull value then null else Convert.ToBase64String(QueryTranslatorCharacters.ordinalBytes(unbox<string> value))
                    JsonRawPrimitive.WriteString<char,JsonUtf16Writer>(encoded,&destination,&state)
                else JsonValue.WriteQueryOperandLeaf(node.Target,value,&destination,&state)
            | OptionalNode(child,read) when not(isNull value) ->
                pending.Push { Node = child; Reader = ValueSome read; Value = value; Parent = current; Ordinal = 0 }
            | ComponentNode fields when not(isNull value) ->
                for index = fields.Length - 1 downto 0 do
                    let field = fields[index]
                    pending.Push { Node = field.Node; Reader = ValueSome field.Reader; Value = value; Parent = current; Ordinal = index }
            | _ -> ()
            JsonWriteBuffer.append &destination &state ']'
        JsonWriteBuffer.append &destination &state ']'
        System.String(state.Storage,0,state.Written)
