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
    type Admission<'T> = Available of 'T | Unavailable | Unsupported of Type

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

    type private Description = Scalar | Payload of Type | Members of (string * Type)[] | Custom

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
        | ValueNone when DBRefTypeHelpers.isSingleReferenceType t -> ValueNone
        | ValueNone when TypeMetadata.customComparison t = ValueSome true -> ValueSome Custom
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
        | ValueSome Custom -> ValueNone
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

    let private closure root =
        QueryStructuralClosure.analyze (fun definition ->
            if TypeMetadata.customComparison definition = ValueSome true &&
               not(DBRefTypeHelpers.isSingleReferenceType definition) then struct(ValueSome [||],ValueSome definition)
            else struct(originalMembers definition,ValueNone)) (ModelMetadata.TypeTemplate root)

    let finiteClosure root = let struct(bounded,_) = closure root in bounded

    let tryCreate root =
        let types = Dictionary<Type,Node>()
        let failures = Dictionary<Type,Type voption>()
        let nodes = ResizeArray<Node>()
        let active = HashSet<Type>()
        let mutable recursive = false
        let finite = lazy(closure root)
        let rec build (t: Type) =
            match failures.TryGetValue t with
            | true,ValueSome custom -> Unsupported custom
            | true,ValueNone -> Unavailable
            | _ ->
            match types.TryGetValue t with
            | true,node ->
                if active.Contains t then recursive <- true
                Available node
            | _ ->
                let description = describe t
                match description with
                | ValueSome Custom -> failures.Add(t,ValueSome t); Unsupported t
                | _ ->
                let changing = t.IsGenericType && (active |> Seq.exists(fun owner ->
                    owner.IsGenericType && owner.GetGenericTypeDefinition().Equals(t.GetGenericTypeDefinition()) &&
                    not(properSubterm t owner)))
                let struct(bounded,custom) = if changing then finite.Value else struct(true,ValueNone)
                match custom with
                | ValueSome target -> failures.Add(t,ValueSome target); Unsupported target
                | ValueNone when not bounded -> failures.Add(t,ValueNone); Unavailable
                | ValueNone ->
                let node = { Index = nodes.Count; Target = t; Kind = ScalarNode }
                nodes.Add node
                types.Add(t,node)
                active.Add t |> ignore
                let result =
                    match description with
                    | ValueSome(Payload payload) ->
                        match build payload with
                        | Available child ->
                            node.Kind <- OptionalNode(child,reader t "Value" payload)
                            Available node
                        | Unavailable -> Unavailable
                        | Unsupported custom -> Unsupported custom
                    | ValueSome Scalar -> Available node
                    | ValueNone -> Unavailable
                    | ValueSome Custom -> Unsupported t
                    | ValueSome(Members members) ->
                        let fields = ResizeArray<Field>(members.Length)
                        let mutable admitted = true
                        let mutable custom = ValueNone
                        let mutable index = 0
                        while custom.IsNone && index < members.Length do
                            let name,target = members[index]
                            match build target with
                            | Available child -> fields.Add { Name = name; Node = child; Reader = reader t name target }
                            | Unavailable -> admitted <- false
                            | Unsupported target -> custom <- ValueSome target
                            index <- index + 1
                        match custom with
                        | ValueSome target -> Unsupported target
                        | ValueNone when not admitted -> Unavailable
                        | _ ->
                            node.Kind <- ComponentNode(fields.ToArray())
                            Available node
                active.Remove t |> ignore
                match result with
                | Unavailable -> failures.Add(t,ValueNone)
                | Unsupported custom -> failures.Add(t,ValueSome custom)
                | _ -> ()
                result
        match build root with
        | Available node ->
            match node.Kind with
            | ComponentNode _ | OptionalNode({ Kind = ComponentNode _ },_) ->
                Available { Root = node; Nodes = nodes.ToArray(); Recursive = recursive; Types = types }
            | _ -> Unavailable
        | Unavailable -> Unavailable
        | Unsupported custom -> Unsupported custom

    let unsupported (target: Type) =
        raise (NotSupportedException(
            $"Error: Cannot translate custom comparison for '{target}'.\nReason: Its authored comparison has no faithful SQL translation.\nFix: Compare supported scalar keys with the intended order, or call AsEnumerable before the comparison."))

    let requireComparison target =
        match intrinsic target with
        | ValueSome Scalar -> ()
        | _ when DBRefTypeHelpers.isSingleReferenceType target -> ()
        | _ ->
            match tryCreate target with
            | Unsupported custom -> unsupported custom
            | _ -> ()

    let enumValue target value =
        let underlying = TypeMetadata.enumUnderlying target
        underlying,Convert.ChangeType(value,underlying,Globalization.CultureInfo.InvariantCulture)

    type private Selection = { mutable Complete: bool; Children: Dictionary<int,Selection> }

    [<Struct>]
    type private Preparation =
        { Node: Node; Reader: Lazy<Func<obj,obj>> voption; Value: obj; Parent: int; Ordinal: int; Selection: Selection voption }

    let private prepareWithSelection graph selection value =
        let pending = Stack<Preparation>()
        pending.Push { Node = graph.Root; Reader = ValueNone; Value = value; Parent = -1; Ordinal = 0; Selection = selection }
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
            let selected ordinal =
                match item.Selection with
                | ValueNone -> ValueSome ValueNone
                | ValueSome selection when selection.Complete -> ValueSome ValueNone
                | ValueSome selection ->
                    match selection.Children.TryGetValue ordinal with
                    | true, child -> ValueSome(ValueSome child)
                    | _ -> ValueNone
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
                match selected 0 with
                | ValueSome selection -> pending.Push { Node = child; Reader = ValueSome read; Value = value; Parent = current; Ordinal = 0; Selection = selection }
                | ValueNone -> ()
            | ComponentNode fields when not(isNull value) ->
                for index = fields.Length - 1 downto 0 do
                    let field = fields[index]
                    match selected index with
                    | ValueSome selection -> pending.Push { Node = field.Node; Reader = ValueSome field.Reader; Value = value; Parent = current; Ordinal = index; Selection = selection }
                    | ValueNone -> ()
            | _ -> ()
            JsonWriteBuffer.append &destination &state ']'
        JsonWriteBuffer.append &destination &state ']'
        System.String(state.Storage,0,state.Written)

    /// Invocation data is flat; only original component readers traverse the value.
    let prepare graph value = prepareWithSelection graph ValueNone value

    /// Projected demands retain ancestor presence without reading unused siblings.
    let prepareSelected graph (paths: seq<int array>) value =
        let create () = { Complete = false; Children = Dictionary<int,Selection>() }
        let root = create()
        for path in paths do
            let mutable current = root
            for ordinal in path do
                match current.Children.TryGetValue ordinal with
                | true, child -> current <- child
                | _ ->
                    let child = create()
                    current.Children.Add(ordinal,child)
                    current <- child
            current.Complete <- true
        prepareWithSelection graph (ValueSome root) value

    let originalPreparation target =
        let shape =
            if typeof<string>.Equals target then
                let node = { Index = 0; Target = target; Kind = ScalarNode }
                let types = Dictionary<Type,Node>()
                types.Add(target,node)
                Available { Root=node; Nodes=[|node|]; Types=types; Recursive=false }
            else tryCreate target
        match shape with
        | Available graph when typeof<string>.Equals target || graph.Root.Kind<>ScalarNode ->
            let project parent access =
                let rec child (node:Node) access =
                    match node.Kind, access with
                    | OptionalNode(payload,_), ClrMember("Value",_) -> ValueSome struct(payload.Target,[|0|])
                    | ComponentNode fields, _ ->
                        let selected =
                            match access with
                            | ClrMember(name,_) | StoredLabel(:? string as name) -> fields |> Array.tryFindIndex(fun field -> field.Name=name)
                            | StoredLabel(:? int as index) when index>=0 && index<fields.Length && not(Utils.isTuple node.Target && fields.Length=8 && index=7) -> Some index
                            | _ -> None
                        match selected,access with
                        | Some ordinal,_ -> ValueSome struct(fields[ordinal].Node.Target,[|ordinal|])
                        | None,StoredLabel(:? int as index) when Utils.isTuple node.Target && fields.Length=8 && index>=7 ->
                            child fields[7].Node (StoredLabel(box(index-7)))
                            |> ValueOption.map(fun struct(target,path) -> struct(target,Array.append [|7|] path))
                        | _ -> ValueNone
                    | _ -> ValueNone
                match graph.Types.TryGetValue parent with true,node -> child node access | _ -> ValueNone
            ValueSome(struct((fun paths value -> box(prepareSelected graph paths value)),project))
        | _ -> ValueNone
