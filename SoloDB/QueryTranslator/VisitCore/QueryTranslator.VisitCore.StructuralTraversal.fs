namespace SoloDatabase

open System
open System.Collections.Generic
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorSerialization
open SoloDatabase.QuerySerializationSql
open SoloDatabase.QueryStructuralShape

/// SQL visits one typed component at a time and retains only pending coordinates.
module internal QueryStructuralTraversal =
    type Root =
        | Prepared of SqlExpr
        | PreparedProjection of SqlExpr * SqlExpr
        | PreparedRows of string * SqlExpr
        | Selected of SqlExpr * QuerySerializationInput
    type Operations = {
        Child: string -> Node -> QueryProducedValue -> QueryProducedValue
        Payload: Node -> QueryProducedValue -> QueryProducedValue
        Scalar: Type -> QueryProducedValue -> SqlExpr
        RawScalar: Type -> QueryProducedValue -> SqlExpr
        Presence: QueryProducedValue -> SqlExpr
        NecessaryRange: Type -> bool -> SqlExpr -> SqlExpr -> SqlExpr voption
    }
    type private Access = Current | Member of string | Payload
    type private Stored = { Input: QuerySerializationInput; Access: Access }
    type private Context = { Node: Node; Left: Stored voption; Right: Stored voption }

    let private integer value = SqlExpr.Literal(SqlLiteral.Integer(int64 value))
    let private text value = SqlExpr.Literal(SqlLiteral.String value)
    let private boolean value = SqlExpr.Literal(SqlLiteral.Boolean value)
    let private binary op left right = SqlExpr.Binary(left,op,right)
    let private choose condition yes no = SqlExpr.CaseExpr((condition,yes),[],Some no)
    let private at value path = SqlExpr.FunctionCall("jsonb_extract",[value;text path])
    let private core source projections =
        { Distinct = false; Projections = ProjectionSetOps.ofList projections; Source = source
          Joins = []; Where = None; GroupBy = []; Having = None; OrderBy = []; Limit = None; Offset = None }
    let private query value = { Ctes = []; Body = SingleSelect value }
    let private projection name value = { Alias = Some name; Expr = value }

    // These typed consumers demand child layout and physical ownership, not an
    // accumulated writer-access history once no produced children remain.
    let rec internal scopeInput (input: QuerySerializationInput) =
        { input with Source = ValueNone; Carrier = CanonicalToken; Navigation = Array.empty
                     Operand = if input.Children.Length = 0 then ValueNone else input.Operand
                     Children = input.Children |> Array.map scopeInput }

    let internal mapInput map (input: QuerySerializationInput) =
        let rec loop (input: QuerySerializationInput) =
            { input with
                OriginalComparisonOperand = input.OriginalComparisonOperand |> ValueOption.map (fun original -> original.WithSource(lazy (map original.Data.Value), map original.Path))
                Operand = input.Operand |> ValueOption.map(fun operand ->
                    { Descriptor = map operand.Descriptor
                      Accesses = operand.Accesses |> Array.map(fun step ->
                        { Kind = map step.Kind; Label = map step.Label; MemberType = map step.MemberType; ActualType = map step.ActualType }) })
                PhysicalOwner = input.PhysicalOwner |> ValueOption.map(fun owner -> { owner with Descriptor = map owner.Descriptor })
                ReferenceOwner = input.ReferenceOwner |> ValueOption.map(fun owner -> { owner with Descriptor = map owner.Descriptor })
                Children = input.Children |> Array.map loop }
        loop input

    let rec private translateCore outcome nextName relation operations graph left right =
        let original side value (input:QuerySerializationInput) other =
            let original=input.OriginalComparisonOperand.Value
            original.Demand()
            let prepared=if original.Depth=0 then Prepared original.Data.Value else PreparedProjection(original.Data.Value,original.Path)
            let stored=Selected(value,{input with OriginalComparisonOperand=ValueNone})
            let apply value=if side then translateCore outcome nextName relation operations graph value other else translateCore outcome nextName relation operations graph other value
            let candidate,fallback=apply prepared,apply stored
            choose (SqlExpr.Unary(UnaryOperator.IsNotNull,original.Data.Value)) (SqlExpr.Coalesce(candidate,[fallback])) fallback
        match left,right with
        | Selected(value,input),_ when input.OriginalComparisonOperand.IsSome -> original true value input right
        | _,Selected(value,input) when input.OriginalComparisonOperand.IsSome -> original false value input left
        | _ -> translateWork outcome nextName relation operations graph left right

    and private translateWork outcome nextName relation operations graph left right =
        let work = nextName()
        let current = nextName()
        let successor = nextName()
        let column name = SqlExpr.Column(Some current,name)
        let contexts = ResizeArray<Context>()
        let sameStored left right =
            match left,right with
            | ValueNone,ValueNone -> true
            | ValueSome left,ValueSome right -> left.Access = right.Access && left.Input = right.Input
            | _ -> false
        let register node left right =
            let existing = contexts |> Seq.tryFindIndex(fun context ->
                context.Node.Index = node.Index && sameStored context.Left left && sameStored context.Right right)
            match existing with
            | Some index -> index
            | None ->
                let index = contexts.Count
                contexts.Add { Node = node; Left = left; Right = right }
                index
        let roots = ResizeArray<CteBinding * string>()
        let ctes = ResizeArray<CteBinding>()
        let joins = ResizeArray<JoinShape>()
        let prepared = Dictionary<string, string * string>()
        let root side = function
            | Selected(value,input) ->
                let token = input.Source |> ValueOption.defaultWith(fun () -> canonicalToken input.Carrier value)
                let name,alias = nextName(),nextName()
                let binding = { Name = name; Materialized = true; Query = query(core None [projection "value" token]) }
                roots.Add(binding,alias)
                SqlExpr.Column(Some alias,"value"),ValueSome { Input = scopeInput input; Access = Current },text "$"
            | PreparedRows(name,node) ->
                let alias=nextName()
                joins.Add(ConditionedJoin(JoinKind.Inner,BaseTable(name,Some alias),
                    binary BinaryOperator.Eq (SqlExpr.Column(Some alias,"node")) (column side)))
                prepared.Add(side,(name,alias))
                SqlExpr.Literal SqlLiteral.Null,ValueNone,node
            | (Prepared value | PreparedProjection(value,_)) as original ->
                let alias = nextName()
                let rows=QueryStructuralOperand.rows nextName value
                let name=rows.Name
                ctes.Add rows
                joins.Add(ConditionedJoin(JoinKind.Inner,BaseTable(name,Some alias),
                    binary BinaryOperator.Eq (SqlExpr.Column(Some alias,"node")) (column side)))
                prepared.Add(side,(name,alias))
                let initial=
                    match original with
                    | PreparedProjection(_,path) ->
                        let cursor,node=QueryStructuralOperand.position nextName name path
                        ctes.Add cursor
                        node
                    | _ -> integer 0
                value,ValueNone,initial
        let leftRoot,leftInput,leftCoordinate = root "left" left
        let rightRoot,rightInput,rightCoordinate = root "right" right
        let rootContext = register graph.Root leftInput rightInput
        let rootValue side = if side = "left" then leftRoot else rightRoot
        let token side = SqlExpr.FunctionCall("jsonb",[binary BinaryOperator.JsonText (rootValue side) (column side)])
        let produced side (stored: Stored) =
            let raw = token side
            let parent = DirectValue(raw,{ stored.Input with Source = ValueSome raw; Carrier = CanonicalToken })
            parent
        let selected side context stored =
            let parent = produced side stored
            match stored.Access with
            | Current -> parent
            | Member name -> operations.Child name context.Node parent
            | Payload -> operations.Payload context.Node parent
        let missing side context stored =
            match stored with
            | ValueSome stored ->
                operations.Presence (selected side context stored)
            | ValueNone ->
                let _,alias = prepared[side]
                SqlExpr.Column(Some alias,"absent")
        let stack = column "pending"
        let frame = at stack "$[#-1]"
        let active = integer -1
        let terminal result = SqlExpr.JsonArrayExpr [integer 0;integer 0;leftCoordinate;rightCoordinate;SqlExpr.JsonArrayExpr [];result]
        let advance context slot left right pending =
            SqlExpr.JsonArrayExpr [integer context;integer slot;left;right;pending;active]
        let equalResult=if outcome then integer 2 else boolean(relation = BinaryOperator.Le || relation = BinaryOperator.Ge)
        let exhausted = terminal equalResult
        let resume = choose (binary BinaryOperator.Gt (SqlExpr.FunctionCall("json_array_length",[stack])) (integer 0))
                        (SqlExpr.JsonArrayExpr [at frame "$[0]";at frame "$[1]";at frame "$[2]";at frame "$[3]";
                                               SqlExpr.FunctionCall("jsonb_remove",[stack;text "$[#-1]"]);active]) exhausted
        let absent leftMissing rightMissing present =
            choose leftMissing (choose rightMissing resume (terminal(boolean(relation = BinaryOperator.Lt || relation = BinaryOperator.Le))))
                (choose rightMissing (terminal(boolean(relation = BinaryOperator.Gt || relation = BinaryOperator.Ge))) present)
        let boundField side ordinal field =
            let name,_ = prepared[side]
            let alias = nextName()
            let body = core (Some(BaseTable(name,Some alias))) [projection field (SqlExpr.Column(Some alias,field))]
            let body =
                { body with
                    Where = Some(binary BinaryOperator.And
                        (binary BinaryOperator.Eq (SqlExpr.Column(Some alias,"parent")) (column side))
                        (binary BinaryOperator.Eq (SqlExpr.Column(Some alias,"ordinal")) (integer ordinal)))
                    Limit = Some(integer 1) }
            SqlExpr.ScalarSubquery(query body)
        let boundChild side ordinal = boundField side ordinal "node"
        let childContext side context child name ordinal stored =
            match stored with
            | ValueNone -> ValueNone,boundChild side ordinal
            | ValueSome stored ->
                let parent = produced side stored
                if child.Kind = ScalarNode then
                    ValueSome { Input = stored.Input; Access = Member name },column side
                else
                    let access = ClrMember(name,child.Target)
                    let struct(raw,segments) = serializationAccess parent.Input access
                    let selected = operations.Child name child parent
                    let suffix = (JsonPathOps.text segments).Substring(1)
                    let coordinate = if suffix.Length = 0 then column side else SqlExpr.FunctionCall("concat",[column side;text suffix])
                    let parentCoordinate =
                        if suffix.Length = 0 then column side else
                        SqlExpr.FunctionCall("substr",[column side;integer 1;
                            binary BinaryOperator.Sub (SqlExpr.FunctionCall("length",[column side])) (SqlExpr.FunctionCall("length",[text suffix]))])
                    let map = SqlExpr.map(function
                        | SqlExpr.Column(Some alias,name) when alias = current && name = side -> parentCoordinate
                        | expression -> expression)
                    let input = selected.Input |> scopeInput |> mapInput map
                    ValueSome { Input = { input with SourceOffset = raw.SourceOffset }; Access = Current },coordinate
        let payloadContext side context child stored =
            match stored with
            | ValueNone -> ValueNone,boundChild side 0
            | ValueSome stored ->
                if child.Kind = ScalarNode then ValueSome { stored with Access = Payload },column side
                else
                    let input = (operations.Payload child (produced side stored)).Input |> scopeInput
                    ValueSome { Input = input; Access = Current },column side
        let preparedScalar target raw =
            if typeof<string>.Equals target then SqlExpr.FunctionCall("base64",[at raw "$"])
            else operations.Scalar target (DirectValue(raw,
                { QuerySerializationInput.ForType target with Source = ValueSome raw; Carrier = CanonicalToken }))
        let scalar side context stored =
            match stored with
            | ValueSome stored -> operations.Scalar context.Node.Target (selected side context stored)
            | ValueNone ->
                let _,alias = prepared[side]
                let raw = SqlExpr.Column(Some alias,"payload")
                preparedScalar context.Node.Target raw
        let compareScalar (target: Type) left right onEqual finish =
            let result = choose (binary BinaryOperator.Eq left right) onEqual (finish(binary relation left right))
            let leftNull,rightNull = SqlExpr.Unary(UnaryOperator.IsNull,left),SqlExpr.Unary(UnaryOperator.IsNull,right)
            if typeof<float>.Equals target || typeof<single>.Equals target then
                choose (binary BinaryOperator.Or leftNull rightNull) (finish(boolean false)) result
            elif target.IsValueType then result else
                choose leftNull (choose rightNull onEqual (finish(boolean(relation = BinaryOperator.Lt || relation = BinaryOperator.Le))))
                    (choose rightNull (finish(boolean(relation = BinaryOperator.Gt || relation = BinaryOperator.Ge))) result)
        // The first scalar precedes every recursive continuation. Its prepared
        // occurrence is fixed at one, so the direct prefix needs no second walk.
        let prefix =
            let wholeRoot=function Prepared _ | Selected _ -> true | _ -> false
            match graph.Root.Kind with
            | ComponentNode fields when fields.Length > 1 && fields[0].Node.Kind = ScalarNode && wholeRoot left && wholeRoot right ->
                let field = fields[0]
                let value = function
                    | Prepared value ->
                        let token = SqlExpr.FunctionCall("jsonb",[binary BinaryOperator.JsonText value (text "$[1][3]")])
                        if typeof<string>.Equals field.Node.Target then
                            let value = preparedScalar field.Node.Target token
                            value,value
                        else
                            let produced = DirectValue(token,{ QuerySerializationInput.ForType field.Node.Target with Source = ValueSome token; Carrier = CanonicalToken })
                            operations.RawScalar field.Node.Target produced,operations.Scalar field.Node.Target produced
                    | PreparedProjection _ | PreparedRows _ -> invalidOp "A projected prepared root has no fixed first occurrence."
                    | Selected(value,input) ->
                        let produced = operations.Child field.Name field.Node (DirectValue(value,input))
                        operations.RawScalar field.Node.Target produced,operations.Scalar field.Node.Target produced
                let missing = function
                    | Prepared value -> at value "$[0][2]"
                    | Selected(value,input) -> operations.Presence(DirectValue(value,input))
                    | PreparedProjection _ | PreparedRows _ -> invalidOp "A projected prepared root has no fixed presence occurrence."
                let (leftRaw,leftValue),(rightRaw,rightValue) = value left,value right
                let range =
                    if not graph.Root.Target.IsValueType then ValueNone else
                    match left,right with
                    | Selected _,Prepared _ -> operations.NecessaryRange field.Node.Target false leftRaw rightRaw
                    | Prepared _,Selected _ -> operations.NecessaryRange field.Node.Target true rightRaw leftRaw
                    | _ -> ValueNone
                let finish residual =
                    let present = compareScalar field.Node.Target leftValue rightValue residual id
                    let result =
                        if graph.Root.Target.IsValueType then present else
                        choose (missing left)
                            (choose (missing right) equalResult (boolean(relation = BinaryOperator.Lt || relation = BinaryOperator.Le)))
                            (choose (missing right) (boolean(relation = BinaryOperator.Gt || relation = BinaryOperator.Ge)) present)
                    match range with ValueSome range -> choose range result (boolean false) | ValueNone -> result
                1,finish
            | _ -> 0,id
        let initialSlot,finish = prefix
        let bodies = ResizeArray<SqlExpr * SqlExpr>()
        let mutable index = 0
        while index < contexts.Count do
            let context = contexts[index]
            let body =
                let hasOriginal=function ValueSome stored -> stored.Input.OriginalComparisonOperand.IsSome | _ -> false
                if context.Node.Kind<>ScalarNode && (hasOriginal context.Left || hasOriginal context.Right) then
                    let root side stored =
                        match stored with
                        | ValueSome stored ->
                            match selected side context stored with
                            | DirectValue(value,input) -> Selected(value,input)
                            | _ -> invalidOp "A bound structural occurrence must be direct."
                        | ValueNone -> let rows,_=prepared[side] in PreparedRows(rows,column side)
                    let graph={graph with Root=context.Node}
                    let result=translateCore true nextName relation operations graph (root "left" context.Left) (root "right" context.Right)
                    let alias=nextName()
                    let value=SqlExpr.Column(Some alias,"value")
                    SqlExpr.ScalarSubquery(query(core (Some(FromJsonEach(SqlExpr.JsonArrayExpr [result],Some alias)))
                        [projection "value" (choose (binary BinaryOperator.Eq value (integer 2)) resume (terminal value))]))
                else
                match context.Node.Kind with
                | ScalarNode ->
                    let leftValue,rightValue = scalar "left" context context.Left,scalar "right" context context.Right
                    compareScalar context.Node.Target leftValue rightValue resume terminal
                | OptionalNode(child,_) ->
                    let left,leftCoordinate = payloadContext "left" context child context.Left
                    let right,rightCoordinate = payloadContext "right" context child context.Right
                    let child = register child left right
                    absent (missing "left" context context.Left) (missing "right" context context.Right)
                        (advance child 0 leftCoordinate rightCoordinate stack)
                | ComponentNode fields ->
                    let rec continuation ordinal =
                        if ordinal = fields.Length then resume else
                        let field = fields[ordinal]
                        if field.Node.Kind = ScalarNode then
                            let value side = function
                                | ValueSome stored -> operations.Child field.Name field.Node (produced side stored) |> operations.Scalar field.Node.Target
                                | ValueNone -> preparedScalar field.Node.Target (boundField side ordinal "payload")
                            compareScalar field.Node.Target (value "left" context.Left) (value "right" context.Right)
                                (continuation(ordinal+1)) terminal
                        else
                            let left,leftCoordinate = childContext "left" context field.Node field.Name ordinal context.Left
                            let right,rightCoordinate = childContext "right" context field.Node field.Name ordinal context.Right
                            let child = register field.Node left right
                            let pending =
                                if ordinal + 1 = fields.Length then stack else
                                SqlExpr.FunctionCall("jsonb_insert",[stack;text "$[#]";
                                    SqlExpr.JsonArrayExpr [integer index;integer(ordinal+1);column "left";column "right"]])
                            advance child 0 leftCoordinate rightCoordinate pending
                    // Finite scalar runs remain lazy SQL in one state. Only a
                    // structural child needs a pending sibling continuation.
                    let choices = fields |> Array.mapi(fun ordinal _ -> ordinal) |> Array.choose(fun ordinal ->
                        if ordinal = 0 || fields[ordinal-1].Node.Kind <> ScalarNode || (index = rootContext && ordinal = initialSlot) then
                            Some(binary BinaryOperator.Eq (column "slot") (integer ordinal),continuation ordinal)
                        else None)
                    let present =
                        if choices.Length = 0 then resume else
                        SqlExpr.CaseExpr(choices[0],choices[1..] |> Array.toList,Some resume)
                    if context.Node.Target.IsValueType then present else
                    choose (binary BinaryOperator.Eq (column "slot") (integer 0))
                        (absent (missing "left" context context.Left) (missing "right" context context.Right) present) present
            bodies.Add(binary BinaryOperator.Eq (column "context") (integer index),body)
            index <- index + 1
        let step = SqlExpr.CaseExpr(bodies[0],bodies |> Seq.skip 1 |> Seq.toList,Some(terminal(boolean false)))
        let initial = core None
                            [projection "context" (integer rootContext);projection "slot" (integer initialSlot)
                             projection "left" leftCoordinate;projection "right" rightCoordinate
                             projection "pending" (SqlExpr.JsonArrayExpr []);projection "status" active]
        let next = SqlExpr.Column(Some successor,"value")
        let recursive = core (Some(BaseTable(work,Some current)))
                            (["context";"slot";"left";"right";"pending";"status"]
                             |> List.mapi(fun index name -> projection name (at next ("$[" + string index + "]"))))
        let recursive = { recursive with
                            Joins = List.ofSeq joins @ [CrossJoin(FromJsonEach(SqlExpr.JsonArrayExpr [step],Some successor))]
                            Where = Some(binary BinaryOperator.Eq (column "status") active) }
        ctes.Add { Name = work; Materialized = false; Query = { Ctes = []; Body = UnionAllSelect(initial,[recursive]) } }
        let valid=
            [left,leftCoordinate;right,rightCoordinate]
            |> List.choose(fun (root,coordinate) -> match root with PreparedProjection _ -> Some(SqlExpr.Unary(UnaryOperator.IsNotNull,coordinate)) | _ -> None)
        let status=valid |> List.fold(fun status valid -> choose valid status (SqlExpr.Literal SqlLiteral.Null)) (SqlExpr.Column(None,"status"))
        let result = core (Some(BaseTable(work,None))) [projection "status" status]
        let result =
            { result with
                Where = Some(binary BinaryOperator.Ge (SqlExpr.Column(None,"status")) (integer 0))
                Limit = Some(integer 1) }
        let traversal = SqlExpr.ScalarSubquery { Ctes = List.ofSeq ctes; Body = SingleSelect result }
        let bound =
            if roots.Count = 0 then traversal else
            // Each selected token is one scalar operand, bound outside the
            // recursive scope; pending states never carry a subtree copy.
            let first,alias = roots[0]
            let body = core (Some(BaseTable(first.Name,Some alias))) [projection "value" traversal]
            let body = { body with Joins = roots |> Seq.skip 1 |> Seq.map(fun (binding,alias) -> CrossJoin(BaseTable(binding.Name,Some alias))) |> Seq.toList }
            SqlExpr.ScalarSubquery { Ctes = roots |> Seq.map fst |> Seq.toList; Body = SingleSelect body }
        finish bound

    let translate nextName relation operations graph left right =
        translateCore false nextName relation operations graph left right
