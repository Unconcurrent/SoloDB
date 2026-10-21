namespace SoloDatabase

open System
open System.Collections.Generic
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorSerialization
open SoloDatabase.QuerySerializationSql
open SoloDatabase.QueryStructuralShape

/// A unary preorder traversal emits ordered primitive chunks, retaining pending siblings.
module internal QueryOrderingTraversal =
    type Root = Selected of QueryProducedValue | Prepared of SqlExpr * SqlExpr voption
    type private Access = Current | Member of string | Payload
    type private Stored = { Input: QuerySerializationInput; Access: Access }
    type private Context = { Node: Node; Role: QueryOrderingPolicy.Role; Stored: Stored voption }
    let private integer value = SqlExpr.Literal(SqlLiteral.Integer(int64 value))
    let private text value = SqlExpr.Literal(SqlLiteral.String value)
    let private binary op left right = SqlExpr.Binary(left,op,right)
    let private choose condition yes no = SqlExpr.CaseExpr((condition,yes),[],Some no)
    let private at value path = SqlExpr.FunctionCall("jsonb_extract",[value;text path])
    let private projection name value = { Alias = Some name; Expr = value }
    let private core source projections =
        { Distinct = false; Projections = ProjectionSetOps.ofList projections; Source = source
          Joins = []; Where = None; GroupBy = []; Having = None; OrderBy = []; Limit = None; Offset = None }
    let private query core = { Ctes = []; Body = SingleSelect core }

    let rec encode nextName variables (node: Node) role root =
        match root with
        | Selected(DirectValue(value,input)) when input.OriginalOrderOperand.IsSome ->
            let original=input.OriginalOrderOperand.Value
            original.Demand()
            let prepared=encode nextName variables node role (Prepared(original.Data.Value,if original.Depth=0 then ValueNone else ValueSome original.Path))
            let stored=encode nextName variables node role (Selected(DirectValue(value,{input with OriginalOrderOperand=ValueNone})))
            let prepared=if original.Depth=0 then prepared else SqlExpr.Coalesce(prepared,[stored])
            choose (SqlExpr.Unary(UnaryOperator.IsNotNull,original.Data.Value)) prepared stored
        | _ -> encodeCore nextName variables node role root

    and private encodeCore nextName variables (node: Node) role root =
        let work,current = nextName(),nextName()
        let stateFields = [|"context";"slot";"coordinate";"pending";"chunk";"done"|]
        let field state name =
            let index = stateFields |> Array.findIndex ((=) name)
            at state ("$[" + string index + "]")
        let column name = field (SqlExpr.Column(Some current,"state")) name
        let coordinate = column "coordinate"
        let contexts = ResizeArray<Context>()
        let register node role stored =
            match contexts |> Seq.tryFindIndex(fun context -> context.Node.Index=node.Index && context.Role=role && context.Stored=stored) with
            | Some index -> index
            | None -> let index=contexts.Count in contexts.Add {Node=node;Role=role;Stored=stored}; index
        let bindings = ResizeArray<CteBinding>()
        let rootBindings = ResizeArray<CteBinding>()
        let rootAlias = nextName()
        let joins = ResizeArray<JoinShape>()
        let mutable preparedRows = ""
        let mutable preparedAlias = ""
        let stored,initialCoordinate =
            match root with
            | Selected(DirectValue(value,input)) ->
                let name=nextName()
                let token=input.Source |> ValueOption.defaultWith(fun () -> canonicalToken input.Carrier value)
                rootBindings.Add {Name=name;Materialized=true;Query=query(core None [projection "value" token])}
                ValueSome {Input=QueryStructuralTraversal.scopeInput input;Access=Current},text "$"
            | Selected _ -> invalidOp "An ordering traversal requires its once-bound key."
            | Prepared(value,path) ->
                preparedRows<-nextName()
                preparedAlias<-nextName()
                let alias=nextName()
                let entry=SqlExpr.Column(Some alias,"value")
                bindings.Add {Name=preparedRows;Materialized=true;Query=query(core (Some(FromJsonEach(SqlExpr.FunctionCall("jsonb",[value]),Some alias)))
                    [projection "node" (SqlExpr.Column(Some alias,"key"));projection "parent" (at entry "$[0]")
                     projection "ordinal" (at entry "$[1]");projection "absent" (at entry "$[2]")
                     projection "payload" (SqlExpr.FunctionCall("jsonb",[binary BinaryOperator.JsonText entry (text "$[3]")]))])}
                joins.Add(ConditionedJoin(JoinKind.Inner,BaseTable(preparedRows,Some preparedAlias),
                    binary BinaryOperator.Eq (SqlExpr.Column(Some preparedAlias,"node")) coordinate))
                let initial =
                    match path with
                    | ValueNone -> integer 0
                    | ValueSome path ->
                        let cursor,parent,child=nextName(),nextName(),nextName()
                        let depth=SqlExpr.Column(Some parent,"depth")
                        let count=SqlExpr.FunctionCall("json_array_length",[path])
                        let step=core (Some(BaseTable(cursor,Some parent)))
                                    [projection "depth" (binary BinaryOperator.Add depth (integer 1));projection "node" (SqlExpr.Column(Some child,"node"))]
                        let step={step with Joins=[ConditionedJoin(JoinKind.Inner,BaseTable(preparedRows,Some child),
                                                binary BinaryOperator.And
                                                    (binary BinaryOperator.Eq (SqlExpr.Column(Some child,"parent")) (SqlExpr.Column(Some parent,"node")))
                                                    (binary BinaryOperator.Eq (SqlExpr.Column(Some child,"ordinal"))
                                                        (SqlExpr.FunctionCall("jsonb_extract",[path;SqlExpr.FunctionCall("concat",[text "$[";depth;text "]"])]))))]
                                            Where=Some(binary BinaryOperator.Lt depth count)}
                        bindings.Add {Name=cursor;Materialized=false;Query={Ctes=[];Body=UnionAllSelect(core None [projection "depth" (integer 0);projection "node" (integer 0)],[step])}}
                        let result=core (Some(BaseTable(cursor,Some parent))) [projection "node" (SqlExpr.Column(Some parent,"node"))]
                        SqlExpr.ScalarSubquery(query {result with Where=Some(binary BinaryOperator.Eq depth count);Limit=Some(integer 1)})
                ValueNone,initial
        let rootContext=register node role stored
        let rootColumn = SqlExpr.Column(Some rootAlias,"value")
        let token (input: QuerySerializationInput) =
            let root = rootColumn
            if isSqlScalarResult input.OriginalType then
                SqlExpr.FunctionCall("jsonb",[binary BinaryOperator.JsonText root coordinate])
            else SqlExpr.FunctionCall("jsonb_extract",[root;coordinate])
        let produced (stored: Stored)=
            let token=token stored.Input
            DirectValue(token,{stored.Input with Source=ValueSome token;Carrier=CanonicalToken})
        let selected context stored =
            let parent=produced stored
            match stored.Access with
            | Current -> parent
            | Member name -> QueryStructuralAccess.child variables nextName name context.Node.Target parent
            | Payload -> QueryStructuralAccess.payload nextName parent
        let missing context =
            match context.Stored with
            | ValueSome stored when stored.Access=Current && not(isSqlScalarResult stored.Input.OriginalType) ->
                let kind = SqlExpr.FunctionCall("json_type", [rootColumn; coordinate])
                binary BinaryOperator.Or (SqlExpr.Unary(UnaryOperator.IsNull,kind)) (binary BinaryOperator.Eq kind (text "null"))
            | ValueSome stored -> QueryProduced.consume nextName (fun value input -> SqlExpr.Unary(UnaryOperator.IsNull,nullCarrier input.Carrier value)) (selected context stored)
            | ValueNone -> SqlExpr.Column(Some preparedAlias,"absent")
        let preparedField ordinal field =
            let alias=nextName()
            let body=core (Some(BaseTable(preparedRows,Some alias))) [projection field (SqlExpr.Column(Some alias,field))]
            let body =
                { body with
                    Where = Some(binary BinaryOperator.And
                        (binary BinaryOperator.Eq (SqlExpr.Column(Some alias,"parent")) coordinate)
                        (binary BinaryOperator.Eq (SqlExpr.Column(Some alias,"ordinal")) (integer ordinal)))
                    Limit = Some(integer 1) }
            SqlExpr.ScalarSubquery(query body)
        let childContext context child name ordinal =
            match context.Stored with
            | ValueNone -> ValueNone,preparedField ordinal "node"
            | ValueSome stored ->
                if child.Kind=ScalarNode then ValueSome {Input=stored.Input;Access=Member name},coordinate else
                let parent=produced stored
                let struct(raw,segments)=serializationAccess parent.Input (ClrMember(name,child.Target))
                let childValue=QueryStructuralAccess.child variables nextName name child.Target parent
                let suffix=(JsonPathOps.text segments).Substring(1)
                let childCoordinate=if suffix.Length=0 then coordinate else SqlExpr.FunctionCall("concat",[coordinate;text suffix])
                let parentCoordinate=if suffix.Length=0 then coordinate else SqlExpr.FunctionCall("substr",[coordinate;integer 1;
                    binary BinaryOperator.Sub (SqlExpr.FunctionCall("length",[coordinate])) (SqlExpr.FunctionCall("length",[text suffix]))])
                let map=SqlExpr.map(function
                    | value when value=coordinate -> parentCoordinate
                    | value -> value)
                let input=childValue.Input |> QueryStructuralTraversal.scopeInput |> QueryStructuralTraversal.mapInput map
                ValueSome {Input={input with SourceOffset=raw.SourceOffset};Access=Current},childCoordinate
        let payloadContext context child =
            match context.Stored with
            | ValueNone -> ValueNone,preparedField 0 "node"
            | ValueSome stored when child.Kind=ScalarNode -> ValueSome {stored with Access=Payload},coordinate
            | ValueSome stored ->
                let input=(QueryStructuralAccess.payload nextName (produced stored)).Input |> QueryStructuralTraversal.scopeInput
                ValueSome {Input=input;Access=Current},coordinate
        let scalar context preparedPayload =
            let target=context.Node.Target
            match context.Stored with
            | ValueSome stored ->
                let produced = selected context stored
                QueryStructuralAccess.scalar variables nextName target produced
                |> QueryOrderingPolicy.scalar context.Role target
            | ValueNone ->
                let raw=preparedPayload |> Option.defaultValue (SqlExpr.Column(Some preparedAlias,"payload"))
                if typeof<string>.Equals target then
                    let units=SqlExpr.FunctionCall("base64",[at raw "$"])
                    if context.Role=QueryOrderingPolicy.FSharp then units else QueryOrderingPrimitives.cultureString (context.Role=QueryOrderingPolicy.ObjectDefault) units
                else
                    DirectValue(raw,{QuerySerializationInput.ForType target with Source=ValueSome raw;Carrier=CanonicalToken})
                    |> QueryStructuralAccess.scalar variables nextName target |> QueryOrderingPolicy.scalar context.Role target
        let chunk context preparedPayload =
            match context.Stored with
            | ValueSome stored ->
                let value=selected context stored
                if value.Input.OriginalOrderOperand.IsSome then encode nextName variables context.Node context.Role (Selected value)
                else QueryOrderingPolicy.chunk context.Role context.Node.Target (scalar context preparedPayload)
            | ValueNone -> QueryOrderingPolicy.chunk context.Role context.Node.Target (scalar context preparedPayload)
        let pending=column "pending"
        let frame=at pending "$[#-1]"
        // Selected child coordinates extend their ancestor path. A pending
        // continuation needs its prefix length, not another copy of that path.
        let savedCoordinate = if stored.IsSome then SqlExpr.FunctionCall("length",[coordinate]) else coordinate
        let resumedCoordinate =
            if stored.IsSome then SqlExpr.FunctionCall("substr",[coordinate;integer 1;at frame "$[2]"])
            else at frame "$[2]"
        let state context slot coordinate pending chunk doneValue =
            SqlExpr.JsonArrayExpr [context;slot;coordinate;pending;chunk;doneValue]
        let advance context slot coordinate pending chunk = state (integer context) (integer slot) coordinate pending chunk (integer 0)
        let resume chunk =
            choose (binary BinaryOperator.Gt (SqlExpr.FunctionCall("json_array_length",[pending])) (integer 0))
                (state (at frame "$[0]") (at frame "$[1]") resumedCoordinate
                    (SqlExpr.FunctionCall("jsonb_remove",[pending;text "$[#-1]"])) chunk (integer 0))
                (state (integer rootContext) (integer -1) initialCoordinate (SqlExpr.JsonArrayExpr []) chunk (integer 1))
        let bodies=ResizeArray<SqlExpr*SqlExpr>()
        let mutable index=0
        while index<contexts.Count do
            let context=contexts[index]
            let childRole=QueryOrderingPolicy.children context.Role context.Node
            let body=
                match context.Stored with
                | ValueSome stored when stored.Access=Current && stored.Input.OriginalOrderOperand.IsSome ->
                    resume(encode nextName variables context.Node context.Role (Selected(produced stored)))
                | _ ->
                match context.Node.Kind with
                | ScalarNode -> resume(chunk context None)
                | OptionalNode(child,_) ->
                    let stored,coordinate=payloadContext context child
                    let childContext=register child childRole stored
                    choose (missing context) (resume(text "0")) (advance childContext -1 coordinate pending (text "1"))
                | ComponentNode fields ->
                    let transition ordinal prefix =
                        if ordinal=fields.Length then resume prefix
                        elif fields[ordinal].Node.Kind=ScalarNode then
                            let mutable finish=ordinal+1
                            while finish<fields.Length && fields[finish].Node.Kind=ScalarNode do finish<-finish+1
                            let chunks =
                                [| yield prefix
                                   for index in ordinal..finish-1 do
                                    let field=fields[index]
                                    let stored,_=childContext context field.Node field.Name index
                                    let child={Node=field.Node;Role=childRole;Stored=stored}
                                    let payload=if stored.IsNone then Some(preparedField index "payload") else None
                                    yield chunk child payload |]
                            let rec concatenate first count =
                                if count=1 then chunks[first] else
                                let left=count/2
                                SqlExpr.FunctionCall("concat",[concatenate first left;concatenate (first+left) (count-left)])
                            let chunk=concatenate 0 chunks.Length
                            if finish=fields.Length then resume chunk else advance index finish coordinate pending chunk
                        else
                            let field=fields[ordinal]
                            let stored,childCoordinate=childContext context field.Node field.Name ordinal
                            let child=register field.Node childRole stored
                            let stack=if ordinal+1=fields.Length then pending else SqlExpr.FunctionCall("jsonb_insert",[pending;text "$[#]";
                                SqlExpr.JsonArrayExpr [integer index;integer(ordinal+1);savedCoordinate]])
                            advance child -1 childCoordinate stack prefix
                    let choices =
                        [|for ordinal in 0..fields.Length-1 do
                            if ordinal=0 || fields[ordinal].Node.Kind<>ScalarNode || fields[ordinal-1].Node.Kind<>ScalarNode then
                                yield binary BinaryOperator.Eq (column "slot") (integer ordinal),transition ordinal (text "")|]
                    let consume=if choices.Length=0 then resume(text "") else SqlExpr.CaseExpr(choices[0],choices[1..] |> Array.toList,Some(resume(text "")))
                    let beginValue=transition 0 (text(if context.Node.Target.IsValueType then "" else "1"))
                    let beginValue=if context.Node.Target.IsValueType then beginValue else choose (missing context) (resume(text "0")) beginValue
                    choose (binary BinaryOperator.Lt (column "slot") (integer 0)) beginValue consume
            bodies.Add(binary BinaryOperator.Eq (column "context") (integer index),body)
            index<-index+1
        // Typed child access has already selected the writer and literal suffix.
        // Read that suffix from the bound root without copying its parent subtree.
        let directPath expression =
            let path (suffix:string) = SqlExpr.FunctionCall("concat", [coordinate; text(suffix.Substring(1))])
            SqlExpr.map (function
                | SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.FunctionCall("jsonb_extract", [root; current]); SqlExpr.Literal(SqlLiteral.String suffix)])
                    when root=rootColumn && current=coordinate && suffix.StartsWith("$",StringComparison.Ordinal) ->
                    SqlExpr.FunctionCall("jsonb_extract", [root; path suffix])
                | SqlExpr.Binary(SqlExpr.FunctionCall("jsonb_extract", [root; current]),BinaryOperator.JsonText,SqlExpr.Literal(SqlLiteral.String suffix))
                    when root=rootColumn && current=coordinate && suffix.StartsWith("$",StringComparison.Ordinal) ->
                    SqlExpr.Binary(root,BinaryOperator.JsonText,path suffix)
                | value -> value) expression
        let step=SqlExpr.CaseExpr(bodies[0],bodies |> Seq.skip 1 |> Seq.toList,Some(resume(text ""))) |> directPath
        let initial =
            core None
                [projection "state" (state (integer rootContext) (integer -1) initialCoordinate (SqlExpr.JsonArrayExpr []) (text "") (integer 0))
                 projection "ordinal" (integer 0)]
        let recursive =
            core (Some(BaseTable(work,Some current)))
                [projection "state" step
                 projection "ordinal" (binary BinaryOperator.Add (SqlExpr.Column(Some current,"ordinal")) (integer 1))]
        let recursive =
            { recursive with
                Joins = List.ofSeq joins
                Where = Some(binary BinaryOperator.Eq (column "done") (integer 0)) }
        bindings.Add {Name=work;Materialized=false;Query={Ctes=[];Body=UnionAllSelect(initial,[recursive])}}
        let encoded=SqlExpr.AggregateCall(AggregateKind.GroupConcat,Some(field (SqlExpr.Column(None,"state")) "chunk"),false,Some(text ""),[SqlExpr.Column(None,"ordinal"),SortDirection.Asc])
        let encoded=
            match root with
            | Prepared(_,ValueSome _) ->
                // A projected path through an absent ancestor has no node.
                // Its existing selected/default owner supplies the result.
                choose (SqlExpr.Unary(UnaryOperator.IsNotNull,initialCoordinate)) encoded (SqlExpr.Literal SqlLiteral.Null)
            | _ -> encoded
        let value=SqlExpr.ScalarSubquery {Ctes=List.ofSeq bindings;Body=SingleSelect(core (Some(BaseTable(work,None))) [projection "value" encoded])}
        if rootBindings.Count=0 then value else
        SqlExpr.ScalarSubquery {Ctes=List.ofSeq rootBindings;Body=SingleSelect(core (Some(BaseTable(rootBindings[0].Name,Some rootAlias))) [projection "value" value])}
