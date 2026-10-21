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
    type Root = Selected of QueryProducedValue | Prepared of SqlExpr
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

    let encode nextName variables (node: Node) role root =
        let work,current,successor = nextName(),nextName(),nextName()
        let column name = SqlExpr.Column(Some current,name)
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
            | Prepared value ->
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
                ValueNone,integer 0
        let rootContext=register node role stored
        let token()=SqlExpr.FunctionCall("jsonb",[binary BinaryOperator.JsonText (SqlExpr.Column(Some rootAlias,"value")) coordinate])
        let produced (stored: Stored)=
            let token=token()
            DirectValue(token,{stored.Input with Source=ValueSome token;Carrier=CanonicalToken})
        let selected context stored =
            let parent=produced stored
            match stored.Access with
            | Current -> parent
            | Member name -> QueryStructuralAccess.child variables nextName name context.Node.Target parent
            | Payload -> QueryStructuralAccess.payload nextName parent
        let missing context =
            match context.Stored with
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
                    binary BinaryOperator.Sub (SqlExpr.FunctionCall("length",[coordinate])) (integer suffix.Length)])
                let map=SqlExpr.map(function
                    | SqlExpr.Column(Some alias,"coordinate") when alias=current -> parentCoordinate
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
        let scalar context =
            let target=context.Node.Target
            match context.Stored with
            | ValueSome stored ->
                let produced = selected context stored
                (if typeof<string>.Equals target then QueryStructuralAccess.stringValue nextName produced
                 else QueryStructuralAccess.scalar variables nextName target produced)
                |> QueryOrderingPolicy.scalar context.Role target
            | ValueNone ->
                let raw=SqlExpr.Column(Some preparedAlias,"payload")
                if typeof<string>.Equals target then
                    let units=SqlExpr.FunctionCall("base64",[at raw "$"])
                    if context.Role=QueryOrderingPolicy.FSharp then units else QueryOrderingPrimitives.cultureString (context.Role=QueryOrderingPolicy.ObjectDefault) units
                else
                    DirectValue(raw,{QuerySerializationInput.ForType target with Source=ValueSome raw;Carrier=CanonicalToken})
                    |> QueryStructuralAccess.scalar variables nextName target |> QueryOrderingPolicy.scalar context.Role target
        let pending=column "pending"
        let frame=at pending "$[#-1]"
        let state context slot coordinate pending chunk doneValue =
            SqlExpr.JsonArrayExpr [context;slot;coordinate;pending;chunk;doneValue]
        let advance context slot coordinate pending chunk = state (integer context) (integer slot) coordinate pending chunk (integer 0)
        let resume chunk =
            choose (binary BinaryOperator.Gt (SqlExpr.FunctionCall("json_array_length",[pending])) (integer 0))
                (state (at frame "$[0]") (at frame "$[1]") (at frame "$[2]")
                    (SqlExpr.FunctionCall("jsonb_remove",[pending;text "$[#-1]"])) chunk (integer 0))
                (state (integer rootContext) (integer -1) initialCoordinate (SqlExpr.JsonArrayExpr []) chunk (integer 1))
        let bodies=ResizeArray<SqlExpr*SqlExpr>()
        let mutable index=0
        while index<contexts.Count do
            let context=contexts[index]
            let childRole=QueryOrderingPolicy.children context.Role context.Node
            let body=
                match context.Node.Kind with
                | ScalarNode -> resume(QueryOrderingPolicy.chunk context.Role context.Node.Target (scalar context))
                | OptionalNode(child,_) ->
                    let stored,coordinate=payloadContext context child
                    let childContext=register child childRole stored
                    choose (missing context) (resume(text "0")) (advance childContext -1 coordinate pending (text "1"))
                | ComponentNode fields ->
                    let choices=fields |> Array.mapi(fun ordinal field ->
                        let stored,childCoordinate=childContext context field.Node field.Name ordinal
                        let child=register field.Node childRole stored
                        let stack=if ordinal+1=fields.Length then pending else SqlExpr.FunctionCall("jsonb_insert",[pending;text "$[#]";
                            SqlExpr.JsonArrayExpr [integer index;integer(ordinal+1);coordinate]])
                        binary BinaryOperator.Eq (column "slot") (integer ordinal),advance child -1 childCoordinate stack (text ""))
                    let consume=if choices.Length=0 then resume(text "") else SqlExpr.CaseExpr(choices[0],choices[1..] |> Array.toList,Some(resume(text "")))
                    let beginValue=advance index 0 coordinate pending (text(if context.Node.Target.IsValueType then "" else "1"))
                    let beginValue=if context.Node.Target.IsValueType then beginValue else choose (missing context) (resume(text "0")) beginValue
                    choose (binary BinaryOperator.Lt (column "slot") (integer 0)) beginValue consume
            bodies.Add(binary BinaryOperator.Eq (column "context") (integer index),body)
            index<-index+1
        let step=SqlExpr.CaseExpr(bodies[0],bodies |> Seq.skip 1 |> Seq.toList,Some(resume(text "")))
        let initial =
            core None
                [projection "context" (integer rootContext)
                 projection "slot" (integer -1)
                 projection "coordinate" initialCoordinate
                 projection "pending" (SqlExpr.JsonArrayExpr [])
                 projection "chunk" (text "")
                 projection "done" (integer 0)
                 projection "ordinal" (integer 0)]
        let next=SqlExpr.Column(Some successor,"value")
        let columns=["context";"slot";"coordinate";"pending";"chunk";"done"]
        let recursive =
            core (Some(BaseTable(work,Some current)))
                ((columns |> List.mapi(fun ordinal name -> projection name (at next ("$["+string ordinal+"]"))))
                 @ [projection "ordinal" (binary BinaryOperator.Add (column "ordinal") (integer 1))])
        let recursive =
            { recursive with
                Joins = List.ofSeq joins @ [CrossJoin(FromJsonEach(SqlExpr.JsonArrayExpr [step],Some successor))]
                Where = Some(binary BinaryOperator.Eq (column "done") (integer 0)) }
        bindings.Add {Name=work;Materialized=false;Query={Ctes=[];Body=UnionAllSelect(initial,[recursive])}}
        let encoded=SqlExpr.AggregateCall(AggregateKind.GroupConcat,Some(SqlExpr.Column(None,"chunk")),false,Some(text ""),[SqlExpr.Column(None,"ordinal"),SortDirection.Asc])
        let value=SqlExpr.ScalarSubquery {Ctes=List.ofSeq bindings;Body=SingleSelect(core (Some(BaseTable(work,None))) [projection "value" encoded])}
        if rootBindings.Count=0 then value else
        SqlExpr.ScalarSubquery {Ctes=List.ofSeq rootBindings;Body=SingleSelect(core (Some(BaseTable(rootBindings[0].Name,Some rootAlias))) [projection "value" value])}
