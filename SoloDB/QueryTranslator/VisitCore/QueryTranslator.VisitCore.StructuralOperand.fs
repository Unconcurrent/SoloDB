namespace SoloDatabase

open SoloDatabase.SqlModel

/// Typed original occurrences share one flat-row and ordinal-path interpretation.
module internal QueryStructuralOperand =
    let private integer value = SqlExpr.Literal(SqlLiteral.Integer(int64 value))
    let private text value = SqlExpr.Literal(SqlLiteral.String value)
    let private binary op left right = SqlExpr.Binary(left,op,right)
    let private at value path = SqlExpr.FunctionCall("jsonb_extract",[value;text path])
    let private projection name value = {Alias=Some name;Expr=value}
    let private core source projections =
        {Distinct=false;Projections=ProjectionSetOps.ofList projections;Source=source;Joins=[];Where=None
         GroupBy=[];Having=None;OrderBy=[];Limit=None;Offset=None}
    let private query core = {Ctes=[];Body=SingleSelect core}

    let rows nextName data =
        let name,alias=nextName(),nextName()
        let entry=SqlExpr.Column(Some alias,"value")
        let body=core (Some(FromJsonEach(SqlExpr.FunctionCall("jsonb",[data]),Some alias)))
                    [projection "node" (SqlExpr.Column(Some alias,"key"));projection "parent" (at entry "$[0]")
                     projection "ordinal" (at entry "$[1]");projection "absent" (at entry "$[2]")
                     projection "payload" (SqlExpr.FunctionCall("jsonb",[binary BinaryOperator.JsonText entry (text "$[3]")]))
                     projection "entry" (SqlExpr.FunctionCall("jsonb",[entry]))]
        {Name=name;Materialized=true;Query=query body}

    let position nextName rows path =
        let cursor,parent,child=nextName(),nextName(),nextName()
        let depth=SqlExpr.Column(Some parent,"depth")
        let count=SqlExpr.FunctionCall("json_array_length",[path])
        let step=core (Some(BaseTable(cursor,Some parent)))
                    [projection "depth" (binary BinaryOperator.Add depth (integer 1));projection "node" (SqlExpr.Column(Some child,"node"))]
        let step={step with Joins=[ConditionedJoin(JoinKind.Inner,BaseTable(rows,Some child),
                                binary BinaryOperator.And
                                    (binary BinaryOperator.Eq (SqlExpr.Column(Some child,"parent")) (SqlExpr.Column(Some parent,"node")))
                                    (binary BinaryOperator.Eq (SqlExpr.Column(Some child,"ordinal"))
                                        (SqlExpr.FunctionCall("jsonb_extract",[path;SqlExpr.FunctionCall("concat",[text "$[";depth;text "]"])]))))]
                            Where=Some(binary BinaryOperator.Lt depth count)}
        let binding={Name=cursor;Materialized=false;Query={Ctes=[];Body=UnionAllSelect(core None [projection "depth" (integer 0);projection "node" (integer 0)],[step])}}
        let result=core (Some(BaseTable(cursor,Some parent))) [projection "node" (SqlExpr.Column(Some parent,"node"))]
        binding,SqlExpr.ScalarSubquery(query {result with Where=Some(binary BinaryOperator.Eq depth count);Limit=Some(integer 1)})

    /// Resolve one node, retaining absent nodes separately from missing paths.
    let consume nextName (original:QueryOriginalComparisonOperand) finish fallback =
        original.Demand()
        let rows=rows nextName original.Data.Value
        let bindings,node=
            if original.Depth=0 then [rows],integer 0 else
            let cursor,node=position nextName rows.Name original.Path
            [rows;cursor],node
        let alias=nextName()
        let body=core (Some(BaseTable(rows.Name,Some alias))) [projection "entry" (SqlExpr.Column(Some alias,"entry"))]
        let lookup=SqlExpr.ScalarSubquery {Ctes=bindings;Body=SingleSelect {body with Where=Some(binary BinaryOperator.Eq (SqlExpr.Column(Some alias,"node")) node);Limit=Some(integer 1)}}
        let selected=nextName()
        let entry=SqlExpr.Column(Some selected,"value")
        let result=SqlExpr.CaseExpr((SqlExpr.Unary(UnaryOperator.IsNotNull,entry),finish entry),[],Some fallback)
        SqlExpr.ScalarSubquery(query (core (Some(FromJsonEach(SqlExpr.JsonArrayExpr [lookup],Some selected))) [projection "value" result]))
