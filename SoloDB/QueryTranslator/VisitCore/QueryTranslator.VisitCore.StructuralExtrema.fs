namespace SoloDatabase

open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorSerialization

/// Structural extrema select an actual value packet, never independent field minima.
module internal QueryStructuralExtrema =
    let select nextName variables target direction (source: SelectCore) produced ties =
        let (packet,input,terms,bindings),parentBindings =
            QueryProduced.bindTerms nextName (fun value input ->
                let produced=DirectValue(value,input)
                let terms,bindings=QueryStructuralOrdering.prepare nextName variables target direction source.Where (QueryStructuralOrdering.Selected produced)
                QueryProduced.packet produced,input,terms,bindings) produced
        let selected =
            { source with
                Projections=ProjectionSetOps.ofList [{Alias=None;Expr=packet}]
                Joins=source.Joins @ parentBindings @ bindings
                OrderBy=terms @ ties
                Limit=Some(SqlExpr.Literal(SqlLiteral.Integer 1L));Offset=None }
        PackedValue(SqlExpr.ScalarSubquery {Ctes=[];Body=SingleSelect selected},rebaseSerializationInput input)

    let defaultResult target selected fallback = QueryProduced.defaultIfEmpty target selected fallback

    /// The complete selected packet distinguishes an empty source from a real null value.
    let rows nextName variables target emptyError selected =
        match selected with
        | DirectValue _ -> invalidArg (nameof selected) "An extremum requires its selected row packet."
        | PackedValue(packet,input) ->
            let alias=nextName()
            let present=SqlExpr.Unary(UnaryOperator.IsNotNull,SqlExpr.Column(Some alias,"value"))
            let actual=QueryProduced.fromBoundPacket alias input
            let fallback=
                match emptyError with
                | Some error -> DirectValue(error,{input with Carrier=CanonicalToken;Source=ValueSome error;StructuralDefault=false})
                | None -> QueryDefault.value variables target
            let output=QueryProduced.choose target present actual fallback
            let identity=
                match emptyError with
                | Some _ -> SqlExpr.CaseExpr((present,SqlExpr.Literal(SqlLiteral.Integer -1L)),[],Some(SqlExpr.Literal SqlLiteral.Null))
                | None -> SqlExpr.Literal(SqlLiteral.Integer -1L)
            let core =
                { Distinct=false;Projections=ProjectionSetOps.ofList [{Alias=Some "Id";Expr=identity};{Alias=Some "Value";Expr=SqlExpr.Literal SqlLiteral.Null}]
                  Source=Some(FromJsonEach(SqlExpr.JsonArrayExpr [packet],Some alias));Joins=[];Where=None;GroupBy=[];Having=None;OrderBy=[];Limit=None;Offset=None }
            QueryProduced.publishCore nextName core output false
