namespace SoloDatabase.Metadata.FSharp

open System.Collections.Generic
open FSharp.Compiler.CodeAnalysis
open FSharp.Compiler.Symbols
open FSharp.Compiler.Syntax
open FSharp.Compiler.Text

/// Parsed bodies are selected by checked expression contracts and original binding identities.
module internal QueryExpressionInputs =
    let rec isExpression (typ: FSharpType) =
        let typ = typ.StripAbbreviations()
        typ.HasTypeDefinition &&
        (typ.TypeDefinition.TryFullName = Some "System.Linq.Expressions.Expression`1" ||
         typ.TypeDefinition.IsArrayType && isExpression typ.GenericArguments[0])

    let create (inputs: IDictionary<string,ParsedInput>) file contains (uses: FSharpSymbolUse[]) instantiate contexts =
        let bindings = ResizeArray<SynBinding>()
        let assignments = ResizeArray<range * SynExpr>()
        let lambdas = ResizeArray<range>()
        for input in inputs.Values do
            ParsedInput.fold (fun () _ node ->
                match node with
                | SyntaxNode.SynBinding binding -> bindings.Add binding
                | SyntaxNode.SynExpr(SynExpr.LongIdentSet(longDotId=target;expr=value)) -> assignments.Add(target.Range,value)
                | SyntaxNode.SynExpr(SynExpr.DotSet(longDotId=target;rhsExpr=value)) -> assignments.Add(target.Range,value)
                | SyntaxNode.SynExpr(SynExpr.Set(targetExpr=target;rhsExpr=value)) -> assignments.Add(target.Range,value)
                | SyntaxNode.SynExpr(SynExpr.Record(recordFields=fields)) ->
                    for SynExprRecordField(fieldName=(target,_);expr=value) in fields do
                        value |> Option.iter(fun value -> assignments.Add(target.Range,value))
                | SyntaxNode.SynExpr(SynExpr.Lambda(range = location)) -> lambdas.Add location
                | _ -> ()) () input

        let arguments (useSite: FSharpSymbolUse) =
            let rec head expression =
                match expression with
                | SynExpr.Paren(expr = inner) | SynExpr.TypeApp(expr = inner) -> head inner
                | SynExpr.App(funcExpr = func) -> head func
                | _ -> expression.Range
            let called expression =
                let target = head expression
                contains target useSite.Range && target.End = useSite.Range.End
            let rec groups expression =
                match expression with
                | SynExpr.Paren(expr = inner) -> groups inner
                | SynExpr.App(funcExpr = func; argExpr = argument) when called func ->
                    groups func @ [argument]
                | _ -> []
            let visitor =
                { new SyntaxVisitorBase<range * SynExpr list>() with
                    override _.VisitExpr(_, _, traverse, expression) =
                        match expression with
                        | SynExpr.App(funcExpr = func) when called func ->
                            Some(expression.Range, groups expression)
                        | _ -> traverse expression }
            SyntaxTraversal.Traverse(useSite.Range.Start, inputs[file useSite.Range.FileName], visitor)

        let rec items expression =
            match expression with
            | SynExpr.Paren(expr = inner) -> items inner
            | SynExpr.Tuple(exprs = values) -> values
            | _ -> [expression]

        let rec named expression =
            match expression with
            | SynExpr.Paren(expr = inner) -> named inner
            | SynExpr.App(funcExpr = SynExpr.App(funcExpr = SynExpr.LongIdent(longDotId = operation); argExpr = SynExpr.Ident name); argExpr = value)
                when operation.LongIdent |> List.map _.idText = ["op_Equality"] -> Some(name.idText,value)
            | _ -> None

        let selected (method: FSharpMemberOrFunctionOrValue) (useSite: FSharpSymbolUse) =
            match arguments useSite with
            | None -> []
            | Some(_, groups) ->
                [ for index, argument in groups |> List.indexed do
                    if index < method.CurriedParameterGroups.Count then
                        let parameters = method.CurriedParameterGroups[index]
                        for itemIndex, item in items argument |> List.indexed do
                            let selected =
                                match named item with
                                | Some(name,value) -> parameters |> Seq.tryFind(fun parameter -> parameter.Name = Some name) |> Option.map(fun parameter -> parameter,value)
                                | None when itemIndex < parameters.Count -> Some(parameters[itemIndex],item)
                                | None when parameters.Count > 0 && parameters[parameters.Count - 1].IsParamArrayArg -> Some(parameters[parameters.Count - 1],item)
                                | None -> None
                            match selected with
                            | Some(parameter,value) -> yield parameter,value
                            | _ -> () ]

        let ends = uses |> Array.filter(fun part -> not part.IsFromDefinition)
                   |> Array.groupBy(fun part -> struct(file part.Range.FileName,part.Range.EndLine,part.Range.EndColumn)) |> dict
        let writes = Dictionary<FSharpSymbol,ResizeArray<SynExpr>>()
        for target,value in assignments do
            match ends.TryGetValue(struct(file target.FileName,target.EndLine,target.EndColumn)) with
            | true, parts ->
                for part in parts do
                    if contains target part.Range then
                        match writes.TryGetValue part.Symbol with
                        | true, values -> values.Add value
                        | _ -> writes.Add(part.Symbol,ResizeArray([value]))
            | _ -> ()

        let producers context (useSite: FSharpSymbolUse) =
            let symbol = useSite.Symbol
            let associated =
                [ yield symbol
                  match symbol with
                  | :? FSharpMemberOrFunctionOrValue as value when value.IsProperty ->
                      if value.HasGetterMethod then yield value.GetterMethod
                      if value.HasSetterMethod then yield value.SetterMethod
                  | _ -> () ]
            [ for original in associated do
                match writes.TryGetValue original with
                | true, values -> for value in values do yield context,value
                | _ -> ()
              match symbol.DeclarationLocation with
              | Some declared ->
                for SynBinding(expr=body) as binding in bindings do
                    if contains binding.RangeOfHeadPattern declared then
                        yield context,body
                        match symbol with
                        | :? FSharpMemberOrFunctionOrValue as parameter ->
                            for definition in uses do
                                match definition.Symbol with
                                | :? FSharpMemberOrFunctionOrValue as owner when definition.IsFromDefinition &&
                                    (owner.IsFunction || owner.IsMethod) && contains binding.RangeOfHeadPattern definition.Range ->
                                    for call in uses do
                                        if not call.IsFromDefinition && call.Symbol.Equals owner then
                                            for actual in contexts call do
                                                for input,value in selected owner call do
                                                    if input.Name = Some parameter.DisplayName then yield actual @ call.GenericArguments,value
                                | _ -> ()
                        | _ -> ()
              | None -> () ]

        let expressionArguments context method useSite =
            selected method useSite |> List.choose(fun (parameter,value) ->
                if isExpression (instantiate (context @ useSite.GenericArguments) parameter.Type) then Some value.Range else None)

        let rec origins context visited location =
            [ yield! lambdas |> Seq.filter(contains location)
              for part in uses do
                if contains location part.Range && not part.IsFromDefinition then
                    match part.Symbol with
                    | :? FSharpMemberOrFunctionOrValue as value when
                        isExpression (instantiate (context @ part.GenericArguments) value.FullType) ||
                        value.FullType.IsFunctionType && isExpression (instantiate (context @ part.GenericArguments) value.ReturnParameter.Type) ->
                        match (value :> FSharpSymbol).DeclarationLocation with
                        | Some declared when not (visited |> List.exists((=) declared)) ->
                            for actual,body in producers (context @ part.GenericArguments) part do
                                yield! origins actual (declared :: visited) body.Range
                        | _ -> ()
                    | _ -> () ]
            |> List.distinct

        arguments, (fun context method useSite -> expressionArguments context method useSite |> List.collect(origins (context @ useSite.GenericArguments) [])), selected, producers
