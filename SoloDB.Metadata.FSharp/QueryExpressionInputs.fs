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

    let create (inputs: IDictionary<string,ParsedInput>) file contains (uses: FSharpSymbolUse[]) =
        let bindings = ResizeArray<SynBinding>()
        let lambdas = ResizeArray<range>()
        for input in inputs.Values do
            ParsedInput.fold (fun () _ node ->
                match node with
                | SyntaxNode.SynBinding binding -> bindings.Add binding
                | SyntaxNode.SynExpr(SynExpr.Lambda(range = location)) -> lambdas.Add location
                | _ -> ()) () input

        let arguments (useSite: FSharpSymbolUse) =
            let rec groups expression =
                match expression with
                | SynExpr.Paren(expr = inner) -> groups inner
                | SynExpr.App(funcExpr = func; argExpr = argument) when contains func.Range useSite.Range ->
                    groups func @ [argument]
                | _ -> []
            let visitor =
                { new SyntaxVisitorBase<range * SynExpr list>() with
                    override _.VisitExpr(_, _, traverse, expression) =
                        match expression with
                        | SynExpr.App(funcExpr = func) when contains func.Range useSite.Range ->
                            Some(expression.Range, groups expression)
                        | _ -> traverse expression }
            SyntaxTraversal.Traverse(useSite.Range.Start, inputs[file useSite.Range.FileName], visitor)

        let rec items expression =
            match expression with
            | SynExpr.Paren(expr = inner) -> items inner
            | SynExpr.Tuple(exprs = values) -> values
            | _ -> [expression]

        let selected (method: FSharpMemberOrFunctionOrValue) useSite =
            match arguments useSite with
            | None -> []
            | Some(_, groups) ->
                [ for index, argument in groups |> List.indexed do
                    if index < method.CurriedParameterGroups.Count then
                        let parameters = method.CurriedParameterGroups[index]
                        for itemIndex, item in items argument |> List.indexed do
                            if parameters.Count > 0 &&
                               isExpression parameters[min itemIndex (parameters.Count - 1)].Type then
                                yield item.Range ]

        let rec origins visited location =
            [ yield! lambdas |> Seq.filter(contains location)
              for part in uses do
                if contains location part.Range && not part.IsFromDefinition then
                    match part.Symbol with
                    | :? FSharpMemberOrFunctionOrValue as value when
                        isExpression value.FullType ||
                        value.FullType.IsFunctionType && isExpression value.ReturnParameter.Type ->
                        match (value :> FSharpSymbol).DeclarationLocation with
                        | Some declared when not (visited |> List.exists((=) declared)) ->
                            for binding in bindings do
                                if contains binding.RangeOfHeadPattern declared then
                                    let (SynBinding(expr = body)) = binding
                                    yield! origins (declared :: visited) body.Range
                                    // A parameter binding has no initializer: follow the actual source calls.
                                    for definition in uses do
                                        match definition.Symbol with
                                        | :? FSharpMemberOrFunctionOrValue as owner when definition.IsFromDefinition &&
                                            (owner.IsFunction || owner.IsMethod) && contains binding.RangeOfHeadPattern definition.Range ->
                                            for call in uses do
                                                if not call.IsFromDefinition && call.Symbol.Equals owner then
                                                    for input in selected owner call do
                                                        yield! origins (declared :: visited) input
                                        | _ -> ()
                        | _ -> ()
                    | _ -> () ]
            |> List.distinct

        arguments, (fun method useSite -> selected method useSite |> List.collect(origins []))
