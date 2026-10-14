namespace SoloDatabase.Metadata.FSharp

open System
open System.Collections.Generic
open FSharp.Compiler.CodeAnalysis
open FSharp.Compiler.Symbols
open FSharp.Compiler.Syntax
open FSharp.Compiler.Text
open Mono.Cecil
open SoloDatabase.Metadata.Generation
open SoloDatabase.Metadata.Tools

/// Acquire the actual input before the public object parameter erases its type.
module internal RawSqlInputs =
    let collect (moduleDefinition: ModuleDefinition) (mapper: CompilerReferences.References)
                (assemblies: seq<FSharpAssembly>) (uses: FSharpSymbolUse[]) (contains: range -> range -> bool)
                (instantiate: (FSharpGenericParameter * FSharpType) list -> FSharpType -> FSharpType)
                (contexts: FSharpSymbolUse -> (FSharpGenericParameter * FSharpType) list list)
                (selectedArguments: FSharpMemberOrFunctionOrValue -> FSharpSymbolUse -> (FSharpParameter * SynExpr) list)
                (bindings: SynBinding[]) =
        let core = assemblies |> Seq.find(fun assembly -> assembly.SimpleName = "FSharp.Core")
        let read, dispose = ArgumentSourceFacts.create moduleDefinition mapper assemblies
        try
            let result = Dictionary<string,GeneratedArgumentSource>(StringComparer.Ordinal)
            let add source =
                let source: GeneratedArgumentSource = source
                result[GeneratedJsonDefinitions.Identity source.Type] <- source
            let reference (substitutions: (FSharpGenericParameter * FSharpType) list) =
                let rec parameterType (parameter: FSharpGenericParameter) =
                    match substitutions |> List.tryFind(fun (candidate,_) -> candidate.Equals parameter) with
                    | Some(_,typ) when not typ.IsGenericParameter || not(typ.GenericParameter.Equals parameter) -> mapper.Reference parameterType (instantiate substitutions typ)
                    | _ -> invalidOp "An SQL argument source has an open checked generic parameter."
                mapper.Reference parameterType
            let rec head expression =
                match expression with
                | SynExpr.Paren(expr=inner) | SynExpr.TypeApp(expr=inner) -> head inner
                | SynExpr.App(funcExpr=func) -> head func
                | _ -> expression.Range
            let boxed expression =
                let target = head expression
                uses |> Array.tryFind(fun part ->
                    contains target part.Range && target.End = part.Range.End &&
                    match part.Symbol with
                    | :? FSharpMemberOrFunctionOrValue as method ->
                        method.CompiledName = "Box" && (method.DeclaringEntity |> Option.exists(fun entity ->
                            entity.Assembly.QualifiedName = core.QualifiedName && entity.TryFullName = Some "Microsoft.FSharp.Core.Operators"))
                    | _ -> false)
            let rec origins substitutions visited expression =
                let publish typ = read (reference substitutions) (instantiate substitutions typ) |> Option.iter add
                match expression with
                | SynExpr.Paren(expr=inner) | SynExpr.Typed(expr=inner) | SynExpr.Upcast(expr=inner)
                | SynExpr.Downcast(expr=inner) | SynExpr.InferredUpcast(expr=inner) | SynExpr.InferredDowncast(expr=inner) -> origins substitutions visited inner
                | SynExpr.App(funcExpr=func;argExpr=argument) when (boxed func).IsSome ->
                    let part = (boxed func).Value
                    for _,typ in part.GenericArguments do publish typ
                    origins (substitutions @ part.GenericArguments) visited argument
                | SynExpr.AnonRecd(isStruct=isStruct;recordFields=fields) ->
                    let field = fields |> List.tryPick(fun (label,_,_) ->
                        uses |> Array.tryPick(fun part ->
                            match part.Symbol with
                            | :? FSharpField as field when field.IsAnonRecordField && contains label.Range part.Range -> Some field
                            | _ -> None))
                    match field with
                    | Some field ->
                        let details,arguments,_ = field.AnonRecordFieldDetails
                        let arguments = arguments |> Array.map(instantiate substitutions)
                        add(ArgumentSourceFacts.anonymous mapper (reference substitutions) details isStruct arguments)
                    | None -> ()
                | SynExpr.Record(recordFields=fields) ->
                    for SynExprRecordField(fieldName=(label,_)) in fields do
                        for part in uses do
                            if contains label.Range part.Range then
                                match part.Symbol with
                                | :? FSharpField as field ->
                                    field.DeclaringEntity |> Option.iter(fun owner ->
                                        read (reference (substitutions @ part.GenericArguments))
                                            (instantiate (substitutions @ part.GenericArguments) (owner.AsType())) |> Option.iter add)
                                | _ -> ()
                | SynExpr.IfThenElse(thenExpr=yes;elseExpr=no) ->
                    origins substitutions visited yes
                    no |> Option.iter(origins substitutions visited)
                | SynExpr.Sequential(expr2=last) | SynExpr.LetOrUse(body=last) -> origins substitutions visited last
                | SynExpr.Match(clauses=clauses) | SynExpr.MatchBang(clauses=clauses) ->
                    for SynMatchClause(resultExpr=body) in clauses do origins substitutions visited body
                | _ ->
                    let target = head expression
                    for part in uses do
                        if not part.IsFromDefinition && contains target part.Range && target.End = part.Range.End then
                            let actual = substitutions @ part.GenericArguments
                            match part.Symbol with
                            | :? FSharpMemberOrFunctionOrValue as value ->
                                let typ = if value.IsFunction || value.IsMethod then value.ReturnParameter.Type else value.FullType
                                read (reference actual) (instantiate actual typ) |> Option.iter add
                                match (value :> FSharpSymbol).DeclarationLocation with
                                | Some declared when not(List.contains declared visited) ->
                                    for SynBinding(expr=body) as binding in bindings do
                                        if contains binding.RangeOfHeadPattern declared then
                                            origins actual (declared::visited) body
                                            // A source parameter is supplied by the bounded original callers.
                                            for definition in uses do
                                                match definition.Symbol with
                                                | :? FSharpMemberOrFunctionOrValue as owner when definition.IsFromDefinition &&
                                                    (owner.IsFunction || owner.IsMethod) && contains binding.RangeOfHeadPattern definition.Range ->
                                                    for call in uses do
                                                        if not call.IsFromDefinition && call.Symbol.Equals owner then
                                                            for context in contexts call do
                                                                for parameter,input in selectedArguments owner call do
                                                                    if parameter.Name = Some value.DisplayName then origins (context @ call.GenericArguments) (declared::visited) input
                                                | _ -> ()
                                | _ -> ()
                            | :? FSharpField as field when not field.IsAnonRecordField ->
                                publish field.FieldType
                            | :? FSharpEntity as entity -> publish(entity.AsType())
                            | _ -> ()
            for useSite in uses do
                match useSite.Symbol with
                | :? FSharpMemberOrFunctionOrValue as method when not useSite.IsFromDefinition &&
                    (method.DeclaringEntity |> Option.exists(fun owner -> owner.Assembly.SimpleName = "SoloDB")) ->
                    let definition,_ = CompilerReferences.compiledMember method
                    match definition.DeclaringEntity with
                    | Some owner ->
                        for parameter,input in selectedArguments method useSite do
                            let typ = parameter.Type.StripAbbreviations()
                            if typ.HasTypeDefinition && TypeSupport.SqlArgumentParameter(owner.Assembly.SimpleName,
                                owner.TryFullName |> Option.defaultValue owner.BasicQualifiedName,
                                parameter.Name |> Option.defaultValue "",typ.TypeDefinition.TryFullName |> Option.defaultValue "") then
                                for context in contexts useSite do origins (context @ useSite.GenericArguments) [] input
                    | None -> ()
                | _ -> ()
            result.Values |> Seq.toArray
        finally dispose()
