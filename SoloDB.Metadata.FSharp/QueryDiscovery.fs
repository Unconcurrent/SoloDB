namespace SoloDatabase.Metadata.FSharp

open System
open System.Collections.Generic
open System.IO
open FSharp.Compiler.CodeAnalysis
open FSharp.Compiler.Symbols
open FSharp.Compiler.Syntax
open FSharp.Compiler.Text
open Mono.Cecil
open SoloDatabase.Metadata.Tools

/// Checked substitutions own type facts; syntax bounds each supplied query call.
module internal QueryDiscovery =
    let collect (project: FSharpCheckProjectResults) (moduleDefinition: ModuleDefinition) (inputs: IDictionary<string, ParsedInput>) projectDirectory report =
        let file path = Path.GetFullPath(if Path.IsPathRooted(path) then path else Path.Combine(projectDirectory, path))
        let contains (outer: range) (inner: range) =
            file outer.FileName = file inner.FileName && (outer.StartLine, outer.StartColumn) <= (inner.StartLine, inner.StartColumn)
            && (inner.EndLine, inner.EndColumn) <= (outer.EndLine, outer.EndColumn)
        let currentVersion =
            project.AssemblySignature.Attributes
            |> Seq.tryFind(fun attribute -> attribute.AttributeType.FullName = "System.Reflection.AssemblyVersionAttribute")
            |> Option.map(fun attribute -> Version(attribute.ConstructorArguments[0] |> snd |> unbox<string>))
            |> Option.defaultValue(Version(0,0,0,0))
        let referenceWith = CompilerReferences.create moduleDefinition currentVersion (project.ProjectContext.GetReferencedAssemblies() |> Seq.find(fun assembly -> assembly.SimpleName = "FSharp.Core") |> _.QualifiedName)
        let uses = project.GetAllUsesOfAllSymbols()
        let operations = ResizeArray<GeneratedQueryOperation>()
        let calls = Dictionary<string, GeneratedQueryCall>(StringComparer.Ordinal)
        let witnesses = Dictionary<string,GeneratedQueryWitness>(StringComparer.Ordinal)
        let lambdas = Dictionary<string,GeneratedQueryLambda>(StringComparer.Ordinal)
        let seen = HashSet<string>(StringComparer.Ordinal)
        let bound (context: (FSharpGenericParameter * FSharpType) list) (useSite: FSharpSymbolUse) =
            let bindings = Dictionary<FSharpGenericParameter, FSharpType>()
            for parameter,typ in context @ useSite.GenericArguments do bindings[parameter] <- typ
            let rec actual parameter =
                match bindings.TryGetValue parameter with
                | true, typ when not typ.IsGenericParameter || typ.GenericParameter <> parameter -> referenceWith actual typ
                | _ -> invalidOp (sprintf "A query type at %O has no checked use-site substitution: %s." useSite.Range (parameter.Name + " bindings=" + (useSite.GenericArguments |> Seq.map(fun (parameter,typ) -> parameter.Name + "=" + typ.ToString()) |> String.concat ";") + " symbol=" + useSite.Symbol.ToString()))
            referenceWith actual
        let scope firstArgument (useSite: FSharpSymbolUse) =
            let rec supplied expression =
                match expression with
                | SynExpr.Paren(expr = inner) -> supplied inner
                | SynExpr.Tuple(exprs = first :: _) when firstArgument -> supplied first
                | _ -> expression.Range
            let visitor =
                { new SyntaxVisitorBase<range * range>() with
                    override _.VisitExpr(_, _, traverse, expression) =
                        match traverse expression with
                        | Some result -> Some result
                        | None ->
                            match expression with
                            | SynExpr.App(funcExpr = func; argExpr = argument; range = callRange) when contains func.Range useSite.Range -> Some(callRange, supplied argument)
                            | _ -> None }
            match SyntaxTraversal.Traverse(useSite.Range.Start, inputs[file useSite.Range.FileName], visitor) with
            | Some result -> result
            | None -> invalidOp (sprintf "The checked query call at %O has no parsed argument scope." useSite.Range)
        let genericOwners (useSite: FSharpSymbolUse) =
            let visitor =
                { new SyntaxVisitorBase<SynBinding list>() with
                    override _.VisitExpr(path, _, traverse, expression) =
                        match traverse expression with
                        | Some result -> Some result
                        | None when contains expression.Range useSite.Range ->
                            Some(path |> List.choose(function SyntaxNode.SynBinding binding -> Some binding | _ -> None))
                        | None -> None }
            let bindings = SyntaxTraversal.Traverse(useSite.Range.Start, inputs[file useSite.Range.FileName], visitor) |> Option.defaultValue []
            bindings |> List.collect(fun binding ->
                uses |> Array.choose(fun candidate ->
                    match candidate.Symbol with
                    | :? FSharpMemberOrFunctionOrValue as helper when candidate.IsFromDefinition && helper.GenericParameters.Count > 0 &&
                        contains binding.RangeOfHeadPattern candidate.Range -> Some helper
                    | _ -> None) |> Array.toList)
        let rec contexts (visited: Set<string>) (useSite: FSharpSymbolUse) =
            let owners = genericOwners useSite
            ( [ [] ], owners ) ||> List.fold(fun previous owner ->
                let key = owner.FullName + "|" + owner.DeclarationLocation.ToString()
                if visited.Contains key then [] else
                let calls = uses |> Array.filter(fun candidate -> not candidate.IsFromDefinition && candidate.Symbol.Equals(owner))
                [ for call in calls do
                    for parent in contexts (visited.Add key) call do
                        for own in previous do yield parent @ call.GenericArguments @ own ])
        let roots = ResizeArray<FSharpType>()
        let rec closed (typ: FSharpType) =
            not typ.IsGenericParameter && (typ.GenericArguments |> Seq.forall closed)
        let soloDBUse (method: FSharpMemberOrFunctionOrValue) =
            method.DeclaringEntity |> Option.exists(fun entity ->
                entity.Assembly.SimpleName = "SoloDB" &&
                (entity.Namespace |> Option.exists(fun name -> name.StartsWith("SoloDatabase",StringComparison.Ordinal))) &&
                not(entity.CompiledName.Contains("Metadata")))
        for useSite in uses do
            match useSite.Symbol with
            | :? FSharpMemberOrFunctionOrValue as method when not useSite.IsFromDefinition && soloDBUse method ->
                let mutable complete = false
                for context in contexts Set.empty useSite do
                    let substitutions = context @ useSite.GenericArguments
                    let actuals = useSite.GenericArguments |> List.map(fun (_,typ) ->
                        let mutable actual = typ
                        for _ = 0 to substitutions.Length do actual <- actual.Instantiate substitutions
                        actual)
                    if actuals |> List.forall closed then
                        complete <- true
                        for actual in actuals do
                            if not actual.IsFunctionType then roots.Add actual
                if not complete && not useSite.GenericArguments.IsEmpty then
                    report useSite.Range method.FullName
            | _ -> ()
        let sequence (typ: FSharpType) =
            let typ = typ.StripAbbreviations()
            if typ.HasTypeDefinition && typ.GenericArguments.Count = 1 &&
               (typ.TypeDefinition.TryFullName = Some "System.Linq.IQueryable`1" || typ.TypeDefinition.TryFullName = Some "System.Linq.IOrderedQueryable`1")
            then Some typ.GenericArguments[0] else None
        let rec addLambda (reference: FSharpType -> TypeReference) (typ: FSharpType) =
            let typ = typ.StripAbbreviations()
            if typ.HasTypeDefinition then
                match typ.TypeDefinition.TryFullName with
                | Some "System.Linq.Expressions.Expression`1" -> addLambda reference typ.GenericArguments[0]
                | Some name when typ.TypeDefinition.Namespace = Some "System" && typ.TypeDefinition.CompiledName.StartsWith("Func`",StringComparison.Ordinal) ->
                    let actual = reference typ
                    let parameters = typ.GenericArguments |> Seq.take(typ.GenericArguments.Count - 1) |> Seq.map reference |> Seq.toArray
                    let lambda = GeneratedQueryLambda(actual,parameters)
                    lambdas[lambda.Identity] <- lambda
                | _ -> ()
        let rec addParameterFacts (reference: FSharpType -> TypeReference) (typ: FSharpType) =
            let typ = typ.StripAbbreviations()
            if typ.HasTypeDefinition then
                match typ.TypeDefinition.TryFullName with
                | Some "System.Linq.Expressions.Expression`1" -> addParameterFacts reference typ.GenericArguments[0]
                | Some _ when typ.TypeDefinition.Namespace = Some "System" && typ.TypeDefinition.CompiledName.StartsWith("Func`",StringComparison.Ordinal) ->
                    let actual = reference typ
                    let parameters = typ.GenericArguments |> Seq.take(typ.GenericArguments.Count - 1) |> Seq.map reference |> Seq.toArray
                    let lambda = GeneratedQueryLambda(actual,parameters,false)
                    if not(lambdas.ContainsKey lambda.Identity) then lambdas[lambda.Identity] <- lambda
                | _ -> ()
        let add isSequence (source: TypeReference) (arguments: TypeReference) (result: TypeReference) =
            let key = string isSequence + "|" + source.FullName + "," + source.Scope.ToString() + "|" + arguments.FullName + "," + arguments.Scope.ToString() + "|" + result.FullName + "," + result.Scope.ToString()
            if seen.Add key then operations.Add(GeneratedQueryOperation(isSequence, source, arguments, result))
        // Ordinary operators need the same closed provider routes as Compile.
        // Compose only the checked type transitions present in this compilation.
        let typeKey (typ: TypeReference) = typ.FullName + "," + typ.Scope.ToString()
        let knownSources = HashSet<string>(StringComparer.Ordinal)
        for typ in roots do knownSources.Add(typeKey (referenceWith (fun _ -> invalidOp "An inferred model has an open type argument.") typ)) |> ignore
        let rec addAttributedSources entities =
            for entity: FSharpEntity in entities do
                if entity.GenericParameters.Count = 0 &&
                   (entity.Attributes |> Seq.exists(fun attribute -> attribute.AttributeType.TryFullName = Some "SoloDatabase.SoloDBAOTAttribute")) then
                    knownSources.Add(typeKey (referenceWith (fun _ -> invalidOp "A closed model has an open type argument.") (entity.AsType()))) |> ignore
                addAttributedSources entity.NestedEntities
        addAttributedSources project.AssemblySignature.Entities
        let providerTypes = Dictionary<string,FSharpType>(StringComparer.Ordinal)
        let providers = ResizeArray<GeneratedProviderOperation>()
        let providerSeen = HashSet<string>(StringComparer.Ordinal)
        let addProvider isSequence (source: TypeReference) (result: TypeReference) =
            let key = string isSequence + "|" + source.FullName + "," + source.Scope.ToString() + "|" + result.FullName + "," + result.Scope.ToString()
            if providerSeen.Add key then
                providers.Add(GeneratedProviderOperation(isSequence,source,result))
                true
            else false
        for useSite in uses do
            match useSite.Symbol with
            | :? FSharpMemberOrFunctionOrValue as operation when not useSite.IsFromDefinition &&
                (operation.DeclaringEntity |> Option.exists(fun entity -> entity.TryFullName = Some "System.Linq.Queryable")) ->
                for context in contexts Set.empty useSite do
                    let substitutions = context @ useSite.GenericArguments
                    let actual (typ: FSharpType) =
                        let mutable value = typ.StripAbbreviations()
                        for _ = 0 to substitutions.Length do value <- value.Instantiate substitutions
                        value
                    let first = operation.CurriedParameterGroups |> Seq.collect id |> Seq.tryHead
                    match first |> Option.bind(fun parameter -> sequence (actual parameter.Type)) with
                    | Some source when closed source ->
                        let result = actual operation.ReturnParameter.Type
                        if closed result then
                            let reference = bound context useSite
                            for parameter in operation.CurriedParameterGroups |> Seq.collect id do
                                addParameterFacts reference (actual parameter.Type)
                            let sourceReference = reference source
                            providerTypes[typeKey sourceReference] <- source
                            match sequence result with
                            | Some element ->
                                let resultReference = reference element
                                providerTypes[typeKey resultReference] <- element
                                addProvider true sourceReference resultReference |> ignore
                            | None ->
                                let resultReference = reference result
                                providerTypes[typeKey resultReference] <- result
                                addProvider false sourceReference resultReference |> ignore
                    | _ -> ()
            | _ -> ()
        let directProviders = providers.ToArray()
        providers.Clear()
        providerSeen.Clear()
        for operation in directProviders do
            if knownSources.Contains(typeKey operation.Source) then
                addProvider operation.Sequence operation.Source operation.Result |> ignore
        let sameType (left: TypeReference) (right: TypeReference) =
            left.FullName = right.FullName && left.Scope.ToString() = right.Scope.ToString()
        let mutable providerIndex = 0
        while providerIndex < providers.Count do
            let previous = providers[providerIndex]
            if previous.Sequence then
                for next in directProviders do
                    if sameType previous.Result next.Source then
                        addProvider next.Sequence previous.Source next.Result |> ignore
            providerIndex <- providerIndex + 1
        for operation in providers do
            roots.Add providerTypes[typeKey operation.Result]
        let addMemberWitness = QueryMemberWitnesses.create witnesses
        let addWitness context (part: FSharpSymbolUse) =
            match part.Symbol with
            | :? FSharpMemberOrFunctionOrValue as memberValue when not part.IsFromDefinition ->
                addMemberWitness (bound context part) memberValue
            | _ -> ()
        let sites =
            uses |> Array.choose(fun useSite ->
                match useSite.Symbol with
                | :? FSharpMemberOrFunctionOrValue as method when (method.CompiledName = "Compile" || method.CompiledName = "CompileQuotation")
                    && (method.DeclaringEntity |> Option.exists(fun entity -> entity.Namespace = Some "SoloDatabase")) ->
                    let callScope, argumentScope = scope true useSite
                    Some(useSite, method, callScope, argumentScope)
                | _ -> None)
        for useSite, method, callScope, argumentScope in sites do
          for context in contexts Set.empty useSite do
            let reference = bound context useSite
            let query = method.CurriedParameterGroups |> Seq.collect id |> Seq.head |> fun argument -> argument.Type.StripAbbreviations()
            let callable = query.GenericArguments[0].StripAbbreviations()
            let callableArguments =
                if method.CompiledName = "CompileQuotation" then
                    let parameters = ResizeArray<FSharpType>()
                    let mutable remaining = callable
                    // The declaration determines arity; a function-valued result is not another argument.
                    for _ = 1 to method.GenericParameters.Count do
                        if not remaining.IsFunctionType then invalidOp "A quotation query declaration must supply a curried function."
                        parameters.Add remaining.GenericArguments[0]
                        remaining <- remaining.GenericArguments[1].StripAbbreviations()
                    parameters.Add remaining
                    let signature = parameters |> Seq.map reference |> Seq.toArray
                    let delegateType = GenericInstanceType(TypeReference("System", "Func`" + string signature.Length, moduleDefinition,
                        AssemblyNameReference.Parse("netstandard, Version=2.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51")))
                    for argument in signature do delegateType.GenericArguments.Add argument
                    let lambda = GeneratedQueryLambda(delegateType,signature[..signature.Length-2])
                    lambdas[lambda.Identity] <- lambda
                    parameters.ToArray()
                else
                    addLambda reference callable
                    callable.GenericArguments |> Seq.toArray
            let querySource = callableArguments[0].StripAbbreviations()
            let source = reference querySource.GenericArguments[0]
            let actualArguments =
                callableArguments |> Seq.skip 1 |> Seq.take (callableArguments.Length - 2)
                |> Seq.map reference |> Seq.toArray
            let arguments =
                if actualArguments.Length = 1 then actualArguments[0]
                elif actualArguments.Length = 0 then
                    let core = project.ProjectContext.GetReferencedAssemblies() |> Seq.find(fun assembly -> assembly.SimpleName = "FSharp.Core")
                    TypeReference("Microsoft.FSharp.Core", "Unit", moduleDefinition, AssemblyNameReference.Parse(core.QualifiedName))
                else
                    let root = TypeReference("System", "ValueTuple`" + string actualArguments.Length, moduleDefinition,
                        AssemblyNameReference.Parse("netstandard, Version=2.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51"), true)
                    let closed = GenericInstanceType(root)
                    for typ in actualArguments do closed.GenericArguments.Add typ
                    closed :> TypeReference
            for part in uses do
                if contains argumentScope part.Range && not (sites |> Array.exists(fun (_,_,other,_) -> other <> callScope && contains callScope other && contains other part.Range)) then
                    match part.Symbol with
                    | :? FSharpMemberOrFunctionOrValue as operation when not part.IsFromDefinition && operation.IsMethod ->
                        let reference = bound context part
                        let substitutions = context @ useSite.GenericArguments @ part.GenericArguments
                        for parameter in operation.CurriedParameterGroups |> Seq.collect id do addLambda reference parameter.Type
                        for typ in seq { yield operation.ReturnParameter.Type
                                         for parameter in operation.CurriedParameterGroups |> Seq.collect id do yield parameter.Type } do
                            let mutable actual = typ.StripAbbreviations()
                            for _ = 0 to substitutions.Length do actual <- actual.Instantiate substitutions
                            if closed actual && actual.HasTypeDefinition && (sequence actual |> Option.isNone) then
                                let element =
                                    if actual.TypeDefinition.IsArrayType then Some actual.GenericArguments[0]
                                    else
                                        seq { yield actual; yield! actual.AllInterfaces }
                                        |> Seq.tryFind(fun candidate ->
                                            candidate.HasTypeDefinition &&
                                            candidate.TypeDefinition.TryFullName = Some "System.Collections.Generic.IEnumerable`1")
                                        |> Option.map(fun contract -> contract.GenericArguments[0])
                                match element with
                                | Some item when actual.TypeDefinition.TryFullName <> Some "System.String" ->
                                    roots.Add actual
                                    add true source arguments (reference item)
                                | _ -> ()
                    | _ -> ()
                    match part.Symbol with
                    | :? FSharpMemberOrFunctionOrValue as value when not value.IsMethod ->
                        let substitutions = context @ useSite.GenericArguments @ part.GenericArguments
                        let mutable actual = value.FullType.StripAbbreviations()
                        for _ = 0 to substitutions.Length do actual <- actual.Instantiate substitutions
                        if closed actual && not actual.IsFunctionType && (actual.IsTupleType || actual.HasTypeDefinition) && (sequence actual |> Option.isNone) then roots.Add actual
                        if closed actual && actual.IsFunctionType then
                            let reference = bound context part
                            let declaring = reference actual
                            let definition = (declaring :?> GenericInstanceType).ElementType
                            let method = MethodReference("Invoke",definition.GenericParameters[1],declaring,HasThis=true)
                            method.Parameters.Add(ParameterDefinition(definition.GenericParameters[0]))
                            let argument,result = reference actual.GenericArguments[0],reference actual.GenericArguments[1]
                            let key=declaring.FullName+"|Invoke"
                            calls[key] <- GeneratedQueryCall(method,result,[|argument|])

                    | _ -> ()
                    addWitness context part
                    match part.Symbol with
                    | :? FSharpMemberOrFunctionOrValue as operation when
                        operation.DeclaringEntity |> Option.exists(fun entity -> entity.TryFullName = Some "System.Linq.Queryable") ->
                        let result = operation.ReturnParameter.Type.StripAbbreviations()
                        let reference = bound context part
                        match sequence result with
                        | Some element -> add true source arguments (reference element)
                        | None -> add false source arguments (reference result)
                    | :? FSharpMemberOrFunctionOrValue as operation when operation.IsConstructor && not part.IsFromDefinition ->
                        let reference=bound context part
                        let declaring=reference operation.ApparentEnclosingType
                        let target=MethodReference(".ctor",moduleDefinition.TypeSystem.Void,declaring,HasThis=true)
                        let definition=match declaring with :? GenericInstanceType as closed -> closed.ElementType | _ -> declaring
                        let signature=referenceWith(fun parameter ->
                            let index=operation.DeclaringEntity.Value.GenericParameters |> Seq.findIndex(fun candidate -> candidate.Equals parameter)
                            definition.GenericParameters[index] :> TypeReference)
                        let parameters=operation.CurriedParameterGroups |> Seq.collect id |> Seq.map _.Type
                                       |> Seq.filter(fun typ -> let actual=typ.StripAbbreviations() in not(actual.HasTypeDefinition && actual.TypeDefinition.TryFullName = Some "Microsoft.FSharp.Core.Unit")) |> Seq.toArray
                        for parameter in parameters do target.Parameters.Add(ParameterDefinition(signature parameter))
                        let arguments=parameters |> Array.map reference
                        calls[target.FullName] <- GeneratedQueryCall(target,declaring,arguments)
                    | :? FSharpMemberOrFunctionOrValue as operation when (operation.IsMethod || operation.IsFunction) && not part.IsFromDefinition &&
                        operation.DeclaringEntity.IsSome && not operation.IsConstructor ->
                        let reference = bound context part
                        let target = MethodReference(operation.CompiledName, moduleDefinition.TypeSystem.Void,
                                                     reference operation.ApparentEnclosingType, HasThis = operation.IsInstanceMemberInCompiledCode)
                        for parameter in operation.GenericParameters do target.GenericParameters.Add(GenericParameter(parameter.Name,target))
                        let signature = referenceWith(fun parameter ->
                            match operation.GenericParameters |> Seq.tryFindIndex(fun candidate -> candidate.Equals parameter) with
                            | Some index -> target.GenericParameters[index] :> TypeReference
                            | None ->
                                context @ useSite.GenericArguments @ part.GenericArguments
                                |> List.find(fun (candidate,_) -> candidate.Equals parameter)
                                |> snd |> reference)
                        target.ReturnType <- signature operation.ReturnParameter.Type
                        let parameters = operation.CurriedParameterGroups |> Seq.collect id |> Seq.map _.Type |> Seq.toArray
                        for parameter in parameters do target.Parameters.Add(ParameterDefinition(signature parameter))
                        let method =
                            if operation.GenericParameters.Count = 0 then target else
                            let closed = GenericInstanceMethod(target)
                            for parameter in operation.GenericParameters do
                                closed.GenericArguments.Add(reference (context @ useSite.GenericArguments @ part.GenericArguments |> List.find(fun (candidate,_) -> candidate.Equals parameter) |> snd))
                            closed :> MethodReference
                        let result = reference operation.ReturnParameter.Type
                        let arguments = parameters |> Array.map reference
                        let key = method.FullName + "|" + result.FullName + "|" + String.Join(";",arguments |> Array.map _.FullName)
                        calls[key] <- GeneratedQueryCall(method,result,arguments)
                    | _ -> ()
        // Ordinary Queryable arguments also construct quotations before SoloDB receives them.
        for useSite in uses do
            match useSite.Symbol with
            | :? FSharpMemberOrFunctionOrValue as operation when not useSite.IsFromDefinition &&
                (operation.DeclaringEntity |> Option.exists(fun entity -> entity.TryFullName = Some "System.Linq.Queryable")) ->
                let _, argumentScope = scope false useSite
                for context in contexts Set.empty useSite do
                    for part in uses do
                        if contains argumentScope part.Range then addWitness context part
            | _ -> ()
        operations.ToArray(), (calls.Values |> Seq.toArray), (witnesses.Values |> Seq.toArray), (lambdas.Values |> Seq.toArray), roots.ToArray(), providers.ToArray()
