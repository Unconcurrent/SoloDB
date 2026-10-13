namespace SoloDatabase.Metadata.FSharp

open System
open System.Collections.Generic
open FSharp.Compiler.Symbols
open Mono.Cecil

/// Compiler-established CLR references shared by every generated operation.
module internal CompilerReferences =
    let create (moduleDefinition: ModuleDefinition) (currentVersion: Version) (coreIdentity: string) =
        let typeReferences = Dictionary<string, TypeReference>(StringComparer.Ordinal)
        let rec referenceWith (parameter: FSharpGenericParameter -> TypeReference) (typ: FSharpType) =
            let typ = typ.StripAbbreviations()
            if typ.IsGenericParameter then parameter typ.GenericParameter
            elif typ.IsFunctionType then
                let root=TypeReference("Microsoft.FSharp.Core","FSharpFunc`2",moduleDefinition,AssemblyNameReference.Parse(coreIdentity))
                root.GenericParameters.Add(GenericParameter("T",root))
                root.GenericParameters.Add(GenericParameter("TResult",root))
                let closed=GenericInstanceType(root)
                for argument in typ.GenericArguments do closed.GenericArguments.Add(referenceWith parameter argument)
                closed :> TypeReference
            elif typ.IsTupleType then
                let arguments = typ.GenericArguments |> Seq.map (referenceWith parameter) |> Seq.toArray
                let standard = AssemblyNameReference.Parse("netstandard, Version=2.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51")
                let rec tupleReference (parts: TypeReference[]) =
                    let arity = min 8 parts.Length
                    let root = TypeReference("System", (if typ.IsStructTupleType then "ValueTuple" else "Tuple") + "`" + string arity, moduleDefinition, standard, typ.IsStructTupleType)
                    for index = 0 to arity - 1 do root.GenericParameters.Add(GenericParameter("T" + string index, root))
                    let closed = GenericInstanceType(root)
                    for index = 0 to min 7 parts.Length - 1 do closed.GenericArguments.Add(parts[index])
                    if parts.Length > 7 then closed.GenericArguments.Add(tupleReference parts[7..])
                    closed :> TypeReference
                tupleReference arguments
            else
                let entity = typ.TypeDefinition
                let identity = entity.Assembly.QualifiedName + ":" + (if entity.IsArrayType then entity.CompiledName else entity.BasicQualifiedName)
                let root =
                    match typeReferences.TryGetValue identity with
                    | true, value -> value
                    | _ ->
                        let scope =
                            if String.IsNullOrEmpty(entity.Assembly.QualifiedName) then AssemblyNameReference(entity.Assembly.SimpleName, currentVersion)
                            else AssemblyNameReference.Parse(entity.Assembly.QualifiedName)
                        let parent =
                            match entity.DeclaringEntity with
                            | Some declaring when not declaring.IsNamespace -> Some(referenceWith parameter (declaring.AsType()))
                            | _ -> None
                        let value = TypeReference(defaultArg entity.Namespace "", entity.CompiledName, moduleDefinition, scope, entity.IsValueType || entity.IsEnum)
                        for parameter in entity.GenericParameters do value.GenericParameters.Add(GenericParameter(parameter.Name,value))
                        match parent with Some declaring -> value.DeclaringType <- declaring | _ -> ()
                        typeReferences.Add(identity, value)
                        value
                if entity.IsByRef then ByReferenceType(referenceWith parameter typ.GenericArguments[0]) :> TypeReference
                elif entity.IsArrayType then
                    if entity.ArrayRank = 1 then ArrayType(referenceWith parameter typ.GenericArguments[0]) :> TypeReference
                    else
                        let array = ArrayType(referenceWith parameter typ.GenericArguments[0], entity.ArrayRank)
                        for index = 0 to array.Dimensions.Count - 1 do array.Dimensions[index] <- ArrayDimension(Nullable 0, Nullable())
                        array :> TypeReference
                elif typ.GenericArguments.Count = 0 then root
                else
                    let closed = GenericInstanceType(root)
                    for argument in typ.GenericArguments do closed.GenericArguments.Add(referenceWith parameter argument)
                    closed :> TypeReference
        referenceWith
