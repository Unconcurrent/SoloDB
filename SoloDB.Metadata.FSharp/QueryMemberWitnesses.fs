namespace SoloDatabase.Metadata.FSharp

open System
open System.Collections.Generic
open Mono.Cecil
open SoloDatabase.Metadata.Tools
open FSharp.Compiler.Symbols

/// Source member witnesses share exact signatures and once-only metadata acquisition.
module internal QueryMemberWitnesses =
    let create (witnesses: IDictionary<string, GeneratedQueryWitness>) =
        let seen = HashSet<string>(StringComparer.Ordinal)
        fun (reference: FSharpType -> TypeReference) (memberValue: FSharpMemberOrFunctionOrValue) ->
            if memberValue.DeclaringEntity.IsSome &&
               (memberValue.IsProperty || memberValue.IsMethod || memberValue.IsFunction || memberValue.IsConstructor) then
                let memberValue, _ = CompilerReferences.compiledMember memberValue
                let entity = memberValue.DeclaringEntity.Value
                let prefix = entity.XmlDocSig.Substring(2) + "."
                let documentation = memberValue.XmlDocSig
                if documentation.Length > 2 && documentation.Substring(2).StartsWith(prefix, StringComparison.Ordinal) then
                    let declaring = reference memberValue.ApparentEnclosingType
                    let signature = documentation.Substring(2 + prefix.Length)
                    let key = declaring.FullName + "," + declaring.Scope.ToString() + "|" + signature
                    if seen.Add key then
                        witnesses[key] <- GeneratedQueryWitness(signature, declaring)
                        // Inline documentation includes source witnesses, not the CLR parameter list.
                        if memberValue.IsFunction then
                            match entity.Assembly.FileName with
                            | Some path ->
                                use input = AssemblyDefinition.ReadAssembly(path)
                                let definition = input.MainModule.GetType(declaring.GetElementType().FullName)
                                if not(isNull definition) then
                                    witnesses.Remove key |> ignore
                                    for method in definition.Methods do
                                        if method.Name = memberValue.CompiledName || method.Name = memberValue.CompiledName + "$W" then
                                            let signature = QueryConstructionWitnesses.MethodSignature method
                                            let key = declaring.FullName + "," + declaring.Scope.ToString() + "|" + signature
                                            witnesses[key] <- GeneratedQueryWitness(signature, declaring)
                            | None -> ()
