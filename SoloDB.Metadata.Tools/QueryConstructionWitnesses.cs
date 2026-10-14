using System;
using System.Collections.Generic;
using System.Linq;
using Mono.Cecil;

namespace SoloDatabase.Metadata.Tools;

/// <summary>Metadata needed by the caller's FSharp.Core expression converter.</summary>
public static class QueryConstructionWitnesses
{
    public static GeneratedQueryWitness[] Read(string path, ModuleDefinition output)
    {
        using var core = AssemblyDefinition.ReadAssembly(path);
        var result = new List<GeneratedQueryWitness>();
        // These named lookups run unconditionally in the converter's initializer.
        // Read the referenced definitions so overload and assembly identity stay exact.
        foreach (var ownerName in new[] { "Microsoft.FSharp.Core.ExtraTopLevelOperators", "Microsoft.FSharp.Core.ExtraTopLevelOperators/Checked" })
        {
            var owner = core.MainModule.GetType(ownerName) ?? throw new NotSupportedException("The referenced FSharp.Core has no expression-construction conversion owner: " + ownerName);
            var names = ownerName.EndsWith("/Checked", StringComparison.Ordinal)
                ? new[] { "ToSByte", "ToByte" } : new[] { "ToSByte", "ToByte", "ToDouble", "ToSingle" };
            foreach (var name in names)
            {
                var method = owner.Methods.Single(candidate => candidate.Name == name && candidate.IsPublic);
                result.Add(new GeneratedQueryWitness(MethodSignature(method), output.ImportReference(owner)));
            }
        }
        // Whole query quotations contain these compiler-authored nodes even though
        // token-free lowering consumes them without calling the external converter.
        var quotationOwner = core.MainModule.GetType("Microsoft.FSharp.Linq.RuntimeHelpers.LeafExpressionConverter")
            ?? throw new NotSupportedException("The referenced FSharp.Core has no quotation construction owner.");
        foreach (var name in new[] { "QuotationToLambdaExpression", "SubstHelper" })
        {
            var method = quotationOwner.Methods.Single(candidate => candidate.Name == name && candidate.IsPublic);
            result.Add(new GeneratedQueryWitness(MethodSignature(method), output.ImportReference(quotationOwner)));
        }
        return result.ToArray();
    }

    public static string MethodSignature(MethodDefinition method) =>
        method.Name + (method.HasGenericParameters ? "``" + method.GenericParameters.Count : "") +
        "(" + string.Join(",", method.Parameters.Select(parameter => Signature(parameter.ParameterType))) + ")";

    private static string Signature(TypeReference type)
    {
        if (type is GenericParameter parameter) return (parameter.Type == GenericParameterType.Method ? "``" : "`") + parameter.Position;
        if (type is GenericInstanceType generic)
        {
            var name = generic.ElementType.FullName.Replace('/', '.');
            return System.Text.RegularExpressions.Regex.Replace(name, "`[0-9]+", "") + "{" + string.Join(",", generic.GenericArguments.Select(Signature)) + "}";
        }
        if (type is ByReferenceType reference) return Signature(reference.ElementType) + "@";
        if (type is ArrayType array) return Signature(array.ElementType) + (array.IsVector ? "[]" : "[" + string.Join(",", Enumerable.Repeat("0:", array.Rank)) + "]");
        return type.FullName.Replace('/', '.');
    }
}
