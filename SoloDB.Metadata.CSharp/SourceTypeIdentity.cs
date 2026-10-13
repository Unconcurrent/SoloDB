using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;

namespace SoloDatabase.Metadata.CSharp;

// Build-only structural identities never name inaccessible types in generated CSharp.
internal static class SourceTypeIdentity
{
    internal static bool Accessible(Compilation compilation, ITypeSymbol type) => type switch {
        IArrayTypeSymbol array => Accessible(compilation,array.ElementType),
        INamedTypeSymbol { IsAnonymousType: true } anonymous => anonymous.GetMembers().OfType<IPropertySymbol>().All(property => Accessible(compilation,property.Type)),
        INamedTypeSymbol named => compilation.IsSymbolAccessibleWithin(named,compilation.Assembly) &&
            (named.ContainingType == null || Accessible(compilation,named.ContainingType)) &&
            named.TypeArguments.All(argument => Accessible(compilation,argument)),
        ITypeParameterSymbol => false,
        _ => true
    };

    internal static bool Closed(ITypeSymbol type) => type switch {
        IArrayTypeSymbol array => Closed(array.ElementType),
        INamedTypeSymbol named => !named.IsRefLikeType && (named.ContainingType == null || Closed(named.ContainingType)) && named.TypeArguments.All(Closed),
        _ => false
    };

    internal static string Encode(ITypeSymbol type)
    {
        string Text(string value) => value.Length + ":" + value;
        if (type is IArrayTypeSymbol array) return "A" + array.Rank + ":" + Encode(array.ElementType);
        if (type is not INamedTypeSymbol named) throw new InvalidOperationException("A generated type identity must be closed: " + type);
        if (named.IsAnonymousType)
        {
            var properties = named.GetMembers().OfType<IPropertySymbol>().ToArray();
            return "O" + properties.Length + ":" + string.Concat(properties.Select(property => Text(property.Name) + Encode(property.Type)));
        }
        var chain = new Stack<INamedTypeSymbol>();
        for (var current = named; current != null; current = current.ContainingType) chain.Push(current);
        var name = (named.ContainingNamespace.IsGlobalNamespace ? "" : named.ContainingNamespace.ToDisplayString() + ".") +
            string.Join("/",chain.Select(item => item.MetadataName));
        var arguments = chain.SelectMany(item => item.TypeArguments).ToArray();
        return "N" + Text(named.ContainingAssembly.Identity.GetDisplayName()) + Text(name) + arguments.Length + ":" +
            string.Concat(arguments.Select(Encode));
    }
}
