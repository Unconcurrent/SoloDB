using System.Collections.Generic;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace SoloDatabase.Metadata.CSharp;

// Original compiler facts share one binding lifetime. Substitutions and origins
// remain with their contextual traversals, rather than this compilation cache.
internal sealed class QueryFacts
{
    private readonly Dictionary<SyntaxTree,SemanticModel> models = new();
    private readonly Dictionary<SyntaxNode,SymbolInfo> symbols = new();
    private readonly Dictionary<ExpressionSyntax,TypeInfo> types = new();
    private readonly Dictionary<SyntaxNode,IOperation?> operations = new();

    internal QueryFacts(Compilation compilation) => Compilation = compilation;
    internal Compilation Compilation { get; }

    internal SemanticModel Model(SyntaxTree tree)
    {
        if (!models.TryGetValue(tree,out var model)) models.Add(tree,model = Compilation.GetSemanticModel(tree));
        return model;
    }

    internal SymbolInfo Symbol(SyntaxNode syntax)
    {
        if (!symbols.TryGetValue(syntax,out var symbol)) symbols.Add(syntax,symbol = Model(syntax.SyntaxTree).GetSymbolInfo(syntax));
        return symbol;
    }

    internal TypeInfo Type(ExpressionSyntax syntax)
    {
        if (!types.TryGetValue(syntax,out var type)) types.Add(syntax,type = Model(syntax.SyntaxTree).GetTypeInfo(syntax));
        return type;
    }

    internal IOperation? Operation(SyntaxNode syntax)
    {
        if (!operations.TryGetValue(syntax,out var operation)) operations.Add(syntax,operation = Model(syntax.SyntaxTree).GetOperation(syntax));
        return operation;
    }
}
