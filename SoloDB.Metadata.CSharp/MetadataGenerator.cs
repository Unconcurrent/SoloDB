using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.CSharp;

[Generator(LanguageNames.CSharp)]
public sealed class MetadataGenerator : IIncrementalGenerator
{
    private static readonly DiagnosticDescriptor Unsupported = new("SOLO001", "Generated metadata shape is unavailable",
        "Cannot generate metadata for '{0}': {1}", "SoloDB", DiagnosticSeverity.Error, true);

    private static readonly DiagnosticDescriptor InferenceUnavailable = new("SOLO003", "Generated coverage cannot be inferred",
        "Cannot infer closed types for SoloDB use '{0}'. Supply explicit SoloDBAOT model roots and a closed query signature for any runtime-selected use.", "SoloDB", DiagnosticSeverity.Warning, true);

    private static readonly DiagnosticDescriptor InferenceLimited = new("SOLO003", "Generated coverage cannot be inferred",
        "Generated coverage inference through '{0}' stopped after ten recursive re-entries. Deeper coverage was not inferred; supply explicit SoloDBAOT model roots and closed query signatures for those uses.", "SoloDB", DiagnosticSeverity.Warning, true);

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var contexts = context.SyntaxProvider.ForAttributeWithMetadataName(
            "SoloDatabase.SoloDBAOTAttribute",
            static (node, _) => node is TypeDeclarationSyntax,
            static (ctx, _) => (INamedTypeSymbol)ctx.TargetSymbol).Collect();
        var helper = context.AnalyzerConfigOptionsProvider.Select(static (options, _) =>
            options.GlobalOptions.TryGetValue("build_property.SoloDBGeneratedHelper", out var enabled) && enabled == "true");
        context.RegisterSourceOutput(contexts.Combine(context.CompilationProvider).Combine(helper),
            static (ctx, input) => Generate(ctx, input.Left.Right, input.Left.Left, input.Right));
    }

    private static void Generate(SourceProductionContext context, Compilation compilation, ImmutableArray<INamedTypeSymbol> contexts, bool helper)
    {
        const string surface = "internal static class SoloDBAOT { public static void Register() {} }";
        var existing = compilation.Assembly.GlobalNamespace.GetTypeMembers("SoloDBAOT");
        if (existing.Length != 0)
        {
            context.ReportDiagnostic(Diagnostic.Create(Unsupported,existing[0].Locations.FirstOrDefault(),existing[0].ToDisplayString(),
                "the project-local SoloDBAOT registration entry conflicts with this declaration"));
            return;
        }
        var candidates = compilation.SyntaxTrees.SelectMany(tree => tree.GetRoot().DescendantNodes().OfType<InvocationExpressionSyntax>())
            .Where(call => call.Expression is MemberAccessExpressionSyntax member && member.Name.Identifier.ValueText == "Register" ||
                call.Expression is IdentifierNameSyntax name && name.Identifier.ValueText == "Register").ToArray();
        var active = false;
        if (candidates.Length != 0 && compilation is CSharpCompilation activation)
        {
            compilation = activation.AddSyntaxTrees(CSharpSyntaxTree.ParseText(surface,(CSharpParseOptions?)compilation.SyntaxTrees.FirstOrDefault()?.Options));
            var entry = compilation.Assembly.GlobalNamespace.GetTypeMembers("SoloDBAOT").Single();
            active = candidates.Any(call => compilation.GetSemanticModel(call.SyntaxTree).GetSymbolInfo(call).Symbol is IMethodSymbol method &&
                SymbolEqualityComparer.Default.Equals(method.ContainingType,entry) && method.Name == "Register" && method.Parameters.Length == 0);
        }
        if (!active)
        {
            context.AddSource("SoloDBAOT.g.cs",surface);
            return;
        }
        if (!helper)
        {
            context.ReportDiagnostic(Diagnostic.Create(Unsupported,candidates[0].GetLocation(),"SoloDBAOT.Register",
                "the SoloDB package build targets must be imported"));
            return;
        }
        // Public properties can have private accessors in referenced models. The
        // normal compiler import policy hides those symbols; semantic generation
        // needs the complete metadata of these exact references.
        if (compilation is CSharpCompilation csharp)
        {
            compilation = csharp.WithOptions(csharp.Options.WithMetadataImportOptions(MetadataImportOptions.All));
            contexts = contexts.Select(target =>
            {
                var declaration = target.DeclaringSyntaxReferences[0].GetSyntax();
                return (INamedTypeSymbol)compilation.GetSemanticModel(declaration.SyntaxTree).GetDeclaredSymbol(declaration)!;
            }).ToImmutableArray();
        }
        if (helper)
            context.AddSource("OperationRegistration.g.cs", """
                // <auto-generated/>
                [assembly: global::SoloDatabase.Generated.MetadataActivation]
                namespace SoloDatabase.Generated {
                    [global::System.AttributeUsage(global::System.AttributeTargets.Assembly)]
                    internal sealed class MetadataActivationAttribute : global::System.Attribute { }

                    [global::System.AttributeUsage(global::System.AttributeTargets.Assembly, AllowMultiple = true)]
                    internal sealed class CodecRegistrationAttribute : global::System.Attribute {
                        public string[] AssignableTo { get; set; }
                        public bool HelperOnly { get; set; }
                        public string Element { get; set; }
                        public string SequenceElement { get; set; }
                        public bool SupportsValuesWrapper { get; set; }
                        public bool[] References { get; set; }
                        public bool[] HasPolicies { get; set; }
                        public int[] OnDelete { get; set; }
                        public int[] OnOwnerDelete { get; set; }
                        public bool[] Unique { get; set; }
                        public int[] OrderBy { get; set; }
                        public CodecRegistrationAttribute(string model, bool sealedType, bool abstractType, bool constructFromMembers,
                            string[] names, string[] declaringTypes, string[] valueTypes,
                            bool[] fields, bool[] read, bool[] write, bool[] ignored, bool[] many, int shape, string[] collection, bool populate) { }
                        public bool NonGenericEnumerable { get; set; }
                        public bool DictionaryReadOnly { get; set; }
                        public bool Formattable { get; set; }
                        public string[] RecordFields { get; set; }
                        public bool UnionType { get; set; }
                        public string IndexedRead { get; set; }
                        public string IndexedWrite { get; set; }
                        public CodecRegistrationAttribute(global::System.Type model, bool sealedType, bool abstractType, bool constructFromMembers,
                            string[] names, global::System.Type[] declaringTypes, global::System.Type[] valueTypes,
                            bool[] fields, bool[] read, bool[] write, bool[] ignored, bool[] many, int shape, global::System.Type[] collection, bool populate) { }
                    }
                    [global::System.AttributeUsage(global::System.AttributeTargets.Assembly, AllowMultiple = true)]
                    internal sealed class SourceQueryRegistrationAttribute : global::System.Attribute {
                        public SourceQueryRegistrationAttribute(string method, string[] types) { }
                    }
                    [global::System.AttributeUsage(global::System.AttributeTargets.Assembly, AllowMultiple = true)]
                    internal sealed class SourceQueryOperationRegistrationAttribute : global::System.Attribute {
                        public SourceQueryOperationRegistrationAttribute(int kind, string owner, string name, string[] genericTypes, string[] argumentTypes) { }
                    }
                    [global::System.AttributeUsage(global::System.AttributeTargets.Assembly, AllowMultiple = true)]
                    internal sealed class QueryOperationRegistrationAttribute : global::System.Attribute {
                        public QueryOperationRegistrationAttribute(int kind, global::System.Type owner, string name,
                            global::System.Type[] genericTypes, global::System.Type[] argumentTypes, string registrationType, string entry) { }
                    }
                }
                """);
        var sites = QueryRegistrations.Analyze(compilation,(location,method) =>
            context.ReportDiagnostic(Diagnostic.Create(InferenceUnavailable,location,method)),(location,method) =>
            context.ReportDiagnostic(Diagnostic.Create(InferenceLimited,location,method)));
        var selectedRoots = contexts.Distinct(SymbolEqualityComparer.Default).Cast<INamedTypeSymbol>()
            .SelectMany(type => type.GetAttributes().Where(attribute => attribute.AttributeClass?.ToDisplayString() == "SoloDatabase.SoloDBAOTAttribute")
                .Select(attribute => attribute.ConstructorArguments.Length == 0 ? (ITypeSymbol)type : attribute.ConstructorArguments[0].Value as ITypeSymbol))
            .Where(type => type != null).Cast<ITypeSymbol>().ToArray();
        var roots = selectedRoots.Concat(QueryRegistrations.InferredRoots(compilation,sites,selectedRoots));
        var models = DescribeRoots(context,compilation,roots);
        const string registrationType = "MetadataOperations";
        const string queryRegistration = "MetadataOperationsQueries";
        var queryCode = QueryRegistrations.Collect(compilation,models.Select(model => model.TypeName),sites,queryRegistration);
        context.AddSource(queryRegistration + ".g.cs","// <auto-generated/>\nextern alias SoloDBGeneratedOperations;\n" + queryCode.Descriptors + "namespace SoloDatabase.Generated { internal static class " +
            queryRegistration + " {\n" + queryCode.Helpers + "}}\n");
        context.AddSource(registrationType + ".g.cs","extern alias SoloDBGeneratedOperations;\n" +
            CSharpEmitter.Descriptors(models,registrationType));
        context.AddSource("SoloDBAOT.g.cs", """
            // <auto-generated/>
            extern alias SoloDBGeneratedOperations;
            internal static class SoloDBAOT {
                private static readonly object gate = new object();
                private static bool complete;
                public static void Register() {
                    lock (gate) {
                        if (complete) return;
                        SoloDBGeneratedOperations::SoloDatabase.Generated.Registration.Register();
                        complete = true;
                    }
                }
            }
            """);
    }

    private static readonly SymbolDisplayFormat TypeDisplay = SymbolDisplayFormat.FullyQualifiedFormat.WithMiscellaneousOptions(
        SymbolDisplayFormat.FullyQualifiedFormat.MiscellaneousOptions | SymbolDisplayMiscellaneousOptions.ExpandValueTuple);

    private static List<Model> DescribeRoots(SourceProductionContext context, Compilation compilation, IEnumerable<ITypeSymbol> roots)
    {
        var queue = new Queue<ITypeSymbol>();
        foreach (var root in roots) queue.Enqueue(root);
        var visited = new HashSet<ITypeSymbol>(SymbolEqualityComparer.Default);
        var models = new List<Model>();
        var sources = new List<(Model Model,ITypeSymbol Type)>();
        while (queue.Count != 0)
        {
            var type = queue.Dequeue().WithNullableAnnotation(NullableAnnotation.NotAnnotated);
            if (!visited.Add(type) || type is ITypeParameterSymbol || type.TypeKind == TypeKind.Error) continue;
            if (type is INamedTypeSymbol open && open.IsUnboundGenericType) continue;
            var model = Describe(context, compilation, type, queue);
            if (model != null)
            {
                var symbols = new Dictionary<string,ITypeSymbol>(StringComparer.Ordinal);
                void Remember(ITypeSymbol item)
                {
                    var key = item.ToDisplayString(TypeDisplay);
                    if (symbols.ContainsKey(key)) return;
                    symbols.Add(key,item);
                    if (item is IArrayTypeSymbol array) Remember(array.ElementType);
                    if (item is INamedTypeSymbol named)
                    {
                        if (named.EnumUnderlyingType != null) Remember(named.EnumUnderlyingType);
                        if (named.ContainingType != null) Remember(named.ContainingType);
                        foreach (var argument in named.TypeArguments) Remember(argument);
                    }
                }
                Remember(type);
                if (type is INamedTypeSymbol target)
                    for (var current = target; current != null; current = current.BaseType)
                    {
                        Remember(current);
                        foreach (var member in current.GetMembers())
                        {
                            if (member is IPropertySymbol property) Remember(property.Type);
                            if (member is IFieldSymbol field) Remember(field.Type);
                        }
                        foreach (var contract in current.AllInterfaces) Remember(contract);
                    }
                var used = new[] { model.TypeName, model.ElementType, model.IdentityType, model.KeyType }
                    .Concat(model.TypeArguments).Concat(model.CollectionArguments)
                    .Concat(model.Members.SelectMany(member => new[] { member.TypeName,member.DeclaringType }))
                    .Where(name => name != null).Cast<string>().Distinct(StringComparer.Ordinal).ToArray();
                if (used.Any(name => !SourceTypeIdentity.Closed(symbols[name])))
                {
                    context.ReportDiagnostic(Diagnostic.Create(Unsupported,type.Locations.FirstOrDefault(),type.ToDisplayString(),
                        "the model contains an open, pointer or byref-like operation type"));
                    continue;
                }
                model.HelperOnly = used.Any(name => !SourceTypeIdentity.Accessible(compilation,symbols[name]));
                model.TypeIdentities = used.ToDictionary(name => name,name => SourceTypeIdentity.Encode(symbols[name]),StringComparer.Ordinal);
            }
            if (model != null)
            {
                var element = type is IArrayTypeSymbol array ? array.ElementType : type is INamedTypeSymbol named
                    ? new[] { named }.Concat(named.AllInterfaces).FirstOrDefault(contract => contract.OriginalDefinition.SpecialType == SpecialType.System_Collections_Generic_IEnumerable_T)?.TypeArguments[0] : null;
                model.SequenceElementType = element?.ToDisplayString(TypeDisplay);
                if (element != null)
                {
                    var identities = model.TypeIdentities.ToDictionary(pair => pair.Key,pair => pair.Value,StringComparer.Ordinal);
                    identities[model.SequenceElementType!] = SourceTypeIdentity.Encode(element);
                    model.TypeIdentities = identities;
                }
            }
            if (model != null) { models.Add(model); sources.Add((model,type)); }
        }
        foreach (var source in sources)
            source.Model.AssignableTo = sources.Where(target => !target.Type.IsValueType && !target.Type.IsSealed && target.Type.SpecialType != SpecialType.System_Object)
                .Where(target => {
                    var conversion = compilation.ClassifyConversion(source.Type,target.Type);
                    return conversion.IsImplicit && (conversion.IsIdentity || conversion.IsReference || conversion.IsBoxing);
                }).Select(target => SourceTypeIdentity.Encode(target.Type)).ToArray();
        return models;
    }

    private static Model? Describe(SourceProductionContext context, Compilation compilation, ITypeSymbol type, Queue<ITypeSymbol> dependencies)
    {
        var name = type.ToDisplayString(TypeDisplay);
        string Definition(ITypeSymbol value) => value is INamedTypeSymbol symbol
            ? symbol.ContainingNamespace.ToDisplayString() + "." + symbol.OriginalDefinition.MetadataName : value.MetadataName;
        var contracts = type is INamedTypeSymbol namedContracts ? namedContracts.AllInterfaces.Concat(new[] { namedContracts }).ToArray() : Array.Empty<INamedTypeSymbol>();
        INamedTypeSymbol? Contract(string identity) => contracts.FirstOrDefault(contract => Definition(contract) == identity);
        var arrayFact = type as IArrayTypeSymbol;
        var decision = TypeSupport.Select(new TypeSupportFacts(Definition(type),arrayFact?.Rank ?? 0,
            arrayFact == null ? "" : Definition(arrayFact.ElementType),type.TypeKind == TypeKind.Enum,
            type is INamedTypeSymbol tuple && tuple.ContainingNamespace.ToDisplayString() == "System" && tuple.Name is "Tuple" or "ValueTuple",
            Contract("System.Collections.Generic.IDictionary`2") != null,Contract("System.Collections.Generic.IReadOnlyDictionary`2") != null,
            Contract("System.Collections.IDictionary") != null,Contract("System.Collections.Generic.IEnumerable`1") != null,Contract("System.Linq.IGrouping`2") != null));
        foreach (var child in decision.NamedChildren) dependencies.Enqueue(compilation.GetTypeByMetadataName(child)!);
        foreach (var child in TypeSupport.OptionalChildren(decision.Shape))
            if (compilation.GetTypeByMetadataName(child) is { } resolved) dependencies.Enqueue(resolved);
        if (decision.GenericChildren && type is INamedTypeSymbol genericFact)
            foreach (var child in genericFact.TypeArguments) dependencies.Enqueue(child);
        var contractArguments = decision.Contract.Length == 0 ? Array.Empty<ITypeSymbol>() : Contract(decision.Contract)!.TypeArguments.ToArray();
        foreach (var child in contractArguments) dependencies.Enqueue(child);
        var bsonDocument = decision.Shape == TypeShape.BsonDocument;
        if (decision.Shape is TypeShape.Scalar or TypeShape.DynamicObject || decision.NamedChildren.Length != 0)
            return New(decision.Shape);
        if (arrayFact != null)
        {
            dependencies.Enqueue(arrayFact.ElementType);
            var model = New(decision.Shape,arrayFact.ElementType);
            model.NonGenericEnumerable = arrayFact.Rank != 1;
            model.SupportsValuesWrapper = TypeSupport.SupportsValuesWrapper(Definition(arrayFact.ElementType));
            return model;
        }
        if (type is not INamedTypeSymbol named) return Reject("the compiler type has no named operation shape");
        var constructFlags = compilation.GetTypeByMetadataName("Microsoft.FSharp.Core.SourceConstructFlags");
        int Flag(string flag) => System.Convert.ToInt32(constructFlags?.GetMembers(flag).OfType<IFieldSymbol>().SingleOrDefault()?.ConstantValue);
        bool Kind(ISymbol symbol, string kind) {
            var mapping = symbol.GetAttributes().FirstOrDefault(attribute =>
                attribute.AttributeClass?.ToDisplayString() == "Microsoft.FSharp.Core.CompilationMappingAttribute");
            return constructFlags != null && mapping != null && mapping.ConstructorArguments.Length > 0 &&
                (System.Convert.ToInt32(mapping.ConstructorArguments[0].Value) & Flag("KindMask")) == Flag(kind);
        }
        var unionType = Kind(named,"SumType");
        if (unionType) {
            foreach (var method in named.GetMembers().OfType<IMethodSymbol>().Where(method => Kind(method,"UnionCase")))
                foreach (var parameter in method.Parameters) dependencies.Enqueue(parameter.Type);
            foreach (var nested in named.GetTypeMembers())
                foreach (var property in nested.GetMembers().OfType<IPropertySymbol>().Where(property => Kind(property,"Field"))) dependencies.Enqueue(property.Type);
        }
        if (named.IsAnonymousType) return Reject("anonymous carriers require inferred registration in their declaring scope");
        if (decision.Shape == TypeShape.Enum)
        {
            dependencies.Enqueue(named.EnumUnderlyingType!);
            return New(TypeShape.Enum, named.EnumUnderlyingType);
        }
        if (decision.Shape == TypeShape.Tuple)
        {
            var arguments = named.TypeArguments;
            var tupleMembers = arguments.Select((argument,index) => new Member(index == 7 ? "Rest" : "Item" + (index + 1),
                argument.ToDisplayString(TypeDisplay), named.IsValueType, true, false, false)).ToArray();
            foreach (var argument in arguments) dependencies.Enqueue(argument);
            return new Model(name,named.MetadataName,TypeShape.Tuple,named.IsValueType,true,false,null,tupleMembers) { TypeArguments = Arguments(type) };
        }
        if (decision.RegistrationKind != CodecRegistrationKind.Model)
        {
            var arguments = decision.Arguments == CodecArguments.TargetAndContract ? contractArguments : named.TypeArguments.ToArray();
            var model = New(decision.Shape,arguments.Length > decision.ElementArgument ? arguments[decision.ElementArgument] : null);
            model.CollectionArguments = contractArguments.Select(argument => argument.ToDisplayString(TypeDisplay)).ToArray();
            if (decision.KeyArgument >= 0) model.KeyType = arguments[decision.KeyArgument].ToDisplayString(TypeDisplay);
            if (decision.IdentityArgument >= 0) model.IdentityType = arguments[decision.IdentityArgument].ToDisplayString(TypeDisplay);
            return model;
        }
        var abstractType = named.IsAbstract || named.TypeKind == TypeKind.Interface;
        if (decision.Contract.Length != 0 || decision.Shape == TypeShape.NonGenericDictionary)
        {
            var arguments = decision.Contract.Length == 0 ? Array.Empty<ITypeSymbol>() : Contract(decision.Contract)!.TypeArguments.ToArray();
            foreach (var argument in arguments) dependencies.Enqueue(argument);
            var model = New(decision.Shape,arguments.LastOrDefault());
            model.AbstractType = abstractType;
            model.CollectionArguments = arguments.Select(argument => argument.ToDisplayString(TypeDisplay)).ToArray();
            model.PopulateCollection = TypeSupport.PopulateCollection(decision.Shape,named.TypeArguments.Length,
                Contract("System.Collections.IList") != null,
                named.InstanceConstructors.Any(constructor => constructor.DeclaredAccessibility == Accessibility.Public && constructor.Parameters.Length == 0));
            return model;
        }
        string? indexedRead = null, indexedWrite = null;
        var members = new List<Member>();
        var names = new HashSet<string>(StringComparer.Ordinal);
        void PropertyPolicy(IPropertySymbol property,Member fact)
        {
            var referenceShape = TypeSupport.ReferenceShape(Definition(property.Type));
            fact.Reference = referenceShape != TypeShape.Object;
            fact.ManyReference = TypeSupport.ReferenceMany(referenceShape);
            var policy = property.GetAttributes().FirstOrDefault(attribute => attribute.AttributeClass?.ToDisplayString() == "SoloDatabase.Attributes.SoloRefAttribute");
            if (policy != null)
            {
                fact.HasReferencePolicy = true;
                foreach (var argument in policy.NamedArguments)
                    switch (argument.Key)
                    {
                        case "OnDelete": fact.OnDelete = (int)argument.Value.Value!; break;
                        case "OnOwnerDelete": fact.OnOwnerDelete = (int)argument.Value.Value!; break;
                        case "Unique": fact.Unique = (bool)argument.Value.Value!; break;
                        case "OrderBy": fact.OrderBy = (int)argument.Value.Value!; break;
                    }
            }
        }
        for (INamedTypeSymbol? current = named; current != null; current = current.BaseType)
        {
            foreach (var member in current.GetMembers())
            {
                if (member is IPropertySymbol staticProperty && staticProperty.IsStatic && !staticProperty.IsIndexer &&
                    staticProperty.DeclaredAccessibility == Accessibility.Public && staticProperty.GetMethod != null && staticProperty.SetMethod == null)
                {
                    if (names.Add(staticProperty.Name + ":" + staticProperty.Type.ToDisplayString(TypeDisplay) + ":"))
                    {
                        members.Add(new Member(staticProperty.Name,staticProperty.Type.ToDisplayString(TypeDisplay),false,false,false,false)
                        { DeclaringType = staticProperty.ContainingType.ToDisplayString(TypeDisplay) });
                        dependencies.Enqueue(staticProperty.Type);
                    }
                    continue;
                }
                if ((member.IsStatic && !(named.IsValueType && member is IFieldSymbol)) || member.DeclaredAccessibility != Accessibility.Public) continue;
                if (member is IPropertySymbol property)
                {
                    if (!names.Add(property.Name + ":" + property.Type.ToDisplayString(TypeDisplay) + ":" + string.Join(",", property.Parameters.Select(parameter => parameter.Type.ToDisplayString(TypeDisplay))))) continue;
                    if (property.IsIndexer)
                    {
                        // Indexed access has independent lazy codec failure, rather than rejecting a supported collection writer.
                        string Format(ITypeSymbol item) => item.SpecialType is SpecialType.System_Object or SpecialType.System_String || item.SpecialType == SpecialType.None
                            ? item.ToDisplayString(new SymbolDisplayFormat(typeQualificationStyle: SymbolDisplayTypeQualificationStyle.NameAndContainingTypesAndNamespaces))
                            : item.Name;
                        var getter = property.GetMethod;
                        var accessor = getter ?? property.SetMethod!;
                        var parameters = property.Parameters.Select(parameter => Format(parameter.Type));
                        if (getter == null) parameters = parameters.Concat(new[] { Format(property.Type) });
                        var signature = Format(accessor.ReturnType) + " " + accessor.MetadataName + "(" + string.Join(", ", parameters) + ")";
                        if (property.SetMethod != null) indexedRead ??= signature;
                        if (getter != null && !property.GetAttributes().Any(attribute => attribute.AttributeClass?.ToDisplayString() == "System.Runtime.Serialization.IgnoreDataMemberAttribute")) indexedWrite ??= signature;
                        members.Add(new Member(property.MetadataName, property.Type.ToDisplayString(TypeDisplay), false, false, false, false)
                        { DeclaringType = property.ContainingType.ToDisplayString(TypeDisplay) });
                        PropertyPolicy(property,members[members.Count - 1]);
                        dependencies.Enqueue(property.Type);
                        continue;
                    }
                    var read = property.GetMethod != null;
                    var write = property.SetMethod != null;
                    var ignored = property.GetAttributes().Any(attribute => attribute.AttributeClass?.ToDisplayString() == "System.Runtime.Serialization.IgnoreDataMemberAttribute");
                    members.Add(new Member(property.Name, property.Type.ToDisplayString(TypeDisplay), false, read, write, ignored)
                    {
                        DeclaringType = property.ContainingType.ToDisplayString(TypeDisplay),
                        HelperRead = read && (!SymbolEqualityComparer.Default.Equals(property.ContainingType, named) || !compilation.IsSymbolAccessibleWithin(property.GetMethod!, compilation.Assembly)),
                        HelperWrite = write && (!SymbolEqualityComparer.Default.Equals(property.ContainingType, named) || property.SetMethod!.IsInitOnly || !compilation.IsSymbolAccessibleWithin(property.SetMethod, compilation.Assembly))
                    });
                    PropertyPolicy(property,members[members.Count - 1]);
                    dependencies.Enqueue(property.Type);
                }
                else if (member is IFieldSymbol field)
                {
                    members.Add(new Member(field.Name, field.Type.ToDisplayString(TypeDisplay), true, true, named.IsValueType && !field.IsReadOnly && !field.IsConst, false)
                    { DeclaringType = field.ContainingType.ToDisplayString(TypeDisplay) });
                    dependencies.Enqueue(field.Type);
                }
            }
        }
        // Reflection's struct serializer/deserializer enumerates fields before properties.
        members = members.OrderBy(member => member.Field ? 0 : 1).ToList();
        var properties = members.Where(member => !member.Field).ToArray();
        var recordType = Kind(named,"RecordType");
        var sqlProperties = named.GetMembers().OfType<IPropertySymbol>().Count(property =>
            property.DeclaredAccessibility == Accessibility.Public && property.SetMethod != null);
        if (TypeSupport.SqlRowFieldFallback(named.IsValueType,recordType,sqlProperties))
            foreach (var field in named.GetMembers().OfType<IFieldSymbol>().Where(field => !field.IsStatic &&
                field.DeclaredAccessibility != Accessibility.Public && field.DeclaredAccessibility != Accessibility.Private))
                dependencies.Enqueue(field.Type);
        var recordFields = recordType ? named.GetMembers().OfType<IPropertySymbol>()
            .Select(property => (property, mapping: property.GetAttributes().FirstOrDefault(attribute =>
                attribute.AttributeClass?.ToDisplayString() == "Microsoft.FSharp.Core.CompilationMappingAttribute")))
            .Where(pair => pair.mapping != null && pair.mapping.ConstructorArguments.Length > 1 &&
                (System.Convert.ToInt32(pair.mapping.ConstructorArguments[0].Value) & Flag("KindMask")) == Flag("Field"))
            .OrderBy(pair => System.Convert.ToInt32(pair.mapping!.ConstructorArguments.Last().Value))
            .Select(pair => pair.property.Name).ToArray() : System.Array.Empty<string>();
        var propertyTypes = (recordType ? recordFields.Select(field => properties.Single(property => property.Name == field)) : properties)
            .Select(member => member.TypeName).ToArray();
        var memberConstructor = named.InstanceConstructors.Any(ctor =>
            (!named.IsValueType || !ctor.IsImplicitlyDeclared) &&
            ctor.DeclaredAccessibility == Accessibility.Public &&
            ctor.Parameters.Select(parameter => parameter.Type.ToDisplayString(TypeDisplay)).SequenceEqual(propertyTypes));

        var constructors = named.InstanceConstructors.Where(ctor => ctor.Parameters.Length == 0).ToArray();
        var constructor = constructors.FirstOrDefault(ctor => ctor.DeclaredAccessibility == Accessibility.Public);
        return new Model(name, named.MetadataName, bsonDocument ? TypeShape.BsonDocument : TypeShape.Object, named.IsValueType, named.IsSealed,
            !abstractType && constructor != null, null, members)
        {
            TypeArguments = Arguments(type),
            AbstractType = abstractType,
            ConstructFromMembers = !abstractType && memberConstructor,
            RecordType = recordType, RecordFields = recordFields, UnionType = unionType,
            IndexedRead = indexedRead, IndexedWrite = indexedWrite,
            DictionaryReadOnly = ReadOnlyMatches(),
            Formattable = Contract("System.IFormattable") != null,
            NonGenericEnumerable = !bsonDocument && contracts.Any(contract => contract.ToDisplayString() == "System.Collections.IEnumerable")
        };

        Model New(TypeShape shape, ITypeSymbol? element = null) => new(name, type.MetadataName, shape, type.IsValueType, type.IsSealed,
            false, element?.ToDisplayString(TypeDisplay), Array.Empty<Member>()) {
                TypeArguments = Arguments(type),
                AbstractType = type is INamedTypeSymbol original && (original.IsAbstract || original.TypeKind == TypeKind.Interface),
                DictionaryReadOnly = ReadOnlyMatches(), Formattable = Contract("System.IFormattable") != null
            };
        bool ReadOnlyMatches()
        {
            var selected = decision.Contract.Length == 0
                ? type is INamedTypeSymbol declared ? declared.TypeArguments.ToArray() : Array.Empty<ITypeSymbol>()
                : contractArguments;
            return selected.Length == 2 && contracts.Any(readOnly =>
                Definition(readOnly) == "System.Collections.Generic.IReadOnlyDictionary`2" && readOnly.TypeArguments.Length == 2 &&
                selected.Zip(readOnly.TypeArguments,(left,right) => SymbolEqualityComparer.Default.Equals(left,right)).All(equal => equal));
        }
        string[] Arguments(ITypeSymbol input) => input is INamedTypeSymbol symbol ?
            (symbol.ContainingType == null ? Array.Empty<string>() : Arguments(symbol.ContainingType))
                .Concat(symbol.TypeArguments.Select(argument => argument.ToDisplayString(TypeDisplay))).ToArray() : Array.Empty<string>();
        Model? Reject(string reason)
        {
            context.ReportDiagnostic(Diagnostic.Create(Unsupported, type.Locations.FirstOrDefault(), type.ToDisplayString(), reason));
            return null;
        }
    }
}
