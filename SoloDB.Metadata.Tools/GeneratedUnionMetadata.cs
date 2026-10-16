using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.FSharp.Core;
using Mono.Cecil;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

// Imported unions use their original case attributes and ordered implementation members.
public static class GeneratedUnionMetadata
{
    private static CustomAttribute? Mapping(ICustomAttributeProvider owner) => owner.CustomAttributes.FirstOrDefault(
        attribute => attribute.AttributeType.FullName == "Microsoft.FSharp.Core.CompilationMappingAttribute");

    private static bool Kind(ICustomAttributeProvider owner, SourceConstructFlags kind) => Mapping(owner) is { } mapping &&
        ((SourceConstructFlags)Convert.ToInt32(mapping.ConstructorArguments[0].Value) & SourceConstructFlags.KindMask) == kind;

    public static GeneratedModel[] Collect(ModuleDefinition module, TypeReference source, bool? hostedReferences = null) =>
        Collect(module,source,hostedReferences,null);

    internal static GeneratedModel[] Collect(ModuleDefinition module,TypeReference source,bool? hostedReferences,Func<TypeReference,string>? signature)
    {
        var root = source.Resolve();
        var actual = module.ImportReference(source);
        actual.IsValueType = root.IsValueType;
        if (actual is GenericInstanceType genericRoot) genericRoot.ElementType.IsValueType = root.IsValueType;
        TypeReference Closed(TypeReference type)
        {
            if (type is GenericParameter parameter && source is GenericInstanceType instance)
                return module.ImportReference(instance.GenericArguments[parameter.Position]);
            if (type is ArrayType array) return new ArrayType(Closed(array.ElementType),array.Rank);
            if (type is GenericInstanceType generic)
            {
                var closed = new GenericInstanceType(module.ImportReference(generic.ElementType));
                foreach (var argument in generic.GenericArguments) closed.GenericArguments.Add(Closed(argument));
                return closed;
            }
            return module.ImportReference(type);
        }
        var representations = root.NestedTypes.Where(type =>
            type.BaseType != null && (type.BaseType is GenericInstanceType generic ? generic.ElementType.FullName : type.BaseType.FullName) == root.FullName).ToArray();
        var tags = root.NestedTypes.FirstOrDefault(type => type.Name == "Tags")?.Fields
            .Where(field => field.HasConstant).Select(field => (name: field.Name, tag: Convert.ToInt32(field.Constant))).ToArray();
        var factories = root.Methods.Where(method => method.IsPublic && method.IsStatic && Kind(method,SourceConstructFlags.UnionCase)).ToArray();
        tags ??= factories.Select(method => (name: method.Name.StartsWith("get_",StringComparison.Ordinal) ? method.Name.Substring(4) : method.Name.Substring(3),
            tag: Convert.ToInt32(Mapping(method)!.ConstructorArguments[1].Value))).ToArray();
        var fieldProperties = new[] { root }.Concat(representations).SelectMany(type => type.Properties)
            .Where(property => Kind(property,SourceConstructFlags.Field)).ToArray();
        var cases = tags.OrderBy(item => item.tag).Select(item => {
            var fields = fieldProperties.Where(property => {
                var arguments = Mapping(property)!.ConstructorArguments;
                return arguments.Count < 3 ? item.tag == 0 : Convert.ToInt32(arguments[1].Value) == item.tag;
            }).OrderBy(property => Convert.ToInt32(Mapping(property)!.ConstructorArguments.Last().Value)).ToArray();
            var factory = factories.SingleOrDefault(method => Convert.ToInt32(Mapping(method)!.ConstructorArguments[1].Value) == item.tag);
            MethodReference? reference = factory == null ? null : module.ImportReference(factory);
            if (reference != null) reference.DeclaringType = actual;
            return new GeneratedUnionCase(item.name,item.tag,reference,fields.Select(field => field.Name).ToArray(),fields.Select(field => Closed(field.PropertyType)).ToArray());
        }).ToArray();
        var hosted = hostedReferences ?? ContainsReferences(source,new HashSet<string>(StringComparer.Ordinal));
        var models = new List<GeneratedModel>();
        Add(root,actual,null);
        foreach (var representation in representations)
        {
            TypeReference type = module.ImportReference(representation);
            if (source is GenericInstanceType instance)
            {
                var closed = new GenericInstanceType(type);
                foreach (var argument in instance.GenericArguments) closed.GenericArguments.Add(module.ImportReference(argument));
                type = closed;
            }
            Add(representation,type,actual);
        }
        return models.ToArray();

        void Add(TypeDefinition definition, TypeReference type, TypeReference? unionRoot)
        {
            var facts = new List<Member>();
            var members = new List<GeneratedMember>();
            var seen = new HashSet<string>(StringComparer.Ordinal);
            foreach (var declaring in unionRoot == null ? new[] { definition } : new[] { definition,root })
                foreach (var property in declaring.Properties.Where(property => property.GetMethod is { IsPublic: true, IsStatic: false } && property.Parameters.Count == 0))
                {
                    if (!seen.Add(property.Name)) continue;
                    var valueType = Closed(property.PropertyType);
                    var ignored = property.CustomAttributes.Any(attribute => attribute.AttributeType.FullName == "System.Runtime.Serialization.IgnoreDataMemberAttribute");
                    facts.Add(new Member(property.Name,valueType.FullName,false,true,false,ignored));
                    var getter = module.ImportReference(property.GetMethod); getter.DeclaringType = declaring == root ? actual : type;
                    members.Add(new GeneratedMember(valueType,getter,null,null));
                }
            var policy = new Model(type.FullName,root.FullName.Replace('/','+'),TypeShape.Object,
                definition.IsValueType,definition.IsSealed,false,null,facts)
                { AbstractType = definition.IsAbstract, RequiresTypeInformation = definition.IsAbstract || HelperAssembly.InheritedAttribute(definition,"SoloDatabase.Attributes.PolymorphicAttribute") };
            var model = new GeneratedModel(policy,type,null,members.ToArray())
                { UnionRoot = unionRoot, UnionCases = cases, UnionHostedReferences = hosted };
            TypeReference original = definition;
            if (source is GenericInstanceType originalArguments) {
                var closed = new GenericInstanceType(definition);
                foreach (var argument in originalArguments.GenericArguments) closed.GenericArguments.Add(argument);
                original = closed;
            }
            model.OriginalProperties = GeneratedRowFields.Properties(module,original,name => model.AccessAssemblies.Add(name),signature: signature);
            model.CloneConstructor = GeneratedRowFields.DefaultConstructor(module,original);
            if (model.CloneConstructor != null) model.AccessAssemblies.Add(root.Module.Assembly.Name.Name);
            if (!definition.IsPublic && !definition.IsNestedPublic) model.AccessAssemblies.Add(root.Module.Assembly.Name.Name);
            models.Add(model);
        }
    }

    private static bool ContainsReferences(TypeReference type, HashSet<string> seen)
    {
        var original = type is GenericInstanceType instance ? instance.ElementType : type;
        if (TypeSupport.ReferenceShape(original.FullName) != TypeShape.Object) return true;
        if (!seen.Add(type.FullName + "," + type.Scope.Name)) return false;
        if (type is ArrayType array) return ContainsReferences(array.ElementType,seen);
        if (type is GenericInstanceType generic && generic.GenericArguments.Any(argument => ContainsReferences(argument,seen))) return true;
        if (type.IsPrimitive || type.FullName == "System.String") return false;
        var definition = type.Resolve();
        if (definition.IsEnum) return false;
        TypeReference Closed(TypeReference candidate) {
            if (candidate is GenericParameter parameter && type is GenericInstanceType instance) return instance.GenericArguments[parameter.Position];
            if (candidate is ArrayType array) return new ArrayType(Closed(array.ElementType),array.Rank);
            if (candidate is GenericInstanceType generic) {
                var result = new GenericInstanceType(generic.ElementType);
                foreach (var argument in generic.GenericArguments) result.GenericArguments.Add(Closed(argument));
                return result;
            }
            return candidate;
        }
        return new[] { definition }.Concat(definition.NestedTypes.Where(nested => nested.BaseType != null &&
            (nested.BaseType is GenericInstanceType generic ? generic.ElementType.FullName : nested.BaseType.FullName) == definition.FullName))
            .SelectMany(owner => owner.Properties).Any(property => property.GetMethod is { IsPublic: true, IsStatic: false } &&
                property.Parameters.Count == 0 && ContainsReferences(Closed(property.PropertyType),seen))
            || (definition.BaseType != null && ContainsReferences(Closed(definition.BaseType),seen));
    }
}
