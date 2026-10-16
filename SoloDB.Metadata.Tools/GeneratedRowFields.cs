using System;
using System.Linq;
using Mono.Cecil;
using SoloDatabase.Metadata.CodecGeneration;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

// Original CLR properties and fallback fields, before each consumer selects its roles.
internal static class GeneratedRowFields
{
    internal static int Named(GeneratedModel model,string name,bool instance) => TypeSupport.NamedProperty(name,instance,
        model.OriginalProperties ?? Array.Empty<GeneratedProperty>(),
        new Func<GeneratedProperty,string>(property => property.Member.Name),
        new Func<GeneratedProperty,bool>(property => property.Member.Static));

    internal static GeneratedProperty[] Merge(params GeneratedProperty[][] sources)
    {
        var result = new System.Collections.Generic.List<GeneratedProperty>();
        var seen = new System.Collections.Generic.HashSet<string>(StringComparer.Ordinal);
        var slots = new System.Collections.Generic.HashSet<string>(StringComparer.Ordinal);
        foreach (var property in sources.SelectMany(source => source))
        {
            var member = property.Member;
            if (property.VirtualSlot != null && !slots.Add(property.VirtualSlot)) continue;
            var key = Signature(member);
            if (seen.Add(key)) result.Add(property);
        }
        return result.ToArray();
    }

    internal static GeneratedProperty[] Inherited(GeneratedProperty[] properties) => properties.Where(property => !property.Member.Static)
        .Select(property => {
            var member = property.Member;
            var getter = property.PrivateGetter ? null! : member.Getter;
            var setter = property.PrivateSetter ? null! : member.Setter;
            return new GeneratedProperty(new CodecMember<TypeReference,MethodReference,FieldReference>(member.Name,member.Type,member.Field,
                getter,setter,false,getter != null,setter != null,member.Ignored,member.Many),property.Identity,property.PrivateGetter,property.PrivateSetter,property.VirtualSlot,property.Attributes,property.DeferredSchema);
        }).ToArray();

    internal static bool SameSignatureType(TypeReference left,TypeReference right) => GeneratedJsonDefinitions.SignatureIdentity(left) == GeneratedJsonDefinitions.SignatureIdentity(right);

    internal static string Signature(CodecMember<TypeReference,MethodReference,FieldReference> member)
    {
        var method = member.Getter?.Value ?? member.Setter?.Value;
        var value = member.Getter != null ? method!.ReturnType : method?.Parameters.Last().ParameterType ?? member.Type;
        var parameters = method == null ? Array.Empty<TypeReference>() : (member.Getter != null ? method.Parameters : method.Parameters.Take(method.Parameters.Count - 1)).Select(parameter => parameter.ParameterType);
        return member.Name + ":" + (method?.CallingConvention.ToString() ?? "Default") + ":" + !member.Static + ":" + GeneratedJsonDefinitions.SignatureIdentity(value) + ":" + string.Join(";",parameters.Select(GeneratedJsonDefinitions.SignatureIdentity));
    }

    internal static string? VirtualSlot(PropertyDefinition property,TypeReference owner)
    {
        var accessor = property.GetMethod ?? property.SetMethod;
        if (accessor == null || !accessor.IsVirtual) return null;
        while (HelperAssembly.ParentProperty(property,owner) is { } parent) { property = parent.Property; owner = parent.Owner; }
        accessor = property.GetMethod ?? property.SetMethod!;
        return SlotIdentity(owner,accessor.Name,accessor.ReturnType,accessor.Parameters.Select(parameter => parameter.ParameterType).ToArray());
    }

    internal static string SlotIdentity(TypeReference owner,string name,TypeReference result,TypeReference[] parameters) =>
        GeneratedJsonDefinitions.Identity(owner) + ":" + name + ":" + GeneratedJsonDefinitions.SignatureIdentity(result) + ":" + string.Join(";",parameters.Select(GeneratedJsonDefinitions.SignatureIdentity));

    internal static TypeReference[] IndexParameters(CodecMember<TypeReference,MethodReference,FieldReference> member)
    {
        var method = member.Getter?.Value ?? member.Setter?.Value;
        if (method == null) return Array.Empty<TypeReference>();
        var parameters = member.Getter != null ? method.Parameters : method.Parameters.Take(method.Parameters.Count - 1);
        return parameters.Select(parameter => OriginalArgumentSources.Close(parameter.ParameterType,method.DeclaringType)).ToArray();
    }

    private static bool Many(TypeReference type,Func<TypeReference,bool?>? source)
    {
        if (type is ArrayType or GenericParameter) return false;
        var fact = source?.Invoke(type);
        if (fact.HasValue) return fact.Value;
        var definition = type.Resolve();
        if (TypeSupport.ManyContract(definition.Module.Assembly.Name.Name,definition.FullName)) return true;
        return definition.Interfaces.Any(contract => Many(OriginalArgumentSources.Close(contract.InterfaceType,type),source)) ||
            (definition.BaseType != null && Many(OriginalArgumentSources.Close(definition.BaseType,type),source));
    }

    internal static MethodReference? DefaultConstructor(ModuleDefinition module,TypeReference type)
    {
        if (type is ArrayType) return null;
        var constructor = type.Resolve().Methods.FirstOrDefault(method => method.IsConstructor && !method.IsStatic && method.Parameters.Count == 0);
        if (constructor == null) return null;
        var result = module.ImportReference(constructor); result.DeclaringType = module.ImportReference(type);
        return result;
    }
    internal static CodecMember<TypeReference,MethodReference,FieldReference>[] Read(ModuleDefinition module,TypeReference type)
    {
        if (!type.IsValueType || type is ArrayType) return Array.Empty<CodecMember<TypeReference,MethodReference,FieldReference>>();
        var definition = type.Resolve();
        return definition.Fields.Where(field => !field.IsPublic && !field.IsPrivate && !field.IsStatic)
            .Select(field => {
                var reference = module.ImportReference(field); reference.DeclaringType = module.ImportReference(type);
                return new CodecMember<TypeReference,MethodReference,FieldReference>(field.Name,Closed(module,field.FieldType,type),
                    Microsoft.FSharp.Core.FSharpOption<FieldReference>.Some(reference),null!,null!,false,true,
                    !field.IsInitOnly && !field.IsLiteral,false,false);
            }).ToArray();
    }
    internal static GeneratedProperty[] Properties(ModuleDefinition module,TypeReference type,Action<string> access,bool inherited = false,Func<TypeReference,bool?>? many = null,bool identities = true)
    {
        if (type is ArrayType) return Array.Empty<GeneratedProperty>();
        var members = new System.Collections.Generic.List<GeneratedProperty>();
        TypeReference? current = type;
        while (current != null && current.FullName != "System.Object")
        {
            var definition = current.Resolve();
            var declaring = module.ImportReference(current);
            if (declaring is not TypeDefinition) declaring.IsValueType = definition.IsValueType;
            if (declaring is GenericInstanceType generic && generic.ElementType is not TypeDefinition)
                generic.ElementType.IsValueType = definition.IsValueType;
            foreach (var property in definition.Properties)
            {
                if (!(property.SetMethod?.IsPublic == true || property.GetMethod?.IsPublic == true)) continue;
                var accessor = property.GetMethod ?? property.SetMethod!;
                if ((inherited || !ReferenceEquals(current,type)) && accessor.IsStatic) continue;
                bool Visible(TypeDefinition type) => type.DeclaringType == null ? type.IsPublic : type.IsNestedPublic && Visible(type.DeclaringType);
                if (property.SetMethod?.IsPublic == false || property.GetMethod?.IsPublic == false || !Visible(definition))
                    access(definition.Module.Assembly.Name.Name);
                var value = Closed(module,property.PropertyType,current);
                MethodReference? Accessor(MethodDefinition? method) {
                    if (method == null || (inherited || !ReferenceEquals(current,type)) && method.IsPrivate) return null;
                    var imported = module.ImportReference(method); imported.DeclaringType = declaring; return imported;
                }
                var getter = Accessor(property.GetMethod); var setter = Accessor(property.SetMethod);
                members.Add(new GeneratedProperty(new CodecMember<TypeReference,MethodReference,FieldReference>(property.Name,value,null!,
                    getter == null ? null! : Microsoft.FSharp.Core.FSharpOption<MethodReference>.Some(getter),
                    setter == null ? null! : Microsoft.FSharp.Core.FSharpOption<MethodReference>.Some(setter),accessor.IsStatic,
                    getter != null,setter != null,false,identities && getter != null && setter != null && !accessor.IsStatic &&
                        Many(Closed(definition.Module,property.PropertyType,current),many)),
                    identities ? HelperAssembly.IdentityGenerator(module,property,access) : null,
                    property.GetMethod?.IsPrivate == true,property.SetMethod?.IsPrivate == true,VirtualSlot(property,current),identities ? OriginalAttributeFacts.Read(module,property,access) : null));
            }
            current = definition.BaseType == null ? null : Closed(definition.Module,definition.BaseType,current);
        }
        return Merge(members.ToArray());
    }

    private static TypeReference Closed(ModuleDefinition module,TypeReference value,TypeReference owner) =>
        module.ImportReference(OriginalArgumentSources.Close(value,owner));
}
