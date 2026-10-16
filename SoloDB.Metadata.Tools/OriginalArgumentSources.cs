using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.FSharp.Core;
using Mono.Cecil;
using SoloDatabase.Metadata.CodecGeneration;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

// Imported original CLR properties, independent of JSON member admission.
public static class OriginalArgumentSources
{
    public static TypeReference Close(TypeReference value, TypeReference owner)
    {
        if (value is GenericParameter parameter && parameter.Type == GenericParameterType.Type && owner is GenericInstanceType closed)
            return closed.GenericArguments[parameter.Position];
        if (value is ArrayType array)
        {
            var result = new ArrayType(Close(array.ElementType,owner),array.Rank);
            for(var i=0;i<array.Dimensions.Count;i++) result.Dimensions[i]=array.Dimensions[i];
            return result;
        }
        if (value is ByReferenceType reference) return new ByReferenceType(Close(reference.ElementType,owner));
        if (value is RequiredModifierType required) return new RequiredModifierType(Close(required.ModifierType,owner),Close(required.ElementType,owner));
        if (value is OptionalModifierType optional) return new OptionalModifierType(Close(optional.ModifierType,owner),Close(optional.ElementType,owner));
        if (value is GenericInstanceType instance)
        {
            var element = instance.ElementType;
            if (element is not TypeDefinition) element.IsValueType = instance.IsValueType;
            var result = new GenericInstanceType(element) { IsValueType = instance.IsValueType };
            foreach(var argument in instance.GenericArguments) result.GenericArguments.Add(Close(argument,owner));
            return result;
        }
        return value;
    }

    public static GeneratedArgumentSource? Read(ModuleDefinition module,TypeReference original) => Read(module,original,null);

    internal static GeneratedArgumentSource? Read(ModuleDefinition module,TypeReference original,Func<TypeReference,string>? signature)
    {
        if (original is GenericInstanceType nullable && nullable.ElementType.FullName == "System.Nullable`1") original=nullable.GenericArguments[0];
        var dictionary = new HashSet<string>(StringComparer.Ordinal);
        bool Direct(TypeReference current)
        {
            if (!dictionary.Add(GeneratedJsonDefinitions.Identity(current))) return false;
            var original = current as GenericInstanceType;
            if (TypeSupport.DirectSqlArguments(original?.ElementType.FullName ?? current.FullName,
                original?.GenericArguments.Select(argument => argument.FullName).ToArray() ?? Array.Empty<string>())) return true;
            var definition=current.Resolve();
            return definition.Interfaces.Any(item=>Direct(Close(item.InterfaceType,current))) ||
                definition.BaseType != null && Direct(Close(definition.BaseType,current));
        }
        if (original is not ArrayType && Direct(original)) return null;
        var access=new HashSet<string>(StringComparer.Ordinal);
        var first = original is ArrayType
            ? new TypeReference("System","Array",original.Module,original.Module.TypeSystem.CoreLibrary) : original;
        var members = GeneratedRowFields.Properties(module,first,name => access.Add(name),identities: false,signature: signature)
            .Select(property => property.Member)
            .Where(member => TypeSupport.SqlArgumentProperty(true,member.Getter != null,member.Static,
                GeneratedRowFields.IndexParameters(member).Length != 0)).ToArray();
        var result=new GeneratedArgumentSource(module.ImportReference(original),members);
        foreach(var name in access) result.AccessAssemblies.Add(name);
        return result;
    }
}
