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
            var result=new GenericInstanceType(instance.ElementType);
            foreach(var argument in instance.GenericArguments) result.GenericArguments.Add(Close(argument,owner));
            return result;
        }
        return value;
    }

    public static GeneratedArgumentSource? Read(ModuleDefinition module,TypeReference original)
    {
        if (original is GenericInstanceType nullable && nullable.ElementType.FullName == "System.Nullable`1") original=nullable.GenericArguments[0];
        var dictionary = new HashSet<string>(StringComparer.Ordinal);
        bool Direct(TypeReference current)
        {
            if (!dictionary.Add(GeneratedJsonDefinitions.Identity(current))) return false;
            if (current.FullName is "SoloDatabase.SqlArguments" or "SoloDatabase.ParameterValues" or "System.Collections.IDictionary") return true;
            if (current is GenericInstanceType pair && pair.ElementType.FullName == "System.Collections.Generic.IDictionary`2" &&
                pair.GenericArguments[0].FullName == "System.String" && pair.GenericArguments[1].FullName == "System.Object") return true;
            var definition=current.Resolve();
            return definition.Interfaces.Any(item=>Direct(Close(item.InterfaceType,current))) ||
                definition.BaseType != null && Direct(Close(definition.BaseType,current));
        }
        if (original is not ArrayType && Direct(original)) return null;
        var access=new HashSet<string>(StringComparer.Ordinal);
        var members=new List<CodecMember<TypeReference,MethodReference,FieldReference>>();
        var names=new HashSet<string>(StringComparer.Ordinal);
        var first = original is ArrayType
            ? new TypeReference("System","Array",original.Module,original.Module.TypeSystem.CoreLibrary) : original;
        for (var current=first;current!=null;)
        {
            var definition=current.Resolve();
            var declaring=module.ImportReference(current);
            declaring.IsValueType=definition.IsValueType;
            if (declaring is GenericInstanceType generic) generic.ElementType.IsValueType=definition.IsValueType;
            bool Visible(TypeDefinition type) => type.DeclaringType == null ? type.IsPublic : type.IsNestedPublic && Visible(type.DeclaringType);
            if (!Visible(definition)) access.Add(definition.Module.Assembly.Name.Name);
            foreach(var property in definition.Properties)
            {
                var getter=property.GetMethod;
                if (!TypeSupport.SqlArgumentProperty(getter?.IsPublic == true || property.SetMethod?.IsPublic == true,
                    getter!=null,getter?.IsStatic == true,property.Parameters.Count!=0)) continue;
                var value=module.ImportReference(Close(property.PropertyType,current));
                if (!names.Add(property.Name+"|"+GeneratedJsonDefinitions.Identity(value))) continue;
                var read=module.ImportReference(getter!);read.DeclaringType=declaring;
                if (!getter!.IsPublic) access.Add(definition.Module.Assembly.Name.Name);
                members.Add(new CodecMember<TypeReference,MethodReference,FieldReference>(property.Name,value,null!,
                    FSharpOption<MethodReference>.Some(read),null!,false,true,false,false,false));
            }
            current=definition.BaseType==null ? null : Close(definition.BaseType,current);
        }
        var result=new GeneratedArgumentSource(module.ImportReference(original),members.ToArray());
        foreach(var name in access) result.AccessAssemblies.Add(name);
        return result;
    }
}
