using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Reflection.Metadata;
using System.Reflection.Metadata.Ecma335;
using System.Reflection.PortableExecutable;
using Mono.Cecil;
using Microsoft.FSharp.Core;
using SoloDatabase.Metadata.CodecGeneration;
using TypeReference = Mono.Cecil.TypeReference;
using ModuleDefinition = Mono.Cecil.ModuleDefinition;
using PropertyDefinition = Mono.Cecil.PropertyDefinition;
using FieldDefinition = Mono.Cecil.FieldDefinition;
using Constant = SoloDatabase.Metadata.CodecGeneration.AttributeConstant<Mono.Cecil.TypeReference>;

namespace SoloDatabase.Metadata.Tools;

// Decode original metadata through the SDK reader; construction belongs to the shared emitter.
internal static class OriginalAttributeFacts
{
    internal static AttributeConstruction<TypeReference,MethodReference,FieldReference>[] Read(
        ModuleDefinition output,PropertyDefinition property,Action<string> access)
    {
        if (!property.HasCustomAttributes) return Array.Empty<AttributeConstruction<TypeReference,MethodReference,FieldReference>>();
        using var reader = new Reader(output,property.Module);
        return reader.Read(property,access);
    }

    // Completion reads immutable original property tokens while rewriting only
    // generated factory bodies. Keep its image and decoded facts in that lifetime.
    internal sealed class Reader(ModuleDefinition output,ModuleDefinition original) : IDisposable
    {
        private PEReader? image;
        private MetadataReader? metadata;
        private readonly Types provider = new(original);
        private readonly Dictionary<PropertyDefinition,(AttributeConstruction<TypeReference,MethodReference,FieldReference>[] Facts,string[] Access)> properties = new();

        internal AttributeConstruction<TypeReference,MethodReference,FieldReference>[] Read(PropertyDefinition property,Action<string> access)
        {
            if (!property.HasCustomAttributes) return Array.Empty<AttributeConstruction<TypeReference,MethodReference,FieldReference>>();
            if (properties.TryGetValue(property,out var cached))
            {
                foreach (var dependency in cached.Access) access(dependency);
                return cached.Facts;
            }
            if (image == null)
            {
                var stream = File.OpenRead(original.FileName);
                try { image = new PEReader(stream); }
                catch { stream.Dispose(); throw; }
            }
            var dependencies = new List<string>();
            var facts = Decode(output,property,metadata ??= image.GetMetadataReader(),provider,dependency =>
            {
                dependencies.Add(dependency);
                access(dependency);
            });
            properties.Add(property,(facts,dependencies.ToArray()));
            return facts;
        }

        public void Dispose() => image?.Dispose();
    }

    private static AttributeConstruction<TypeReference,MethodReference,FieldReference>[] Decode(
        ModuleDefinition output,PropertyDefinition property,MetadataReader metadata,Types provider,Action<string> access)
    {
        var original = metadata.GetPropertyDefinition(MetadataTokens.PropertyDefinitionHandle(checked((int)property.MetadataToken.RID)));
        return original.GetCustomAttributes().Select((handle,index) => {
            var attribute = property.CustomAttributes[index];
            var decoded = metadata.GetCustomAttribute(handle).DecodeValue(provider);
            var constructor = output.ImportReference(attribute.Constructor);
            var fieldOrdinal = 0;
            var propertyOrdinal = 0;
            var assignments = decoded.NamedArguments.Select(argument => {
                var name = argument.Name!;
                var field = argument.Kind == CustomAttributeNamedArgumentKind.Field;
                var declared = field ? attribute.Fields[fieldOrdinal++].Argument.Type : attribute.Properties[propertyOrdinal++].Argument.Type;
                var member = NamedMember(output,attribute.AttributeType,name,declared,field,access);
                if (member == null) return null;
                return new AttributeAssignment<TypeReference,MethodReference,FieldReference>(member,
                    Value(output,member.Type,argument.Type,argument.Value),argument.Kind == CustomAttributeNamedArgumentKind.Property);
            }).Where(value => value != null).Select(value => value!).ToArray();
            access(attribute.AttributeType.Resolve().Module.Assembly.Name.Name);
            return new AttributeConstruction<TypeReference,MethodReference,FieldReference>(output.ImportReference(attribute.AttributeType),constructor,
                decoded.FixedArguments.Select((argument,ordinal) => Value(output,constructor.Parameters[ordinal].ParameterType,argument.Type,argument.Value)).ToArray(),assignments);
        }).ToArray();
    }

    private static CodecMember<TypeReference,MethodReference,FieldReference>? NamedMember(ModuleDefinition output,TypeReference owner,string name,TypeReference valueType,bool field,Action<string> access)
    {
        if (field)
        {
            var selected = Field(owner,name);
            var reference = output.ImportReference(selected.Field); reference.DeclaringType = output.ImportReference(selected.Owner);
            return new(name,output.ImportReference(OriginalArgumentSources.Close(selected.Field.FieldType,selected.Owner)),
                FSharpOption<FieldReference>.Some(reference),null!,null!,selected.Field.IsStatic,true,true,false,false);
        }
        var member = GeneratedRowFields.Properties(output,owner,access,identities:false)
            .Single(candidate => candidate.Member.Name == name && SourceTypeIdentity.SameSignature(candidate.Member.Type,valueType,owner.Module.AssemblyResolver) && GeneratedRowFields.IndexParameters(candidate.Member).Length == 0).Member;
        return member.Setter == null || !new MetadataResolver(owner.Module.AssemblyResolver).Resolve(member.Setter.Value).IsPublic ? null : member;
    }

    private static (FieldDefinition Field,TypeReference Owner) Field(TypeReference owner,string name)
    {
        for (var current = owner; current != null; current = current.Resolve().BaseType is { } parent ? OriginalArgumentSources.Close(parent,current) : null)
        {
            var field = current.Resolve().Fields.FirstOrDefault(field => field.Name == name && field.IsPublic &&
                (ReferenceEquals(current,owner) || !field.IsStatic));
            if (field != null) return (field,current);
        }
        throw new InvalidOperationException("The original attribute field is absent: " + owner.FullName + "." + name);
    }

    private static Constant Value(ModuleDefinition module,TypeReference declared,TypeReference actual,object? value)
    {
        Constant result = value switch {
            null => Constant.NullConstant,
            bool number => Constant.NewIntegerConstant(number ? 1 : 0),
            char number => Constant.NewIntegerConstant(number),
            sbyte number => Constant.NewIntegerConstant(number),
            byte number => Constant.NewIntegerConstant(number),
            short number => Constant.NewIntegerConstant(number),
            ushort number => Constant.NewIntegerConstant(number),
            int number => Constant.NewIntegerConstant(number),
            uint number => Constant.NewIntegerConstant(unchecked((int)number)),
            long number => Constant.NewLongConstant(number),
            ulong number => Constant.NewLongConstant(unchecked((long)number)),
            float number => Constant.NewSingleConstant(number),
            double number => Constant.NewDoubleConstant(number),
            string text => Constant.NewStringConstant(text),
            TypeReference type => Constant.NewTypeConstant(module.ImportReference(type)),
            ImmutableArray<CustomAttributeTypedArgument<TypeReference>> array => array.IsDefault ? Constant.NullConstant :
                Constant.NewArrayConstant(module.ImportReference(((ArrayType)actual).ElementType),array.Select(item =>
                    Value(module,((ArrayType)actual).ElementType,item.Type,item.Value)).ToArray()),
            _ => throw new InvalidOperationException("The compiler supplied an unsupported attribute constant: " + value.GetType())
        };
        return declared.FullName == "System.Object" && actual.IsValueType && value != null
            ? Constant.NewBoxedConstant(module.ImportReference(actual),result) : result;
    }

    private sealed class Types(ModuleDefinition module) : ICustomAttributeTypeProvider<TypeReference>
    {
        public TypeReference GetPrimitiveType(PrimitiveTypeCode code) => code switch {
            PrimitiveTypeCode.Boolean => module.TypeSystem.Boolean, PrimitiveTypeCode.Byte => module.TypeSystem.Byte,
            PrimitiveTypeCode.SByte => module.TypeSystem.SByte, PrimitiveTypeCode.Char => module.TypeSystem.Char,
            PrimitiveTypeCode.Int16 => module.TypeSystem.Int16, PrimitiveTypeCode.UInt16 => module.TypeSystem.UInt16,
            PrimitiveTypeCode.Int32 => module.TypeSystem.Int32, PrimitiveTypeCode.UInt32 => module.TypeSystem.UInt32,
            PrimitiveTypeCode.Int64 => module.TypeSystem.Int64, PrimitiveTypeCode.UInt64 => module.TypeSystem.UInt64,
            PrimitiveTypeCode.Single => module.TypeSystem.Single, PrimitiveTypeCode.Double => module.TypeSystem.Double,
            PrimitiveTypeCode.String => module.TypeSystem.String, PrimitiveTypeCode.Object => module.TypeSystem.Object,
            _ => throw new InvalidOperationException("Invalid attribute primitive: " + code)
        };
        public TypeReference GetSystemType() => module.TypeSystem.CoreLibrary is AssemblyNameReference scope
            ? new TypeReference("System","Type",module,scope) : throw new InvalidOperationException("The original core library is absent.");
        public bool IsSystemType(TypeReference type) => type.FullName == "System.Type";
        public TypeReference GetSZArrayType(TypeReference elementType) => new ArrayType(elementType);
        public TypeReference GetTypeFromDefinition(MetadataReader reader,TypeDefinitionHandle handle,byte rawTypeKind) =>
            (TypeReference)module.LookupToken(MetadataTokens.GetToken(handle));
        public TypeReference GetTypeFromReference(MetadataReader reader,TypeReferenceHandle handle,byte rawTypeKind) =>
            (TypeReference)module.LookupToken(MetadataTokens.GetToken(handle));
        public TypeReference GetTypeFromSerializedName(string name)
        {
            return SerializedType(TypeName.Parse(name.AsSpan(),new TypeNameParseOptions { MaxNodes = int.MaxValue }));
        }
        private TypeReference SerializedType(TypeName name)
        {
            if (name.IsArray)
            {
                var array = new ArrayType(SerializedType(name.GetElementType()),name.GetArrayRank());
                if (name.IsVariableBoundArrayType && array.Rank == 1) array.Dimensions[0] = new ArrayDimension(0,null);
                return array;
            }
            if (name.IsByRef) return new ByReferenceType(SerializedType(name.GetElementType()));
            if (name.IsPointer) return new PointerType(SerializedType(name.GetElementType()));
            if (name.IsConstructedGenericType)
            {
                var instance = new GenericInstanceType(SerializedType(name.GetGenericTypeDefinition()));
                foreach (var argument in name.GetGenericArguments()) instance.GenericArguments.Add(SerializedType(argument));
                return instance;
            }
            var metadataName = MetadataName(name);
            if (name.AssemblyName == null && module.GetType(metadataName) is { } local) return local;
            var assembly = name.AssemblyName?.FullName ?? module.TypeSystem.CoreLibrary.ToString();
            var original = assembly == module.Assembly.Name.FullName ? module.Assembly
                : module.AssemblyResolver.Resolve(AssemblyNameReference.Parse(assembly));
            return module.ImportReference(SourceTypeIdentity.Definition(original,metadataName));
        }
        private static string MetadataName(TypeName name)
        {
            var escaped = name.IsNested ? name.Name : name.FullName;
            var text = new System.Text.StringBuilder(escaped.Length);
            for (var index = 0; index < escaped.Length; index++)
            {
                if (escaped[index] == '\\') index++;
                text.Append(escaped[index]);
            }
            return name.IsNested ? MetadataName(name.DeclaringType) + "/" + text : text.ToString();
        }
        public PrimitiveTypeCode GetUnderlyingEnumType(TypeReference type) => (PrimitiveTypeCode)(int)type.Resolve().Fields.Single(field => field.Name == "value__").FieldType.MetadataType;
    }
}
