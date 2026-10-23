using System;
using System.Collections.Generic;
using System.Linq;
using Mono.Cecil;
using Microsoft.FSharp.Core;
using SoloDatabase.Metadata.CodecGeneration;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

// One helper generation owns these definitions; no table is emitted into the product.
internal sealed class GeneratedJsonDefinitions
{
    private readonly Dictionary<(string Role,string Target,string Representation,string Policy),MethodDefinition> methods;
    private readonly GeneratedJsonDefinitions? native;
    private readonly string[] references;
    private bool utf8VectorPrepared;
    private bool utf16VectorPrepared;
    private FSharpOption<CodecBase64VectorApi<TypeReference,MethodReference>> utf8Vector = null!;
    private FSharpOption<CodecBase64VectorApi<TypeReference,MethodReference>> utf16Vector = null!;

    internal GeneratedJsonDefinitions(string[]? references = null)
    { this.references = references ?? Array.Empty<string>(); methods = new(); }
    private GeneratedJsonDefinitions(GeneratedJsonDefinitions native)
    { this.native = native; references = native.references; methods = native.methods; Stored = true; }
    internal List<GeneratedSchemaIndexes.Witness>? SchemaWitnesses { get; set; }
    internal MethodDefinition? EmptyAttributes { get; set; }
    internal bool Stored { get; }
    internal string MethodName(string name) => Stored ? name + "Stored" : name;
    internal GeneratedJsonDefinitions NativeView => native ?? this;
    internal GeneratedJsonDefinitions StoredView() => new(this);
    private string Role(string role) => TypeSupport.OperationRole(Stored,role);

    internal FSharpOption<CodecBase64VectorApi<TypeReference,MethodReference>> Base64Vector(ModuleDefinition module,bool utf16)
    {
        if(native != null) return native.Base64Vector(module,utf16);
        if(utf16) {
            if(!utf16VectorPrepared) {
                utf16Vector=GeneratedBase64Vector.Read(module,references,true);
                utf16VectorPrepared=true;
            }
            return utf16Vector;
        }
        if(!utf8VectorPrepared) {
            utf8Vector=GeneratedBase64Vector.Read(module,references,false);
            utf8VectorPrepared=true;
        }
        return utf8Vector;
    }

    internal static string Identity(TypeReference type) => Identity(type,false);
    internal static string SignatureIdentity(TypeReference type) => Identity(type,true);
    internal static string SignatureIdentity(TypeReference type,Func<TypeReference,TypeReference> definition) => Identity(type,true,definition);
    private static string Identity(TypeReference type,bool signature,Func<TypeReference,TypeReference>? definition = null)
    {
        if (type is GenericInstanceType generic)
            return "G" + Identity(generic.ElementType,signature,definition) + string.Concat(generic.GenericArguments.Select(value => Part(Identity(value,signature,definition))));
        if (type is ArrayType array)
        {
            var dimensions = signature ? string.Concat(array.Dimensions.Select(dimension => Part(dimension.ToString()))) : "";
            return "A" + (array.IsVector ? "vector" : array.Rank.ToString(System.Globalization.CultureInfo.InvariantCulture)) + dimensions + Part(Identity(array.ElementType,signature,definition));
        }
        if (type is RequiredModifierType required) return "Q" + Part(Identity(required.ModifierType,signature,definition)) + Part(Identity(required.ElementType,signature,definition));
        if (type is OptionalModifierType optional) return "O" + Part(Identity(optional.ModifierType,signature,definition)) + Part(Identity(optional.ElementType,signature,definition));
        if (type is TypeSpecification specification)
            return type.MetadataType.ToString() + Part(Identity(specification.ElementType,signature,definition));
        if (type is GenericParameter parameter)
            return "P" + parameter.Type + ":" + parameter.Position + (signature ? "" : ":" + parameter.Owner);
        var assemblyName = type.Scope is ModuleDefinition definitionModule ? definitionModule.Assembly.Name.Name : type.Scope.Name;
        if (signature && (type.IsPrimitive || TypeSupport.ClrPrimitiveSignature(type.FullName,assemblyName))) return type.FullName;
        if (definition != null) type = definition(type);
        var scope = type.Scope is AssemblyNameReference assembly ? assembly.FullName
            : type.Scope is ModuleDefinition module ? module.Assembly.Name.FullName : type.Scope.Name;
        return "N" + Part(scope) + Part(type.FullName);
    }
    private static string Part(string value) => value.Length.ToString(System.Globalization.CultureInfo.InvariantCulture) + ":" + value;

    internal static bool SameType(TypeReference left,TypeReference right) => Identity(left) == Identity(right);

    internal bool TryGet(string role,TypeReference target,TypeReference representation,string policy,out MethodDefinition method)
        => methods.TryGetValue((Role(role),Identity(target),Identity(representation),policy),out method!);

    internal void Add(string role,TypeReference target,TypeReference representation,string policy,MethodDefinition method)
        => methods.Add((Role(role),Identity(target),Identity(representation),policy),method);
}
