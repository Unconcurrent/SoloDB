using System;
using System.Collections.Generic;
using Mono.Cecil;
using SoloDatabase.Metadata.Generation;
using SoloDatabase.Metadata.CodecGeneration;

namespace SoloDatabase.Metadata.Tools;

// Compiler-established original identities paired with shared serialization facts.
public sealed class GeneratedModel
{
    public GeneratedModel(Model facts, TypeReference type, MethodReference? constructor, GeneratedMember[] members)
    { Facts = facts; Type = type; Constructor = constructor; Members = members; }
    public Model Facts { get; }
    public TypeReference[] AssignableTo { get; set; } = Array.Empty<TypeReference>();
    // Unavailable original interface facts use the existing exact-type writer guard.
    public TypeReference[] UnknownInterfaces { get; set; } = Array.Empty<TypeReference>();
    public TypeReference Type { get; }
    public MethodReference? Constructor { get; }
    public MethodReference? DefaultConstructor { get; set; }
    internal MethodReference? CloneConstructor { get; set; }
    public GeneratedMember[] Members { get; }
    internal GeneratedProperty[]? OriginalProperties { get; set; }
    internal CodecMember<TypeReference,MethodReference,FieldReference>[] SqlFields { get; set; } = Array.Empty<CodecMember<TypeReference,MethodReference,FieldReference>>();
    public TypeReference[] CollectionArguments { get; set; } = Array.Empty<TypeReference>();
    public TypeReference? ElementType { get; set; }
    public TypeReference? SequenceElement { get; set; }
    public GeneratedUnionCase[]? UnionCases { get; set; }
    public TypeReference? UnionRoot { get; set; }
    public bool UnionHostedReferences { get; set; }
    public ISet<string> AccessAssemblies { get; } = new HashSet<string>(StringComparer.Ordinal);
}

public sealed class GeneratedUnionCase
{
    public GeneratedUnionCase(string name, int tag, MethodReference? factory, string[] fieldNames, TypeReference[] fieldTypes)
    { Name = name; Tag = tag; Factory = factory; FieldNames = fieldNames; FieldTypes = fieldTypes; }
    public string Name { get; }
    public int Tag { get; }
    public MethodReference? Factory { get; }
    public string[] FieldNames { get; }
    public TypeReference[] FieldTypes { get; }
}

public sealed class GeneratedMember
{
    public GeneratedMember(TypeReference type, MethodReference? getter, MethodReference? setter, FieldReference? field)
    { Type = type; Getter = getter; Setter = setter; Field = field; QueryWritable = setter != null; }
    public TypeReference Type { get; }
    public MethodReference? Getter { get; }
    public MethodReference? Setter { get; }
    public FieldReference? Field { get; }
    public bool QueryWritable { get; set; }
    public bool StaticField { get; set; }

}


// The original public CLR declaration precedes JSON, SQL, identity and clone role selection.
internal sealed class GeneratedProperty
{
    internal GeneratedProperty(CodecMember<TypeReference,MethodReference,FieldReference> member, GeneratedIdGenerator? identity = null, bool privateGetter = false, bool privateSetter = false,string? virtualSlot = null,AttributeConstruction<TypeReference,MethodReference,FieldReference>[]? attributes = null,bool deferredSchema = false)
    { Member = member; Identity = identity; PrivateGetter = privateGetter; PrivateSetter = privateSetter; VirtualSlot = virtualSlot; DeferredSchema = deferredSchema; Attributes = attributes ?? Array.Empty<AttributeConstruction<TypeReference,MethodReference,FieldReference>>(); }
    internal CodecMember<TypeReference,MethodReference,FieldReference> Member { get; }
    internal GeneratedIdGenerator? Identity { get; }
    internal bool PrivateGetter { get; }
    internal bool PrivateSetter { get; }
    internal string? VirtualSlot { get; }
    internal bool DeferredSchema { get; }
    internal AttributeConstruction<TypeReference,MethodReference,FieldReference>[] Attributes { get; }
}

internal sealed class GeneratedIdGenerator
{
    internal GeneratedIdGenerator(TypeReference? type, MethodReference? constructor, bool family)
    { Type = type; Constructor = constructor; Family = family; }
    internal TypeReference? Type { get; }
    internal MethodReference? Constructor { get; }
    internal bool Family { get; }
}
