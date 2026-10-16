using System.Collections.Generic;

namespace SoloDatabase.Metadata.Generation;

// These are design-time facts. No descriptor is carried into database execution.
public sealed class Member
{
    public Member(string name, string typeName, bool field, bool read, bool write, bool ignored)
    {
        Name = name;
        TypeName = typeName;
        Field = field;
        Read = read;
        Write = write;
        Ignored = ignored;
    }
    public string Name { get; }
    public string? DeclaringType { get; set; }
    public bool Reference { get; set; }
    public bool ManyReference { get; set; }
    public bool HasReferencePolicy { get; set; }
    public int OnDelete { get; set; }
    public int OnOwnerDelete { get; set; } = 3;
    public bool Unique { get; set; }
    public int OrderBy { get; set; }
    public bool HelperRead { get; set; }
    public bool HelperWrite { get; set; }
    public string TypeName { get; }
    public bool Field { get; }
    public bool Read { get; }
    public bool Write { get; }
    public bool Ignored { get; }
}

public sealed class Model
{
    public Model(string typeName, string runtimeName, TypeShape shape, bool valueType,
        bool sealedType, bool parameterlessConstructor, string? elementType, IReadOnlyList<Member> members)
    {
        TypeName = typeName;
        RuntimeName = runtimeName;
        Shape = shape;
        ValueType = valueType;
        SealedType = sealedType;
        ParameterlessConstructor = parameterlessConstructor;
        ElementType = elementType;
        Members = members;
    }
    public string[] AssignableTo { get; set; } = System.Array.Empty<string>();
    public bool HelperOnly { get; set; }
    public IReadOnlyDictionary<string,string> TypeIdentities { get; set; } = new Dictionary<string,string>();
    public string[] CollectionArguments { get; set; } = System.Array.Empty<string>();
    public bool PopulateCollection { get; set; }
    public bool NonGenericEnumerable { get; set; }
    public bool DictionaryReadOnly { get; set; }
    public bool Formattable { get; set; }
    public string? IndexedRead { get; set; }
    public string? IndexedWrite { get; set; }
    public string? SequenceElementType { get; set; }
    public string[] TypeArguments { get; set; } = System.Array.Empty<string>();
    public string TypeName { get; }
    public string RuntimeName { get; }
    public TypeShape Shape { get; }
    public bool ValueType { get; }
    public bool SealedType { get; }
    public bool ParameterlessConstructor { get; }
    public bool AbstractType { get; set; }
    public bool ConstructFromMembers { get; set; }
    public bool RecordType { get; set; }
    public bool UnionType { get; set; }
    public string[] RecordFields { get; set; } = System.Array.Empty<string>();
    public bool IdentityTrivialGetter { get; set; }
    public bool SupportsValuesWrapper { get; set; }
    public bool RequiresTypeInformation { get; set; }
    public string? IdentityType { get; set; }
    public string? KeyType { get; set; }
    public string? ElementType { get; }
    public IReadOnlyList<Member> Members { get; }
}
