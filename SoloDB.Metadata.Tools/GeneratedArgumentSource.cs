using System;
using System.Collections.Generic;
using Mono.Cecil;
using SoloDatabase.Metadata.CodecGeneration;

namespace SoloDatabase.Metadata.Tools;

// Original CLR input facts; no serializer shape or construction policy is implied.
public sealed class GeneratedArgumentSource
{
    internal GeneratedArgumentSource(TypeReference type, CodecMember<TypeReference,MethodReference,FieldReference>[] members)
    { Type = type; Members = members; }
    public TypeReference Type { get; }
    internal CodecMember<TypeReference,MethodReference,FieldReference>[] Members { get; }
    public ISet<string> AccessAssemblies { get; } = new HashSet<string>(StringComparer.Ordinal);
}
