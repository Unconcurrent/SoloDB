using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace SoloDatabase.Metadata.Generation;

public static class CSharpEmitter
{
    public static string Descriptors(IReadOnlyList<Model> models, string registrationType)
    {
        var output = new StringBuilder();
        foreach (var model in models)
        {
            string Type(string name) => model.HelperOnly ? Quote(model.TypeIdentities[name]) : "typeof(" + name + ")";
            string Types(IEnumerable<string> names) => "new " + (model.HelperOnly ? "string" : "global::System.Type") + "[]{" + string.Join(",",names.Select(Type)) + "}";
            output.Append("[assembly: global::SoloDatabase.Generated.CodecRegistration(").Append(Type(model.TypeName)).Append(',')
                .Append(model.SealedType ? "true" : "false").Append(',').Append(model.AbstractType ? "true" : "false").Append(',')
                .Append(model.ConstructFromMembers || model.Shape == TypeShape.Tuple ? "true" : "false").Append(',')
                .Append("new string[]{").Append(string.Join(",",model.Members.Select(member => Quote(member.Name)))).Append("},")
                .Append(Types(model.Members.Select(member => member.DeclaringType ?? model.TypeName))).Append(',')
                .Append(Types(model.Members.Select(member => member.TypeName))).Append(',');
            foreach (var facts in new Func<Member,bool>[] { member => member.Field, member => member.Read, member => member.Write, member => member.Ignored, member => member.ManyReference })
                output.Append("new bool[]{").Append(string.Join(",",model.Members.Select(member => facts(member) ? "true" : "false"))).Append("},");
            output.Append((int)model.Shape).Append(',').Append(Types(model.CollectionArguments)).Append(',').Append(model.PopulateCollection ? "true" : "false").Append(",UnionType=").Append(model.UnionType ? "true" : "false").Append(",RecordFields=").Append(model.RecordType ? "new string[]{" + string.Join(",",model.RecordFields.Select(Quote)) + "}" : "null").Append(",NonGenericEnumerable=").Append(model.NonGenericEnumerable ? "true" : "false").Append(",IndexedRead=").Append(model.IndexedRead == null ? "null" : Quote(model.IndexedRead)).Append(",IndexedWrite=").Append(model.IndexedWrite == null ? "null" : Quote(model.IndexedWrite)).Append(",HelperOnly=").Append(model.HelperOnly ? "true" : "false")
                .Append(",DictionaryReadOnly=").Append(model.DictionaryReadOnly ? "true" : "false")
                .Append(",Formattable=").Append(model.Formattable ? "true" : "false")
                .Append(",AssignableTo=new string[]{").Append(string.Join(",",model.AssignableTo.Select(Quote))).Append("}")
                .Append(",SupportsValuesWrapper=").Append(model.SupportsValuesWrapper ? "true" : "false")
                .Append(",Element=").Append(model.ElementType == null ? "null" : Quote(model.TypeIdentities[model.ElementType]))
                .Append(",SequenceElement=").Append(model.SequenceElementType == null ? "null" : Quote(model.TypeIdentities[model.SequenceElementType]));
            foreach (var pair in new (string Name,Func<Member,bool> Value)[] {
                ("References",member => member.Reference),
                ("HasPolicies",member => member.HasReferencePolicy),("Unique",member => member.Unique) })
                output.Append(',').Append(pair.Name).Append("=new bool[]{").Append(string.Join(",",model.Members.Select(member => pair.Value(member) ? "true" : "false"))).Append('}');
            foreach (var pair in new (string Name,Func<Member,int> Value)[] {
                ("OnDelete",member => member.OnDelete),("OnOwnerDelete",member => member.OnOwnerDelete),("OrderBy",member => member.OrderBy) })
                output.Append(',').Append(pair.Name).Append("=new int[]{").Append(string.Join(",",model.Members.Select(member => pair.Value(member)))).Append('}');
            output.Append(")]\n");
        }
        return output.ToString();

    }

    public static string Quote(string value)
    {
        var output = new StringBuilder("\"");
        foreach (var c in value)
        {
            switch (c)
            {
                case '\"': output.Append("\\\""); break;
                case '\\': output.Append("\\\\"); break;
                default:
                    if (c < ' ' || char.IsSurrogate(c)) output.Append("\\u").Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
                    else output.Append(c);
                    break;
            }
        }
        return output.Append('"').ToString();
    }
}
