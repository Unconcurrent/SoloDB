using System;
using System.Globalization;
using System.Linq;
using Mono.Cecil;

namespace SoloDatabase.Metadata.Tools;

// Decodes compiler-established closed identities only during helper completion.
internal static class SourceTypeIdentity
{
    internal static TypeReference Resolve(ModuleDefinition consumer, string identity)
    {
        var position = 0;
        int Number()
        {
            var end = identity.IndexOf(':',position);
            if (end < 0) throw new InvalidOperationException("Incomplete generated type identity.");
            var value = int.Parse(identity.Substring(position,end-position),CultureInfo.InvariantCulture);
            position = end+1;
            return value;
        }
        string Text()
        {
            var length = Number();
            var value = identity.Substring(position,length);
            position += length;
            return value;
        }
        TypeReference Type()
        {
            var kind = identity[position++];
            if (kind == 'R') return new ByReferenceType(Type());
            if (kind == 'A')
            {
                var rank = Number();
                return new ArrayType(Type(),rank);
            }
            if (kind == 'O')
            {
                var propertyCount = Number();
                var names = new string[propertyCount]; var values = new TypeReference[propertyCount];
                for (var index=0; index<propertyCount; index++) { names[index] = Text(); values[index] = Type(); }
                var anonymousDefinition = consumer.Types.Single(type => type.Name.StartsWith("<>f__AnonymousType",StringComparison.Ordinal) &&
                    type.CustomAttributes.Any(attribute => attribute.AttributeType.FullName == "System.Runtime.CompilerServices.CompilerGeneratedAttribute") &&
                    type.GenericParameters.Count == propertyCount && type.Properties.Select(property => property.Name).SequenceEqual(names) &&
                    type.Properties.Select((property,index) => property.PropertyType is GenericParameter parameter && parameter.Position == index).All(value => value));
                if (propertyCount == 0) return anonymousDefinition;
                var anonymousInstance = new GenericInstanceType(anonymousDefinition);
                foreach (var value in values) anonymousInstance.GenericArguments.Add(value);
                return anonymousInstance;
            }
            if (kind != 'N') throw new InvalidOperationException("Unknown generated type identity.");
            var assemblyName = Text();
            var name = Text();
            var count = Number();
            var assembly = consumer.Assembly.Name.FullName == assemblyName ? consumer.Assembly :
                consumer.AssemblyResolver.Resolve(AssemblyNameReference.Parse(assemblyName));
            var definition = assembly.MainModule.GetType(name) ??
                assembly.MainModule.ExportedTypes.SingleOrDefault(type => type.FullName == name)?.Resolve() ??
                throw new InvalidOperationException("The exact generated type is absent from current inputs: " + name + ", " + assemblyName);
            if (definition.GenericParameters.Count != count)
                throw new InvalidOperationException("The generated type arguments do not match current inputs: " + name);
            if (count == 0) return definition;
            var closed = new GenericInstanceType(definition);
            for (var index=0; index<count; index++) closed.GenericArguments.Add(Type());
            return closed;
        }
        var result = Type();
        if (position != identity.Length) throw new InvalidOperationException("Unexpected generated type identity tail.");
        return result;
    }
}
