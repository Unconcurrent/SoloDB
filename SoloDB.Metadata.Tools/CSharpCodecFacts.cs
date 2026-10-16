using System;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

internal static class CSharpCodecFacts
{
    internal static TypeReference ImportType(ModuleDefinition module, TypeReference source)
    {
        if (source is ByReferenceType referenceType) return new ByReferenceType(ImportType(module,referenceType.ElementType));
        if (source is RequiredModifierType required) return new RequiredModifierType(ImportType(module,required.ModifierType),ImportType(module,required.ElementType));
        if (source is OptionalModifierType optional) return new OptionalModifierType(ImportType(module,optional.ModifierType),ImportType(module,optional.ElementType));
        if (source is ArrayType array)
        {
            var result = new ArrayType(ImportType(module,array.ElementType),array.Rank);
            for (var index = 0; index < array.Dimensions.Count; index++) result.Dimensions[index] = array.Dimensions[index];
            return result;
        }
        if (source is GenericInstanceType genericType)
        {
            var element = ImportType(module,genericType.ElementType);
            var result = new GenericInstanceType(element);
            foreach (var argument in genericType.GenericArguments) result.GenericArguments.Add(ImportType(module,argument));
            return result;
        }
        var reference = module.ImportReference(source);
        if (source is not GenericParameter && reference is not TypeDefinition)
            reference.IsValueType = source.Resolve().IsValueType;
        return reference;
    }

    internal static void Register(GeneratedJsonDefinitions definitions,AssemblyDefinition consumer, AssemblyDefinition product, ModuleDefinition module,
        TypeDefinition owner, ILProcessor entry, Action<string> access)
    {
        var index = 0;
        var models = new System.Collections.Generic.List<GeneratedModel>();
        var unions = new System.Collections.Generic.List<GeneratedModel>();
        var helperModels = new System.Collections.Generic.List<GeneratedModel>();
        foreach (var attribute in consumer.CustomAttributes.Where(attribute =>
            attribute.AttributeType.FullName == "SoloDatabase.Generated.CodecRegistrationAttribute"))
        {
            var arguments = attribute.ConstructorArguments;
            var helperOnly = attribute.Properties.Any(property => property.Name == "HelperOnly" && property.Argument.Value is true);
            TypeReference Source(object value) => value is TypeReference reference ? reference :
                SourceTypeIdentity.Resolve(consumer.MainModule,(string)value);
            bool Flag(string name) => attribute.Properties.Any(property => property.Name == name && property.Argument.Value is true);
            T[] Property<T>(string name) => attribute.Properties.FirstOrDefault(property => property.Name == name).Argument.Value is CustomAttributeArgument[] values
                ? values.Select(value => (T)value.Value).ToArray() : System.Array.Empty<T>();
            string? Text(string name) => attribute.Properties.FirstOrDefault(property => property.Name == name).Argument.Value as string;
            TypeReference ImportType(TypeReference source) => CSharpCodecFacts.ImportType(module,source);
            var sourceType = Source(arguments[0].Value);
            if (attribute.Properties.Any(property => property.Name == "UnionType" && property.Argument.Value is true)) {
                foreach (var union in GeneratedUnionMetadata.Collect(module,sourceType)) {
                    unions.Add(union);
                    access(sourceType.Resolve().Module.Assembly.Name.Name);
                    foreach (var name in union.AccessAssemblies) access(name);
                }
                continue;
            }

            var definition = sourceType.Resolve();
            var type = ImportType(sourceType);
            if (type is GenericInstanceType closedSource)
            {
                var original = new GenericInstanceType(definition);
                foreach (var argument in closedSource.GenericArguments) original.GenericArguments.Add(argument);
                sourceType = original;
            }
            if (sourceType is not ArrayType) type.IsValueType = definition.IsValueType;
            if (type is GenericInstanceType generic) generic.ElementType.IsValueType = definition.IsValueType;
            T[] Array<T>(int ordinal) => ((CustomAttributeArgument[])arguments[ordinal].Value).Select(value => (T)value.Value).ToArray();
            TypeReference[] Types(int ordinal) => ((CustomAttributeArgument[])arguments[ordinal].Value).Select(value => Source(value.Value)).ToArray();
            var collection = Types(13).Select(ImportType).ToArray();
            var populate = (bool)arguments[14].Value;
            var names = Array<string>(4);
            var sourceDeclaring = Types(5);
            var declaring = sourceDeclaring.Select(ImportType).ToArray();
            var values = Types(6).Select(ImportType).ToArray();
            var fields = Array<bool>(7);
            var read = Array<bool>(8);
            var write = Array<bool>(9);
            var ignored = Array<bool>(10);
            var many = Array<bool>(11);
            var references = Property<bool>("References");
            var policies = Property<bool>("HasPolicies"); var onDelete = Property<int>("OnDelete");
            var onOwnerDelete = Property<int>("OnOwnerDelete"); var unique = Property<bool>("Unique"); var order = Property<int>("OrderBy");
            if (new[] { declaring.Length,values.Length,fields.Length,read.Length,write.Length,ignored.Length,many.Length }.Any(count => count != names.Length))
                throw new InvalidOperationException("Inconsistent generated codec member facts.");
            var facts = new Member[names.Length];
            var members = new GeneratedMember[names.Length];
            var originalProperties = GeneratedRowFields.Properties(module,sourceType,access);
            for (var i = 0; i < names.Length; i++)
            {
                var declared = declaring[i];
                var memberOwner = sourceDeclaring[i].Resolve();
                declared.IsValueType = memberOwner.IsValueType;
                if (declared is GenericInstanceType closed) closed.ElementType.IsValueType = memberOwner.IsValueType;
                var effective = fields[i] ? null : originalProperties.SingleOrDefault(property =>
                    property.Member.Name == names[i] && GeneratedJsonDefinitions.SameType(property.Member.Type,values[i]) &&
                    GeneratedRowFields.IndexParameters(property.Member).Length == 0 &&
                    GeneratedJsonDefinitions.SameType(property.Member.Getter?.Value.DeclaringType ?? property.Member.Setter!.Value.DeclaringType,declared));
                if (!fields[i]) { read[i] &= effective?.Member.Getter != null; write[i] &= effective?.Member.Setter != null; }
                facts[i] = new Member(names[i],values[i].FullName,fields[i],read[i],write[i],ignored[i]) {
                    ManyReference = many[i], DeclaringType = declared.FullName,
                    Reference = references.Length != 0 && references[i],
                    HasReferencePolicy = policies.Length != 0 && policies[i], OnDelete = onDelete.Length == 0 ? 0 : onDelete[i],
                    OnOwnerDelete = onOwnerDelete.Length == 0 ? 3 : onOwnerDelete[i], Unique = unique.Length != 0 && unique[i],
                    OrderBy = order.Length == 0 ? 0 : order[i] };
                var staticField = false;
                FieldReference? field = null;
                MethodReference? getter = null;
                MethodReference? setter = null;
                if (!fields[i] && !read[i] && !write[i])
                {
                    // Indexed constructor facts need their value type, not an accessor.
                    members[i] = new GeneratedMember(module.ImportReference(values[i]),null,null,null);
                    continue;
                }
                if (fields[i])
                {
                    var selected = memberOwner.Fields.Single(field => field.Name == names[i]);
                    staticField = selected.IsStatic;
                    field = module.ImportReference(selected); field.DeclaringType = module.ImportReference(declared);
                }
                else
                {
                    getter = effective?.Member.Getter?.Value;
                    setter = effective?.Member.Setter?.Value;
                }
                members[i] = new GeneratedMember(module.ImportReference(values[i]),getter,setter,field) { StaticField = staticField, QueryWritable = setter != null || (field != null && !memberOwner.Fields.Single(candidate => candidate.Name == names[i]).IsInitOnly && !memberOwner.Fields.Single(candidate => candidate.Name == names[i]).IsLiteral) };
                access(memberOwner.Module.Assembly.Name.Name);
            }
            var construct = (bool)arguments[3].Value;
            var shape = (TypeShape)(int)arguments[12].Value;
            TypeReference Closed(TypeReference target)
            {
                if (target is GenericParameter parameter && parameter.Type == GenericParameterType.Type && type is GenericInstanceType instance)
                    return instance.GenericArguments[parameter.Position];
                if (target is ArrayType array) return new ArrayType(Closed(array.ElementType),array.Rank);
                if (target is GenericInstanceType genericType)
                {
                    var result = new GenericInstanceType(genericType.ElementType);
                    foreach (var argument in genericType.GenericArguments) result.GenericArguments.Add(Closed(argument));
                    return result;
                }
                return target;
            }
            var recordFieldFact = attribute.Properties.FirstOrDefault(property => property.Name == "RecordFields").Argument.Value as CustomAttributeArgument[];
            var recordFields = recordFieldFact?.Select(field => (string)field.Value).ToArray();
            var parameterTypes = recordFields != null
                ? recordFields.Select(name => values[System.Array.FindIndex(names, candidate => candidate == name)].FullName).ToArray()
                : values.Where((_,ordinal) => (!fields[ordinal] || shape == TypeShape.Tuple)).Select(value => value.FullName).ToArray();
            var constructor = sourceType is ArrayType ? null : definition.Methods.FirstOrDefault(method => method.IsConstructor && !method.IsStatic &&
                (shape == TypeShape.BsonDocument ? method.Parameters.Count == 1 && method.Parameters[0].ParameterType.FullName == "SoloDatabase.JsonSerializator.JsonValue" : construct ? method.Parameters.Select(parameter => Closed(parameter.ParameterType).FullName).SequenceEqual(parameterTypes) : method.IsPublic && method.Parameters.Count == 0));
            if (shape == TypeShape.CustomSequence)
                constructor = definition.Methods.FirstOrDefault(method => method.IsConstructor && !method.IsStatic && method.IsPublic &&
                    (populate ? method.Parameters.Count == 0 : method.Parameters.Count == 1 && Closed(method.Parameters[0].ParameterType).FullName == "System.Collections.Generic.IEnumerable`1<" + collection[0].FullName + ">"));
            MethodReference? constructorReference = null;
            if (constructor != null) { constructorReference = module.ImportReference(constructor); constructorReference.DeclaringType = module.ImportReference(type); }
            var policy = new Model(type.FullName,type.FullName,shape,type.IsValueType,(bool)arguments[1].Value,
                constructor != null && !construct,null,facts) { AbstractType = (bool)arguments[2].Value, ConstructFromMembers = construct, PopulateCollection = populate,
                    RecordType = recordFields != null, RecordFields = recordFields ?? System.Array.Empty<string>(),
                    RequiresTypeInformation = (bool)arguments[2].Value || HelperAssembly.InheritedAttribute(sourceType,"SoloDatabase.Attributes.PolymorphicAttribute"), SupportsValuesWrapper = Flag("SupportsValuesWrapper"),
                    NonGenericEnumerable = Flag("NonGenericEnumerable"),
                    DictionaryReadOnly = Flag("DictionaryReadOnly"), Formattable = Flag("Formattable"),
                    IndexedRead = attribute.Properties.FirstOrDefault(property => property.Name == "IndexedRead").Argument.Value as string,
                    IndexedWrite = attribute.Properties.FirstOrDefault(property => property.Name == "IndexedWrite").Argument.Value as string };
            TypeReference? OptionalType(string name) => Text(name) is string identity ? ImportType(SourceTypeIdentity.Resolve(consumer.MainModule,identity)) : null;
            var defaultConstructor = sourceType is ArrayType ? null : definition.Methods.FirstOrDefault(method => method.IsConstructor && !method.IsStatic && method.IsPublic && method.Parameters.Count == 0);
            MethodReference? defaultReference = null;
            if (defaultConstructor != null) { defaultReference = module.ImportReference(defaultConstructor); defaultReference.DeclaringType = type; }
            var generatedModel = new GeneratedModel(policy,type,constructorReference,members) {
                AssignableTo = Property<string>("AssignableTo").Select(identity => ImportType(SourceTypeIdentity.Resolve(consumer.MainModule,identity))).ToArray(),
                CollectionArguments = collection, ElementType = OptionalType("Element"), SequenceElement = OptionalType("SequenceElement"),
                DefaultConstructor = defaultReference,
                CloneConstructor = GeneratedRowFields.DefaultConstructor(module,sourceType) };
            generatedModel.OriginalProperties = originalProperties;
            if (shape == TypeShape.Object) {
                if (TypeSupport.SqlRowFieldFallback(type.IsValueType,policy.RecordType,generatedModel.OriginalProperties.Count(property => property.Member.Setter != null)))
                    generatedModel.SqlFields = GeneratedRowFields.Read(module,sourceType);
            }
            if (helperOnly) helperModels.Add(generatedModel); else models.Add(generatedModel);
            access(definition.Module.Assembly.Name.Name);
        }
        var carriers = new System.Collections.Generic.HashSet<string>(StringComparer.Ordinal);
        var sourceCarriers = new System.Collections.Generic.HashSet<string>(consumer.CustomAttributes.Where(attribute => attribute.AttributeType.FullName == "SoloDatabase.Generated.SourceQueryRegistrationAttribute" &&
            (string)attribute.ConstructorArguments[0].Value == "RegisterAnonymous").Select(attribute => SourceTypeIdentity.Resolve(consumer.MainModule,(string)((CustomAttributeArgument[])attribute.ConstructorArguments[1].Value)[0].Value).FullName),StringComparer.Ordinal);
        var carrierTypes = consumer.CustomAttributes.Where(attribute => attribute.AttributeType.FullName == "SoloDatabase.Generated.SourceQueryRegistrationAttribute" &&
            (string)attribute.ConstructorArguments[0].Value == "RegisterAnonymous")
            .Select(attribute => SourceTypeIdentity.Resolve(consumer.MainModule,(string)((CustomAttributeArgument[])attribute.ConstructorArguments[1].Value)[0].Value)).ToList();
        foreach (var type in carrierTypes)
        {
                    if (!carriers.Add(type.FullName)) continue;
                    var definition = type.Resolve();
                    TypeReference Closed(TypeReference source)
                    {
                        if (source is GenericParameter parameter && parameter.Type == GenericParameterType.Type && type is GenericInstanceType instance)
                            return module.ImportReference(instance.GenericArguments[parameter.Position]);
                        if (source is GenericInstanceType generic)
                        {
                            var result = new GenericInstanceType(module.ImportReference(generic.ElementType));
                            foreach (var argument in generic.GenericArguments) result.GenericArguments.Add(Closed(argument));
                            return result;
                        }
                        if (source is ArrayType array) return new ArrayType(Closed(array.ElementType),array.Rank);
                        return module.ImportReference(source);
                    }
                    var actual = module.ImportReference(type);
                    var properties = definition.Properties.ToArray();
                    var facts = properties.Select(property => new Member(property.Name,Closed(property.PropertyType).FullName,false,true,false,false)).ToArray();
                    var members = properties.Select(property => {
                        var getter = module.ImportReference(property.GetMethod); getter.DeclaringType = actual;
                        return new GeneratedMember(Closed(property.PropertyType),getter,null,null);
                    }).ToArray();
                    var constructor = module.ImportReference(definition.Methods.Single(method => method.IsConstructor && !method.IsStatic));
                    constructor.DeclaringType = actual;
                    var carrier = new GeneratedModel(new Model(actual.FullName,actual.FullName,TypeShape.Object,false,true,false,null,facts)
                        { ConstructFromMembers = true },actual,constructor,members) { OriginalProperties = GeneratedRowFields.Properties(module,type,access) };
                    if (sourceCarriers.Contains(type.FullName)) helperModels.Add(carrier); else models.Add(carrier);
                    access(definition.Module.Assembly.Name.Name);
                }
        GeneratedJsonAssembly.RegisterModels(definitions,module,owner,entry,product,models.Concat(helperModels).Concat(unions).ToArray(),index);
    }
}
