using System;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

// Mechanical publication of original descriptive facts; policy stays shared.
internal static class GeneratedTypeDescriptions
{
    internal static void Register(ModuleDefinition module,ILProcessor il,AssemblyDefinition product,
        GeneratedModel model,GeneratedModel[] models)
    {
        var api = new GeneratedJsonAssembly.Api(module,product);
        void Boolean(bool value) => il.Emit(value ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
        void Array<T>(TypeReference element,T[] values,Action<T> value)
        {
            il.Emit(OpCodes.Ldc_I4,values.Length); il.Emit(OpCodes.Newarr,element);
            for(var index=0;index<values.Length;index++)
            {
                il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4,index); value(values[index]);
                il.Emit(element.FullName == "System.Boolean" ? OpCodes.Stelem_I1 : element.FullName == "System.Int32" ? OpCodes.Stelem_I4 : OpCodes.Stelem_Ref);
            }
        }
        void Strings(string[] values) => Array(module.TypeSystem.String,values,value => il.Emit(OpCodes.Ldstr,value));
        void Types(TypeReference[] values) => Array(api.Type,values,value => api.TypeToken(il,value));
        var template = api.ModelRegistration("TemplateParameter").ReturnType;
        void Templates(TypeReference?[] values) => Array(template,values,value =>
        {
            if(value == null) il.Emit(OpCodes.Call,api.ModelRegistration("TemplateUnavailable"));
            else GeneratedJsonAssembly.TypeTemplate(il,value,api);
        });
        var codec = GeneratedCodec.Model(model);
        var selected = CodecEmitter.writtenMembers(codec)
            .Concat(Enumerable.Range(0,codec.Members.Length).Where(index => codec.Members[index].Many && codec.Members[index].Readable && !codec.Members[index].Ignored)).ToArray();
        var members = selected.Select(index => codec.Members[index]).ToArray();
        var arguments = model.Type is ArrayType array ? new[] { array.ElementType }
            : model.CollectionArguments.Length != 0 ? model.CollectionArguments
            : model.Type is GenericInstanceType generic ? generic.GenericArguments.ToArray() : System.Array.Empty<TypeReference>();
        if(model.Facts.Shape == TypeShape.Tuple)
            arguments = CodecTupleEmitter.leaves(GeneratedCodec.Tuple(model,models)).Select(path => path.Last().Item2.Type).ToArray();
        var cases = model.UnionRoot == null ? model.UnionCases ?? System.Array.Empty<GeneratedUnionCase>() : System.Array.Empty<GeneratedUnionCase>();
        il.Emit(OpCodes.Ldc_I4,(int)model.Facts.Shape);
        Boolean(model.Facts.AbstractType); Boolean(model.Facts.RequiresTypeInformation); Boolean(model.Facts.RecordType);
        Boolean(model.Facts.NonGenericEnumerable); Types(arguments);
        Strings(members.Select(member => member.Name).ToArray());
        Types(members.Select(member => member.Type).ToArray());
        Array(module.TypeSystem.Boolean,members.Select(member => member.Many).ToArray(),Boolean);
        TypeReference? Signature(CodecMember<TypeReference,MethodReference,FieldReference> member)
        {
            var method = member.Getter?.Value ?? member.Setter?.Value;
            var declaring = method?.DeclaringType ?? member.Field?.Value.DeclaringType;
            var value = member.Getter?.Value.ReturnType ?? member.Field?.Value.FieldType ?? member.Setter?.Value.Parameters.Last().ParameterType;
            if (value == null || declaring == null || model.UnionRoot != null ||
                GeneratedJsonDefinitions.SameType(declaring.GetElementType(),model.Type.GetElementType())) return value;
            // Calls keep their original physical signature. Description operands
            // instead follow the acquired definition's inherited substitution.
            var physical = declaring.Resolve();
            TypeReference? current = model.Type.Resolve();
            while (current != null)
            {
                var definition = current.Resolve();
                if (definition.FullName == physical.FullName && definition.Module.Assembly.Name.FullName == physical.Module.Assembly.Name.FullName)
                    return OriginalArgumentSources.Close(value,current);
                current = definition.BaseType == null ? null : OriginalArgumentSources.Close(definition.BaseType,current);
            }
            return value;
        }
        Templates(selected.Select(index => model.Members[index].DescriptionSignature ?? Signature(codec.Members[index])).ToArray());
        Strings(cases.Select(item => item.Name).ToArray());
        Array(module.TypeSystem.Int32,cases.Select(item => item.Tag).ToArray(),tag => il.Emit(OpCodes.Ldc_I4,tag));
        Array(new ArrayType(module.TypeSystem.String),cases.Select(item => item.FieldNames).ToArray(),Strings);
        Array(new ArrayType(api.Type),cases.Select(item => item.FieldTypes).ToArray(),Types);
        Array(new ArrayType(template),cases.Select(item => item.FieldSignatures?.Select(value => (TypeReference?)value).ToArray() ?? item.Factory?.Parameters.Select(parameter => (TypeReference?)parameter.ParameterType).ToArray()
            ?? new TypeReference?[item.FieldNames.Length]).ToArray(),Templates);
        if(model.UnionRoot != null) api.TypeToken(il,model.UnionRoot); else il.Emit(OpCodes.Ldnull);
        Strings(model.Facts.EnumNames); Strings(model.Facts.EnumValues);
        if(model.Facts.Shape == TypeShape.Enum && model.ElementType != null) api.TypeToken(il,model.ElementType);
        else il.Emit(OpCodes.Ldnull);
        Boolean(model.Facts.EnumFlags);
        Boolean(codec.Members.Any(member => member.Name == "Id" && member.Field == null && member.Readable && member.Writable && member.Type.FullName == "System.Int64"));
        var identity = model.OriginalProperties?.FirstOrDefault(property => property.Identity != null)?.Member.Name;
        if(identity == null) il.Emit(OpCodes.Ldnull); else il.Emit(OpCodes.Ldstr,identity);
        Types(models.Where(candidate => !GeneratedJsonDefinitions.SameType(candidate.Type,model.Type) &&
            candidate.AssignableTo.Any(target => GeneratedJsonDefinitions.SameType(target,model.Type)))
            .Select(candidate => candidate.Type).ToArray());
        var definition = product.MainModule.GetType("SoloDatabase.TypeDescriptions").Methods.Single(method => method.Name == "RegisterFacts");
        var call = new GenericInstanceMethod(module.ImportReference(definition));
        call.GenericArguments.Add(module.ImportReference(model.Type));
        il.Emit(OpCodes.Call,call);
    }
}
