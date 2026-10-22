using System;
using System.Collections.Generic;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.Generation;
using SoloDatabase.Metadata.CodecGeneration;

namespace SoloDatabase.Metadata.Tools;

// Codec construction and registration within the shared generated assembly owner.
public static partial class GeneratedJsonAssembly
{
    internal static void RegisterDefaultConstruction(ModuleDefinition module, TypeDefinition owner, ILProcessor il,
        AssemblyDefinition product, TypeReference type, MethodReference? constructor, int index)
    {
        var api = new Api(module,product);
        var create = Method(owner,"Default" + index,module.TypeSystem.Object);
        GeneratedCodec.DefaultConstruction(create,type,constructor);
        api.TypeToken(il,type);
        Delegate(il,api.Function(module.TypeSystem.Object),create);
        il.Emit(OpCodes.Call,module.ImportReference(product.MainModule.GetType("SoloDatabase.QueryValueMetadata")
            .Methods.Single(method => method.Name == "RegisterDefaultConstruction")));
    }

    internal static MethodDefinition MemberReader(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,
        TypeReference type,bool valueType,bool nullable,CodecMember<TypeReference,MethodReference,FieldReference> member,
        bool boxedReceiver,bool boxedValue,string suffix)
    {
        var result = boxedValue ? module.TypeSystem.Object : module.ImportReference(member.Type);
        var operation = member.Getter != null
            ? GeneratedJsonDefinitions.Identity(member.Getter.Value.DeclaringType) + member.Getter.Value.FullName
            : GeneratedJsonDefinitions.Identity(member.Field.Value.DeclaringType) + member.Field.Value.FullName;
        var policy = operation + "|" + valueType + "|" + nullable + "|" + boxedReceiver + "|" + boxedValue + "|" + member.Static;
        if (definitions.TryGet("member-read",type,result,policy,out var existing)) return existing;
        var getter = Method(owner,"ReadMember" + suffix,result,boxedReceiver ? module.TypeSystem.Object : module.ImportReference(type));
        QueryOperationEmitter.memberRead(new CecilCodecIL(getter),type,valueType,nullable,member,member.Type.IsValueType,boxedReceiver,boxedValue);
        definitions.Add("member-read",type,result,policy,getter);
        return getter;
    }

    internal static void RegisterMemberReader(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,ILProcessor il,
        AssemblyDefinition product,TypeReference type,bool valueType,bool nullable,
        CodecMember<TypeReference,MethodReference,FieldReference> member,string suffix)
    {
        var api = new Api(module,product);
        var getter = MemberReader(definitions,module,owner,type,valueType,nullable,member,valueType,valueType,suffix);
        if (valueType)
        {
            api.TypeToken(il,type); il.Emit(OpCodes.Ldstr,member.Name);
            il.Emit(member.Field != null ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0); api.TypeToken(il,member.Type);
            Delegate(il,api.Function(module.TypeSystem.Object,module.TypeSystem.Object),getter);
            il.Emit(OpCodes.Call,module.ImportReference(product.MainModule.GetType("SoloDatabase.QueryValueMetadata")
                .Methods.Single(method => method.Name == "RegisterGeneratedReader")));
        }
        else
        {
            il.Emit(OpCodes.Ldstr,member.Name); il.Emit(member.Field != null ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
            Delegate(il,api.Function(type,member.Type),getter);
            il.Emit(OpCodes.Call,api.MemberRegistration(type,member.Type));
        }
    }

    internal static void RegisterObject(GeneratedJsonDefinitions definitions,ModuleDefinition module, TypeDefinition owner, ILProcessor il,
        AssemblyDefinition product, GeneratedModel model, int index, GeneratedModel[] models)
    {
        var api = new Api(module, product);
        var type = module.ImportReference(model.Type);
        var references = new GeneratedQueryReferences(module,product);
        if (model.Facts.ValueType)
        {
            references.Box(owner,il,type);
            var accessors = model.Members.Where(member => member.Getter != null && member.Setter != null)
                .SelectMany(member => new[] { new GeneratedQueryCall(member.Getter!,member.Type,Array.Empty<TypeReference>()),
                    new GeneratedQueryCall(member.Setter!,module.TypeSystem.Void,new[] { member.Type }) }).ToArray();
            GeneratedQueryCalls.Register(module,owner,il,product,accessors);
        }
        foreach (var member in model.Members)
            if (member.Field != null && member.QueryWritable)
                references.Field(owner,il,member.Field.DeclaringType,member.Field,member.Type,member.StaticField);
        var codec = GeneratedCodec.Model(model);
        var memberFacts = codec.Members;
        for (var memberIndex = 0; memberIndex < memberFacts.Length; memberIndex++)
            if (memberFacts[memberIndex].Readable)
                RegisterMemberReader(definitions,module,owner,il,product,type,model.Facts.ValueType,false,memberFacts[memberIndex],index + "_" + memberIndex);
        for (var memberIndex = 0; memberIndex < model.Members.Length; memberIndex++)
        {
            var fact = model.Facts.Members[memberIndex];
            if (!fact.Write && !model.Members[memberIndex].QueryWritable) continue;
            var member = model.Members[memberIndex];
            var writeMember = Method(owner,"AssignMember" + index + "_" + memberIndex,module.TypeSystem.Void,module.TypeSystem.Object,module.TypeSystem.Object);
            QueryOperationEmitter.memberWrite(new CecilCodecIL(writeMember),type,model.Facts.ValueType,
                memberFacts[memberIndex],true,false,true);
            api.TypeToken(il,type); il.Emit(OpCodes.Ldstr,fact.Name);
            il.Emit(fact.Field ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0); api.TypeToken(il,member.Type);
            var action = new GenericInstanceType(new TypeReference("System","Action`2",module,module.AssemblyReferences[0]));
            action.GenericArguments.Add(module.TypeSystem.Object); action.GenericArguments.Add(module.TypeSystem.Object);
            Delegate(il,action,writeMember);
            il.Emit(OpCodes.Call,module.ImportReference(product.MainModule.GetType("SoloDatabase.QueryValueMetadata").Methods.Single(method => method.Name == "RegisterGeneratedSetter")));
        }
        if (model.Constructor != null && model.Facts.Shape == TypeShape.Object)
        {
            var arguments = model.Facts.ConstructFromMembers
                ? codec.ConstructorMembers.Select(ordinal => memberFacts[ordinal].Type).ToArray()
                : model.Constructor.Parameters.Select(parameter => module.ImportReference(parameter.ParameterType)).ToArray();
            RegisterConstruction(module,owner,il,product,model,arguments,index);
        }
        if (CodecPreparation.dynamicCompanion(model.Facts.Shape,model.Facts.NonGenericEnumerable)) il.Emit(OpCodes.Call,api.Metadata("RegisterDynamicObject"));
        var witnesses = new List<GeneratedQueryWitness>();
        if (model.UnionCases != null)
        {
            if (model.UnionRoot == null)
            {
                if (model.UnionCases.Length > 1) GeneratedQueries.PreserveMembers(module,il.Body.Method,product,type,128 | 256);
                TypeReference root=type is GenericInstanceType closed ? closed.ElementType : type;
                var tags=new TypeReference("","Tags",module,root.Scope) { DeclaringType=root };
                foreach (var item in model.UnionCases.Where(item => item.Factory != null))
                {
                    witnesses.Add(new GeneratedQueryWitness(item.Factory!.Name,type));
                    if (model.UnionCases.Length > 1)
                    {
                        witnesses.Add(new GeneratedQueryWitness(item.Name,tags));
                    }
                }
            }
        }
        GeneratedQueries.Preserve(module,il.Body.Method,product,witnesses.ToArray());
        if (model.UnionCases != null && model.UnionRoot == null)
        {
            var nullBacked = !model.Facts.Members.Any(member => member.Name == "Tag");
            il.Emit(nullBacked ? OpCodes.Ldc_I4_0 : OpCodes.Ldc_I4_1);
            il.Emit(OpCodes.Ldc_I4,model.UnionCases.Length); il.Emit(OpCodes.Newarr,module.TypeSystem.Int32);
            for (var ordinal=0;ordinal<model.UnionCases.Length;ordinal++)
            { il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4,ordinal); il.Emit(OpCodes.Ldc_I4,model.UnionCases[ordinal].Tag); il.Emit(OpCodes.Stelem_I4); }
            il.Emit(OpCodes.Ldc_I4,model.UnionCases.Length); il.Emit(OpCodes.Newarr,new ArrayType(module.TypeSystem.String));
            for (var ordinal=0;ordinal<model.UnionCases.Length;ordinal++)
            {
                var fields=model.UnionCases[ordinal].FieldNames;
                il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4,ordinal); il.Emit(OpCodes.Ldc_I4,fields.Length); il.Emit(OpCodes.Newarr,module.TypeSystem.String);
                for(var field=0;field<fields.Length;field++)
                { il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4,field); il.Emit(OpCodes.Ldstr,fields[field]); il.Emit(OpCodes.Stelem_Ref); }
                il.Emit(OpCodes.Stelem_Ref);
            }
            il.Emit(OpCodes.Ldc_I4,model.UnionCases.Length); il.Emit(OpCodes.Newarr,new ArrayType(api.Type));
            for (var ordinal=0;ordinal<model.UnionCases.Length;ordinal++)
            {
                var fields=model.UnionCases[ordinal].FieldTypes;
                il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4,ordinal); il.Emit(OpCodes.Ldc_I4,fields.Length); il.Emit(OpCodes.Newarr,api.Type);
                for(var field=0;field<fields.Length;field++)
                { il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4,field); api.TypeToken(il,fields[field]); il.Emit(OpCodes.Stelem_Ref); }
                il.Emit(OpCodes.Stelem_Ref);
            }
            il.Emit(OpCodes.Call,api.Metadata("RegisterUnionStorage",type));
            var tagMethod = new MethodReference(nullBacked ? "GetTag" : "get_Tag",module.TypeSystem.Int32,type) { HasThis=!nullBacked };
            if (nullBacked)
            {
                TypeReference parameter=type;
                if (type is GenericInstanceType closed)
                {
                    var signature=new GenericInstanceType(closed.ElementType);
                    foreach (var argument in closed.ElementType.GenericParameters) signature.GenericArguments.Add(argument);
                    parameter=signature;
                }
                tagMethod.Parameters.Add(new ParameterDefinition(parameter));
            }
            foreach (var item in model.UnionCases.Where(item => item.Factory != null))
            {
                var create = Method(owner,"CreateUnionCase" + index + "_" + item.Tag,module.TypeSystem.Object,module.TypeSystem.Object,new ArrayType(module.TypeSystem.Object));
                QueryOperationEmitter.call(new CecilCodecIL(create),new GeneratedQueryReferences(module,product).Reference,new GeneratedQueryReferences(module,product).Arguments,new GeneratedQueryReferences(module,product).Receiver(type),type,model.Facts.ValueType,false,true,item.Factory!,item.FieldTypes,type,model.Facts.ValueType,false,true);
                var test=Method(owner,"TestUnionCase" + index + "_" + item.Tag,module.TypeSystem.Boolean,module.TypeSystem.Object);
                QueryOperationEmitter.unionTest(new CecilCodecIL(test),type,model.Facts.ValueType,tagMethod,nullBacked,item.Tag);
                var factory=Method(owner,"UnionFactory" + index + "_" + item.Tag,module.TypeSystem.Object,new ArrayType(module.TypeSystem.Object));
                QueryOperationEmitter.withoutReceiver(new CecilCodecIL(factory),create);
                api.TypeToken(il,type); il.Emit(OpCodes.Ldc_I4,item.Tag);
                Delegate(il,api.Function(new ArrayType(module.TypeSystem.Object),module.TypeSystem.Object),factory);
                Delegate(il,api.Function(module.TypeSystem.Object,module.TypeSystem.Boolean),test);
                il.Emit(OpCodes.Call,module.ImportReference(product.MainModule.GetType("SoloDatabase.QueryValueMetadata").Methods.Single(method => method.Name == "RegisterGeneratedCase")));
            }
        }
        var write = Writer(owner, model, index, api);
        var read = Reader(owner, model, index, api);
        Delegate(il, api.Function(type, api.Json), write);
        Delegate(il, api.Function(api.Json, type), read);
        if (model.Facts.Shape == TypeShape.CustomSequence)
        {
            il.Emit(OpCodes.Call,api.SequenceCodec(type,model.CollectionArguments[0]));
        }
        else
        {
            if (model.Facts.Shape is TypeShape.CustomDictionary or TypeShape.CustomReadOnlyDictionary)
            {
                api.TypeToken(il,model.CollectionArguments[1]);
                il.Emit(OpCodes.Call,api.ShapeMethod("DictionaryValues"));
            }
            else if (model.Facts.NonGenericEnumerable || model.Facts.Shape is TypeShape.BsonDocument or TypeShape.NonGenericDictionary) il.Emit(OpCodes.Call,api.ShapeMethod("get_Uninterpreted"));
            else Shape(il, owner, model, index, api);
            il.Emit(OpCodes.Newobj, api.CodecConstructor(type));
        }
        il.Emit(OpCodes.Call, api.Metadata("RegisterGenerated", type));
        GeneratedRawCodec.Register(definitions,module,owner,il,product,model,index,models);
    }

    private static void RegisterFacts(ILProcessor il, GeneratedModel model, Api api)
    {
        var type = model.Type;
        var names = model.Facts.Members.Select(member => member.Name).ToArray();
        var values = model.Members.Select(member => member.Type).ToArray();
        var fields = model.Facts.Members.Select(member => member.Field).ToArray();
        var element = model.SequenceElement;
        api.TypeToken(il,type);
        il.Emit(OpCodes.Ldc_I4,names.Length); il.Emit(OpCodes.Newarr,api.Module.TypeSystem.String);
        for (var index = 0; index < names.Length; index++)
        { il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4,index); il.Emit(OpCodes.Ldstr,names[index]); il.Emit(OpCodes.Stelem_Ref); }
        il.Emit(OpCodes.Ldc_I4,values.Length); il.Emit(OpCodes.Newarr,api.Type);
        for (var index = 0; index < values.Length; index++)
        { il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4,index); api.TypeToken(il,values[index]); il.Emit(OpCodes.Stelem_Ref); }
        il.Emit(OpCodes.Ldc_I4,fields.Length); il.Emit(OpCodes.Newarr,api.Module.TypeSystem.Boolean);
        for (var index = 0; index < fields.Length; index++)
        { il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4,index); il.Emit(fields[index] ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0); il.Emit(OpCodes.Stelem_I1); }
        if (model.Facts.StandardComparison) MemberTemplates(il,model,api);
        il.Emit(OpCodes.Call,api.ModelRegistration(model.Facts.StandardComparison ? "RegisterOriginalMembers" : "RegisterMembers"));
        if (element != null) { api.TypeToken(il,type); api.TypeToken(il,element); il.Emit(OpCodes.Call,api.ModelRegistration("RegisterSequence")); }
    }

    private static void MemberTemplates(ILProcessor il, GeneratedModel model, Api api)
    {
        var template = api.ModelRegistration("TemplateParameter").ReturnType;
        void Original(TypeReference value)
        {
            if (value is GenericParameter parameter)
            {
                il.Emit(OpCodes.Ldc_I4,parameter.Position);
                il.Emit(OpCodes.Call,api.ModelRegistration("TemplateParameter"));
            }
            else if (value is ArrayType array)
            {
                il.Emit(OpCodes.Ldc_I4,array.Rank); Original(array.ElementType);
                il.Emit(OpCodes.Call,api.ModelRegistration("TemplateElement"));
            }
            else
            {
                var arguments = value is GenericInstanceType closed ? closed.GenericArguments.ToArray() : Array.Empty<TypeReference>();
                api.TypeToken(il,value is GenericInstanceType instance ? instance.ElementType : value);
                il.Emit(OpCodes.Ldc_I4,arguments.Length); il.Emit(OpCodes.Newarr,template);
                for (var index = 0; index < arguments.Length; index++)
                { il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4,index); Original(arguments[index]); il.Emit(OpCodes.Stelem_Ref); }
                il.Emit(OpCodes.Call,api.ModelRegistration("TemplateNamed"));
            }
        }
        il.Emit(OpCodes.Ldc_I4,model.Members.Length); il.Emit(OpCodes.Newarr,template);
        for (var index = 0; index < model.Members.Length; index++)
        {
            var member = model.Members[index];
            var original = member.Getter?.ReturnType ?? member.Field?.FieldType ?? member.Setter?.Parameters.Last().ParameterType;
            il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4,index);
            if (original == null) il.Emit(OpCodes.Call,api.ModelRegistration("TemplateUnavailable")); else Original(original);
            il.Emit(OpCodes.Stelem_Ref);
        }
    }

    private static MethodDefinition Writer(TypeDefinition owner, GeneratedModel model, int index, Api api)
    {
        var method = Method(owner, "Write" + index, api.Json, api.Module.ImportReference(model.Type));
        GeneratedCodec.Writer(method, model, api);
        return method;
    }

    private static MethodDefinition Reader(TypeDefinition owner, GeneratedModel model, int index, Api api)
    {
        var method = Method(owner, "Read" + index, api.Module.ImportReference(model.Type), api.Json);
        GeneratedCodec.Reader(method, model, api);
        return method;
    }

    private static void Shape(ILProcessor il, TypeDefinition owner, GeneratedModel model, int index, Api api)
    {
        var type = api.Module.ImportReference(model.Type);
        var codec = GeneratedCodec.Model(model);
        var included = CodecEmitter.writtenMembers(codec);
        il.Emit(OpCodes.Ldc_I4, included.Length); il.Emit(OpCodes.Newarr, api.StoredMember);
        for (var position = 0; position < included.Length; position++)
        {
            var i = included[position]; var fact = model.Facts.Members[i]; var member = model.Members[i];
            var getter = Method(owner, "Member" + index + "_" + i, api.Module.ImportReference(member.Type), type);
            GeneratedCodec.Getter(getter, codec, i);
            il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4, position);
            api.TypeToken(il, member.Field?.DeclaringType ?? member.Getter!.DeclaringType);
            il.Emit(OpCodes.Ldstr, fact.Name); il.Emit(OpCodes.Ldstr, fact.Name); il.Emit(fact.Field ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
            Delegate(il, api.Function(type, member.Type), getter);
            il.Emit(OpCodes.Call, api.Metadata("Member", type, member.Type)); il.Emit(OpCodes.Stelem_Ref);
        }
        il.Emit(OpCodes.Call, api.ShapeMethod("ObjectMembers"));
    }

    internal static void RegisterConstruction(ModuleDefinition module, TypeDefinition owner, ILProcessor il,
        AssemblyDefinition product, GeneratedModel model, TypeReference[] arguments, int index)
    {
        var api = new Api(module,product);
        var type = module.ImportReference(model.Type);
        var create = Method(owner,"Construct" + index,module.TypeSystem.Object,new ArrayType(module.TypeSystem.Object));
        GeneratedCodec.Construction(create,model,arguments);
        api.TypeToken(il,type);
        il.Emit(OpCodes.Ldc_I4,arguments.Length); il.Emit(OpCodes.Newarr,api.Type);
        for (var ordinal = 0; ordinal < arguments.Length; ordinal++)
        { il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4,ordinal); api.TypeToken(il,arguments[ordinal]); il.Emit(OpCodes.Stelem_Ref); }
        Delegate(il,api.Function(new ArrayType(module.TypeSystem.Object),module.TypeSystem.Object),create);
        il.Emit(OpCodes.Call,api.ConstructionRegistration());
    }

}
