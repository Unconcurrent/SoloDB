using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.FSharp.Core;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.Generation;
using SoloDatabase.Metadata.CodecGeneration;

namespace SoloDatabase.Metadata.Tools;

internal static class GeneratedTuples
{
    internal static void Register(GeneratedJsonDefinitions definitions,ModuleDefinition module, TypeDefinition owner, ILProcessor entry,
        AssemblyDefinition product, GeneratedModel model, int index, GeneratedModel[] models, bool includeRows = true)
    {
        var api = new Api(module, owner, product);
        var type = module.ImportReference(model.Type);
        var leaves = CodecTupleEmitter.leaves(GeneratedCodec.Tuple(model,models));
        GeneratedModel Find(TypeReference value) => models.First(candidate => GeneratedJsonDefinitions.SameType(candidate.Type,value));
        for (var ordinal = 0; ordinal < model.Members.Length; ordinal++)
        {
            var member = model.Members[ordinal];
            if (model.Facts.ValueType && member.Field != null)
                new GeneratedQueryReferences(module,product).Field(owner,entry,type,member.Field,member.Type,false);
            var readMember = api.Method(owner,"ReadTuplePhysical" + index + "_" + ordinal,module.TypeSystem.Object,true,module.TypeSystem.Object);
            QueryOperationEmitter.memberRead(new CecilCodecIL(readMember),type,model.Facts.ValueType,false,
                GeneratedCodec.Model(model).Members[ordinal],member.Type.IsValueType,true,true);
            entry.Emit(OpCodes.Ldtoken,type); entry.Emit(OpCodes.Call,api.TypeHandle());
            entry.Emit(OpCodes.Ldstr,ordinal == 7 ? "Rest" : "Item" + (ordinal+1));
            entry.Emit(member.Field != null ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
            entry.Emit(OpCodes.Ldtoken,module.ImportReference(member.Type)); entry.Emit(OpCodes.Call,api.TypeHandle());
            api.Delegate(entry,api.Function(module.TypeSystem.Object,module.TypeSystem.Object),readMember);
            entry.Emit(OpCodes.Call,api.Product("QueryValueMetadata","RegisterGeneratedReader"));
        }
        var array = new ArrayType(api.Json);
        var write = api.Method(owner, "WriteTuple" + index, array, true, type);
        var codecApi = new GeneratedJsonAssembly.Api(module,product);
        GeneratedCodec.TupleWriter(write,model,models,codecApi);
        var read = api.Method(owner, "ReadTuple" + index, type, true, api.Json);
        GeneratedCodec.TupleReader(read,model,models,codecApi);
        api.Delegate(entry, api.Function(type, array), write);
        api.Delegate(entry, api.Function(api.Json, type), read);
        var stored = module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonStoredMember"));
        entry.Emit(OpCodes.Ldc_I4, leaves.Length); entry.Emit(OpCodes.Newarr, stored);
        for (var i = 0; i < leaves.Length; i++)
        {
            var leaf = leaves[i][leaves[i].Length - 1].Item2; var getter = api.Method(owner, "TupleMember" + index + "_" + i, module.ImportReference(leaf.Type), true, type);
            GeneratedCodec.TupleGetter(getter,model,models,i);
            entry.Emit(OpCodes.Dup); entry.Emit(OpCodes.Ldc_I4, i);
            entry.Emit(OpCodes.Ldtoken, type); entry.Emit(OpCodes.Call, api.TypeHandle());
            entry.Emit(OpCodes.Ldstr, "Item" + (i + 1)); entry.Emit(OpCodes.Ldstr, "Item" + (i + 1));
            entry.Emit(model.Facts.ValueType ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
            api.Delegate(entry, api.Function(type, leaf.Type), getter);
            entry.Emit(OpCodes.Call, api.Product("JsonSerializator.JsonMetadata", "Member", type, leaf.Type)); entry.Emit(OpCodes.Stelem_Ref);
        }
        entry.Emit(OpCodes.Call, api.Product("JsonSerializator.JsonMetadata", "RegisterTuple", type));
        GeneratedRawCodec.Register(definitions,module,owner,entry,product,model,index,models);
        GeneratedJsonAssembly.RegisterConstruction(module,owner,entry,product,model,model.Members.Select(member => module.ImportReference(member.Type)).ToArray(),index);
        if (includeRows)
        {
            RegisterRows(api, entry, model, index, leaves, Find, models);
            RegisterDefault(api, entry, model, index, models);
        }
    }

    private static void RegisterRows(Api api, ILProcessor entry, GeneratedModel model, int index, Tuple<bool,CodecMember<TypeReference,MethodReference,FieldReference>>[][] leaves, Func<TypeReference, GeneratedModel> find, GeneratedModel[] models)
    {
        var module = api.Module; var type = module.ImportReference(model.Type);
        var shape = module.ImportReference(api.Definition("SqlRowShape")); var row = module.ImportReference(api.Definition("SqlRow"));
        var mapper = new TypeDefinition("", "TupleRow" + index, TypeAttributes.NestedPrivate | TypeAttributes.Sealed, api.Owner.BaseType); api.Owner.NestedTypes.Add(mapper);
        var start = new FieldDefinition("start", FieldAttributes.Private | FieldAttributes.InitOnly, module.TypeSystem.Int32); mapper.Fields.Add(start);
        var ctor = api.Method(mapper, ".ctor", module.TypeSystem.Void, false, module.TypeSystem.Int32);
        ctor.Attributes |= MethodAttributes.SpecialName | MethodAttributes.RTSpecialName;
        var il = ctor.Body.GetILProcessor(); il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Call, new MethodReference(".ctor", module.TypeSystem.Void, api.Owner.BaseType) { HasThis = true });
        il.Emit(OpCodes.Ldarg_0); il.Emit(OpCodes.Ldarg_1); il.Emit(OpCodes.Stfld, start); il.Emit(OpCodes.Ret);
        var read = api.Method(mapper,"Read",type,false,row); il = read.Body.GetILProcessor();
        SqlRowEmitter.tupleRow(new CecilCodecIL(read),GeneratedSqlRows.Api(module,api.ProductAssembly,models),GeneratedCodec.Tuple(model,models),
            new Action<int>(position => {
                il.Emit(OpCodes.Ldarg_0); il.Emit(OpCodes.Ldfld,start);
                il.Emit(OpCodes.Ldc_I4,position); il.Emit(OpCodes.Add);
            }));
        var factory = api.Method(api.Owner, "BindTupleRow" + index, api.Function(row, type), true, shape); il = factory.Body.GetILProcessor();
        il.Emit(OpCodes.Ldarg_0); il.Emit(OpCodes.Ldstr, model.Type.Name); il.Emit(OpCodes.Ldc_I4, leaves.Length);
        il.Emit(OpCodes.Callvirt, api.Product("SqlRowShape", "RequireTupleColumns"));
        il.Emit(OpCodes.Ldarg_0); il.Emit(OpCodes.Callvirt, api.Product("SqlRowShape", "get_Start")); il.Emit(OpCodes.Newobj, ctor);
        il.Emit(OpCodes.Ldftn, read); il.Emit(OpCodes.Newobj, api.DelegateConstructor((GenericInstanceType)factory.ReturnType)); il.Emit(OpCodes.Ret);
        api.Delegate(entry, api.Function(shape, factory.ReturnType), factory);
        var mapping = api.Closed(module.ImportReference(api.Definition("SqlRowMapping`1")), type);
        entry.Emit(OpCodes.Newobj, api.OnType("SqlRowMapping`1", ".ctor", mapping)); entry.Emit(OpCodes.Call, api.OnType("SqlRowMapping`1", "Register", mapping));
    }

    private static void RegisterDefault(Api api,ILProcessor entry,GeneratedModel model,int index,GeneratedModel[] models)
    {
        var module = api.Module; var serial = 0;
        var exception = new TypeReference("System","Exception",module,api.Owner.BaseType.Scope);
        var wrapped = new TypeReference("System.Reflection","TargetInvocationException",module,api.Owner.BaseType.Scope);
        var wrapper = new MethodReference(".ctor",module.TypeSystem.Void,wrapped) { HasThis = true };
        wrapper.Parameters.Add(new ParameterDefinition(exception));
        var create = SqlTupleDefaultEmitter.prepare(GeneratedCodec.Tuple(model,models),
            new Func<TypeReference,bool>(target => target.IsValueType),
            new Func<TypeReference,FSharpOption<MethodReference>>(target => {
                var fact = models.First(candidate => GeneratedJsonDefinitions.SameType(candidate.Type,target));
                return fact.DefaultConstructor == null ? null! : FSharpOption<MethodReference>.Some(module.ImportReference(fact.DefaultConstructor));
            }),exception,wrapper,
            new Func<TypeReference,Tuple<CodecIL<TypeReference,MethodReference,FieldReference>,MethodReference>>(target => {
                var method = api.Method(api.Owner,"DefaultTuple" + index + "_" + serial++,module.ImportReference(target),true);
                return Tuple.Create<CodecIL<TypeReference,MethodReference,FieldReference>,MethodReference>(new CecilCodecIL(method),method);
            }));
        var actual = module.ImportReference(model.Type);
        var function = api.Closed(new TypeReference("System", "Func`1", module, api.Owner.BaseType.Scope), actual);
        api.Delegate(entry, function, create);
        var defaults = api.Closed(module.ImportReference(api.Definition("SqlRowDefault`1")), actual);
        entry.Emit(OpCodes.Newobj, api.OnType("SqlRowDefault`1", ".ctor", defaults)); entry.Emit(OpCodes.Call, api.OnType("SqlRowDefault`1", "Register", defaults));
    }

    private sealed class Api
    {
        private readonly AssemblyDefinition product;
        internal Api(ModuleDefinition module, TypeDefinition owner, AssemblyDefinition product) { Module = module; Owner = owner; this.product = product; Json = module.ImportReference(Definition("JsonSerializator.JsonValue")); }
        internal AssemblyDefinition ProductAssembly => product;
        internal ModuleDefinition Module { get; }
        internal TypeDefinition Owner { get; }
        internal TypeReference Json { get; }
        internal TypeDefinition Definition(string name) => product.MainModule.GetType("SoloDatabase." + name);
        internal MethodReference Product(string type, string name, params TypeReference[] arguments)
        {
            var method = Module.ImportReference(Definition(type).Methods.Single(candidate => candidate.Name == name && (candidate.IsPublic || candidate.IsAssembly) && candidate.GenericParameters.Count == arguments.Length));
            if (arguments.Length == 0) return method;
            var closed = new GenericInstanceMethod(method); foreach (var argument in arguments) closed.GenericArguments.Add(Module.ImportReference(argument)); return closed;
        }
        internal MethodReference OnType(string type, string name, GenericInstanceType actual)
        { var method = Module.ImportReference(Definition(type).Methods.Single(candidate => candidate.Name == name)); method.DeclaringType = actual; return method; }
        internal GenericInstanceType Closed(TypeReference type, params TypeReference[] arguments)
        { var closed = new GenericInstanceType(type); foreach (var argument in arguments) closed.GenericArguments.Add(Module.ImportReference(argument)); return closed; }
        internal GenericInstanceType Function(TypeReference input, TypeReference output) => Closed(new TypeReference("System", "Func`2", Module, Owner.BaseType.Scope), input, output);
        internal MethodDefinition Method(TypeDefinition owner, string name, TypeReference result, bool isStatic, params TypeReference[] arguments)
        {
            var method = new MethodDefinition(name, MethodAttributes.Public | MethodAttributes.HideBySig | (isStatic ? MethodAttributes.Static : 0), result); method.Body.InitLocals = true;
            foreach (var argument in arguments) method.Parameters.Add(new ParameterDefinition(argument)); owner.Methods.Add(method); return method;
        }
        internal MethodReference DelegateConstructor(GenericInstanceType type)
        { var ctor = new MethodReference(".ctor", Module.TypeSystem.Void, type) { HasThis = true }; ctor.Parameters.Add(new ParameterDefinition(Module.TypeSystem.Object)); ctor.Parameters.Add(new ParameterDefinition(Module.TypeSystem.IntPtr)); return ctor; }
        internal void Delegate(ILProcessor il, GenericInstanceType type, MethodReference method)
        { il.Emit(OpCodes.Ldnull); il.Emit(OpCodes.Ldftn, method); il.Emit(OpCodes.Newobj, DelegateConstructor(type)); }
        internal MethodReference TypeHandle()
        { var type = new TypeReference("System", "Type", Module, Owner.BaseType.Scope); var method = new MethodReference("GetTypeFromHandle", type, type); method.Parameters.Add(new ParameterDefinition(new TypeReference("System", "RuntimeTypeHandle", Module, Owner.BaseType.Scope, true))); return method; }
    }
}
