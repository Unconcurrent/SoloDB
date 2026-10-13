using System;
using System.Collections.Generic;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.Generation;
using SoloDatabase.Metadata.CodecGeneration;

namespace SoloDatabase.Metadata.Tools;

internal static class GeneratedTuples
{
    private sealed class Leaf
    {
        internal Leaf(GeneratedMember[] path) { Path = path; }
        internal GeneratedMember[] Path { get; }
        internal TypeReference Type => Path[Path.Length - 1].Type;
    }

    internal static void Register(GeneratedJsonDefinitions definitions,ModuleDefinition module, TypeDefinition owner, ILProcessor entry,
        AssemblyDefinition product, GeneratedModel model, int index, GeneratedModel[] models, bool includeRows = true)
    {
        var api = new Api(module, owner, product);
        var type = module.ImportReference(model.Type);
        var leaves = new List<Leaf>();
        void Collect(GeneratedModel tuple, GeneratedMember[] path)
        {
            for (var i = 0; i < tuple.Members.Length; i++)
            {
                var member = tuple.Members[i];
                var current = path.Concat(new[] { member }).ToArray();
                if (i == 7) Collect(Find(member.Type), current);
                else leaves.Add(new Leaf(current));
            }
        }
        GeneratedModel Find(TypeReference value) => models.First(candidate => GeneratedJsonDefinitions.SameType(candidate.Type,value));
        Collect(model, Array.Empty<GeneratedMember>());
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
        entry.Emit(OpCodes.Ldc_I4, leaves.Count); entry.Emit(OpCodes.Newarr, stored);
        for (var i = 0; i < leaves.Count; i++)
        {
            var leaf = leaves[i]; var getter = api.Method(owner, "TupleMember" + index + "_" + i, module.ImportReference(leaf.Type), true, type);
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
            RegisterDefault(api, entry, model, index, Find);
        }
    }

    private static void RegisterRows(Api api, ILProcessor entry, GeneratedModel model, int index, List<Leaf> leaves, Func<TypeReference, GeneratedModel> find, GeneratedModel[] models)
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
        var read = api.Method(mapper, "Read", type, false, row); il = read.Body.GetILProcessor(); var ordinal = 0;
        void ReadTuple(GeneratedModel tuple)
        {
            for (var i = 0; i < tuple.Members.Length; i++)
            {
                var member = tuple.Members[i]; var fact = find(member.Type);
                if (i == 7) { ReadTuple(fact); continue; }
                var position = ordinal++; var result = new VariableDefinition(module.ImportReference(member.Type)); read.Body.Variables.Add(result);
                var present = il.Create(OpCodes.Nop); var complete = il.Create(OpCodes.Nop);
                il.Emit(OpCodes.Ldarga, read.Parameters[0]); LoadOrdinal(position);
                il.Emit(OpCodes.Call, api.Product("SqlRow", "IsDBNull")); il.Emit(OpCodes.Brfalse, present);
                if (fact.Facts.ValueType && fact.Facts.Shape != TypeShape.Nullable)
                {
                    il.Emit(OpCodes.Ldstr, member.Type.Name); il.Emit(OpCodes.Ldc_I4, position);
                    il.Emit(OpCodes.Call, api.Product("SqlRowShape", "TupleNull")); il.Emit(OpCodes.Throw);
                }
                else { il.Emit(OpCodes.Ldloca, result); il.Emit(OpCodes.Initobj, result.VariableType); }
                il.Emit(OpCodes.Br, complete); il.Append(present);
                il.Emit(OpCodes.Ldarg_1); LoadOrdinal(position);
                il.Emit(OpCodes.Call,GeneratedSqlRows.ReadMethod(module,api.ProductAssembly,member.Type,models)); il.Emit(OpCodes.Stloc,result);
                il.Append(complete); il.Emit(OpCodes.Ldloc, result);
            }
            il.Emit(OpCodes.Newobj, module.ImportReference(tuple.Constructor!));
        }
        void LoadOrdinal(int position)
        { il.Emit(OpCodes.Ldarg_0); il.Emit(OpCodes.Ldfld, start); il.Emit(OpCodes.Ldc_I4, position); il.Emit(OpCodes.Add); }
        ReadTuple(model); il.Emit(OpCodes.Ret);
        var factory = api.Method(api.Owner, "BindTupleRow" + index, api.Function(row, type), true, shape); il = factory.Body.GetILProcessor();
        il.Emit(OpCodes.Ldarg_0); il.Emit(OpCodes.Ldstr, model.Type.Name); il.Emit(OpCodes.Ldc_I4, leaves.Count);
        il.Emit(OpCodes.Callvirt, api.Product("SqlRowShape", "RequireTupleColumns"));
        il.Emit(OpCodes.Ldarg_0); il.Emit(OpCodes.Callvirt, api.Product("SqlRowShape", "get_Start")); il.Emit(OpCodes.Newobj, ctor);
        il.Emit(OpCodes.Ldftn, read); il.Emit(OpCodes.Newobj, api.DelegateConstructor((GenericInstanceType)factory.ReturnType)); il.Emit(OpCodes.Ret);
        api.Delegate(entry, api.Function(shape, factory.ReturnType), factory);
        var mapping = api.Closed(module.ImportReference(api.Definition("SqlRowMapping`1")), type);
        entry.Emit(OpCodes.Newobj, api.OnType("SqlRowMapping`1", ".ctor", mapping)); entry.Emit(OpCodes.Call, api.OnType("SqlRowMapping`1", "Register", mapping));
    }

    private static void RegisterDefault(Api api, ILProcessor entry, GeneratedModel model, int index, Func<TypeReference, GeneratedModel> find)
    {
        var module = api.Module; var serial = 0;
        MethodDefinition Create(GeneratedModel current, bool root)
        {
            var type = module.ImportReference(current.Type);
            var method = api.Method(api.Owner, "DefaultTuple" + index + "_" + serial++, type, true); var il = method.Body.GetILProcessor();
            var result = new VariableDefinition(type); method.Body.Variables.Add(result);
            if (root && current.Facts.ValueType)
            { il.Emit(OpCodes.Ldloca, result); il.Emit(OpCodes.Initobj, type); il.Emit(OpCodes.Ldloc, result); il.Emit(OpCodes.Ret); return method; }
            var arguments = new List<VariableDefinition>();
            for (var i = 0; i < current.Members.Length; i++)
            {
                var member = current.Members[i]; var value = new VariableDefinition(module.ImportReference(member.Type)); method.Body.Variables.Add(value); arguments.Add(value);
                var fact = find(member.Type);
                if (i == 7) il.Emit(OpCodes.Call, Create(fact, false));
                else if (fact.Facts.ValueType && fact.Constructor?.Parameters.Count == 0) il.Emit(OpCodes.Call, Create(fact, false));
                else { il.Emit(OpCodes.Ldloca, value); il.Emit(OpCodes.Initobj, value.VariableType); il.Emit(OpCodes.Ldloc, value); }
                il.Emit(OpCodes.Stloc, value);
            }
            var begin = il.Create(OpCodes.Nop); il.Append(begin);
            foreach (var value in arguments) il.Emit(OpCodes.Ldloc, value);
            il.Emit(OpCodes.Newobj, module.ImportReference(current.Constructor!)); il.Emit(OpCodes.Stloc, result);
            var done = il.Create(OpCodes.Nop); il.Emit(OpCodes.Leave, done);
            var handler = il.Create(OpCodes.Nop); il.Append(handler);
            var wrapped = new TypeReference("System.Reflection", "TargetInvocationException", module, api.Owner.BaseType.Scope);
            var ctor = new MethodReference(".ctor", module.TypeSystem.Void, wrapped) { HasThis = true };
            var exception = new TypeReference("System", "Exception", module, api.Owner.BaseType.Scope);
            ctor.Parameters.Add(new ParameterDefinition(exception)); il.Emit(OpCodes.Newobj, ctor); il.Emit(OpCodes.Throw);
            il.Append(done); il.Emit(OpCodes.Ldloc, result); il.Emit(OpCodes.Ret);
            method.Body.ExceptionHandlers.Add(new ExceptionHandler(ExceptionHandlerType.Catch)
            { TryStart = begin, TryEnd = handler, HandlerStart = handler, HandlerEnd = done, CatchType = exception });
            return method;
        }
        var create = Create(model, true); var actual = module.ImportReference(model.Type);
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
        internal void Delegate(ILProcessor il, GenericInstanceType type, MethodDefinition method)
        { il.Emit(OpCodes.Ldnull); il.Emit(OpCodes.Ldftn, method); il.Emit(OpCodes.Newobj, DelegateConstructor(type)); }
        internal MethodReference TypeHandle()
        { var type = new TypeReference("System", "Type", Module, Owner.BaseType.Scope); var method = new MethodReference("GetTypeFromHandle", type, type); method.Parameters.Add(new ParameterDefinition(new TypeReference("System", "RuntimeTypeHandle", Module, Owner.BaseType.Scope, true))); return method; }
    }
}
