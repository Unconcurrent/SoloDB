using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

internal static class GeneratedObjectSqlRows
{
    internal static void Register(ModuleDefinition module, TypeDefinition owner, ILProcessor il,
        AssemblyDefinition product, GeneratedModel model, int index, GeneratedModel[] models)
    {
        var type = module.ImportReference(model.Type);
        var shape = module.ImportReference(product.MainModule.GetType("SoloDatabase.SqlRowShape"));
        var row = module.ImportReference(product.MainModule.GetType("SoloDatabase.SqlRow"));
        var members = model.Facts.RecordType
            ? model.Facts.RecordFields.Select(name => {
                var ordinal = System.Array.FindIndex(model.Facts.Members.ToArray(), fact => !fact.Field && fact.Name == name);
                return (fact: model.Facts.Members[ordinal], ordinal);
            }).ToArray()
            : model.Facts.Members.Select((fact, ordinal) => (fact, ordinal))
                .Where(pair => !pair.fact.Field && model.Members[pair.ordinal].Setter != null).ToArray();
        var mapper = new TypeDefinition("", "ObjectRow" + index, TypeAttributes.NestedPrivate | TypeAttributes.Sealed, owner.BaseType);
        owner.NestedTypes.Add(mapper);
        var ordinals = new FieldDefinition("ordinals", FieldAttributes.Private | FieldAttributes.InitOnly, new ArrayType(module.TypeSystem.Int32));
        mapper.Fields.Add(ordinals);
        var ctor = Method(mapper, ".ctor", module.TypeSystem.Void, false, ordinals.FieldType);
        ctor.Attributes |= MethodAttributes.SpecialName | MethodAttributes.RTSpecialName;
        var body = ctor.Body.GetILProcessor(); body.Emit(OpCodes.Ldarg_0);
        body.Emit(OpCodes.Call, new MethodReference(".ctor", module.TypeSystem.Void, owner.BaseType) { HasThis = true });
        body.Emit(OpCodes.Ldarg_0); body.Emit(OpCodes.Ldarg_1); body.Emit(OpCodes.Stfld, ordinals); body.Emit(OpCodes.Ret);
        var read = Method(mapper, "Read", type, false, row); read.Body.InitLocals = true;
        body = read.Body.GetILProcessor();
        var isNull = module.ImportReference(product.MainModule.GetType("SoloDatabase.SqlRow").Methods.Single(method => method.Name == "IsDBNull"));
        void Ordinal(int position)
        {
            body.Emit(OpCodes.Ldarg_0); body.Emit(OpCodes.Ldfld, ordinals); body.Emit(OpCodes.Ldc_I4, position); body.Emit(OpCodes.Ldelem_I4);
        }
        void CheckPresent(int position, Instruction absent)
        {
            Ordinal(position); body.Emit(OpCodes.Ldc_I4_0); body.Emit(OpCodes.Blt, absent);
            if (row.IsValueType) body.Emit(OpCodes.Ldarga_S, read.Parameters[0]); else body.Emit(OpCodes.Ldarg_1);
            Ordinal(position); body.Emit(OpCodes.Call, isNull); body.Emit(OpCodes.Brtrue, absent);
        }
        void Value(GeneratedMember member, int position)
        {
            body.Emit(OpCodes.Ldarg_1); Ordinal(position);
            var method = GeneratedSqlRows.ReadMethod(module,product,member.Type,models);
            body.Emit(OpCodes.Call, method);
        }
        if (model.Facts.RecordType)
        {
            var values = members.Select(pair => new VariableDefinition(module.ImportReference(model.Members[pair.ordinal].Type))).ToArray();
            foreach (var value in values) read.Body.Variables.Add(value);
            for (var position = 0; position < members.Length; position++)
            {
                var absent = Instruction.Create(OpCodes.Nop); CheckPresent(position, absent);
                Value(model.Members[members[position].ordinal], position); body.Emit(OpCodes.Stloc, values[position]); body.Append(absent);
            }
            foreach (var value in values) body.Emit(OpCodes.Ldloc, value);
            body.Emit(OpCodes.Newobj, module.ImportReference(model.Constructor!)); body.Emit(OpCodes.Ret);
        }
        else
        {
            var result = new VariableDefinition(type); read.Body.Variables.Add(result);
            if (model.Constructor != null && model.Constructor.Parameters.Count == 0)
            {
                body.Emit(OpCodes.Newobj, module.ImportReference(model.Constructor)); body.Emit(OpCodes.Stloc, result);
            }
            else if (!model.Facts.ValueType)
            {
                var create = new GenericInstanceMethod(module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonMetadata")
                    .Methods.Single(method => method.Name == "Uninitialized")));
                create.GenericArguments.Add(type); body.Emit(OpCodes.Call, create); body.Emit(OpCodes.Stloc, result);
            }
            for (var position = 0; position < members.Length; position++)
            {
                var absent = Instruction.Create(OpCodes.Nop); CheckPresent(position, absent);
                body.Emit(model.Facts.ValueType ? OpCodes.Ldloca : OpCodes.Ldloc, result);
                var member = model.Members[members[position].ordinal]; Value(member, position);
                body.Emit(model.Facts.ValueType ? OpCodes.Call : OpCodes.Callvirt, module.ImportReference(member.Setter!)); body.Append(absent);
            }
            body.Emit(OpCodes.Ldloc, result); body.Emit(OpCodes.Ret);
        }
        var factory = Method(owner, "BindObjectRow" + index, Function(module, owner, row, type), true, shape);
        body = factory.Body.GetILProcessor();
        body.Emit(OpCodes.Ldc_I4, members.Length); body.Emit(OpCodes.Newarr, module.TypeSystem.Int32);
        var ordinalMethod = module.ImportReference(product.MainModule.GetType("SoloDatabase.SqlRowShape").Methods.Single(method => method.Name == "Ordinal"));
        for (var position = 0; position < members.Length; position++)
        {
            body.Emit(OpCodes.Dup); body.Emit(OpCodes.Ldc_I4, position); body.Emit(OpCodes.Ldarg_0);
            body.Emit(OpCodes.Ldstr, members[position].fact.Name); body.Emit(OpCodes.Callvirt, ordinalMethod); body.Emit(OpCodes.Stelem_I4);
        }
        body.Emit(OpCodes.Newobj, ctor); body.Emit(OpCodes.Ldftn, read);
        body.Emit(OpCodes.Newobj, DelegateConstructor(module, (GenericInstanceType)factory.ReturnType)); body.Emit(OpCodes.Ret);
        il.Emit(OpCodes.Ldnull); il.Emit(OpCodes.Ldftn, factory);
        il.Emit(OpCodes.Newobj, DelegateConstructor(module, Function(module, owner, shape, factory.ReturnType)));
        var mapping = product.MainModule.GetType("SoloDatabase.SqlRowMapping`1");
        var mappingType = new GenericInstanceType(module.ImportReference(mapping)); mappingType.GenericArguments.Add(type);
        var mappingCtor = module.ImportReference(mapping.Methods.Single(method => method.IsConstructor)); mappingCtor.DeclaringType = mappingType; il.Emit(OpCodes.Newobj, mappingCtor);
        var register = module.ImportReference(mapping.Methods.Single(method => method.Name == "Register")); register.DeclaringType = mappingType; il.Emit(OpCodes.Call, register);
    }

    private static MethodDefinition Method(TypeDefinition owner, string name, TypeReference result, bool isStatic, params TypeReference[] arguments)
    {
        var method = new MethodDefinition(name, MethodAttributes.Public | MethodAttributes.HideBySig | (isStatic ? MethodAttributes.Static : 0), result);
        foreach (var argument in arguments) method.Parameters.Add(new ParameterDefinition(argument)); owner.Methods.Add(method); return method;
    }
    private static GenericInstanceType Function(ModuleDefinition module, TypeDefinition owner, TypeReference input, TypeReference output)
    {
        var function = new GenericInstanceType(new TypeReference("System", "Func`2", module, owner.BaseType.Scope));
        function.GenericArguments.Add(input); function.GenericArguments.Add(output); return function;
    }
    private static MethodReference DelegateConstructor(ModuleDefinition module, GenericInstanceType type)
    {
        var ctor = new MethodReference(".ctor", module.TypeSystem.Void, type) { HasThis = true };
        ctor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object)); ctor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr)); return ctor;
    }
}
