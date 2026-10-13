using System;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;

namespace SoloDatabase.Metadata.Tools;

internal static class GeneratedIdentity
{
    // Match the storage owner's released direct-field getter contract against
    // the selected emitted implementation, without executing any model code.
    internal static bool TrivialGetter(TypeDefinition declaring, string name)
    {
        var getter = declaring.Methods.Single(method => method.Name == name && !method.IsStatic && method.Parameters.Count == 0);
        if (getter.IsVirtual && !getter.IsFinal && !declaring.IsSealed || !getter.HasBody) return false;
        var instructions = getter.Body.Instructions;
        return getter.Body.CodeSize == 7 && instructions.Count == 3 &&
            instructions[0].OpCode == OpCodes.Ldarg_0 && instructions[1].OpCode == OpCodes.Ldfld && instructions[2].OpCode == OpCodes.Ret;
    }

    internal static void Register(ModuleDefinition module, TypeDefinition owner, ILProcessor il,
        AssemblyDefinition product, GeneratedModel model, int index)
    {
        var type = module.ImportReference(model.Type);
        var candidates = model.Facts.Members.Select((fact, ordinal) => (fact, ordinal)).Where(pair =>
            !pair.fact.Field && pair.fact.Name == "Id" && model.Members[pair.ordinal].Type.FullName == "System.Int64" && pair.fact.Read).ToArray();
        if (candidates.Length > 1) throw new InvalidOperationException("Physical identity is ambiguous: " + type.FullName);
        if (candidates.Length == 0) { il.Emit(OpCodes.Ldnull); il.Emit(OpCodes.Ldnull); il.Emit(OpCodes.Ldnull); il.Emit(OpCodes.Ldc_I4_0); }
        else
        {
            var member = model.Members[candidates[0].ordinal];
            var read = Method(owner, "ReadIdentity" + index, module.TypeSystem.Int64, type);
            QueryOperationEmitter.memberRead(new CecilCodecIL(read),type,model.Facts.ValueType,false,
                GeneratedCodec.Model(model).Members[candidates[0].ordinal],false,false,false);
            Delegate(il, Closed(new TypeReference("System", "Func`2", module, owner.BaseType.Scope), type, module.TypeSystem.Int64), read);
            if (member.Setter == null) { il.Emit(OpCodes.Ldnull); il.Emit(OpCodes.Ldnull); il.Emit(OpCodes.Ldc_I4_0); }
            else
            {
                var write = Method(owner, "WriteIdentity" + index, module.TypeSystem.Void, new ByReferenceType(type), module.TypeSystem.Int64);
                QueryOperationEmitter.memberWrite(new CecilCodecIL(write),type,model.Facts.ValueType,
                    GeneratedCodec.Model(model).Members[candidates[0].ordinal],false,true,false);
                var setterType = module.ImportReference(product.MainModule.GetType("SoloDatabase.EntityIdSetter`1"));
                Delegate(il, Closed(setterType, type), write);
                var boxed = Method(owner, "WriteBoxedIdentity" + index, module.TypeSystem.Void, module.TypeSystem.Object, module.TypeSystem.Int64);
                QueryOperationEmitter.memberWrite(new CecilCodecIL(boxed),type,model.Facts.ValueType,
                    GeneratedCodec.Model(model).Members[candidates[0].ordinal],true,false,false);
                Delegate(il, Closed(new TypeReference("System", "Action`2", module, owner.BaseType.Scope), module.TypeSystem.Object, module.TypeSystem.Int64), boxed);
                il.Emit(model.Facts.IdentityTrivialGetter ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
            }
        }
        var operations = product.MainModule.GetType("SoloDatabase.EntityIdOperations`1");
        var ctor = module.ImportReference(operations.Methods.Single(method => method.IsConstructor && method.Parameters.Count == 4));
        ctor.DeclaringType = Closed(module.ImportReference(operations), type); il.Emit(OpCodes.Newobj, ctor);
        var register = module.ImportReference(product.MainModule.GetType("SoloDatabase.EntityIdMetadata").Methods.Single(method => method.Name == "Register"));
        var closed = new GenericInstanceMethod(register); closed.GenericArguments.Add(type); il.Emit(OpCodes.Call, closed);
    }

    private static GenericInstanceType Closed(TypeReference definition, params TypeReference[] arguments)
    {
        var type = new GenericInstanceType(definition);
        foreach (var argument in arguments) type.GenericArguments.Add(argument);
        return type;
    }
    private static MethodDefinition Method(TypeDefinition owner, string name, TypeReference result, params TypeReference[] arguments)
    {
        var method = new MethodDefinition(name, MethodAttributes.Private | MethodAttributes.Static, result);
        foreach (var argument in arguments) method.Parameters.Add(new ParameterDefinition(argument));
        owner.Methods.Add(method); return method;
    }
    private static void Delegate(ILProcessor il, GenericInstanceType type, MethodDefinition method)
    {
        il.Emit(OpCodes.Ldnull); il.Emit(OpCodes.Ldftn, method);
        var ctor = new MethodReference(".ctor", il.Body.Method.Module.TypeSystem.Void, type) { HasThis = true };
        ctor.Parameters.Add(new ParameterDefinition(il.Body.Method.Module.TypeSystem.Object));
        ctor.Parameters.Add(new ParameterDefinition(il.Body.Method.Module.TypeSystem.IntPtr));
        il.Emit(OpCodes.Newobj, ctor);
    }
}
