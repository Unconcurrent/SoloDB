using System;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;

namespace SoloDatabase.Metadata.Tools;

public sealed class GeneratedQueryCall
{
    public GeneratedQueryCall(MethodReference method, TypeReference result, TypeReference[] arguments)
    { Method = method; Result = result; Arguments = arguments; }
    public MethodReference Method { get; }
    public TypeReference Result { get; }
    public TypeReference[] Arguments { get; }
}

internal static class GeneratedQueryCalls
{
    internal static void Register(ModuleDefinition module, TypeDefinition owner, ILProcessor il,
        AssemblyDefinition product, GeneratedQueryCall[] calls)
    {
        var references = new GeneratedQueryReferences(module,product);
        var standard = owner.BaseType.Scope;
        var type = new TypeReference("System", "Type", module, standard);
        var fromHandle = new MethodReference("GetTypeFromHandle", type, type);
        fromHandle.Parameters.Add(new ParameterDefinition(new TypeReference("System", "RuntimeTypeHandle", module, standard, true)));
        void TypeToken(ILProcessor body, TypeReference value)
        { body.Emit(OpCodes.Ldtoken, module.ImportReference(value)); body.Emit(OpCodes.Call, fromHandle); }
        void Types(TypeReference[] values)
        {
            il.Emit(OpCodes.Ldc_I4, values.Length); il.Emit(OpCodes.Newarr, type);
            for (var i = 0; i < values.Length; i++)
            { il.Emit(OpCodes.Dup); il.Emit(OpCodes.Ldc_I4, i); TypeToken(il, values[i]); il.Emit(OpCodes.Stelem_Ref); }
        }
        for (var index = 0; index < calls.Length; index++)
        {
            var call = calls[index];
            var target = module.ImportReference(call.Method);
            var isConstructor = target.Name == ".ctor";
            var returnsVoid = !isConstructor && call.Result.FullName == "System.Void";
            var result = returnsVoid ? module.TypeSystem.Object : module.ImportReference(call.Result);
            var invoke = new MethodDefinition("InvokeQueryCall" + owner.Methods.Count, MethodAttributes.Private | MethodAttributes.Static, result);
            if (!isConstructor) invoke.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));
            invoke.Parameters.Add(new ParameterDefinition(new ArrayType(module.TypeSystem.Object)));
            owner.Methods.Add(invoke);
            var body = invoke.Body.GetILProcessor();
            if (isConstructor)
            {
                QueryOperationEmitter.construction(new CecilCodecIL(invoke),references.Reference,references.Arguments,result,result.IsValueType,target,call.Arguments,false);
                Types(call.Arguments);
                var factory = new GenericInstanceType(new TypeReference("System","Func`2",module,standard));
                factory.GenericArguments.Add(new ArrayType(module.TypeSystem.Object)); factory.GenericArguments.Add(result);
                var factoryConstructor=new MethodReference(".ctor",module.TypeSystem.Void,factory) { HasThis=true };
                factoryConstructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object)); factoryConstructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
                il.Emit(OpCodes.Ldnull); il.Emit(OpCodes.Ldftn,invoke); il.Emit(OpCodes.Newobj,factoryConstructor);
                var registration=new GenericInstanceMethod(module.ImportReference(product.MainModule.GetType("SoloDatabase.QueryValueMetadata").Methods.Single(method => method.Name == "RegisterConstructor")));
                registration.GenericArguments.Add(result); il.Emit(OpCodes.Call,registration);
                continue;
            }
            QueryOperationEmitter.call(new CecilCodecIL(invoke), references.Reference,references.Arguments,references.Receiver(target.DeclaringType),target.DeclaringType,
                target.DeclaringType.IsValueType,
                target.DeclaringType is GenericInstanceType nullable && nullable.ElementType.FullName == "System.Nullable`1",
                !target.HasThis, target, call.Arguments, result, result.IsValueType, returnsVoid, false);
            TypeToken(il, target.DeclaringType); il.Emit(OpCodes.Ldstr, target.Name);
            Types(target is GenericInstanceMethod generic ? generic.GenericArguments.ToArray() : Array.Empty<TypeReference>());
            Types(call.Arguments);
            TypeToken(il, call.Result);
            var function = new GenericInstanceType(new TypeReference("System", "Func`3", module, standard));
            function.GenericArguments.Add(module.TypeSystem.Object);
            function.GenericArguments.Add(new ArrayType(module.TypeSystem.Object));
            function.GenericArguments.Add(result);
            var constructor = new MethodReference(".ctor", module.TypeSystem.Void, function) { HasThis = true };
            constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));
            constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
            il.Emit(OpCodes.Ldnull); il.Emit(OpCodes.Ldftn, invoke); il.Emit(OpCodes.Newobj, constructor);
            var register = new GenericInstanceMethod(module.ImportReference(product.MainModule.GetType("SoloDatabase.QueryValueMetadata")
                .Methods.Single(method => method.Name == "RegisterExactCall")));
            register.GenericArguments.Add(result); il.Emit(OpCodes.Call, register);
        }
    }
}
