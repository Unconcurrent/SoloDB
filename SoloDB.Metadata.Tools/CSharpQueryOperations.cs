using System;
using System.Linq;
using Microsoft.FSharp.Core;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;

namespace SoloDatabase.Metadata.Tools;

// Exact compiler descriptors supply facts; QueryOperationEmitter owns every operation body.
internal static class CSharpQueryOperations
{
    internal static void RegisterArguments(GeneratedJsonDefinitions definitions,AssemblyDefinition consumer,AssemblyDefinition product,
        ModuleDefinition module,TypeDefinition owner,ILProcessor entry,Action<string> access)
    {
        foreach (var descriptor in consumer.CustomAttributes.Where(attribute =>
            attribute.AttributeType.FullName == "SoloDatabase.Generated.SourceQueryRegistrationAttribute" &&
            (string)attribute.ConstructorArguments[0].Value == "RegisterSqlArguments"))
        {
            foreach (var value in (CustomAttributeArgument[])descriptor.ConstructorArguments[1].Value)
            {
                var source = OriginalArgumentSources.Read(module,SourceTypeIdentity.Resolve(consumer.MainModule,(string)value.Value));
                if (source == null) continue;
                GeneratedArgumentCapture.Register(definitions,module,owner,entry,product,source);
                foreach (var assembly in source.AccessAssemblies) access(assembly);
            }
            access(consumer.Name.Name);
        }
    }

    internal static void Register(GeneratedJsonDefinitions definitions,AssemblyDefinition consumer, AssemblyDefinition product, ModuleDefinition module,
        TypeDefinition owner, ILProcessor entry, Action<string> access)
    {
        var references = new GeneratedQueryReferences(module,product);
        var index = 0;
        var directCalls = new System.Collections.Generic.List<GeneratedQueryCall>();
        var sequenceFacts = new System.Collections.Generic.HashSet<string>(StringComparer.Ordinal);
        var carrierFacts = new System.Collections.Generic.HashSet<string>(StringComparer.Ordinal);
        TypeReference Identity(string value) => CSharpCodecFacts.ImportType(module,SourceTypeIdentity.Resolve(consumer.MainModule,value));
        foreach (var descriptor in consumer.CustomAttributes.Where(attribute =>
            attribute.AttributeType.FullName == "SoloDatabase.Generated.SourceQueryRegistrationAttribute"))
        {
            var name = (string)descriptor.ConstructorArguments[0].Value;
            if (name == "RegisterSqlArguments") continue;
            var types = ((CustomAttributeArgument[])descriptor.ConstructorArguments[1].Value).Select(value => Identity((string)value.Value)).ToArray();
            if (name is "RegisterSequenceFacts" or "RegisterParameterFacts")
            {
                GeneratedQueries.RegisterParameterFacts(module,entry,product,types,sequenceFacts,carrierFacts);
                access(product.Name.Name); access(consumer.Name.Name); continue;
            }
            if (name == "RegisterArrayAddress") { references.Array(owner,entry,(ArrayType)types.Single()); continue; }
            if (name == "RegisterAnonymous") continue; // The same resolved identity is consumed by the existing codec fact owner.
            if (name == "RegisterLambda")
            {
                var delegateType = (GenericInstanceType)types[0];
                GeneratedQueries.RegisterParameterFacts(module,entry,product,types.Skip(1),sequenceFacts,carrierFacts);
                var action = delegateType.ElementType.FullName.StartsWith("System.Action`",StringComparison.Ordinal);
                var arity = delegateType.GenericArguments.Count - (action ? 0 : 1);
                var factory = new GenericInstanceMethod(module.ImportReference(product.MainModule.GetType("SoloDatabase.QueryLambdaFactories")
                    .Methods.Single(method => method.Name == (action ? "Action" : "Func") + arity)));
                foreach (var argument in delegateType.GenericArguments) factory.GenericArguments.Add(argument);
                new GeneratedJsonAssembly.Api(module,product).TypeToken(entry,delegateType);
                entry.Emit(OpCodes.Call,factory);
                entry.Emit(OpCodes.Call,module.ImportReference(product.MainModule.GetType("SoloDatabase.QueryValueMetadata").Methods.Single(method => method.Name == "RegisterLambda")));
                access(product.Name.Name);
            }
            else
            {
                var method = new GenericInstanceMethod(module.ImportReference(product.MainModule.GetType("SoloDatabase.QueryMetadata").Methods.Single(method => method.Name == name)));
                foreach (var type in types) method.GenericArguments.Add(type);
                entry.Emit(OpCodes.Call,method);
            }
            access(consumer.Name.Name);
        }
        foreach (var descriptor in consumer.CustomAttributes.Where(attribute =>
            attribute.AttributeType.FullName is "SoloDatabase.Generated.QueryOperationRegistrationAttribute" or "SoloDatabase.Generated.SourceQueryOperationRegistrationAttribute"))
        {
            var data = descriptor.ConstructorArguments;
            var kind = (int)data[0].Value;
            if (kind == 4 || kind == 5 || kind == 6)
            {
                var producer = consumer.MainModule.GetType((string)data[5].Value).Methods.Single(method =>
                    method.Name == (string)data[6].Value && method.IsStatic && method.Parameters.Count == 0);
                var callable = (GenericInstanceType)module.ImportReference(producer.ReturnType);
                MethodReference registerOperation;
                if (kind == 6)
                {
                    GeneratedQueries.RegisterParameterFacts(module,entry,product,
                        ((CustomAttributeArgument[])data[4].Value).Select(value => CSharpCodecFacts.ImportType(module,(TypeReference)value.Value)),sequenceFacts,carrierFacts);
                    new GeneratedJsonAssembly.Api(module,product).TypeToken(entry,CSharpCodecFacts.ImportType(module,(TypeReference)data[1].Value));
                    registerOperation = module.ImportReference(product.MainModule.GetType("SoloDatabase.QueryValueMetadata")
                        .Methods.Single(method => method.Name == "RegisterLambda"));
                }
                else
                {
                    var generic = new GenericInstanceMethod(module.ImportReference(product.MainModule
                        .GetType("SoloDatabase.QueryValueMetadata").Methods.Single(method =>
                            method.Name == (kind == 4 ? "RegisterUnary" : "RegisterBinary"))));
                    foreach (var argument in callable.GenericArguments) generic.GenericArguments.Add(argument);
                    entry.Emit(OpCodes.Ldc_I4, (int)Enum.Parse(typeof(System.Linq.Expressions.ExpressionType), (string)data[2].Value));
                    registerOperation = generic;
                }
                entry.Emit(OpCodes.Call, module.ImportReference(producer));
                entry.Emit(OpCodes.Call, registerOperation);
                access(consumer.Name.Name);
                continue;
            }
            var source = SourceTypeIdentity.Resolve(consumer.MainModule,(string)data[1].Value);
            var definition = source.Resolve();
            var declaring = CSharpCodecFacts.ImportType(module,source);
            var name = (string)data[2].Value;
            TypeReference[] Types(int ordinal) => ((CustomAttributeArgument[])data[ordinal].Value)
                .Select(value => Identity((string)value.Value)).ToArray();
            var generics = Types(3);
            var arguments = Types(4);
            var function = new GenericInstanceType(new TypeReference("System","Func`2",module,owner.BaseType.Scope));
            function.GenericArguments.Add(module.TypeSystem.Object); function.GenericArguments.Add(module.TypeSystem.Object);
            var invoke = new MethodDefinition("QueryOperation" + index++, MethodAttributes.Private | MethodAttributes.Static,
                module.TypeSystem.Object);
            for (var i = 0; i < function.GenericArguments.Count-1; i++)
                invoke.Parameters.Add(new ParameterDefinition(function.GenericArguments[i]));
            owner.Methods.Add(invoke);
            var destination = new CecilCodecIL(invoke);
            TypeReference Closed(TypeReference type)
            {
                if (type is GenericParameter parameter)
                {
                    if (parameter.Type == GenericParameterType.Method) return generics[parameter.Position];
                    if (declaring is GenericInstanceType closed) return closed.GenericArguments[parameter.Position];
                }
                if (type is GenericInstanceType generic)
                {
                    var result = new GenericInstanceType(CSharpCodecFacts.ImportType(module,generic.ElementType));
                    foreach (var argument in generic.GenericArguments) result.GenericArguments.Add(Closed(argument));
                    return result;
                }
                if (type is ArrayType array) return new ArrayType(Closed(array.ElementType),array.Rank);
                if (type is ByReferenceType reference) return new ByReferenceType(Closed(reference.ElementType));
                return CSharpCodecFacts.ImportType(module,type);
            }
            MethodReference Method(MethodDefinition method)
            {
                var reference = module.ImportReference(method);
                reference.DeclaringType = declaring;
                if (!method.HasGenericParameters) return reference;
                var closed = new GenericInstanceMethod(reference);
                foreach (var argument in generics) closed.GenericArguments.Add(argument);
                return closed;
            }
            var nullable = declaring is GenericInstanceType optional && optional.ElementType.FullName == "System.Nullable`1";
            TypeReference? memberType = null;
            if (kind == 0 || kind == 1)
            {
                FieldReference? field = null;
                MethodReference? getter = null;
                TypeReference value;
                bool isStatic;
                if (kind == 0)
                {
                    var selected = definition.Fields.Single(candidate => candidate.Name == name);
                    field = module.ImportReference(selected); field.DeclaringType = declaring;
                    value = Closed(selected.FieldType); isStatic = selected.IsStatic;
                    if (!selected.IsInitOnly && !selected.IsLiteral) references.Field(owner,entry,declaring,field,value,isStatic);
                }
                else
                {
                    var selected = definition.Properties.Single(candidate => candidate.Name == name && candidate.Parameters.Count == 0);
                    getter = Method(selected.GetMethod ?? throw new InvalidOperationException("A generated query property has no getter."));
                    value = Closed(selected.PropertyType); isStatic = selected.GetMethod.IsStatic;
                }
                var member = new CodecMember<TypeReference,MethodReference,FieldReference>(name,value,
                    field == null ? null! : FSharpOption<FieldReference>.Some(field),
                    getter == null ? null! : FSharpOption<MethodReference>.Some(getter),null!,isStatic,true,false,false,false);
                QueryOperationEmitter.memberRead(destination,declaring,definition.IsValueType,nullable,member,value.IsValueType,true,true);
                memberType = value;
            }
            else
            {
                var selected = definition.Methods.Single(candidate => candidate.Name == name &&
                    candidate.GenericParameters.Count == generics.Length && candidate.Parameters.Count == arguments.Length &&
                    candidate.Parameters.Select(parameter => Closed(parameter.ParameterType).FullName).SequenceEqual(arguments.Select(argument => argument.FullName)));
                var target = Method(selected);
                owner.Methods.Remove(invoke);
                directCalls.Add(new GeneratedQueryCall(target,kind == 2 ? declaring : Closed(selected.ReturnType),arguments));
                access(definition.Module.Assembly.Name.Name); access(consumer.Name.Name);
                continue;
            }
            if (memberType != null)
            {
                var api = new GeneratedJsonAssembly.Api(module,product);
                api.TypeToken(entry,declaring); entry.Emit(OpCodes.Ldstr,name);
                entry.Emit(kind == 0 ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
                api.TypeToken(entry,memberType);
            }
            entry.Emit(OpCodes.Ldnull); entry.Emit(OpCodes.Ldftn,invoke);
            var constructor = new MethodReference(".ctor",module.TypeSystem.Void,function) { HasThis = true };
            constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));
            constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
            entry.Emit(OpCodes.Newobj,constructor);
            entry.Emit(OpCodes.Call,module.ImportReference(
                product.MainModule.GetType("SoloDatabase.QueryValueMetadata").Methods.Single(method => method.Name == "RegisterGeneratedReader")));
            access(definition.Module.Assembly.Name.Name);
            access(consumer.Name.Name);
        }
        // Exact source descriptions and compiler-created captures share the operation owner.
        var calls = directCalls;
        var seen = new System.Collections.Generic.HashSet<string>(StringComparer.Ordinal);
        System.Collections.Generic.IEnumerable<TypeDefinition> AllTypes(TypeDefinition type)
        {
            yield return type;
            foreach (var nested in type.NestedTypes)
                foreach (var child in AllTypes(nested)) yield return child;
        }
        TypeReference ClosedCaptureType(TypeReference type, GenericInstanceType? declaring, GenericInstanceMethod? call)
        {
            if (type is GenericParameter parameter)
                return parameter.Type == GenericParameterType.Method && call != null ? call.GenericArguments[parameter.Position] :
                    parameter.Type == GenericParameterType.Type && declaring != null ? declaring.GenericArguments[parameter.Position] : type;
            if (type is GenericInstanceType generic)
            {
                var closed = new GenericInstanceType(generic.ElementType);
                foreach (var argument in generic.GenericArguments) closed.GenericArguments.Add(ClosedCaptureType(argument,declaring,call));
                return closed;
            }
            if (type is ArrayType array) return new ArrayType(ClosedCaptureType(array.ElementType,declaring,call),array.Rank);
            return type;
        }
        void CapturedField(FieldReference captured)
        {
            if (captured.DeclaringType.ContainsGenericParameter ||
                ClosedCaptureType(captured.FieldType,captured.DeclaringType as GenericInstanceType,null).ContainsGenericParameter ||
                !captured.DeclaringType.Resolve().CustomAttributes.Any(attribute =>
                    attribute.AttributeType.FullName == "System.Runtime.CompilerServices.CompilerGeneratedAttribute") ||
                !seen.Add("field|" + captured.FullName)) return;
            var capturedOwner = CSharpCodecFacts.ImportType(module,captured.DeclaringType);
            var value = CSharpCodecFacts.ImportType(module,ClosedCaptureType(captured.FieldType,capturedOwner as GenericInstanceType,null));
            var capturedDefinition = captured.DeclaringType.Resolve().Fields.Single(field => field.Name == captured.Name);
            var field = module.ImportReference(capturedDefinition);
            field.DeclaringType = capturedOwner;
            var read = new MethodDefinition("CapturedField" + index++,MethodAttributes.Private | MethodAttributes.Static,module.TypeSystem.Object);
            read.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));
            owner.Methods.Add(read);
            var member = new CodecMember<TypeReference,MethodReference,FieldReference>(captured.Name,value,
                FSharpOption<FieldReference>.Some(field),null!,null!,capturedDefinition.IsStatic,true,false,false,false);
            if (!capturedDefinition.IsInitOnly && !capturedDefinition.IsLiteral) references.Field(owner,entry,capturedOwner,field,value,capturedDefinition.IsStatic);
            QueryOperationEmitter.memberRead(new CecilCodecIL(read),capturedOwner,capturedOwner.IsValueType,false,member,value.IsValueType,true,true);
            var api = new GeneratedJsonAssembly.Api(module,product);
            api.TypeToken(entry,capturedOwner); entry.Emit(OpCodes.Ldstr,captured.Name); entry.Emit(OpCodes.Ldc_I4_1); api.TypeToken(entry,value);
            var function = api.Function(module.TypeSystem.Object,module.TypeSystem.Object);
            var constructor = new MethodReference(".ctor",module.TypeSystem.Void,function) { HasThis = true };
            constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));
            constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
            entry.Emit(OpCodes.Ldnull); entry.Emit(OpCodes.Ldftn,read); entry.Emit(OpCodes.Newobj,constructor);
            entry.Emit(OpCodes.Call,module.ImportReference(product.MainModule.GetType("SoloDatabase.QueryValueMetadata").Methods.Single(method => method.Name == "RegisterGeneratedReader")));
            access(capturedDefinition.Module.Assembly.Name.Name);
        }
        // Actual closed compiler calls also establish their captured-field identities.
        void CapturedCall(GenericInstanceMethod call)
        {
            if (call.DeclaringType.ContainsGenericParameter || call.GenericArguments.Any(argument => argument.ContainsGenericParameter) ||
                consumer.MainModule.GetType(call.DeclaringType.GetElementType().FullName) == null) return;
            var target = call.Resolve();
            if (target.Module != consumer.MainModule || !target.HasBody) return;
            foreach (var instruction in target.Body.Instructions)
                if (instruction.OpCode == OpCodes.Ldtoken && instruction.Operand is FieldReference field)
                {
                    var declaring = ClosedCaptureType(field.DeclaringType,call.DeclaringType as GenericInstanceType,call);
                    var value = ClosedCaptureType(field.FieldType,declaring as GenericInstanceType,call);
                    CapturedField(new FieldReference(field.Name,value,declaring));
                }
        }
        foreach (var caller in consumer.MainModule.Types.SelectMany(AllTypes).SelectMany(type => type.Methods).Where(method => method.HasBody))
        foreach (var instruction in caller.Body.Instructions)
        {
            // Expression construction supplies exact compiler-created field tokens.
            if (instruction.OpCode == OpCodes.Ldtoken && instruction.Operand is FieldReference captured) CapturedField(captured);
            if (instruction.Operand is not GenericInstanceMethod site) continue;
            CapturedCall(site);
        }
        GeneratedQueryCalls.Register(module,owner,entry,product,calls.ToArray());

    }
}
