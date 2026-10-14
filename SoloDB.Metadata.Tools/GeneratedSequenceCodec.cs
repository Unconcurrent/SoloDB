using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Microsoft.FSharp.Core;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

// Maps original enumeration operands into the shared sequence instruction owner.
internal static partial class GeneratedSequenceCodec
{
    private static GenericInstanceType Closed(ModuleDefinition module, TypeReference definition, params TypeReference[] arguments)
    {
        var result = new GenericInstanceType(module.ImportReference(definition));
        foreach (var argument in arguments) result.GenericArguments.Add(module.ImportReference(argument));
        return result;
    }

    internal static MethodDefinition Reader(GeneratedJsonDefinitions definitions, ModuleDefinition module, TypeDefinition owner,
        AssemblyDefinition product, TypeReference target, TypeReference element, TypeReference reader,
        string policy, bool wrapper, string name, GeneratedModel[] models)
    {
        var api = new GeneratedJsonAssembly.Api(module, product,definitions.Stored);
        TypeReference Product(string type) => module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator." + type));
        var role = "sequence-read:" + policy + (wrapper ? ":wrapper" : "");
        if (definitions.TryGet(role, target, reader, "", out var prepared)) return prepared;
        var context = Closed(module, Product("JsonReadContext`1"), reader);
        var method = new MethodDefinition(definitions.MethodName(name), MethodAttributes.Assembly | MethodAttributes.Static | MethodAttributes.HideBySig, module.ImportReference(target));
        method.Body.InitLocals = true;
        method.Parameters.Add(new ParameterDefinition(new ByReferenceType(context)));
        owner.Methods.Add(method);
        definitions.Add(role, target, reader, "", method);
        var il = new CecilCodecIL(method);
        MethodReference Input(string operation) => api.Operation("JsonRawInput", operation, reader);
        if (TypeSupport.SequenceBinaryReader(policy, element.FullName))
        {
            MethodReference? constructor = null;
            if (policy == "List")
            {
                var original = module.ImportReference(typeof(List<>).GetConstructor(new[] { typeof(IEnumerable<>).MakeGenericType(typeof(List<>).GetGenericArguments()) })!);
                original.DeclaringType = Closed(module, module.ImportReference(typeof(List<>)), element);
                constructor = original;
            }
            CodecSequenceEmitter.binaryReader(il, target, Input("SequenceNull"),
                GeneratedFormattedCodec.Reader(definitions, module, owner, product, new ArrayType(module.TypeSystem.Byte), reader, name + "Bytes"),
                constructor == null ? null! : FSharpOption<MethodReference>.Some(constructor),
                policy == "FSharpList" ? FSharpOption<MethodReference>.Some(api.Operation("JsonRawCollections", "FinishBinaryFSharpList")) : null!,
                TypeSupport.SequenceNullMismatch(policy, element.FullName) ? FSharpOption<MethodReference>.Some(api.Operation("JsonRawCollections", "BinarySequenceMismatch",target)) : null!,
                Product("JsonPrimitiveShapeMismatch"),api.Operation("JsonRawCollections", "BinarySequenceMismatch",target));
            return method;
        }
        var facts = TypeSupport.SequenceConstruction(policy);
        var array = facts.Item1.Length == 0;
        var definition = array ? null : facts.Item1 switch {
            "System.Collections.Generic.Stack`1" => typeof(Stack<>),
            "System.Collections.Generic.Queue`1" => typeof(Queue<>),
            "System.Collections.Generic.HashSet`1" => typeof(HashSet<>),
            "System.Collections.Generic.LinkedList`1" => typeof(LinkedList<>),
            "System.Collections.Generic.List`1" => typeof(List<>),
            _ => throw new InvalidOperationException("Unknown original collection operand.")
        };
        TypeReference working = array ? new ArrayType(element) : Closed(module, module.ImportReference(definition!), element);
        MethodReference? construct = null;
        MethodReference? add = null;
        bool discardAdd = false;
        if (!array)
        {
            construct = module.ImportReference(definition!.GetConstructor(facts.Item3 ? new[] { typeof(int) } : Type.EmptyTypes)!);
            construct.DeclaringType = working;
            var original = definition.GetMethods().Single(value => value.Name == facts.Item2 && value.GetParameters().Length == 1 && value.GetParameters()[0].ParameterType.IsGenericParameter);
            add = module.ImportReference(original);
            add.DeclaringType = working;
            discardAdd = original.ReturnType != typeof(void);
        }
        var operands = new CodecSequenceReaderApi<TypeReference, MethodReference>(context, module.TypeSystem.Int32, working,
            Input("SequenceNull"), null!, api.Operation("JsonRawCollections", "SequenceContext", target, reader),
            Input("SequenceCount"), Input("SequenceElement"), construct!, false, null!, null!, false, add!,
            facts.Item5.Length == 0 ? null! : FSharpOption<MethodReference>.Some(api.Operation("JsonRawCollections", "Finish" + facts.Item5, element)),
            null!,
            false, false,
            Product("JsonPrimitiveShapeMismatch"), Product("JsonScalarShapeMismatch"),
            api.Operation("JsonRawRead", "PrimitiveRetry", element, reader), api.Operation("JsonRawRead", "ScalarRetry", element, reader));
        var serial = 0;
        CodecSequenceEmitter.reader(il, operands,
            FSharpFunc<TypeReference, FSharpOption<MethodReference>>.FromConverter(
                new Converter<TypeReference, FSharpOption<MethodReference>>(child => FSharpOption<MethodReference>.Some(
                    GeneratedChildCodec.ReadCall(definitions, module, owner, product, child, reader, name + "Element" + serial++, models)))),
            FSharpFunc<TypeReference, MethodReference>.FromConverter(new Converter<TypeReference, MethodReference>(child => api.Operation("JsonRawRead", "ReadMember", child, reader))),
            target, element, wrapper, array, facts.Item3, facts.Item4, discardAdd);
        return method;
    }

    private static void Delegate(ILProcessor entry, ModuleDefinition module, AssemblyDefinition product,
        string definition, MethodDefinition body, params TypeReference[] arguments)
    {
        var type = Closed(module, module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator." + definition)), arguments);
        var constructor = new MethodReference(".ctor", module.TypeSystem.Void, type) { HasThis = true };
        constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));
        constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
        entry.Emit(OpCodes.Ldnull); entry.Emit(OpCodes.Ldftn, body); entry.Emit(OpCodes.Newobj, constructor);
    }

    internal static void Register(GeneratedJsonDefinitions definitions, ModuleDefinition module, TypeDefinition owner,
        ILProcessor entry, AssemblyDefinition product, GeneratedModel model, int index, GeneratedModel[] models, bool readersOnly)
    {
        var api = new GeneratedJsonAssembly.Api(module, product,definitions.Stored);
        var element = module.ImportReference(((GenericInstanceType)model.Type).GenericArguments[0]);
        var policy = TypeSupport.ForShape(model.Facts.Shape).Factory;
        entry.Emit(OpCodes.Ldstr,policy);
        if (!readersOnly) entry.Emit(element.FullName == "System.Byte" ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
        foreach (var name in new[] { "Utf8Reader", "Utf16Reader" })
        {
            var reader = module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator." + name));
            var body = Reader(definitions, module, owner, product, model.Type, element, reader, policy, false, "ReadSequence" + name + index, models);
            Delegate(entry, module, product, "JsonDirectReader`2", body, model.Type, reader);
        }
        if (!readersOnly)
        {
            foreach (var pair in new[] { (module.TypeSystem.Byte, "JsonUtf8Writer"), (module.TypeSystem.Char, "JsonUtf16Writer") })
            {
                var output = module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator." + pair.Item2));
                var body = Writer(definitions, module, owner, product, model.Type, element, pair.Item1, output,
                    model.Type.IsValueType, element.FullName == "System.Byte", "WriteSequence" + pair.Item2 + index, models);
                Delegate(entry, module, product, "JsonDirectWriter`2", body, model.Type, pair.Item1);
            }
        }
        entry.Emit(OpCodes.Call, api.Metadata(readersOnly ? "RegisterSequenceReaders" : "RegisterSequenceBodies", model.Type));
        if (TypeSupport.SequenceProducesResult(model.Facts.Shape))
            RegisterResultBodies(definitions, module, owner, entry, product, element, "SequenceResult" + index, models);
    }

    internal static void RegisterResultBodies(GeneratedJsonDefinitions definitions, ModuleDefinition module, TypeDefinition owner,
        ILProcessor entry, AssemblyDefinition product, TypeReference element, string name, GeneratedModel[] models)
    {
        var api = new GeneratedJsonAssembly.Api(module, product,definitions.Stored);
        var enumerable = Closed(module, module.ImportReference(typeof(IEnumerable<>)), element);
        foreach (var pair in new[] { (module.TypeSystem.Byte, "JsonUtf8Writer"), (module.TypeSystem.Char, "JsonUtf16Writer") })
        {
            var output = module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator." + pair.Item2));
            var body = Writer(definitions, module, owner, product, enumerable, element, pair.Item1, output, false,
                element.FullName == "System.Byte", name + pair.Item2, models);
            Delegate(entry, module, product, "JsonDirectWriter`2", body, enumerable, pair.Item1);
        }
        entry.Emit(OpCodes.Call, api.Metadata("RegisterSequenceWriterBodies", element));
    }

    internal static void RegisterWriters(GeneratedJsonDefinitions definitions, ModuleDefinition module, TypeDefinition owner,
        ILProcessor entry, AssemblyDefinition product, TypeReference target, TypeReference element, bool valueType, string name, GeneratedModel[] models)
    {
        var api = new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        foreach (var pair in new[] { (module.TypeSystem.Byte, "JsonUtf8Writer"), (module.TypeSystem.Char, "JsonUtf16Writer") })
        {
            var output = module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator." + pair.Item2));
            var body = Writer(definitions,module,owner,product,target,element,pair.Item1,output,valueType,element.FullName == "System.Byte",name + pair.Item2,models);
            Delegate(entry,module,product,"JsonDirectWriter`2",body,target,pair.Item1);
        }
        entry.Emit(OpCodes.Call,api.Metadata("RegisterRawWriters",target));
    }

    internal static void RegisterRangeArrayBodies(GeneratedJsonDefinitions definitions, ModuleDefinition module, TypeDefinition owner,
        ILProcessor entry, AssemblyDefinition product, TypeReference element, bool wrapper, string name, GeneratedModel[] models)
    {
        var api = new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        var target = new ArrayType(module.ImportReference(element));
        entry.Emit(wrapper ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
        foreach (var readerName in new[] { "Utf8Reader", "Utf16Reader" })
        {
            var reader = module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator." + readerName));
            var body = Reader(definitions,module,owner,product,target,element,reader,"Array",wrapper,name + readerName,models);
            Delegate(entry,module,product,"JsonDirectReader`2",body,target,reader);
        }
        entry.Emit(OpCodes.Call,api.Metadata("RegisterRangeArrayBodies",element));
    }

    internal static MethodDefinition Writer(GeneratedJsonDefinitions definitions, ModuleDefinition module, TypeDefinition owner,
        AssemblyDefinition product, TypeReference target, TypeReference element, TypeReference encoding, TypeReference output,
        bool valueType, bool binary, string name, GeneratedModel[] models, bool nonGeneric = false)
    {
        var api = new GeneratedJsonAssembly.Api(module, product,definitions.Stored);
        TypeReference Product(string type) => module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator." + type));
        var representation = api.RawType("JsonRawOutput",encoding,output);
        var role = nonGeneric ? "non-generic-sequence-write" : binary ? "sequence-binary-write" : "sequence-write";
        if (definitions.TryGet(role, target, representation, "", out var prepared)) return prepared;
        var span = (GenericInstanceType)((ByReferenceType)product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonDirectWriter`2")
            .Methods.Single(method => method.Name == "Invoke").Parameters[1].ParameterType).ElementType;
        var method = new MethodDefinition(definitions.MethodName(name), MethodAttributes.Assembly | MethodAttributes.Static | MethodAttributes.HideBySig, module.TypeSystem.Void);
        method.Body.InitLocals = true;
        method.Parameters.Add(new ParameterDefinition(module.ImportReference(target)));
        method.Parameters.Add(new ParameterDefinition(new ByReferenceType(Closed(module, span.ElementType, encoding))));
        method.Parameters.Add(new ParameterDefinition(new ByReferenceType(Closed(module, Product("JsonWriteState`1"), encoding))));
        owner.Methods.Add(method);
        definitions.Add(role, target, representation, "", method);

        TypeReference enumerable = nonGeneric ? module.ImportReference(typeof(IEnumerable)) : Closed(module, module.ImportReference(typeof(IEnumerable<>)), element);
        TypeReference enumerator = nonGeneric ? module.ImportReference(typeof(IEnumerator)) : Closed(module, module.ImportReference(typeof(IEnumerator<>)), element);
        var getEnumerator = module.ImportReference((nonGeneric ? typeof(IEnumerable) : typeof(IEnumerable<>)).GetMethod("GetEnumerator")!);
        getEnumerator.DeclaringType = enumerable;
        var current = module.ImportReference((nonGeneric ? typeof(IEnumerator) : typeof(IEnumerator<>)).GetProperty("Current")!.GetMethod!);
        current.DeclaringType = enumerator;
        var sequence = new CodecSequenceWriterApi<TypeReference, MethodReference>(enumerator, getEnumerator,
            module.ImportReference(typeof(IEnumerator).GetMethod("MoveNext")!), current,
            module.ImportReference(typeof(IDisposable).GetMethod("Dispose")!),
            nonGeneric ? FSharpOption<TypeReference>.Some(module.ImportReference(typeof(IDisposable))) : null!,
            api.Operation("JsonRawCollections", "CaptureSequenceBytes"),
            binary ? GeneratedOutputCodec.Bytes(definitions, module, owner, product, new ArrayType(module.TypeSystem.Byte), encoding, name + "Bytes") : null!);
        var serial = 0;
        MethodReference Literal(string value) => GeneratedOutputCodec.Constant(definitions, module, owner, product, encoding, value, name + "Literal" + serial++);
        var outputApi = new CodecRawWriterApi<TypeReference, MethodReference>(module.TypeSystem.Boolean,
            FSharpFunc<TypeReference, FSharpOption<MethodReference>>.FromConverter(
                new Converter<TypeReference, FSharpOption<MethodReference>>(child =>
                    FSharpOption<MethodReference>.Some(GeneratedChildCodec.WriteCall(
                        definitions, module, owner, product, child, encoding, output, name + "Element" + serial++, models)))),
            null!, Literal("null"), null!, Literal("["), Literal(","), Literal("]"), null!, null!, null!, null!, null!, null!, null!);
        CodecSequenceEmitter.writer(new CecilCodecIL(method), outputApi, sequence, target, element, valueType, binary);
        return method;
    }
}
