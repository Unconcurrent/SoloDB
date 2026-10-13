using System;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;

namespace SoloDatabase.Metadata.Tools;

// Mechanical operands and fixed callable publication for finite object contracts.
internal static class GeneratedFiniteCodec
{
    private static GenericInstanceType Closed(ModuleDefinition module, TypeReference definition, params TypeReference[] arguments)
    {
        var result = new GenericInstanceType(module.ImportReference(definition));
        foreach (var argument in arguments) result.GenericArguments.Add(module.ImportReference(argument));
        return result;
    }
    internal static CodecFiniteCaptureApi<TypeReference,MethodReference> Api(ModuleDefinition module, AssemblyDefinition product, TypeReference reader)
    {
        var api = new GeneratedJsonAssembly.Api(module,product);
        return new(GeneratedPropertyCodec.Api(module,product,reader),
            module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonSourceRange")),
            api.Operation("JsonTupleInput","Capture",reader),api.Operation("JsonTupleInput","Complete",reader));
    }

    internal static void Register(GeneratedJsonDefinitions definitions, ModuleDefinition module, TypeDefinition owner,
        ILProcessor entry, AssemblyDefinition product, bool marker, bool envelope)
    {
        var api = new GeneratedJsonAssembly.Api(module,product);
        foreach (var isMarker in new[] { true,false })
        {
            if (!(isMarker ? marker : envelope)) continue;
            var result = module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator." + (isMarker ? "JsonMarkerRange" : "JsonEnvelopeRanges")));
            foreach (var encoding in new[] { "Utf8Reader","Utf16Reader" })
            {
                var reader = module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator." + encoding));
                if (!definitions.TryGet("finite-object-capture",result,reader,"",out var body))
                {
                    var context = Closed(module,
                        product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonReadContext`1"),reader);
                    body = new MethodDefinition("Capture" + result.Name + encoding,
                        MethodAttributes.Assembly | MethodAttributes.Static | MethodAttributes.HideBySig,result);
                    body.Body.InitLocals = true;
                    body.Parameters.Add(new ParameterDefinition(new ByReferenceType(context)));
                    owner.Methods.Add(body);
                    definitions.Add("finite-object-capture",result,reader,"",body);
                    var il = new CecilCodecIL(body);
                    var construct = module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator." + result.Name)
                        .Methods.Single(value => value.IsConstructor && !value.IsStatic));
                    CodecFiniteCaptureEmitter.reader(il,Api(module,product,reader),isMarker,construct);
                }
                var callable = Closed(module,
                    product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonFiniteCapture`2"),result,reader);
                var constructor = new MethodReference(".ctor",module.TypeSystem.Void,callable) { HasThis = true };
                constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));
                constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
                entry.Emit(OpCodes.Ldnull); entry.Emit(OpCodes.Ldftn,body); entry.Emit(OpCodes.Newobj,constructor);
                entry.Emit(OpCodes.Call,api.Operation("JsonFiniteInput","Publish",result,reader));
            }
        }
    }
}
