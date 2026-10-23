using System;
using System.Linq;
using Mono.Cecil;
using SoloDatabase.Metadata.CodecGeneration;

namespace SoloDatabase.Metadata.Tools;

// Maps original lexical operands; shared FSharp emission owns inline recognition.
internal static class GeneratedPropertyCodec
{
    internal static CodecNameApi<TypeReference,MethodReference> Api(ModuleDefinition module, AssemblyDefinition product, TypeReference reader)
    {
        var api = new GeneratedJsonAssembly.Api(module,product);
        MethodReference Input(string operation) => api.Operation("JsonNumericInput",operation,reader);
        MethodReference Name(string operation) => api.Operation("JsonNameInput",operation,reader);
        var scope = product.MainModule.AssemblyReferences.Single(reference => reference.Name == "System.Runtime.CompilerServices.Unsafe");
        var unsafeType = module.ImportReference(new TypeReference("System.Runtime.CompilerServices","Unsafe",product.MainModule,scope));
        var add = new MethodReference("Add",module.TypeSystem.Void,unsafeType);
        var element = new GenericParameter("T",add); add.GenericParameters.Add(element);
        add.ReturnType = new ByReferenceType(element); add.Parameters.Add(new ParameterDefinition(new ByReferenceType(element)));
        add.Parameters.Add(new ParameterDefinition(module.TypeSystem.UIntPtr));
        var offset = new GenericInstanceMethod(add); offset.GenericArguments.Add(module.TypeSystem.Byte);
        return new CodecNameApi<TypeReference,MethodReference>(module.TypeSystem.Int32,module.TypeSystem.Boolean,
            new ByReferenceType(module.TypeSystem.Byte),new ByReferenceType(module.TypeSystem.Int32),
            api.Operation("JsonNameInput","Begin",reader),Input("Length"),Input("Position"),Input("Reference"),offset,
            api.Operation("JsonNameInput",reader.Name=="Utf16Reader"?"Utf16Pair":"Utf8Word"),api.Operation("JsonNameInput","Utf8Pair"),
            Name("Quoted"),Name("Identifier"),api.Operation("JsonNameInput","Invalid"),api.Operation("JsonNameInput","Unterminated"),
            Name("Unsupported"),Name("Colon"),Name("Value"),reader.Name=="Utf16Reader");
    }
}
