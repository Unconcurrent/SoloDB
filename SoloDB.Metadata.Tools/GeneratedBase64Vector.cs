using System;
using System.IO;
using System.Linq;
using Microsoft.FSharp.Core;
using Mono.Cecil;
using SoloDatabase.Metadata.CodecGeneration;

namespace SoloDatabase.Metadata.Tools;

// Original consumer references supply operands to the shared instruction emitter.
internal static class GeneratedBase64Vector
{
    internal static FSharpOption<CodecBase64VectorApi<TypeReference,MethodReference>> Read(ModuleDefinition module,string[] references,bool utf16)
    {
        var path=references.SingleOrDefault(path=>Path.GetFileName(path)=="System.Runtime.Intrinsics.dll");
        if(path==null) return null!;
        using var assembly=AssemblyDefinition.ReadAssembly(path,new ReaderParameters { ReadingMode=ReadingMode.Immediate });
        var source=assembly.MainModule;
        var vector=source.GetType("System.Runtime.Intrinsics.Vector128");
        var definition=source.GetType("System.Runtime.Intrinsics.Vector128`1");
        var sse=source.GetType("System.Runtime.Intrinsics.X86.Sse2");
        var ssse=source.GetType("System.Runtime.Intrinsics.X86.Ssse3");
        GenericInstanceType Closed(TypeReference argument) {
            var type=new GenericInstanceType(module.ImportReference(definition));
            type.GenericArguments.Add(module.ImportReference(argument));return type;
        }
        var bytes=Closed(module.TypeSystem.Byte);
        var unsigned=Closed(module.TypeSystem.UInt32);
        var signedBytes=Closed(module.TypeSystem.SByte);
        MethodReference Operation(TypeDefinition owner,string name,params string[] parameters) =>
            module.ImportReference(owner.Methods.Single(method=>method.Name==name && method.GenericParameters.Count==0 &&
                method.Parameters.Select(parameter=>parameter.ParameterType.FullName).SequenceEqual(parameters)));
        var reinterpret=vector.Methods.Single(method=>method.Name=="As" && method.GenericParameters.Count==2);
        MethodReference Cast(TypeReference input,TypeReference output) {
            var method=new GenericInstanceMethod(module.ImportReference(reinterpret));
            method.GenericArguments.Add(input);method.GenericArguments.Add(output);return method;
        }
        MethodReference Binary(string name,TypeReference type)=>Operation(sse,name,type.FullName,type.FullName);
        var unsafeType=module.ImportReference(typeof(System.Runtime.CompilerServices.Unsafe));
        MethodReference Memory(bool write) {
            var method=new MethodReference(write?"WriteUnaligned":"ReadUnaligned",module.TypeSystem.Void,unsafeType);
            var parameter=new GenericParameter("T",method);method.GenericParameters.Add(parameter);
            if(!write)method.ReturnType=parameter;
            method.Parameters.Add(new ParameterDefinition(new ByReferenceType(module.TypeSystem.Byte)));
            if(write)method.Parameters.Add(new ParameterDefinition(parameter));
            var closed=new GenericInstanceMethod(method);closed.GenericArguments.Add(bytes);return closed;
        }
        return FSharpOption<CodecBase64VectorApi<TypeReference,MethodReference>>.Some(new(
            bytes,unsigned,Operation(ssse,"get_IsSupported"),
            Operation(vector,"Create","System.UInt32"),Operation(vector,"Create","System.UInt32","System.UInt32","System.UInt32","System.UInt32"),
            Operation(vector,"Create","System.Byte"),
            Cast(module.TypeSystem.UInt32,module.TypeSystem.Byte),
            Cast(module.TypeSystem.Byte,module.TypeSystem.UInt32),Cast(module.TypeSystem.UInt32,module.TypeSystem.Byte),
            Cast(module.TypeSystem.Byte,module.TypeSystem.SByte),Cast(module.TypeSystem.SByte,module.TypeSystem.Byte),
            Memory(false),Memory(true),Operation(ssse,"Shuffle",bytes.FullName,bytes.FullName),
            Binary("And",bytes),Binary("Or",bytes),
            Operation(sse,"ShiftRightLogical",unsigned.FullName,"System.Byte"),Operation(sse,"ShiftLeftLogical",unsigned.FullName,"System.Byte"),
            Binary("CompareGreaterThan",signedBytes),Binary("Add",bytes),
            utf16?FSharpOption<CodecBase64WideningApi<MethodReference>>.Some(new(Binary("UnpackLow",bytes),Binary("UnpackHigh",bytes))):null));
    }
}
