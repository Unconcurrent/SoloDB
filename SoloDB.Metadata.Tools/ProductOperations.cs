using System;
using System.Collections.Generic;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

// This is the finite product contract, not application model admission.
internal static class ProductOperations
{
    internal const string Owner = "SoloDatabase.JsonSerializator.JsonProductOperations";
    internal const string Version = "CompletionVersion";
    private const int CurrentVersion = 1;

    private static TypeReference IntegerType(ModuleDefinition module, int width, bool signed, bool native) => native
        ? signed ? module.TypeSystem.IntPtr : module.TypeSystem.UIntPtr
        : (width,signed) switch
        {
            (8,true) => module.TypeSystem.SByte, (8,false) => module.TypeSystem.Byte,
            (16,true) => module.TypeSystem.Int16, (16,false) => module.TypeSystem.UInt16,
            (32,true) => module.TypeSystem.Int32, (32,false) => module.TypeSystem.UInt32,
            (64,true) => module.TypeSystem.Int64, (64,false) => module.TypeSystem.UInt64,
            _ => throw new ArgumentOutOfRangeException(nameof(width))
        };

    private static string IntegerName(int width,bool signed,bool utf16,bool native,bool hex) =>
        (native ? "Native" : "") + (signed ? "Int" : "UInt") + width + (hex ? "Hex" : "Decimal") + (utf16 ? "Utf16" : "Utf8");
    private static string CaptureName(bool marker,bool utf16) => "Capture" + (marker ? "Marker" : "Envelope") + (utf16 ? "Utf16" : "Utf8");
    private static TypeReference[] IntegerArguments(ModuleDefinition module) => new TypeReference[]
        { new ByReferenceType(module.TypeSystem.Byte),module.TypeSystem.Int32,new ByReferenceType(module.TypeSystem.Int32) };

    private static MethodDefinition Declaration(TypeDefinition owner,string name,TypeReference result,TypeReference[] arguments)
    {
        var method = owner.Methods.Single(value => value.Name == name);
        string Identity(TypeReference type) => GeneratedJsonDefinitions.SignatureIdentity(type);
        if (!owner.IsNotPublic || !method.IsStatic || method.HasGenericParameters ||
            Identity(method.ReturnType) != Identity(result) ||
            !method.Parameters.Select(value => Identity(value.ParameterType)).SequenceEqual(arguments.Select(Identity)) ||
            !method.CustomAttributes.Any(value => value.AttributeType.FullName == "Microsoft.FSharp.Core.NoCompilerInliningAttribute"))
            throw new InvalidOperationException("The supplied product operation has a different original contract: " + method.FullName);
        return method;
    }

    private static TypeDefinition? Supplied(AssemblyDefinition product)
    {
        var owner = product.MainModule.GetType(Owner);
        if (owner == null) return null;
        var version = owner.Fields.Single(value => value.Name == Version);
        var reference = product.CustomAttributes.Any(value => value.AttributeType.FullName == "System.Runtime.CompilerServices.ReferenceAssemblyAttribute");
        if (!reference && (!version.IsLiteral || !Equals(version.Constant,CurrentVersion)))
            throw new InvalidOperationException("The product operation implementation was not completed: " + product.Name.FullName);
        return owner;
    }

    internal static MethodReference? Integer(ModuleDefinition destination,AssemblyDefinition product,int width,bool signed,bool utf16,bool native,bool hex)
    {
        var owner = Supplied(product);
        return owner == null ? null : destination.ImportReference(Declaration(owner,IntegerName(width,signed,utf16,native,hex),
            IntegerType(product.MainModule,width,signed,native),IntegerArguments(product.MainModule)));
    }

    internal static MethodReference? Rounding(ModuleDefinition destination,AssemblyDefinition product,int width)
    {
        var owner = Supplied(product);
        return owner == null ? null : destination.ImportReference(Declaration(owner,"Round" + width,
            IntegerType(product.MainModule,width,false,false),new[] { product.MainModule.TypeSystem.UInt64,product.MainModule.TypeSystem.Int64,product.MainModule.TypeSystem.Boolean }));
    }

    private static (TypeReference Result, TypeReference Context) CaptureTypes(AssemblyDefinition product,bool marker,bool utf16)
    {
        var module = product.MainModule;
        var context = new GenericInstanceType(module.GetType("SoloDatabase.JsonSerializator.JsonReadContext`1"));
        context.GenericArguments.Add(module.GetType("SoloDatabase.JsonSerializator." + (utf16 ? "Utf16Reader" : "Utf8Reader")));
        return (module.GetType("SoloDatabase.JsonSerializator." + (marker ? "JsonMarkerRange" : "JsonEnvelopeRanges")),new ByReferenceType(context));
    }

    internal static MethodReference? Capture(ModuleDefinition destination,AssemblyDefinition product,bool marker,bool utf16)
    {
        var owner = Supplied(product);
        if (owner == null) return null;
        var types = CaptureTypes(product,marker,utf16);
        return destination.ImportReference(Declaration(owner,CaptureName(marker,utf16),types.Result,new[] { types.Context }));
    }

    internal static MethodReference Offset(ModuleDefinition module,AssemblyDefinition product)
    {
        var scope = product.MainModule.AssemblyReferences.Single(value => value.Name == "System.Runtime.CompilerServices.Unsafe");
        var type = new TypeReference("System.Runtime.CompilerServices","Unsafe",module,scope);
        var method = new MethodReference("Add",module.TypeSystem.Void,type);
        var element = new GenericParameter("T",method); method.GenericParameters.Add(element);
        method.ReturnType = new ByReferenceType(element);
        method.Parameters.Add(new ParameterDefinition(new ByReferenceType(element)));
        method.Parameters.Add(new ParameterDefinition(module.TypeSystem.UIntPtr));
        var call = new GenericInstanceMethod(method); call.GenericArguments.Add(module.TypeSystem.Byte);
        return module.ImportReference(call);
    }

    internal static CodecIntegerApi<TypeReference,MethodReference> IntegerApi(ModuleDefinition module,MethodReference offset)
    {
        MethodReference Constructor(string name,params TypeReference[] arguments)
        {
            var call = new MethodReference(".ctor",module.TypeSystem.Void,new TypeReference("System",name,module,module.TypeSystem.Object.Scope)) { HasThis = true };
            foreach (var argument in arguments) call.Parameters.Add(new ParameterDefinition(argument));
            return call;
        }
        return new(module.TypeSystem.Int32,module.TypeSystem.Int64,module.TypeSystem.UInt32,module.TypeSystem.UInt64,offset,
            Constructor("FormatException",module.TypeSystem.String),Constructor("OverflowException"),Constructor("Exception",module.TypeSystem.String));
    }

    internal static void Complete(AssemblyDefinition product)
    {
        var module = product.MainModule;
        var owner = module.GetType(Owner) ?? throw new InvalidOperationException("The original product has no operation declarations.");
        var completed = new HashSet<MethodDefinition>();
        CecilCodecIL Begin(string name,TypeReference result,params TypeReference[] arguments)
        {
            var method = Declaration(owner,name,result,arguments);
            if (!completed.Add(method) || !method.HasBody || method.DebugInformation.StateMachineKickOffMethod != null)
                throw new InvalidOperationException("The original product operation is not a replaceable static body: " + method.FullName);
            method.DebugInformation.SequencePoints.Clear(); method.DebugInformation.Scope = null;
            method.DebugInformation.CustomDebugInformations.Clear(); method.CustomDebugInformations.Clear();
            method.Body = new MethodBody(method) { InitLocals = true };
            return new CecilCodecIL(method);
        }
        var api = IntegerApi(module,Offset(module,product));
        var targets = new[] { module.TypeSystem.SByte,module.TypeSystem.Byte,module.TypeSystem.Int16,module.TypeSystem.UInt16,
            module.TypeSystem.Int32,module.TypeSystem.UInt32,module.TypeSystem.Int64,module.TypeSystem.UInt64,module.TypeSystem.IntPtr,module.TypeSystem.UIntPtr };
        foreach (var target in targets)
        {
            var shape = TypeSupport.Numeric(target.FullName).Value;
            foreach (var width in shape.Native ? new[] { 32,64 } : new[] { shape.Width })
                foreach (var utf16 in new[] { false,true })
                    foreach (var hex in new[] { false,true })
                    {
                        var il = Begin(IntegerName(width,shape.Signed,utf16,shape.Native,hex),target,IntegerArguments(module));
                        if (hex) CodecIntegerEmitter.hexReader(il,api,width,shape.Signed,utf16,shape.Native);
                        else CodecIntegerEmitter.decimalReader(il,api,width,shape.Signed,utf16,shape.Native);
                    }
        }
        var rounding = new CodecFloatingApi<MethodReference>(module.GetType("SoloDatabase.Numerics.BinaryDecimal").Methods.Single(value => value.Name == "RoundBits"));
        foreach (var width in new[] { 16,32,64 })
            CodecFloatingEmitter.bits(Begin("Round" + width,IntegerType(module,width,false,false),module.TypeSystem.UInt64,module.TypeSystem.Int64,module.TypeSystem.Boolean),rounding,width);
        foreach (var marker in new[] { true,false })
            foreach (var utf16 in new[] { false,true })
            {
                var types = CaptureTypes(product,marker,utf16);
                var reader = module.GetType("SoloDatabase.JsonSerializator." + (utf16 ? "Utf16Reader" : "Utf8Reader"));
                var construct = ((TypeDefinition)types.Result).Methods.Single(value => value.IsConstructor && !value.IsStatic);
                CodecFiniteCaptureEmitter.reader(Begin(CaptureName(marker,utf16),types.Result,types.Context),GeneratedFiniteCodec.Api(module,product,reader),marker,construct);
            }
        if (completed.Count != 55 || completed.Any(method => method.Body.Instructions.Count == 0) ||
            owner.Methods.Any(method => !method.IsConstructor && !completed.Contains(method)))
            throw new InvalidOperationException("The product operation inventory is incomplete.");
        var version = owner.Fields.Single(value => value.Name == Version);
        if (!version.IsLiteral || !Equals(version.Constant,0)) throw new InvalidOperationException("Product completion requires the pristine compiler declarations.");
        version.Constant = CurrentVersion;
    }
}
