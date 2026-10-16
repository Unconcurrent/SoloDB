using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using Microsoft.FSharp.Core;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

// Compiler facts map signatures; both destinations consume the same output macros.
internal static class GeneratedOutputCodec
{
    private static CodecOutputApi<TypeReference,MethodReference,FieldReference> Api(ModuleDefinition module,AssemblyDefinition product,TypeReference element)
    {
        var api = new GeneratedJsonAssembly.Api(module,product);
        var unsafeType = module.ImportReference(typeof(System.Runtime.CompilerServices.Unsafe));
        var offset = new MethodReference("Add",module.TypeSystem.Void,unsafeType);
        var parameter = new GenericParameter("T",offset); offset.GenericParameters.Add(parameter);
        offset.ReturnType = new ByReferenceType(parameter);
        offset.Parameters.Add(new ParameterDefinition(new ByReferenceType(parameter)));
        offset.Parameters.Add(new ParameterDefinition(module.TypeSystem.UIntPtr));
        var add = new GenericInstanceMethod(offset);add.GenericArguments.Add(module.TypeSystem.Byte);
        MethodReference Helper(string name) => api.Operation("JsonWriterRegion",name,element);
        var state = new GenericInstanceType(module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonWriteState`1")));
        state.GenericArguments.Add(module.ImportReference(element));
        var list = new GenericInstanceType(module.ImportReference(typeof(IList<>)));
        list.GenericArguments.Add(module.ImportReference(element));
        var concrete = new GenericInstanceType(module.ImportReference(typeof(List<>)));
        concrete.GenericArguments.Add(module.ImportReference(element));
        var collection = new GenericInstanceType(module.ImportReference(typeof(ICollection<>)));
        collection.GenericArguments.Add(module.ImportReference(element));
        MethodReference Add(TypeReference owner) {
            var method = new MethodReference("Add",module.TypeSystem.Void,owner) { HasThis=true };
            method.Parameters.Add(new ParameterDefinition(((GenericInstanceType)owner).ElementType.GenericParameters[0]));
            return method;
        }
        var sourceList = (GenericInstanceType)product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonWriteState`1").Fields.Single(field => field.Name == "List@").FieldType;
        var fieldList = new GenericInstanceType(module.ImportReference(sourceList.ElementType));
        fieldList.GenericArguments.Add(state.ElementType.GenericParameters[0]);
        return new(module.TypeSystem.Int32,module.TypeSystem.UInt32,module.TypeSystem.UInt64,new ByReferenceType(module.TypeSystem.Byte),
            list,concrete,new FieldReference("List@",fieldList,state),
            new FieldReference("Written",module.TypeSystem.Int32,state),new FieldReference("Discard",module.TypeSystem.Boolean,state),
            Add(collection),Add(concrete),Helper("ReserveSpan"),Helper("ReserveList"),Helper("FitsSpan"),
            Helper("Start"),Helper("Position"),Helper("Advance"),add);
    }

    private static CodecDigitWriterApi<TypeReference,MethodReference,FieldReference> Digits(ModuleDefinition module,AssemblyDefinition product,TypeReference element)
    {
        var api=new GeneratedJsonAssembly.Api(module,product);
        var unsafeType=module.ImportReference(typeof(System.Runtime.CompilerServices.Unsafe));
        MethodReference PairAccess(bool write) {
            var method=new MethodReference(write?"WriteUnaligned":"ReadUnaligned",module.TypeSystem.Void,unsafeType);
            var word=new GenericParameter("T",method);method.GenericParameters.Add(word);
            if(!write)method.ReturnType=word;
            method.Parameters.Add(new ParameterDefinition(new ByReferenceType(module.TypeSystem.Byte)));
            if(write)method.Parameters.Add(new ParameterDefinition(word));
            var closed=new GenericInstanceMethod(method);
            closed.GenericArguments.Add(element.FullName=="System.Char"?module.TypeSystem.UInt32:module.TypeSystem.UInt16);
            return closed;
        }
        return new(Api(module,product,element),api.Operation("JsonWriterRegion","DigitPairReference",element),PairAccess(false),PairAccess(true));
    }

    private static MethodDefinition Body(ModuleDefinition module,TypeDefinition owner,AssemblyDefinition product,TypeReference element,TypeReference input,string name)
    {
        var api = new GeneratedJsonAssembly.Api(module,product);
        var operation = api.Operation("JsonWriterRegion","ReserveSpan",element);
        var method = new MethodDefinition(name,MethodAttributes.Assembly|MethodAttributes.Static|MethodAttributes.HideBySig,module.TypeSystem.Void);
        method.Body.InitLocals = true;
        method.Parameters.Add(new ParameterDefinition(module.ImportReference(input)));
        foreach(var parameter in operation.Parameters.Take(2)) {
            var closed = new GenericInstanceType(((ByReferenceType)parameter.ParameterType).ElementType.GetElementType());
            closed.GenericArguments.Add(module.ImportReference(element));
            method.Parameters.Add(new ParameterDefinition(new ByReferenceType(closed)));
        }
        owner.Methods.Add(method); return method;
    }
    internal static MethodDefinition Property(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,AssemblyDefinition product,TypeReference element,string text,string name)
    {
        if (definitions.TryGet("property-write",module.TypeSystem.Boolean,element,text,out var existing)) return existing;
        var utf16 = element.FullName == "System.Char";
        var units = utf16 ? text.Select(value=>(int)value).ToArray() : Encoding.UTF8.GetBytes(text).Select(value=>(int)value).ToArray();
        var method = Body(module,owner,product,element,module.TypeSystem.Boolean,name);
        CodecOutputEmitter.property(new CecilCodecIL(method),Api(module,product,element),utf16,units);
        definitions.Add("property-write",module.TypeSystem.Boolean,element,text,method);
        return method;
    }
    internal static MethodDefinition Constant(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,AssemblyDefinition product,TypeReference element,string text,string name)
    {
        if (definitions.TryGet("literal-write",module.TypeSystem.Void,element,text,out var existing)) return existing;
        var utf16 = element.FullName == "System.Char";
        var units = utf16 ? text.Select(value=>(int)value).ToArray() : Encoding.UTF8.GetBytes(text).Select(value=>(int)value).ToArray();
        var method = Body(module,owner,product,element,module.TypeSystem.Boolean,name);
        method.Parameters.RemoveAt(0);
        CodecOutputEmitter.constant(new CecilCodecIL(method),Api(module,product,element),utf16,units);
        definitions.Add("literal-write",module.TypeSystem.Void,element,text,method);
        return method;
    }
    internal static MethodDefinition Boolean(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,AssemblyDefinition product,TypeReference element,string name)
    {
        if (definitions.TryGet("boolean-write",module.TypeSystem.Boolean,element,"",out var existing)) return existing;
        var method = Body(module,owner,product,element,module.TypeSystem.Boolean,name);
        CodecOutputEmitter.boolean(new CecilCodecIL(method),Api(module,product,element),element.FullName=="System.Char");
        definitions.Add("boolean-write",module.TypeSystem.Boolean,element,"",method);
        return method;
    }
    internal static MethodDefinition String(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,AssemblyDefinition product,TypeReference element,string name)
    {
        if (definitions.TryGet("string-write",module.TypeSystem.String,element,"",out var existing)) return existing;
        var api=new GeneratedJsonAssembly.Api(module,product);
        var method=Body(module,owner,product,element,module.TypeSystem.String,name);
        var unsafeType=module.ImportReference(typeof(System.Runtime.CompilerServices.Unsafe));
        var read=new MethodReference("ReadUnaligned",module.TypeSystem.Void,unsafeType);
        var value=new GenericParameter("T",read);read.GenericParameters.Add(value);read.ReturnType=value;
        read.Parameters.Add(new ParameterDefinition(new ByReferenceType(module.TypeSystem.Byte)));
        var word=new GenericInstanceMethod(read);word.GenericArguments.Add(module.TypeSystem.UInt64);
        var length=new MethodReference("get_Length",module.TypeSystem.Int32,module.TypeSystem.String){HasThis=true};
        var facts=new CodecStringWriterApi<TypeReference,MethodReference,FieldReference>(Api(module,product,element),
            api.Operation("JsonWriterRegion","StringReference"),length,word,WriteWord());
        MethodReference WriteWord() {
            var write=new MethodReference("WriteUnaligned",module.TypeSystem.Void,unsafeType);
            var value=new GenericParameter("T",write);write.GenericParameters.Add(value);
            write.Parameters.Add(new ParameterDefinition(new ByReferenceType(module.TypeSystem.Byte)));write.Parameters.Add(new ParameterDefinition(value));
            var result=new GenericInstanceMethod(write);result.GenericArguments.Add(module.TypeSystem.UInt32);return result;
        }
        CodecStringWriterEmitter.writer(new CecilCodecIL(method),facts,element.FullName=="System.Char");
        definitions.Add("string-write",module.TypeSystem.String,element,"",method);
        return method;
    }
    internal static MethodDefinition TypeLiteral(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,AssemblyDefinition product,TypeReference target,TypeReference element,string name)
    {
        if (definitions.TryGet("type-literal",target,element,"",out var existing)) return existing;
        var api = new GeneratedJsonAssembly.Api(module,product);
        var method = Body(module,owner,product,element,module.TypeSystem.Boolean,name);
        method.Parameters.RemoveAt(0);
        CodecOutputEmitter.encodedLiteral(new CecilCodecIL(method),Api(module,product,element),new ArrayType(module.ImportReference(element)),
            api.Operation("JsonWriterRegion","TypeNameLiteral",target,element),api.Operation("JsonWriterRegion","LiteralReference",element),element.FullName=="System.Char");
        definitions.Add("type-literal",target,element,"",method);
        return method;
    }
    private static Microsoft.FSharp.Core.FSharpFunc<Microsoft.FSharp.Core.Unit,Microsoft.FSharp.Core.Unit> Prepare(
        ModuleDefinition module,AssemblyDefinition product,CecilCodecIL il,TypeReference target,CodecNumericWriterPreparation plan)
    {
        var api = new GeneratedJsonAssembly.Api(module,product);
        TypeReference Result(string name) => name == target.FullName ? module.ImportReference(target)
            : module.ImportReference(api.Operation("JsonRawPrimitive","Read"+TypeSupport.RawPrimitive(name),
                module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.Utf8Reader"))).ReturnType);
        var core = Result(plan.Core);
        var current = target;
        var steps = plan.Steps.Select(step => {
            var result = Result(step.Item4);
            var call = new MethodReference(step.Item2,result,module.ImportReference(current)) { HasThis=step.Item3 };
            if (!step.Item3) call.Parameters.Add(new ParameterDefinition(module.ImportReference(current)));
            current=result;
            return new CodecWriterConversion<TypeReference,MethodReference>(result,call,step.Item3);
        }).ToArray();
        return CodecScalarWriterEmitter.prepare(il,target,core,steps,plan.Cast,plan.AddressInput);
    }

    private static MethodDefinition Integer(ModuleDefinition module,TypeDefinition owner,AssemblyDefinition product,TypeReference target,TypeReference element,string name,CodecNumericWriterPreparation plan)
    {
        var method = Body(module,owner,product,element,target,name);
        if (!plan.DeferredWidth) {
            var body = new CecilCodecIL(method);
            var input = Prepare(module,product,body,target,plan);
            CodecIntegerWriterEmitter.writerFrom(body,Digits(module,product,element),plan.Width,plan.Signed,element.FullName=="System.Char",input);
            return method;
        }
        var narrow = Body(module,owner,product,element,target,name+"32");
        var wide = Body(module,owner,product,element,target,name+"64");
        CodecIntegerWriterEmitter.writer(new CecilCodecIL(narrow),Digits(module,product,element),32,plan.Signed,element.FullName=="System.Char");
        CodecIntegerWriterEmitter.writer(new CecilCodecIL(wide),Digits(module,product,element),64,plan.Signed,element.FullName=="System.Char");
        var size = new MethodReference("get_Size",module.TypeSystem.Int32,module.TypeSystem.IntPtr);
        CodecEmitter.nativeWidthCall(new CecilCodecIL(method),size,3,narrow,wide);
        return method;
    }

    private static MethodDefinition Decimal(ModuleDefinition module,TypeDefinition owner,AssemblyDefinition product,TypeReference target,TypeReference element,string name,CodecNumericWriterPreparation plan)
    {
        var api = new GeneratedJsonAssembly.Api(module,product);
        var method = Body(module,owner,product,element,target,name);
        var unsafeType = module.ImportReference(typeof(System.Runtime.CompilerServices.Unsafe));
        var read = new MethodReference("ReadUnaligned",module.TypeSystem.Void,unsafeType);
        var parameter = new GenericParameter("T",read);read.GenericParameters.Add(parameter);read.ReturnType=parameter;
        read.Parameters.Add(new ParameterDefinition(new ByReferenceType(module.TypeSystem.Byte)));
        GenericInstanceMethod Read(TypeReference result) {
            var closed = new GenericInstanceMethod(read);closed.GenericArguments.Add(result);return closed;
        }
        var facts = new CodecDecimalWriterApi<TypeReference,MethodReference,FieldReference>(Digits(module,product,element),
            api.Operation("JsonWriterRegion","DecimalSource"),Read(module.TypeSystem.UInt32),Read(module.TypeSystem.UInt64));
        var il = new CecilCodecIL(method);
        var input = Prepare(module,product,il,target,plan);
        CodecDecimalWriterEmitter.writerFrom(il,facts,element.FullName=="System.Char",input);
        return method;
    }

    internal static MethodDefinition Number(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,AssemblyDefinition product,TypeReference target,TypeReference element,string name)
    {
        var plan = CodecNumericPreparation.writer(target.FullName,definitions.Stored,null!);
        if(plan.StoredOperation != null)
            return StoredNumber(definitions,module,owner,product,target,element,name);
        if (definitions.TryGet("numeric-write",target,element,"",out var existing)) return existing;
        var method = plan.Family switch {
            NumericKind.Decimal => Decimal(module,owner,product,target,element,name,plan),
            NumericKind.Binary => Floating(module,owner,product,target,element,name,plan),
            NumericKind.ArbitraryInteger => BigInteger(module,owner,product,target,element,name),
            _ => Integer(module,owner,product,target,element,name,plan)
        };
        definitions.Add("numeric-write",target,element,"",method);
        return method;
    }

    internal static MethodDefinition StoredNumber(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,
        AssemblyDefinition product,TypeReference target,TypeReference element,string name)
    {
        if(definitions.TryGet("stored-number-write",target,element,"",out var existing))return existing;
        var api = new GeneratedJsonAssembly.Api(module,product,true);
        var method=Body(module,owner,product,element,target,definitions.MethodName(name));
        var prepared=module.ImportReference(api.Operation("JsonStoredNumeric","PrepareDecimal").ReturnType);
        var facts=new CodecStoredNumericApi<TypeReference,MethodReference,FieldReference>(Api(module,product,element),prepared,
            api.Operation("JsonStoredNumeric",CodecNumericPreparation.writer(target.FullName,true,null!).StoredOperation.Value),
            api.Operation("JsonStoredNumeric","Length"),api.Operation("JsonStoredNumeric","Utf8Length"),api.Operation("JsonStoredNumeric","Unit"));
        CodecStoredNumericEmitter.writer(new CecilCodecIL(method),facts,element.FullName=="System.Char");
        definitions.Add("stored-number-write",target,element,"",method);
        return method;
    }

    internal static CodecMagnitudeApi<TypeReference,MethodReference> Magnitude(ModuleDefinition module,
        AssemblyDefinition product, MethodReference offset)
    {
        var resize = module.ImportReference(typeof(Array).GetMethods().Single(method => method.Name == "Resize" && method.IsGenericMethodDefinition));
        var closedResize = new GenericInstanceMethod(resize);
        closedResize.GenericArguments.Add(module.TypeSystem.UInt32);
        var limbOffset = new GenericInstanceMethod(((GenericInstanceMethod)offset).ElementMethod);
        limbOffset.GenericArguments.Add(module.TypeSystem.UInt32);
        var magnitude=product.MainModule.GetType("SoloDatabase.Numerics.BigIntegerMagnitude");
        return new CodecMagnitudeApi<TypeReference,MethodReference>(module.TypeSystem.UInt32,new ArrayType(module.TypeSystem.UInt32),
            module.TypeSystem.Int32,module.TypeSystem.UInt64,new ByReferenceType(module.TypeSystem.UInt32),limbOffset,
            module.ImportReference(magnitude.Methods.Single(method=>method.Name=="Reference")),closedResize);
    }

    internal static MethodDefinition BigInteger(ModuleDefinition module,TypeDefinition owner,
        AssemblyDefinition product,TypeReference target,TypeReference element,string name)
    {
        var method = Body(module,owner,product,element,target,name);
        var api = new GeneratedJsonAssembly.Api(module,product);
        var output = Api(module,product,element);
        var preparation=product.MainModule.GetType("SoloDatabase.Numerics.BigIntegerRadix");
        var prepared=product.MainModule.GetType("SoloDatabase.Numerics.BigIntegerRadixDigits");
        MethodReference PreparedMethod(string name)=>module.ImportReference(prepared.Methods.Single(item=>item.Name==name));
        var facts = new CodecBigIntegerWriterApi<TypeReference,MethodReference,FieldReference>(output,
            module.ImportReference(prepared),new ArrayType(module.TypeSystem.UInt32),
            module.ImportReference(preparation.Methods.Single(item=>item.Name=="Raw")),
            PreparedMethod("get_Sign"),PreparedMethod("get_Count"),PreparedMethod("get_Chunks"));
        CodecBigIntegerWriterEmitter.writer(new CecilCodecIL(method),facts,element.FullName == "System.Char");
        return method;
    }

    private static MethodDefinition Floating(ModuleDefinition module,TypeDefinition owner,AssemblyDefinition product,TypeReference target,TypeReference element,string name,CodecNumericWriterPreparation plan)
    {
        var api = new GeneratedJsonAssembly.Api(module,product);
        var method = Body(module,owner,product,element,target,name);
        var unsafeType = module.ImportReference(typeof(System.Runtime.CompilerServices.Unsafe));
        var read = new MethodReference("ReadUnaligned",module.TypeSystem.Void,unsafeType);
        var parameter = new GenericParameter("T",read);read.GenericParameters.Add(parameter);read.ReturnType=parameter;
        read.Parameters.Add(new ParameterDefinition(new ByReferenceType(module.TypeSystem.Byte)));
        var bits = new GenericInstanceMethod(read);bits.GenericArguments.Add(plan.Bits == "System.UInt64" ? module.TypeSystem.UInt64 : plan.Bits == "System.UInt16" ? module.TypeSystem.UInt16 : module.TypeSystem.UInt32);
        var prepared=product.MainModule.GetType("SoloDatabase.Numerics.BinaryDecimalCoefficient");
        var kernel=product.MainModule.GetType("SoloDatabase.Numerics.BinaryDecimal");
        MethodReference Operation(string name)=>module.ImportReference(kernel.Methods.Single(method=>method.Name==name));
        MethodReference Fact(string name)=>module.ImportReference(prepared.Methods.Single(method=>method.Name==name));
        var facts = new CodecFloatingWriterApi<TypeReference,MethodReference,FieldReference>(Digits(module,product,element),module.ImportReference(prepared),
            Operation("Coefficient"),Fact("get_Value"),Fact("get_Exponent"),api.Operation("JsonWriterRegion","Source",module.ImportReference(target)),bits);
        var il = new CecilCodecIL(method);
        var input = Prepare(module,product,il,target,plan);
        CodecFloatingWriterEmitter.writerFrom(il,facts,plan.Width,element.FullName=="System.Char",input);
        return method;
    }
    internal static MethodDefinition Guid(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,AssemblyDefinition product,TypeReference target,TypeReference element,string name)
    {
        if (definitions.TryGet("guid-write",target,element,"",out var existing)) return existing;
        var api = new GeneratedJsonAssembly.Api(module,product);
        var method = Body(module,owner,product,element,target,name);
        var unsafeType = module.ImportReference(typeof(System.Runtime.CompilerServices.Unsafe));
        var read = new MethodReference("ReadUnaligned",module.TypeSystem.Void,unsafeType);
        var parameter = new GenericParameter("T",read);read.GenericParameters.Add(parameter);read.ReturnType=parameter;
        read.Parameters.Add(new ParameterDefinition(new ByReferenceType(module.TypeSystem.Byte)));
        GenericInstanceMethod Read(TypeReference result) {
            var closed = new GenericInstanceMethod(read);closed.GenericArguments.Add(result);return closed;
        }
        var facts = new CodecGuidWriterApi<TypeReference,MethodReference,FieldReference>(Api(module,product,element),
            api.Operation("JsonWriterRegion","TryReserveKnownSpan",element),
            api.Operation("JsonWriterRegion","Source",target),Read(module.TypeSystem.UInt32),Read(module.TypeSystem.UInt16));
        CodecGuidWriterEmitter.writer(new CecilCodecIL(method),facts,element.FullName=="System.Char");
        definitions.Add("guid-write",target,element,"",method);
        return method;
    }

    private static MethodReference WordWrite(ModuleDefinition module,TypeReference word)
    {
        var method=new MethodReference("WriteUnaligned",module.TypeSystem.Void,module.ImportReference(typeof(System.Runtime.CompilerServices.Unsafe)));
        var argument=new GenericParameter("T",method);method.GenericParameters.Add(argument);
        method.Parameters.Add(new ParameterDefinition(new ByReferenceType(module.TypeSystem.Byte)));
        method.Parameters.Add(new ParameterDefinition(argument));
        var closed=new GenericInstanceMethod(method);closed.GenericArguments.Add(word);return closed;
    }

    internal static MethodDefinition Bytes(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,AssemblyDefinition product,TypeReference target,TypeReference element,string name,bool ordinarySequence=false)
    {
        var role=ordinarySequence?"sequence-bytes-write":"bytes-write";
        if (definitions.TryGet(role,target,element,"",out var existing)) return existing;
        var api = new GeneratedJsonAssembly.Api(module,product);
        var method = Body(module,owner,product,element,target,name);
        var facts = new CodecBase64WriterApi<TypeReference,MethodReference,FieldReference>(Api(module,product,element),
            ordinarySequence?null:FSharpOption<MethodReference>.Some(api.Operation("JsonWriterRegion","TryReserveKnownSpan",element)),
            ordinarySequence?FSharpOption<MethodReference>.Some(api.Operation("JsonWriterRegion","Base64LengthOverflow")):null,target,
            api.Operation("JsonWriterRegion","CaptureBytes" + (element.FullName == "System.Char" ? "Utf16" : "Utf8")),api.Operation("JsonWriterRegion","BytesReference"),
            api.Operation("JsonWriterRegion","Base64Reference"),definitions.Base64Vector(module,element.FullName=="System.Char"),
            module.ImportReference(typeof(BitConverter).GetField("IsLittleEndian")),
            WordWrite(module,element.FullName=="System.Char"?module.TypeSystem.UInt64:module.TypeSystem.UInt32));
        CodecBase64WriterEmitter.writer(new CecilCodecIL(method),facts,element.FullName=="System.Char",ordinarySequence);
        definitions.Add(role,target,element,"",method);
        return method;
    }

    internal static void RegisterSequenceBytes(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,
        ILProcessor entry,AssemblyDefinition product)
    {
        var api=new GeneratedJsonAssembly.Api(module,product);
        var target=new ArrayType(module.TypeSystem.Byte);
        foreach(var encoding in new[]{module.TypeSystem.Byte,module.TypeSystem.Char}) {
            var body=Bytes(definitions,module,owner,product,target,encoding,"WriteSequenceBytes"+(encoding.FullName=="System.Char"?"Utf16":"Utf8"),true);
            var delegateType=new GenericInstanceType(module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonDirectWriter`2")));
            delegateType.GenericArguments.Add(target);delegateType.GenericArguments.Add(encoding);
            var constructor=new MethodReference(".ctor",module.TypeSystem.Void,delegateType){HasThis=true};
            constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));
            constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
            entry.Emit(OpCodes.Ldnull);entry.Emit(OpCodes.Ldftn,body);entry.Emit(OpCodes.Newobj,constructor);
        }
        entry.Emit(OpCodes.Call,api.Metadata("RegisterSequenceByteWriters"));
    }

}
