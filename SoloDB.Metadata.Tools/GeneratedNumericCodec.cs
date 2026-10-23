using System;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

// Only operand and original signature mapping belongs here; numeric policy is emitted in FSharp.
internal static class GeneratedNumericCodec
{
    internal static MethodDefinition Reader(GeneratedJsonDefinitions definitions,ModuleDefinition module, TypeDefinition owner,
        AssemblyDefinition product, TypeReference target, TypeReference reader, string name)
    {
        var plan = CodecNumericPreparation.reader(target.FullName,definitions.Stored,null!);
        if(plan.ReuseNative)
            return Reader(definitions.NativeView,module,owner,product,target,reader,name);
        var storedName = plan.StoredReader;
        if (definitions.TryGet("numeric-read",target,reader,"",out var existing)) return existing;
        var api = new GeneratedJsonAssembly.Api(module, product,definitions.Stored);
        var coreName = plan.Core;
        var scalarAdapter = plan.ScalarAdapter;
        var core = scalarAdapter ? module.ImportReference(target)
            : module.ImportReference(api.Operation("JsonRawPrimitive","Read" + TypeSupport.NumericIntrinsicOperation(coreName),reader).ReturnType);
        var adapter = plan.TargetCompatibility;
        var utf16 = reader.Name == "Utf16Reader";
        MethodDefinition BodyResult(string suffix, TypeReference result, params TypeReference[] arguments)
        {
            var body = new MethodDefinition(definitions.MethodName(name + suffix),
                MethodAttributes.Assembly | MethodAttributes.Static | MethodAttributes.HideBySig,
                module.ImportReference(result));
            body.Body.InitLocals = true;
            foreach (var argument in arguments) body.Parameters.Add(new ParameterDefinition(module.ImportReference(argument)));
            owner.Methods.Add(body);
            return body;
        }
        MethodDefinition Body(string suffix, params TypeReference[] arguments) => BodyResult(suffix,core,arguments);
        var context = new GenericInstanceType(module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonReadContext`1")));
        context.GenericArguments.Add(module.ImportReference(reader));
        var entry = BodyResult(plan.StringWrapper ? "String" : "",target,new ByReferenceType(context));
        definitions.Add("numeric-read",target,reader,"",entry);
        var unsafeScope = product.MainModule.AssemblyReferences.Single(reference => reference.Name == "System.Runtime.CompilerServices.Unsafe");
        var unsafeType = module.ImportReference(new TypeReference("System.Runtime.CompilerServices", "Unsafe", product.MainModule, unsafeScope));
        GenericInstanceMethod Offset(TypeReference value, TypeReference index)
        {
            var add = new MethodReference("Add", module.TypeSystem.Void, unsafeType);
            var element = new GenericParameter("T", add);
            add.GenericParameters.Add(element);
            add.ReturnType = new ByReferenceType(element);
            add.Parameters.Add(new ParameterDefinition(new ByReferenceType(element)));
            add.Parameters.Add(new ParameterDefinition(index));
            var result = new GenericInstanceMethod(add);
            result.GenericArguments.Add(value);
            return result;
        }
        var offset = Offset(module.TypeSystem.Byte,module.TypeSystem.UIntPtr);
        MethodDefinition Rounded(int width,TypeReference bits,string suffix)
        {
            var policy = width.ToString(System.Globalization.CultureInfo.InvariantCulture);
            if (definitions.TryGet("binary-rounding",bits,module.TypeSystem.Void,policy,out var existingBits)) return existingBits;
            var body = BodyResult(suffix,bits,module.TypeSystem.UInt64,module.TypeSystem.Int64,module.TypeSystem.Boolean);
            definitions.Add("binary-rounding",bits,module.TypeSystem.Void,policy,body);
            var kernel=product.MainModule.GetType("SoloDatabase.Numerics.BinaryDecimal");
            var rounding = new CodecFloatingApi<MethodReference>(module.ImportReference(kernel.Methods.Single(method=>method.Name=="RoundBits")));
            CodecFloatingEmitter.bits(new CecilCodecIL(body),rounding,width);
            return body;
        }
        var rawArguments = new TypeReference[] { new ByReferenceType(module.TypeSystem.Byte), module.TypeSystem.Int32, new ByReferenceType(module.TypeSystem.Int32) };
        MethodReference Constructor(string type, params TypeReference[] arguments)
        {
            var result = new MethodReference(".ctor",module.TypeSystem.Void,
                new TypeReference("System",type,module,module.TypeSystem.Object.Scope)) { HasThis = true };
            foreach (var argument in arguments) result.Parameters.Add(new ParameterDefinition(argument));
            return result;
        }
        var integerApi = new CodecIntegerApi<TypeReference, MethodReference>(module.TypeSystem.Int32,module.TypeSystem.Int64,
            module.TypeSystem.UInt32,module.TypeSystem.UInt64,offset,
            Constructor("FormatException",module.TypeSystem.String),Constructor("OverflowException"),Constructor("Exception",module.TypeSystem.String));
        MethodDefinition Leaf(int width, bool hex)
        {
            var role = hex ? "integer-hex" : "integer-decimal";
            var policy = width + ":" + plan.Signed + ":" + plan.Native;
            if (definitions.TryGet(role,core,reader,policy,out var existingLeaf)) return existingLeaf;
            var leaf = Body((hex ? "Hex" : "Decimal") + width,rawArguments);
            definitions.Add(role,core,reader,policy,leaf);
            if (hex) CodecIntegerEmitter.hexReader(new CecilCodecIL(leaf),integerApi,width,plan.Signed,utf16,plan.Native);
            else CodecIntegerEmitter.decimalReader(new CecilCodecIL(leaf),integerApi,width,plan.Signed,utf16,plan.Native);
            return leaf;
        }
        MethodDefinition SelectWidth(bool hex)
        {
            if (!plan.DeferredWidth) return Leaf(plan.Width,hex);
            var narrow = Leaf(32,hex);
            var wide = Leaf(64,hex);
            var selected = Body(hex ? "Hex" : "Decimal",rawArguments);
            var size = new MethodReference("get_Size",module.TypeSystem.Int32,module.TypeSystem.IntPtr);
            CodecEmitter.nativeWidthCall(new CecilCodecIL(selected),size,3,narrow,wide);
            return selected;
        }
        var compatibility = scalarAdapter ? GeneratedNumericScalar.Compatibility(definitions,module,owner,product,core,reader,context,name + "Compatibility")
            : api.Operation("JsonRawPrimitive","Read" + TypeSupport.NumericIntrinsicOperation(coreName),reader);
        MethodReference Input(string operation) => api.Operation("JsonNumericInput",operation,reader);
        TypeReference[] Arguments(bool contextArgument) => contextArgument ? rawArguments.Concat(new[] { new ByReferenceType(context) }).ToArray() : rawArguments;
        Microsoft.FSharp.Core.FSharpOption<MethodReference> quoted = null!;
        MethodDefinition QuotedWrapper(MethodReference leaf)
        {
            var wrapper = Body("Quoted",Arguments(plan.QuotedContext));
            var quotedApi = new CodecQuotedNumberApi<TypeReference,MethodReference>(core,module.TypeSystem.Int32,
                offset,leaf,Input("Consumed"),compatibility);
            CodecQuotedNumberEmitter.reader(new CecilCodecIL(wrapper),quotedApi,utf16);
            return wrapper;
        }
        MethodDefinition decimalRead;
        MethodDefinition hexRead;

        switch (plan.Leaf)
        {
            case NumericKind.Inferred:
                var inferredRounded = Rounded(64,module.TypeSystem.UInt64,"RoundDouble");
                var inferredAs = new MethodReference("As",module.TypeSystem.Void,unsafeType);
                var inferredFrom = new GenericParameter("TFrom",inferredAs);
                var inferredTo = new GenericParameter("TTo",inferredAs);
                inferredAs.GenericParameters.Add(inferredFrom); inferredAs.GenericParameters.Add(inferredTo);
                inferredAs.ReturnType = new ByReferenceType(inferredTo);
                inferredAs.Parameters.Add(new ParameterDefinition(new ByReferenceType(inferredFrom)));
                var inferredDoubleReference = new GenericInstanceMethod(inferredAs);
                inferredDoubleReference.GenericArguments.Add(module.TypeSystem.UInt64);
                inferredDoubleReference.GenericArguments.Add(module.TypeSystem.Double);
                var inferredFloating = new CodecFloatingReadApi<TypeReference,MethodReference>(module.TypeSystem.Double,
                    module.TypeSystem.UInt64,module.TypeSystem.Int32,module.TypeSystem.Int64,module.TypeSystem.UInt64,
                    offset,inferredRounded,inferredDoubleReference,api.Operation("JsonRawPrimitive","ReadDouble",reader),integerApi.InvalidNumber);
                var inferredDecimalType = api.Operation("JsonRawPrimitive","ReadDecimal",reader).ReturnType;
                var inferredDecimalConstructor = new MethodReference(".ctor",module.TypeSystem.Void,inferredDecimalType) { HasThis = true };
                foreach (var itemType in new[] { module.TypeSystem.Int32,module.TypeSystem.Int32,module.TypeSystem.Int32,module.TypeSystem.Boolean,module.TypeSystem.Byte })
                    inferredDecimalConstructor.Parameters.Add(new ParameterDefinition(itemType));
                var inferredDecimal = new CodecDecimalApi<TypeReference,MethodReference>(inferredDecimalType,
                    module.TypeSystem.Int32,module.TypeSystem.Int64,module.TypeSystem.UInt64,offset,
                    inferredDecimalConstructor,integerApi.Overflow,integerApi.InvalidNumber);
                var inferredBigType = api.Operation("JsonRawPrimitive","ReadBigInteger",reader).ReturnType;
                var parameterResult = plan.ParameterResult;
                var inferredBigReader = parameterResult ? null! : Reader(definitions,module,owner,product,inferredBigType,reader,name+"BigInteger");
                Microsoft.FSharp.Core.FSharpOption<MethodReference> parameterConstructor = null!;
                if (parameterResult) {
                    var suppliedConstructor = new MethodReference(".ctor",module.TypeSystem.Void,target) { HasThis = true };
                    suppliedConstructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));
                    suppliedConstructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Boolean));
                    parameterConstructor = Microsoft.FSharp.Core.FSharpOption<MethodReference>.Some(suppliedConstructor);
                }
                var inference = new CodecDynamicNumberApi<TypeReference,MethodReference>(integerApi,inferredDecimal,inferredFloating,inferredBigType,inferredBigReader,parameterConstructor);
                decimalRead = Body("Decimal",Arguments(plan.DecimalContext));
                CodecDynamicNumberEmitter.decimalReader(new CecilCodecIL(decimalRead),inference,utf16);
                hexRead = Body("Hex",Arguments(plan.HexContext));
                CodecDynamicNumberEmitter.hexReader(new CecilCodecIL(hexRead),inference,utf16);
                break;
            case NumericKind.NumericText:
                var stringConstructor = new MethodReference(".ctor",module.TypeSystem.Void,module.TypeSystem.String) { HasThis = true };
                stringConstructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Char));
                stringConstructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Int32));
                var textApi = new CodecNumericTextApi<TypeReference,MethodReference>(integerApi,module.TypeSystem.String,
                    new ByReferenceType(module.TypeSystem.Byte),stringConstructor,api.Operation("JsonNumericInput","StringReference"));
                var textMagnitude = GeneratedOutputCodec.Magnitude(module,product,offset);
                decimalRead = Body("Decimal",rawArguments);
                CodecNumericTextEmitter.decimalReader(new CecilCodecIL(decimalRead),textApi,utf16);
                hexRead = Body("Hex",rawArguments);
                CodecNumericTextEmitter.hexReader(new CecilCodecIL(hexRead),textApi,textMagnitude,new ArrayType(module.TypeSystem.UInt32),utf16);
                break;
            case NumericKind.Predicate:
                decimalRead = Body("Decimal",rawArguments);
                CodecNumericBooleanEmitter.decimalReader(new CecilCodecIL(decimalRead),integerApi,utf16);
                hexRead = Body("Hex",rawArguments);
                CodecNumericBooleanEmitter.hexReader(new CecilCodecIL(hexRead),integerApi,utf16);
                break;
            case NumericKind.ArbitraryInteger:
                var construction=product.MainModule.GetType("SoloDatabase.Numerics.BigIntegerMagnitude");
                var arbitraryApi = new CodecBigIntegerApi<TypeReference,MethodReference>(
                    GeneratedOutputCodec.Magnitude(module,product,offset),module.TypeSystem.Int64,offset,
                    module.ImportReference(construction.Methods.Single(method=>method.Name=="Finish")),
                    integerApi.InvalidNumber,integerApi.InvalidHex);
                decimalRead = Body("Decimal",rawArguments);
                CodecBigIntegerEmitter.decimalReader(new CecilCodecIL(decimalRead),arbitraryApi,utf16);
                hexRead = Body("Hex",rawArguments);
                CodecBigIntegerEmitter.hexReader(new CecilCodecIL(hexRead),arbitraryApi,utf16);
                break;
            case NumericKind.Integer:
                decimalRead = SelectWidth(false);
                hexRead = SelectWidth(true);
                break;
            case NumericKind.Binary:
                var bitsType = plan.Bits == "System.UInt64" ? module.TypeSystem.UInt64 : plan.Bits == "System.UInt16" ? module.TypeSystem.UInt16 : module.TypeSystem.UInt32;
                var rounded = Rounded(plan.Width,bitsType,"Rounded");
                var reinterpret = new MethodReference("As",module.TypeSystem.Void,unsafeType);
                var from = new GenericParameter("TFrom",reinterpret);
                var to = new GenericParameter("TTo",reinterpret);
                reinterpret.GenericParameters.Add(from); reinterpret.GenericParameters.Add(to);
                reinterpret.ReturnType = new ByReferenceType(to);
                reinterpret.Parameters.Add(new ParameterDefinition(new ByReferenceType(from)));
                var reference = new GenericInstanceMethod(reinterpret);
                reference.GenericArguments.Add(bitsType); reference.GenericArguments.Add(core);
                var floatingApi = new CodecFloatingReadApi<TypeReference,MethodReference>(core,bitsType,
                    module.TypeSystem.Int32,module.TypeSystem.Int64,module.TypeSystem.UInt64,offset,rounded,reference,compatibility,integerApi.InvalidNumber);
                MethodDefinition FloatingLeaf(string role,string suffix,TypeReference[] arguments,Action<MethodDefinition> emit)
                {
                    var policy = plan.Width.ToString(System.Globalization.CultureInfo.InvariantCulture);
                    if (definitions.TryGet(role,core,reader,policy,out var existingLeaf)) return existingLeaf;
                    var body = Body(suffix,arguments);
                    definitions.Add(role,core,reader,policy,body);
                    emit(body);
                    return body;
                }
                decimalRead = FloatingLeaf("binary-decimal","Decimal",Arguments(plan.DecimalContext),
                    body => CodecFloatingReadEmitter.reader(new CecilCodecIL(body),floatingApi,plan.Width,utf16,false));
                hexRead = FloatingLeaf("binary-hex","Hex",Arguments(plan.HexContext),
                    body => CodecFloatingReadEmitter.hex(new CecilCodecIL(body),integerApi,floatingApi,plan.Width,utf16));
                if (plan.Quote == CodecNumericQuote.Binary)
                {
                    var quotedBinary = FloatingLeaf("binary-quoted","QuotedDecimal",Arguments(plan.QuotedContext),
                        body => CodecFloatingReadEmitter.reader(new CecilCodecIL(body),floatingApi,plan.Width,utf16,true));
                    quoted = Microsoft.FSharp.Core.FSharpOption<MethodReference>.Some(QuotedWrapper(quotedBinary));
                }
                break;
            case NumericKind.Decimal:
                var decimalConstructor = new MethodReference(".ctor",module.TypeSystem.Void,core) { HasThis = true };
                foreach (var parameter in new[] { module.TypeSystem.Int32,module.TypeSystem.Int32,module.TypeSystem.Int32,module.TypeSystem.Boolean,module.TypeSystem.Byte })
                    decimalConstructor.Parameters.Add(new ParameterDefinition(parameter));
                var decimalApi = new CodecDecimalApi<TypeReference,MethodReference>(core,module.TypeSystem.Int32,module.TypeSystem.Int64,
                    module.TypeSystem.UInt64,offset,decimalConstructor,integerApi.Overflow,integerApi.InvalidNumber);
                decimalRead = Body("Decimal",rawArguments);
                CodecDecimalEmitter.reader(new CecilCodecIL(decimalRead),decimalApi,utf16,null!);
                hexRead = Body("Hex",rawArguments);
                CodecDecimalEmitter.hex(new CecilCodecIL(hexRead),integerApi,decimalApi,utf16);
                if (plan.Quote == CodecNumericQuote.Decimal)
                {
                    var quotedDecimal = Body("QuotedDecimal",Arguments(plan.QuotedContext));
                    CodecDecimalEmitter.reader(new CecilCodecIL(quotedDecimal),decimalApi,utf16,
                        Microsoft.FSharp.Core.FSharpOption<MethodReference>.Some(compatibility));
                    quoted = Microsoft.FSharp.Core.FSharpOption<MethodReference>.Some(QuotedWrapper(quotedDecimal));
                }
                break;
            default: throw new InvalidOperationException("Unsupported numeric format.");
        }
        if (plan.Quote == CodecNumericQuote.Integer)
        {
            var quotedApi = new CodecQuotedIntegerApi<TypeReference,MethodReference>(module.TypeSystem.Int32,module.TypeSystem.UInt32,
                module.TypeSystem.UInt64,offset,compatibility);
            MethodDefinition QuotedLeaf(int width)
            {
                var leaf = Body("Quoted" + width,Arguments(plan.QuotedContext));
                CodecQuotedIntegerEmitter.reader(new CecilCodecIL(leaf),quotedApi,width,plan.Signed,utf16,plan.Native);
                return leaf;
            }
            MethodDefinition selected;
            if (!plan.DeferredWidth) selected = QuotedLeaf(plan.Width);
            else
            {
                var narrow = QuotedLeaf(32);
                var wide = QuotedLeaf(64);
                selected = Body("Quoted",Arguments(plan.QuotedContext));
                var size = new MethodReference("get_Size",module.TypeSystem.Int32,module.TypeSystem.IntPtr);
                CodecEmitter.nativeWidthCall(new CecilCodecIL(selected),size,4,narrow,wide);
            }
            quoted = Microsoft.FSharp.Core.FSharpOption<MethodReference>.Some(selected);
        }
        var argument = core;
        var after = plan.Finish.Select(finish =>
        {
            var type = finish.Item1 == target.FullName ? module.ImportReference(target)
                : module.ImportReference(api.Operation("JsonRawPrimitive","Read" + TypeSupport.NumericIntrinsicOperation(finish.Item1),reader).ReturnType);
            var call = new MethodReference(finish.Item2,finish.Item3 ? module.TypeSystem.Void : type,type) { HasThis = finish.Item3 };
            call.Parameters.Add(new ParameterDefinition(argument));
            argument = type;
            return new CodecNumericFinish<MethodReference>(call,finish.Item3);
        }).ToArray();
        var targetCompatibility = adapter ? api.Operation("JsonRawPrimitive","Read" + TypeSupport.NumericIntrinsicOperation(target.FullName),reader) : compatibility;
        Microsoft.FSharp.Core.FSharpOption<Tuple<MethodReference,MethodReference>> predicate = null!,text = null!;
        if (plan.Predicate)
        {
            var zero = new MethodReference("op_Implicit",core,core);
            zero.Parameters.Add(new ParameterDefinition(module.TypeSystem.Int32));
            var compare = new MethodReference("op_Inequality",module.TypeSystem.Boolean,core);
            compare.Parameters.Add(new ParameterDefinition(core)); compare.Parameters.Add(new ParameterDefinition(core));
            predicate = Microsoft.FSharp.Core.FSharpOption<Tuple<MethodReference,MethodReference>>.Some(Tuple.Create<MethodReference,MethodReference>(zero,compare));
        }
        if (plan.Text)
        {
            var culture = new TypeReference("System.Globalization","CultureInfo",module,core.Scope);
            var provider = new TypeReference("System","IFormatProvider",module,core.Scope);
            var invariant = new MethodReference("get_InvariantCulture",culture,culture);
            var format = new MethodReference("ToString",module.TypeSystem.String,core) { HasThis = true };
            format.Parameters.Add(new ParameterDefinition(provider));
            text = Microsoft.FSharp.Core.FSharpOption<Tuple<MethodReference,MethodReference>>.Some(Tuple.Create<MethodReference,MethodReference>(invariant,format));
        }
        var method = plan.StringWrapper ? BodyResult("",target,new ByReferenceType(context)) : entry;
        Microsoft.FSharp.Core.FSharpOption<MethodReference> storedQuoted = null!;
        if(storedName != null) {
            var readKey=plan.StoredCoreArgument ? api.Operation("JsonStoredNumeric",storedName.Value,core,reader)
                : api.Operation("JsonStoredNumeric",storedName.Value,reader);
            storedQuoted=Microsoft.FSharp.Core.FSharpOption<MethodReference>.Some(readKey);
            quoted=null!;
        }
        var driverApi = new CodecNumericDriverApi<TypeReference, MethodReference>(module.ImportReference(target),core,after,predicate,text,module.TypeSystem.Int32,
            rawArguments[0],rawArguments[2],Input("Begin"),Input("Reference"),Input("Length"),Input("Position"),Input("Complete"),offset,
            decimalRead,hexRead,quoted,storedQuoted,plan.DecimalContext,plan.HexContext,targetCompatibility);
        CodecNumericDriverEmitter.reader(new CecilCodecIL(method),driverApi,utf16);
        if (plan.StringWrapper)
        {
            var textMethod = entry;
            var read = new MethodReference("ReadUnaligned",module.TypeSystem.Void,unsafeType);
            var result = new GenericParameter("T",read);
            read.GenericParameters.Add(result);
            read.ReturnType = result;
            read.Parameters.Add(new ParameterDefinition(new ByReferenceType(module.TypeSystem.Byte)));
            var word = new GenericInstanceMethod(read);
            word.GenericArguments.Add(module.TypeSystem.UInt64);
            var stringApi = new CodecStringApi<TypeReference,MethodReference>(module.TypeSystem.Int32,module.TypeSystem.UInt64,
                rawArguments[0],rawArguments[2],Input("Begin"),Input("Reference"),Input("Length"),Input("Position"),Microsoft.FSharp.Core.FSharpOption<MethodReference>.Some(Input("Complete")),
                offset,word,api.Operation("JsonStringInput",utf16 ? "Utf16" : "Utf8"),
                api.Operation("JsonStringInput",utf16 ? "EscapedUtf16" : "EscapedUtf8"),method);
            CodecStringEmitter.reader(new CecilCodecIL(textMethod),stringApi,utf16);
            return textMethod;
        }
        return method;
    }

    internal static void Register(GeneratedJsonDefinitions definitions,ModuleDefinition module, TypeDefinition owner, ILProcessor entry,
        AssemblyDefinition product, TypeReference target, int index, bool intrinsicOnly = false)
    {
        var api = new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        foreach (var encoding in new[] { "Utf8", "Utf16" })
        {
            var reader = module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator." + encoding + "Reader"));
            MethodReference method = TypeSupport.NumericCore(target.FullName) != null
                ? Reader(definitions,module,owner,product,target,reader,"ReadNumeric" + encoding + index)
                : TypeSupport.FormattedReader(target.FullName).Length!=0 ? GeneratedFormattedCodec.Reader(definitions,module,owner,product,target,reader,"ReadFormatted"+encoding+index)
                : api.Operation("JsonRawPrimitive","Read"+TypeSupport.NumericIntrinsicOperation(target.FullName),reader);
            var callable = new GenericInstanceType(module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonDirectReader`2")));
            callable.GenericArguments.Add(module.ImportReference(target)); callable.GenericArguments.Add(reader);
            var constructor = new MethodReference(".ctor",module.TypeSystem.Void,callable) { HasThis = true };
            constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object)); constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
            entry.Emit(OpCodes.Ldnull); entry.Emit(OpCodes.Ldftn,method); entry.Emit(OpCodes.Newobj,constructor);
        }
        if (intrinsicOnly) {
            entry.Emit(OpCodes.Call,api.Metadata("RegisterNumericIntrinsics",target));
            return;
        }
        entry.Emit(OpCodes.Call,api.Metadata("RegisterRawReaders",target));
        foreach(var kind in GeneratedJsonDefinitions.Outputs) {
            var writes = definitions.OutputView(kind);
            foreach (var encoding in new[] { "Utf8", "Utf16" }) {
                var element = encoding == "Utf8" ? module.TypeSystem.Byte : module.TypeSystem.Char;
                var output = writes.OutputType(module,product,element);
                MethodReference method = TypeSupport.NumericWriterCore(target.FullName) != null
                    ? GeneratedOutputCodec.Number(writes,module,owner,product,target,element,"WriteNumeric" + encoding + index)
                    : target.FullName == "System.String" ? GeneratedOutputCodec.String(writes,module,owner,product,element,"WriteString"+encoding+index)
                    : target.FullName == "System.Boolean" ? GeneratedOutputCodec.Boolean(writes,module,owner,product,element,"WriteNumeric"+encoding+index)
                    : TypeSupport.NumericIntrinsicOperation(target.FullName) == "Guid" ? GeneratedOutputCodec.Guid(writes,module,owner,product,target,element,"WriteGuid"+encoding+index)
                    : TypeSupport.NumericIntrinsicOperation(target.FullName) == "Bytes" ? GeneratedOutputCodec.Bytes(writes,module,owner,product,target,element,"WriteBytes"+encoding+index)
                    : api.Operation("JsonRawPrimitive","Write" + TypeSupport.NumericIntrinsicOperation(target.FullName),element,output);
                var callable = new GenericInstanceType(module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonDirectWriter`2")));
                callable.GenericArguments.Add(module.ImportReference(target));callable.GenericArguments.Add(element);
                GeneratedJsonAssembly.Delegate(entry,callable,method);
            }
            GeneratedJsonAssembly.BoxedWriters(definitions,module,owner,entry,product,target);
            entry.Emit(OpCodes.Call,api.Metadata("RegisterOutputWriters",target,writes.OutputType(module,product,module.TypeSystem.Byte),writes.OutputType(module,product,module.TypeSystem.Char)));
        }
    }
}
