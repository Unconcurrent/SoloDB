using System;
using System.Linq;
using Microsoft.FSharp.Core;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;

namespace SoloDatabase.Metadata.Tools;

// Exact compiler facts select addresses; their instructions share the runtime emitter.
internal sealed class GeneratedQueryReferences
{
    private readonly ModuleDefinition module;
    private readonly AssemblyDefinition product;
    internal GeneratedQueryReferences(ModuleDefinition module, AssemblyDefinition product)
    { this.module = module; this.product = product; Reference = module.ImportReference(product.MainModule.GetType("SoloDatabase.QueryReference")); }
    internal TypeReference Reference { get; }
    internal MethodReference Address(TypeReference type)
    {
        var method = new GenericInstanceMethod(module.ImportReference(product.MainModule.GetType("SoloDatabase.QueryReference").Methods.Single(method => method.Name == "Address")));
        method.GenericArguments.Add(module.ImportReference(type));
        return method;
    }
    internal FSharpFunc<TypeReference,FSharpOption<MethodReference>> Arguments =>
        FSharpFunc<TypeReference,FSharpOption<MethodReference>>.FromConverter(new Converter<TypeReference,FSharpOption<MethodReference>>(type =>
            type is ByReferenceType reference ? FSharpOption<MethodReference>.Some(Address(reference.ElementType)) : null!));
    internal FSharpOption<MethodReference> Receiver(TypeReference type) => type.IsValueType ? FSharpOption<MethodReference>.Some(Address(type)) : null!;

    private void Register(TypeDefinition owner, ILProcessor entry, TypeReference declaring, string name, TypeReference value, MethodDefinition method)
    {
        var api = new GeneratedJsonAssembly.Api(module,product);
        var storage = new GenericInstanceMethod(module.ImportReference(product.MainModule.GetType("SoloDatabase.QueryValueMetadata").Methods.Single(method => method.Name == "RegisterStorage")));
        storage.GenericArguments.Add(module.ImportReference(value)); entry.Emit(OpCodes.Call,storage);
        api.TypeToken(entry,declaring); entry.Emit(OpCodes.Ldstr,name); api.TypeToken(entry,value);
        var function = new GenericInstanceType(module.ImportReference(product.MainModule.GetType("SoloDatabase.QueryAddress`1")));
        function.GenericArguments.Add(module.ImportReference(value));
        var constructor = new MethodReference(".ctor",module.TypeSystem.Void,function) { HasThis = true };
        constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object)); constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
        entry.Emit(OpCodes.Ldnull); entry.Emit(OpCodes.Ldftn,method); entry.Emit(OpCodes.Newobj,constructor);
        entry.Emit(OpCodes.Call,module.ImportReference(product.MainModule.GetType("SoloDatabase.QueryValueMetadata").Methods.Single(method => method.Name == "RegisterGeneratedAddress")));
    }
    internal void Field(TypeDefinition owner, ILProcessor entry, TypeReference declaring, FieldReference field, TypeReference value, bool isStatic)
    {
        var method = new MethodDefinition("FieldAddress" + owner.Methods.Count,MethodAttributes.Private | MethodAttributes.Static,new ByReferenceType(module.ImportReference(value)));
        method.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object)); method.Parameters.Add(new ParameterDefinition(new ArrayType(module.TypeSystem.Int32)));
        owner.Methods.Add(method);
        QueryOperationEmitter.fieldAddress(new CecilCodecIL(method),Reference,declaring,declaring.IsValueType,Address(declaring),field,isStatic);
        Register(owner,entry,declaring,field.Name,value,method);
    }
    internal void Array(TypeDefinition owner, ILProcessor entry, ArrayType array)
    {
        var value = array.ElementType;
        var method = new MethodDefinition("ArrayAddress" + owner.Methods.Count,MethodAttributes.Private | MethodAttributes.Static,new ByReferenceType(module.ImportReference(value)));
        method.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object)); method.Parameters.Add(new ParameterDefinition(new ArrayType(module.TypeSystem.Int32)));
        owner.Methods.Add(method);
        var address = new MethodReference("Address",new ByReferenceType(module.ImportReference(value)),array) { HasThis = true };
        for (var index=0; index<array.Rank; index++) address.Parameters.Add(new ParameterDefinition(module.TypeSystem.Int32));
        QueryOperationEmitter.arrayAddress(new CecilCodecIL(method),array,value,array.Rank,address);
        Register(owner,entry,array,"[]",value,method);
    }
    internal void Box(TypeDefinition owner, ILProcessor entry, TypeReference value)
    {
        var method = new MethodDefinition("BoxAddress" + owner.Methods.Count,MethodAttributes.Private | MethodAttributes.Static,new ByReferenceType(module.ImportReference(value)));
        method.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object)); method.Parameters.Add(new ParameterDefinition(new ArrayType(module.TypeSystem.Int32)));
        owner.Methods.Add(method);
        QueryOperationEmitter.boxAddress(new CecilCodecIL(method),value);
        Register(owner,entry,value,"&",value,method);
    }
}
