using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.FSharp.Core;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.CodecGeneration;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

internal static partial class GeneratedDictionaryCodec
{
    internal static MethodDefinition Reader(GeneratedJsonDefinitions definitions,ModuleDefinition module,TypeDefinition owner,
        AssemblyDefinition product,GeneratedModel model,TypeReference reader,string name,GeneratedModel[] models)
    {
        if(definitions.TryGet("dictionary-read",model.Type,reader,"",out var prepared)) return prepared;
        var api=new GeneratedJsonAssembly.Api(module,product,definitions.Stored);
        TypeReference Product(string type)=>module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator."+type));
        var arguments=Arguments(module,model);var key=arguments[0];var value=arguments[1];
        var policy=TypeSupport.DictionaryConstruction(model.Facts.Shape,model.Facts.AbstractType);
        var context=Closed(module,Product("JsonReadContext`1"),reader);
        var method=new MethodDefinition(definitions.MethodName(name),MethodAttributes.Assembly|MethodAttributes.Static|MethodAttributes.HideBySig,module.ImportReference(model.Type));
        method.Body.InitLocals=true;method.Parameters.Add(new ParameterDefinition(new ByReferenceType(context)));owner.Methods.Add(method);
        definitions.Add("dictionary-read",model.Type,reader,"",method);
        TypeReference working=policy.Item1 switch {
            "Dictionary"=>Closed(module,module.ImportReference(typeof(Dictionary<,>)),key,value),
            "SortedDictionary"=>Closed(module,module.ImportReference(typeof(SortedDictionary<,>)),key,value),
            _=>module.ImportReference(model.Type)
        };
        MethodReference construct;bool constructCall=false;
        if(policy.Item1=="Dictionary"||policy.Item1=="SortedDictionary")
            construct=Bind(module,(policy.Item1=="Dictionary"?typeof(Dictionary<,>):typeof(SortedDictionary<,>)).GetConstructor(Type.EmptyTypes)!,working);
        else if(policy.Item1=="Hashtable") construct=module.ImportReference(typeof(System.Collections.Hashtable).GetConstructor(Type.EmptyTypes)!);
        else if(model.DefaultConstructor!=null || model.Constructor is {Parameters.Count:0}) construct=module.ImportReference(model.DefaultConstructor??model.Constructor!);
        else
        {
            if(!definitions.TryGet("collection-construct",model.Type,model.Type,"",out var missing))
            {
                missing=new MethodDefinition(name+"Constructor",MethodAttributes.Assembly|MethodAttributes.Static|MethodAttributes.HideBySig,module.ImportReference(model.Type));owner.Methods.Add(missing);
                definitions.Add("collection-construct",model.Type,model.Type,"",missing);
                CodecSequenceEmitter.missingConstructor(new CecilCodecIL(missing),GeneratedCodec.Api(api).ExceptionConstructor,model.Type.Name);
            }
            construct=missing;constructCall=true;
        }
        var pair=Closed(module,module.ImportReference(typeof(KeyValuePair<,>)),key,value);
        var receiver=policy.Item4?module.ImportReference(typeof(System.Collections.IDictionary))
            :policy.Item3?Closed(module,module.ImportReference(typeof(ICollection<>)),pair)
            :Closed(module,module.ImportReference(typeof(IDictionary<,>)),key,value);
        var range=Product("JsonSourceRange");var entry=Closed(module,module.ImportReference(typeof(KeyValuePair<,>)),module.TypeSystem.String,range);
        var properties=Closed(module,module.ImportReference(typeof(Dictionary<,>)),module.TypeSystem.String,range);
        var iterator=Closed(module,module.ImportReference(typeof(Dictionary<,>).GetNestedType("Enumerator")!),module.TypeSystem.String,range);
        var field=module.ImportReference(product.MainModule.GetType("SoloDatabase.JsonSerializator.JsonReadContext`1").Fields.Single(f=>f.Name=="Properties"));field.DeclaringType=context;
        MethodReference? acquire=null,invoke=null;
        if(!policy.Item4 && key.MetadataType!=MetadataType.String)
        {
            var definition=product.MainModule.GetType("SoloDatabase.JsonSerializator.ConvertStringTo`1");
            acquire=module.ImportReference(definition.Methods.Single(m=>m.Name=="get_Convert"));acquire.DeclaringType=Closed(module,module.ImportReference(definition),key);
            var originalFunction=((GenericInstanceType)definition.Methods.Single(m=>m.Name=="get_Convert").ReturnType).ElementType;
            var function=module.ImportReference(originalFunction);
            if(function.GenericParameters.Count==0)
            {
                function.GenericParameters.Add(new GenericParameter("T",function));
                function.GenericParameters.Add(new GenericParameter("TResult",function));
            }
            invoke=new MethodReference("Invoke",function.GenericParameters[1],Closed(module,function,module.TypeSystem.String,key)) {HasThis=true};
            invoke.Parameters.Add(new ParameterDefinition(function.GenericParameters[0]));
        }
        var add=policy.Item4?module.ImportReference(typeof(System.Collections.IDictionary).GetMethod("Add")!)
            :policy.Item3?Bind(module,typeof(ICollection<>).GetMethod("Add")!,receiver):Bind(module,typeof(IDictionary<,>).GetMethod("Add")!,receiver);
        MethodReference Input(string operation)=>api.Operation("JsonRawInput",operation,reader);
        var operands=new CodecDictionaryReaderApi<TypeReference,MethodReference,FieldReference>(
            context,working,receiver,iterator,entry,module.TypeSystem.String,key,value,
            Input("IsNull"),Input("ObjectProperties"),field,Bind(module,typeof(Dictionary<,>).GetMethod("GetEnumerator")!,properties),
            Bind(module,typeof(Dictionary<,>).GetNestedType("Enumerator")!.GetMethod("MoveNext")!,iterator),
            Bind(module,typeof(Dictionary<,>).GetNestedType("Enumerator")!.GetProperty("Current")!.GetMethod!,iterator),
            Bind(module,typeof(KeyValuePair<,>).GetProperty("Key")!.GetMethod!,entry),Bind(module,typeof(KeyValuePair<,>).GetProperty("Value")!.GetMethod!,entry),
            api.Operation("JsonReadCursor","child",reader),Bind(module,typeof(Dictionary<,>).GetNestedType("Enumerator")!.GetMethod("Dispose")!,iterator),
            construct,constructCall,policy.Item1=="Hashtable",working.IsValueType,!GeneratedJsonDefinitions.SameType(working,model.Type),model.Facts.ValueType,
            acquire==null?null!:Some(acquire),invoke==null?null!:Some(invoke),add,
            policy.Item3?Some(Bind(module,typeof(KeyValuePair<,>).GetConstructors().Single(),pair)):null!,module.ImportReference(typeof(string).GetMethod("op_Equality")!),
            Product("JsonPrimitiveShapeMismatch"),Product("JsonScalarShapeMismatch"),api.Operation("JsonRawRead","PrimitiveRetry",value,reader),api.Operation("JsonRawRead","ScalarRetry",value,reader));
        CodecDictionaryEmitter.reader(new CecilCodecIL(method),operands,
            Function<TypeReference,FSharpOption<MethodReference>>(child=>Some(GeneratedOptionalCodec.ReadCall(definitions,module,owner,product,child,reader,name+"Value",models))),
            Function<TypeReference,MethodReference>(child=>api.Operation("JsonRawRead","ReadMember",child,reader)),model.Type,policy.Item2);
        return method;
    }
}
