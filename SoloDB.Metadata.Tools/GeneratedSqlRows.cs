using System;
using System.Linq;
using SoloDatabase.Metadata.CodecGeneration;
using Mono.Cecil;
using Mono.Cecil.Cil;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.Tools;

internal static class GeneratedSqlRows
{
    internal static void RegisterRaw(ModuleDefinition module, ILProcessor il, AssemblyDefinition product, TypeReference type, TypeReference? enumUnderlying, bool nullableEnum)
    {
        var decision = TypeSupport.Select(new TypeSupportFacts(type is GenericInstanceType instance ? instance.ElementType.FullName : type.FullName,
            type is ArrayType array ? array.Rank : 0,type is ArrayType element ? element.ElementType.FullName : "",enumUnderlying != null,false,false,false,false,false,false));
        var raw = TypeSupport.SqlRawRegistration(decision.Shape,nullableEnum ? TypeShape.Enum : TypeShape.Object);
        TypeReference? underlying = type is GenericInstanceType generic ? generic.GenericArguments[0] : enumUnderlying;
        if (raw.Read.IsSome)
        {
            var sqlValue = product.MainModule.GetType("SoloDatabase.SqlRowValue");
            var operation = raw.Read.Value;
            var read = new GenericInstanceMethod(module.ImportReference(sqlValue.Methods.Single(method => method.Name == operation.Method)));
            foreach (var argument in operation.Arguments) read.GenericArguments.Add(module.ImportReference(Argument(argument,type,underlying,enumUnderlying,null)));
            new GeneratedJsonAssembly.Api(module,product).TypeToken(il,type);
            var function = new GenericInstanceType(new TypeReference("System","Func`3",module,module.AssemblyReferences[0]));
            function.GenericArguments.Add(module.ImportReference(product.MainModule.GetType("SoloDatabase.SqlRow")));
            function.GenericArguments.Add(module.TypeSystem.Int32);
            function.GenericArguments.Add(module.ImportReference(type));
            il.Emit(OpCodes.Ldnull);
            il.Emit(OpCodes.Ldftn,read);
            var constructor = new MethodReference(".ctor",module.TypeSystem.Void,function) { HasThis = true };
            constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object));
            constructor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr));
            il.Emit(OpCodes.Newobj,constructor);
            il.Emit(OpCodes.Call,module.ImportReference(sqlValue.Methods.Single(method => method.Name == raw.Registration)));
            return;
        }
        var method = new GenericInstanceMethod(module.ImportReference(product.MainModule.GetType("SoloDatabase.SqlRowValue")
            .Methods.Single(candidate => candidate.Name == raw.Registration)));
        foreach (var argument in raw.Arguments) method.GenericArguments.Add(module.ImportReference(Argument(argument,type,underlying,enumUnderlying,null)));
        il.Emit(OpCodes.Call,method);
    }

    internal static void Register(ModuleDefinition module, TypeDefinition owner, ILProcessor il,
        AssemblyDefinition product, GeneratedModel model, int index, GeneratedModel[] models)
    {
        var rawEnum = model.Facts.Shape == TypeShape.Enum ? model.ElementType : null;
        var rawNullableEnum = model.Facts.Shape == TypeShape.Nullable && models.Any(candidate =>
            candidate.Facts.Shape == TypeShape.Enum && candidate.Type.FullName == model.ElementType?.FullName);
        RegisterRaw(module,il,product,model.Type,rawEnum,rawNullableEnum);
        if (model.Facts.Shape == TypeShape.Tuple) return; // Registered with its typed construction operations.
        if (model.Facts.Shape == TypeShape.Object)
        {
            GeneratedObjectSqlRows.Register(module, owner, il, product, model, index, models);
            return;
        }
        if (!TypeSupport.ScalarRow(model.Facts.Shape,model.Type.FullName)) return;
        var type = module.ImportReference(model.Type);
        var shape = module.ImportReference(product.MainModule.GetType("SoloDatabase.SqlRowShape"));
        var row = module.ImportReference(product.MainModule.GetType("SoloDatabase.SqlRow"));
        var mapper = new TypeDefinition("", "ScalarRow" + index, TypeAttributes.NestedPrivate | TypeAttributes.Sealed, owner.BaseType);
        owner.NestedTypes.Add(mapper);
        var ordinal = new FieldDefinition("ordinal", FieldAttributes.Private | FieldAttributes.InitOnly, module.TypeSystem.Int32); mapper.Fields.Add(ordinal);
        var ctor = Method(mapper, ".ctor", module.TypeSystem.Void, false, module.TypeSystem.Int32);
        ctor.Attributes |= MethodAttributes.SpecialName | MethodAttributes.RTSpecialName;
        var body = ctor.Body.GetILProcessor(); body.Emit(OpCodes.Ldarg_0);
        body.Emit(OpCodes.Call, new MethodReference(".ctor", module.TypeSystem.Void, owner.BaseType) { HasThis = true });
        body.Emit(OpCodes.Ldarg_0); body.Emit(OpCodes.Ldarg_1); body.Emit(OpCodes.Stfld, ordinal); body.Emit(OpCodes.Ret);
        var read = Method(mapper, "Read", type, false, row); body = read.Body.GetILProcessor();
        SqlRowEmitter.scalarRow(new CecilCodecIL(read),Api(module,product,models),type,new Action<int>(_ => {
            body.Emit(OpCodes.Ldarg_0); body.Emit(OpCodes.Ldfld,ordinal);
        }));
        var factory = Method(owner, "BindScalarRow" + index, Function(module, owner, row, type), true, shape);
        body = factory.Body.GetILProcessor(); body.Emit(OpCodes.Ldarg_0);
        body.Emit(OpCodes.Callvirt, module.ImportReference(product.MainModule.GetType("SoloDatabase.SqlRowShape").Methods.Single(method => method.Name == "get_Start")));
        body.Emit(OpCodes.Newobj, ctor); body.Emit(OpCodes.Ldftn, read); body.Emit(OpCodes.Newobj, DelegateConstructor(module, (GenericInstanceType)factory.ReturnType)); body.Emit(OpCodes.Ret);
        il.Emit(OpCodes.Ldnull); il.Emit(OpCodes.Ldftn, factory); il.Emit(OpCodes.Newobj, DelegateConstructor(module, Function(module, owner, shape, factory.ReturnType)));
        var mapping = product.MainModule.GetType("SoloDatabase.SqlRowMapping`1");
        var mappingType = new GenericInstanceType(module.ImportReference(mapping)); mappingType.GenericArguments.Add(type);
        var mappingCtor = module.ImportReference(mapping.Methods.Single(method => method.IsConstructor)); mappingCtor.DeclaringType = mappingType; il.Emit(OpCodes.Newobj, mappingCtor);
        var register = module.ImportReference(mapping.Methods.Single(method => method.Name == "Register")); register.DeclaringType = mappingType; il.Emit(OpCodes.Call, register);
    }
    internal static SqlRowEmitterApi<TypeReference,MethodReference> Api(ModuleDefinition module,AssemblyDefinition product,GeneratedModel[] models)
    {
        var api = new GeneratedJsonAssembly.Api(module,product);
        return new SqlRowEmitterApi<TypeReference,MethodReference>(
            module.ImportReference(product.MainModule.GetType("SoloDatabase.SqlRow").Methods.Single(method => method.Name == "IsDBNull")),
            new Func<TypeReference,MethodReference>(target => ReadMethod(module,product,target,models)),
            new Func<TypeReference,MethodReference>(target => api.Metadata("Uninitialized",target)),
            new Func<TypeReference,bool>(target => target.IsValueType),
            new Func<TypeReference,bool>(target => target is GenericInstanceType instance && instance.ElementType.FullName == "System.Nullable`1"),
            new Func<TypeReference,string>(target => target.Name),
            module.ImportReference(product.MainModule.GetType("SoloDatabase.SqlRowShape").Methods.Single(method => method.Name == "TupleNull")));
    }

    internal static MethodReference ReadMethod(ModuleDefinition module, AssemblyDefinition product, TypeReference value, GeneratedModel[] models)
    {
        var mapped = TypeSupport.SqlMappedReader(value.FullName);
        if (mapped != null)
            return module.ImportReference(product.MainModule.GetType("SoloDatabase.SqlRowValue").Methods.Single(method => method.Name == mapped.Value));
        GeneratedModel? Fact(TypeReference target) => models.FirstOrDefault(candidate => GeneratedJsonDefinitions.SameType(candidate.Type,target));
        TypeDefinition? OriginalEnum(TypeReference target) {
            if (Fact(target) != null) return null;
            if (target is ArrayType || TypeSupport.RawPrimitive(target.FullName).Length != 0) return null;
            var original = target.Resolve(); return original.IsEnum ? original : null;
        }
        var fact = Fact(value);
        var originalEnum = OriginalEnum(value);
        var definition = value is GenericInstanceType generic ? generic.ElementType.FullName : value.FullName;
        var decision = TypeSupport.Select(new TypeSupportFacts(definition,value is ArrayType array ? array.Rank : 0,
            value is ArrayType elementArray ? elementArray.ElementType.FullName : "",originalEnum != null,false,false,false,false,false,false));
        var shape = fact?.Facts.Shape ?? decision.Shape;
        var element = value is GenericInstanceType instance && instance.GenericArguments.Count > 0 ? instance.GenericArguments[0] : null;
        var child = element == null ? null : Fact(element);
        var elementEnum = element == null ? null : OriginalEnum(element);
        var operation = TypeSupport.SqlReader(shape,child?.Facts.Shape ?? (elementEnum == null ? TypeShape.Object : TypeShape.Enum),value is ArrayType);
        var read = new GenericInstanceMethod(module.ImportReference(product.MainModule.GetType("SoloDatabase.SqlRowValue").Methods.Single(method => method.Name == operation.Method)));
        TypeReference? Underlying(TypeDefinition? target) => target?.Fields.Single(field => field.Name == "value__").FieldType;
        foreach (var argument in operation.Arguments)
            read.GenericArguments.Add(module.ImportReference(Argument(argument,value,element,fact?.ElementType ?? Underlying(originalEnum),child?.ElementType ?? Underlying(elementEnum))));
        return read;
    }

    private static TypeReference Argument(SqlTypeArgument role, TypeReference target, TypeReference? element, TypeReference? underlying, TypeReference? elementUnderlying) => role switch
    {
        SqlTypeArgument.Target => target,
        SqlTypeArgument.Element => element!,
        SqlTypeArgument.EnumUnderlying => underlying!,
        SqlTypeArgument.ElementEnumUnderlying => elementUnderlying!,
        _ => throw new System.ArgumentOutOfRangeException(nameof(role))
    };

    private static MethodDefinition Method(TypeDefinition owner, string name, TypeReference result, bool isStatic, params TypeReference[] arguments)
    {
        var method = new MethodDefinition(name, MethodAttributes.Public | MethodAttributes.HideBySig | (isStatic ? MethodAttributes.Static : 0), result);
        foreach (var argument in arguments) method.Parameters.Add(new ParameterDefinition(argument)); owner.Methods.Add(method); return method;
    }
    private static GenericInstanceType Function(ModuleDefinition module, TypeDefinition owner, TypeReference input, TypeReference output)
    {
        var function = new GenericInstanceType(new TypeReference("System", "Func`2", module, owner.BaseType.Scope));
        function.GenericArguments.Add(input); function.GenericArguments.Add(output); return function;
    }
    private static MethodReference DelegateConstructor(ModuleDefinition module, GenericInstanceType type)
    {
        var ctor = new MethodReference(".ctor", module.TypeSystem.Void, type) { HasThis = true };
        ctor.Parameters.Add(new ParameterDefinition(module.TypeSystem.Object)); ctor.Parameters.Add(new ParameterDefinition(module.TypeSystem.IntPtr)); return ctor;
    }
}
