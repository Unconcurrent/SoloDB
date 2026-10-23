using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Mono.Cecil;
using SR = System.Reflection;

namespace SoloDatabase.Metadata.Tools;

// Reflection selects an operation; only current target images supply its binding.
internal sealed class TargetReflectionImports : IReflectionImporterProvider, IDisposable
{
    private readonly string[] paths;
    private readonly IAssemblyResolver resolver;
    private readonly ModuleDefinition product;
    private readonly Dictionary<string,AssemblyDefinition> images = new(StringComparer.Ordinal);
    private readonly Dictionary<Type,TypeReference> types = new();
    internal TargetReflectionImports(string[] paths,IAssemblyResolver resolver,ModuleDefinition product)
    { this.paths = paths; this.resolver = resolver; this.product = product; }

    private TypeReference Definition(Type type)
    {
        if (types.TryGetValue(type,out var found)) return found;
        var name = type.FullName!.Replace('+','/');
        // The product already binds these operation types to original target contracts.
        // Never choose another reference merely because it declares the same type name.
        foreach (var operand in product.GetTypeReferences().Where(value => value.FullName == name && value.Scope is AssemblyNameReference))
        {
            var scope = (AssemblyNameReference)operand.Scope;
            AssemblyDefinition? selected = null;
            var ambiguous = false;
            foreach (var path in paths)
            {
                if (!File.Exists(path)) continue;
                if (!images.TryGetValue(path,out var image))
                    images.Add(path,image = AssemblyDefinition.ReadAssembly(path,new ReaderParameters { AssemblyResolver = resolver }));
                if (image.Name.FullName == scope.FullName) { selected = image; ambiguous = false; break; }
                if (image.Name.Name != scope.Name || image.Name.Culture != scope.Culture ||
                    !image.Name.PublicKeyToken.SequenceEqual(scope.PublicKeyToken)) continue;
                if (selected != null && selected.Name.FullName != image.Name.FullName) ambiguous = true;
                selected ??= image;
            }
            if (ambiguous) throw new InvalidOperationException("The checked compiler reference identity is ambiguous: " + scope.FullName);
            if (selected != null)
            {
                // This is the actual checked compiler's binding for the original
                // product assembly contract, not an arbitrary facade with this type.
                found = SourceTypeIdentity.Definition(selected,name);
                if (found.IsValueType != type.IsValueType || found.GenericParameters.Count != type.GetGenericArguments().Length)
                    throw new InvalidOperationException("The target product operand differs from its reflection contract: " + type);
                types.Add(type,found);
                return found;
            }
            // A type-only import can preserve an already owned product operand
            // without resolving it to an unrelated framework version.
            types.Add(type,operand);
            return operand;
        }
        throw new InvalidOperationException("The current compiler references do not supply a reflection operation type: " + type);
    }

    public IReflectionImporter GetReflectionImporter(ModuleDefinition module) => new Importer(module,this);
    private sealed class Importer : DefaultReflectionImporter
    {
        private readonly TargetReflectionImports owner;
        internal Importer(ModuleDefinition module,TargetReflectionImports owner) : base(module) { this.owner = owner; }
        protected override IMetadataScope ImportScope(Type type)
        {
            var definition = owner.Definition(type);
            return module.ImportReference(definition).Scope;
        }
        private void RequireDefinition(Type type)
        {
            if (type.IsGenericType) type = type.GetGenericTypeDefinition();
            var reference = owner.Definition(type);
            if (reference is TypeDefinition) return;
            var scope = (AssemblyNameReference)reference.Scope;
            var image = module.AssemblyResolver.Resolve(scope);
            if (image.Name.FullName != scope.FullName)
                throw new InvalidOperationException("The reflected member assembly differs from its original product identity: " + scope.FullName);
            SourceTypeIdentity.Definition(image,reference.FullName);
        }
        public override MethodReference ImportReference(SR.MethodBase method,IGenericParameterProvider context)
        {
            RequireDefinition(method.DeclaringType!);
            var imported = base.ImportReference(method,context);
            var element = imported.GetElementMethod();
            var original = element.Resolve() ?? throw new InvalidOperationException("The target reflection method is absent: " + method);
            string Signature(MethodReference value) => value.Name + ":" + value.HasThis + ":" + value.ExplicitThis + ":" + ((int)value.CallingConvention & 15) + ":" +
                value.GenericParameters.Count + ":" + SourceTypeIdentity.Signature(value.ReturnType,module.AssemblyResolver) + ":" +
                string.Join(";",value.Parameters.Select(parameter => SourceTypeIdentity.Signature(parameter.ParameterType,module.AssemblyResolver)));
            if (Signature(element) != Signature(original))
                throw new InvalidOperationException("The target reflection method signature differs: " + method + " expected " + Signature(original) + " imported " + Signature(element));
            return imported;
        }
        public override FieldReference ImportReference(SR.FieldInfo field,IGenericParameterProvider context)
        {
            RequireDefinition(field.DeclaringType!);
            var imported = base.ImportReference(field,context);
            var original = imported.Resolve() ?? throw new InvalidOperationException("The target reflection field is absent: " + field);
            if (SourceTypeIdentity.Signature(imported.FieldType,module.AssemblyResolver) != SourceTypeIdentity.Signature(original.FieldType,module.AssemblyResolver))
                throw new InvalidOperationException("The target reflection field signature differs: " + field);
            return imported;
        }
    }
    public void Dispose() { foreach (var image in images.Values) image.Dispose(); }
}
