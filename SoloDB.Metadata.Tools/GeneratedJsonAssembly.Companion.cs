using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;

namespace SoloDatabase.Metadata.Tools;

public static partial class GeneratedJsonAssembly
{
    internal static void WriteCompanion(string output,string input,string outputDirectory,string selection,
        AssemblyDefinition product,GeneratedModel[] models,GeneratedQueryOperation[] queries,GeneratedQueryCall[] queryCalls,
        GeneratedQueryWitness[] queryWitnesses,GeneratedQueryLambda[] queryLambdas,GeneratedProviderOperation[] providerOperations,
        string[] references,string[] operationAccess,GeneratedArgumentSource[] argumentSources,string[][] aliases,string[] aliasNames,
        AssemblyNameReference sourceIdentity,string key,string[] dependencies,Action<string,double>? timing = null)
    {
        var watch = timing == null ? null : System.Diagnostics.Stopwatch.StartNew();
        void Phase(string name)
        {
            if (watch == null) return;
            timing!("companion-" + name,watch.Elapsed.TotalSeconds);
            watch.Restart();
        }
        using var resolver = new DefaultAssemblyResolver();
        resolver.AddSearchDirectory(Path.GetDirectoryName(Path.GetFullPath(input))!);
        foreach (var reference in references) resolver.AddSearchDirectory(Path.GetDirectoryName(Path.GetFullPath(reference))!);
        using var consumer = AssemblyDefinition.ReadAssembly(input,new ReaderParameters { AssemblyResolver = resolver });
        if (consumer.Name.FullName != sourceIdentity.FullName)
            throw new InvalidOperationException("The fresh compiler identity differs from its checked source: " + consumer.Name.FullName);
        FSharpConsumerOutput.ReadSymbols(consumer,input);
        using var imports = new TargetReflectionImports(references,resolver,product.MainModule);
        using var helper = HelperAssembly.Create(sourceIdentity.Name + ".SoloDB.Generated",resolver,imports);
        helper.Name.PublicKey = consumer.Name.PublicKey;
        helper.MainModule.Attributes = (helper.MainModule.Attributes & ~ModuleAttributes.StrongNameSigned) |
            (consumer.MainModule.Attributes & ModuleAttributes.StrongNameSigned);
        var module = helper.MainModule;
        var entry = HelperAssembly.EntryPoint(module);
        var owner = new TypeDefinition("SoloDatabase.Generated","Implementation",TypeAttributes.NotPublic |
            TypeAttributes.Abstract | TypeAttributes.Sealed,entry.DeclaringType.BaseType);
        module.Types.Add(owner);
        var core = Method(owner,"RegisterCore",module.TypeSystem.Void);
        core.Attributes = MethodAttributes.Assembly | MethodAttributes.Static;
        entry.Body.GetILProcessor().Emit(OpCodes.Call,core);
        entry.Body.GetILProcessor().Emit(OpCodes.Ret);
        var il = core.Body.GetILProcessor();
        var definitions = new GeneratedJsonDefinitions(references) { SchemaWitnesses = new List<GeneratedSchemaIndexes.Witness>() };
        Phase("inputs");
        GeneratedQueries.Preserve(module,core,product,queryWitnesses);
        GeneratedQueries.RegisterExpressions(module,il,product,queryLambdas);
        GeneratedQueries.Register(module,il,product,queries);
        GeneratedQueries.RegisterProviders(module,il,product,providerOperations);
        GeneratedQueryCalls.Register(module,owner,il,product,queryCalls);
        foreach (var source in argumentSources) GeneratedArgumentCapture.Register(definitions,module,owner,il,product,source);
        Phase("query-construction");
        RegisterModels(definitions,module,owner,il,product,models);
        il.Emit(OpCodes.Ret);
        Phase("model-construction");
        var access = new HashSet<string>(models.SelectMany(model => model.AccessAssemblies)
            .Concat(argumentSources.SelectMany(source => source.AccessAssemblies)).Concat(operationAccess),StringComparer.Ordinal)
            { product.Name.Name, consumer.Name.Name };
        GeneratedSchemaIndexes.Complete(helper,consumer,product,null,name => access.Add(name),
            timing == null ? null : (name,seconds) => timing("companion-schema-" + name,seconds),null,null,definitions.SchemaWitnesses,definitions.EmptyAttributes);
        Phase("schema");
        HelperAssembly.AllowAccess(module,access,owner.BaseType.Scope);
        var enter = module.ImportReference(typeof(System.Threading.Monitor).GetMethod("Enter",new[] { typeof(object) })!);
        var exit = module.ImportReference(typeof(System.Threading.Monitor).GetMethod("Exit",new[] { typeof(object) })!);
        if (aliases.Length != aliasNames.Length) throw new InvalidOperationException("The checked alias identity inventory is incomplete.");
        for (var index = 0; index < aliases.Length; index++)
        {
            var path = string.Join("\0",aliases[index]);
            var facade = consumer.MainModule.GetType(aliasNames[index])
                ?? throw new InvalidOperationException("The fresh compiler image has no original generated alias: " + aliasNames[index]);
            if (!facade.Fields.Any(field => field.Name == "OriginalPath" && field.IsPrivate && field.IsStatic && field.IsLiteral && Equals(field.Constant,path)))
                throw new InvalidOperationException("The original generated alias path differs: " + facade.FullName);
            var method = facade.Methods.Single(method => method.Name == "register" && method.IsPublic && method.IsStatic &&
                method.Parameters.Count == 0 && method.GenericParameters.Count == 0 && method.ReturnType.MetadataType == MetadataType.Void);
            if (!method.HasBody || method.Body.Instructions.Last().OpCode != OpCodes.Throw)
                throw new InvalidOperationException("The generated alias is not an original checking surface: " + facade.FullName);
            method.DebugInformation.SequencePoints.Clear();
            method.DebugInformation.Scope = null;
            method.DebugInformation.CustomDebugInformations.Clear();
            method.Body = new MethodBody(method) { InitLocals = true };
            var call = Method(facade,"RegisterCore",consumer.MainModule.TypeSystem.Void);
            call.Body.GetILProcessor().Emit(OpCodes.Call,consumer.MainModule.ImportReference(entry));
            call.Body.GetILProcessor().Emit(OpCodes.Ret);
            // Existing call tokens must continue to denote the original public entry.
            GuardRegistration(consumer.MainModule,facade,call,method,consumer.MainModule.ImportReference(enter),consumer.MainModule.ImportReference(exit));
        }
        Phase("aliases");
        HelperAssembly.Write(helper,output,key);
        Phase("helper-write");
        FSharpConsumerOutput.PublishConsumer(consumer,input,outputDirectory,selection,key,output,dependencies);
        Phase("consumer-write");
    }

}
