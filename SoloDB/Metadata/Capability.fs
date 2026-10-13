namespace System.Diagnostics.CodeAnalysis

open System

/// Declares a property controlled by a build-delivered trimming feature.
[<AttributeUsage(AttributeTargets.Property, AllowMultiple = false)>]
type internal FeatureSwitchDefinitionAttribute(featureName: string) =
    inherit Attribute()
    member _.FeatureName = featureName

namespace SoloDatabase

open System
open System.Threading

[<AbstractClass; Sealed>]
type internal DynamicMetadataPolicy private () =
    static let enabled =
        let mutable value = false
        not (AppContext.TryGetSwitch("SoloDB.Metadata.DynamicDiscovery", &value)) || value
    [<System.Diagnostics.CodeAnalysis.FeatureSwitchDefinition("SoloDB.Metadata.DynamicDiscovery")>]
    static member Enabled = enabled

/// Dynamic discovery is available to JIT consumers; native consumers use generated operations.
module internal MetadataRuntime =
    let inline supportsDynamicCode() = DynamicMetadataPolicy.Enabled
    let missing (concern: string) (modelType: Type): 'T =
        raise (NotSupportedException($"Generated {concern} operations are required for '{modelType.FullName}'."))
