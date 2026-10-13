namespace System.Diagnostics.CodeAnalysis

open System

/// Member categories used by compiler and linker dependency annotations.
[<Flags>]
type internal DynamicallyAccessedMemberTypes =
    | None = 0
    | PublicParameterlessConstructor = 1
    | PublicConstructors = 3
    | NonPublicConstructors = 4
    | PublicMethods = 8
    | NonPublicMethods = 16
    | PublicFields = 32
    | NonPublicFields = 64
    | PublicNestedTypes = 128
    | NonPublicNestedTypes = 256
    | PublicProperties = 512
    | NonPublicProperties = 1024
    | PublicEvents = 2048
    | NonPublicEvents = 4096
    | Interfaces = 8192
    | All = -1

/// Linker-visible dependency metadata for all supported reference targets.
[<Sealed; AttributeUsage(AttributeTargets.Method ||| AttributeTargets.Constructor ||| AttributeTargets.Field, AllowMultiple = true, Inherited = false)>]
type internal DynamicDependencyAttribute private (signature: string, typ: Type, typeName: string, assemblyName: string, memberTypes: DynamicallyAccessedMemberTypes) =
    inherit Attribute()
    new(signature: string) = DynamicDependencyAttribute(signature,null,null,null,DynamicallyAccessedMemberTypes.None)
    new(signature: string, typ: Type) = DynamicDependencyAttribute(signature,typ,null,null,DynamicallyAccessedMemberTypes.None)
    new(signature: string,typeName: string,assemblyName: string) = DynamicDependencyAttribute(signature,null,typeName,assemblyName,DynamicallyAccessedMemberTypes.None)
    new(memberTypes: DynamicallyAccessedMemberTypes, typ: Type) = DynamicDependencyAttribute(null,typ,null,null,memberTypes)
    new(memberTypes: DynamicallyAccessedMemberTypes,typeName: string,assemblyName: string) = DynamicDependencyAttribute(null,null,typeName,assemblyName,memberTypes)
    member _.MemberSignature = signature
    member _.MemberTypes = memberTypes
    member _.Type = typ
    member _.TypeName = typeName
    member _.AssemblyName = assemblyName
    member val Condition: string = null with get, set
