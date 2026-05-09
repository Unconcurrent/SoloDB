namespace SoloDatabase

open System
open SoloDatabase
open SoloDatabase.Attributes
open System.Reflection
open System.Linq.Expressions


[<AbstractClass; Sealed>]
type internal CustomTypeId<'t> =
    static member val internal Value = 
        typeof<'t>.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
        |> Seq.choose(
            fun p -> 
                match p.GetCustomAttribute<SoloId>(true) with
                | a when Utils.isNull a -> None
                | a -> 
                if not p.CanWrite then
                    raise (System.InvalidOperationException(sprintf "Cannot create a generator for a non writtable parameter '%s' for type %s" p.Name typeof<'t>.FullName))
                match a.IdGenerator with
                | null ->
                    raise (System.InvalidOperationException(sprintf "Generator type for Id property (%s) is null." p.Name))
                | generator -> Some (p, generator)
                ) 
        |> Seq.tryHead
        |> Option.bind(
            fun (p, gt) ->
                if p.PropertyType = typeof<bool> || p.PropertyType = typeof<nativeint> || p.PropertyType = typeof<unativeint> then
                    raise (System.InvalidOperationException(
                        sprintf "Error: Invalid [<SoloId>] type on '%s.%s'.\nReason: '%s' has a value space too small or too platform-dependent to serve as a stable identity key (bool: 2 values; IntPtr/UIntPtr: width depends on runtime).\nFix: Use a wider type — int64, Guid, string, or a custom struct."
                            typeof<'t>.FullName p.Name p.PropertyType.Name))

                if gt.GetInterfaces() |> Seq.exists(fun i -> i.FullName.StartsWith "SoloDatabase.Attributes.IIdGenerator" && not i.IsArray) |> not then
                    raise (System.InvalidOperationException(
                        sprintf "Generator type for Id property (%s) does not implement the IIdGenerator or IIdGenerator<'T> interface." p.Name))

                let instance = (Activator.CreateInstance gt)
                Some {|
                    Generator = instance
                    SetId = fun id o -> p.SetValue(o, id)
                    GetId = fun o -> p.GetValue(o)
                    IdType = p.PropertyType
                    Property = p
                |}
        )

    /// Touches CustomTypeId<'t>.Value once and unwraps the
    /// TypeInitializationException that F# wraps a `static member val`
    /// initializer's throw with, so callers see the actionable
    /// InvalidOperationException directly. All call sites that need to read
    /// CustomTypeId<'t>.Value must go through this helper; reading .Value
    /// directly leaks the .NET wrapper.
    static member internal Get () : _ =
        try
            CustomTypeId<'t>.Value
        with
        | :? System.TypeInitializationException as tie when not (Utils.isNull tie.InnerException) ->
            raise tie.InnerException