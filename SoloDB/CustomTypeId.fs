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
                if not p.CanWrite then failwithf "Cannot create a generator for a non writtable parameter '%s' for type %s" p.Name typeof<'t>.FullName
                match a.IdGenerator with
                | null -> failwithf "Generator type for Id property (%s) is null." p.Name
                | generator -> Some (p, generator)
                ) 
        |> Seq.tryHead
        |> Option.bind(
            fun (p, gt) ->
                if gt.GetInterfaces() |> Seq.exists(fun i -> i.FullName.StartsWith "SoloDatabase.Attributes.IIdGenerator") |> not then
                    failwithf "Generator type for Id property (%s) does not implement the IIdGenerator or IIdGenerator<'T> interface." p.Name

                let instance = (Activator.CreateInstance gt)
                Some {|
                    Generator = instance
                    SetId = fun id o -> p.SetValue(o, id)
                    GetId = fun o -> p.GetValue(o)
                    IdType = p.PropertyType
                    Property = p
                |}
        )