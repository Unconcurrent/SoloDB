﻿namespace SoloDatabase.Attributes

[<System.AttributeUsage(System.AttributeTargets.Property, AllowMultiple = false)>]
type IndexedAttribute(unique: bool) =
    inherit System.Attribute()

    member val Unique = unique