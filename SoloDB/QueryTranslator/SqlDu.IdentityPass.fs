module SoloDatabase.IdentityPass

open SoloDatabase.PassTypes

/// The identity pass: returns input unchanged. Proves the framework pipeline works.
let identity : Pass = {
    Name = "Identity"
    Transform = fun stmt -> stmt
}
