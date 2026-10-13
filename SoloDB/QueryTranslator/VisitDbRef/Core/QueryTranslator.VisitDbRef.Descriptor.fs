namespace SoloDatabase

open System.Linq.Expressions
open SoloDatabase.SqlModel

/// Shared types for DBRefMany query translation.
module internal DBRefManyDescriptor =

    /// Resolved owner reference for a DBRefMany property.
    type DBRefManyOwnerRef = {
        Reference: QueryProducedValue
        ReferenceType: System.Type
    }
