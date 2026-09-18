namespace SoloDatabase

open System.Linq.Expressions
open SoloDatabase.SqlModel

/// Shared types for DBRefMany query translation.
module internal DBRefManyDescriptor =

    /// Resolved owner reference for a DBRefMany property.
    type DBRefManyOwnerRef = {
        OwnerCollection: string
        OwnerAliasSql: string
        OwnerIdExpr: SqlExpr option
        PropertyExpr: System.Linq.Expressions.MemberExpression
    }
