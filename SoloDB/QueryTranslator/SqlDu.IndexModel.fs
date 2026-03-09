module SoloDatabase.IndexModel

open SqlDu.Engine.C1.Spec

// ══════════════════════════════════════════════════════════════
// Index Knowledge Model (C8a)
//
// Read-only model of available indexes for optimizer decisions.
// Binary presence/absence only — no cost model, no cardinality,
// no statistics. Populated from fixture schema, not runtime
// introspection (runtime is a C12 concern).
//
// Index expressions are stored as SqlExpr DU nodes to enable
// structural matching against DU tree nodes during optimization.
// ══════════════════════════════════════════════════════════════

/// A single index entry in the knowledge model.
type IndexEntry = {
    TableName: string
    IndexName: string
    Expression: SqlExpr
    IsUnique: bool
}

/// Read-only index knowledge model.
type IndexModel = {
    Indexes: IndexEntry list
}

/// Empty model (no indexes known).
let emptyModel : IndexModel = { Indexes = [] }

/// Find all indexes for a given table.
let indexesForTable (model: IndexModel) (tableName: string) : IndexEntry list =
    model.Indexes |> List.filter (fun e -> e.TableName = tableName)

/// Check if a table has any indexes in the model.
let hasIndexes (model: IndexModel) (tableName: string) : bool =
    model.Indexes |> List.exists (fun e -> e.TableName = tableName)

// ══════════════════════════════════════════════════════════════
// Fixture Index Models
//
// Hardcoded models matching the EQP fixture schemas.
// These are the proving-ground index models for C8.
// ══════════════════════════════════════════════════════════════

/// C5 EQP fixture: Users table indexes.
/// CREATE INDEX idx_users_name ON "Users"(jsonb_extract(Value, '$.Name'))
/// CREATE INDEX idx_users_age ON "Users"(CAST(jsonb_extract(Value, '$.Age') AS INTEGER))
/// CREATE INDEX idx_users_active ON "Users"(jsonb_extract(Value, '$.IsActive'))
let usersIndexes : IndexEntry list = [
    { TableName = "Users"
      IndexName = "idx_users_name"
      Expression = JsonExtractExpr(None, "Value", JsonPath ["Name"])
      IsUnique = false }
    { TableName = "Users"
      IndexName = "idx_users_age"
      Expression = Cast(JsonExtractExpr(None, "Value", JsonPath ["Age"]), "INTEGER")
      IsUnique = false }
    { TableName = "Users"
      IndexName = "idx_users_active"
      Expression = JsonExtractExpr(None, "Value", JsonPath ["IsActive"])
      IsUnique = false }
]

/// C5 EQP fixture: Events table indexes.
/// CREATE INDEX idx_events_created ON "Events"(CAST(jsonb_extract(Value, '$.CreatedAt') AS INTEGER))
/// CREATE INDEX idx_events_type ON "Events"(jsonb_extract(Value, '$.Type'))
let eventsIndexes : IndexEntry list = [
    { TableName = "Events"
      IndexName = "idx_events_created"
      Expression = Cast(JsonExtractExpr(None, "Value", JsonPath ["CreatedAt"]), "INTEGER")
      IsUnique = false }
    { TableName = "Events"
      IndexName = "idx_events_type"
      Expression = JsonExtractExpr(None, "Value", JsonPath ["Type"])
      IsUnique = false }
]

/// C7 EQP fixture extension: Orders table indexes.
/// CREATE INDEX idx_orders_userid ON "Orders"(CAST(jsonb_extract(Value, '$.UserId') AS INTEGER))
let ordersIndexes : IndexEntry list = [
    { TableName = "Orders"
      IndexName = "idx_orders_userid"
      Expression = Cast(JsonExtractExpr(None, "Value", JsonPath ["UserId"]), "INTEGER")
      IsUnique = false }
]

/// C7 EQP fixture extension: Products table indexes.
/// CREATE INDEX idx_products_name ON "Products"(jsonb_extract(Value, '$.Name'))
let productsIndexes : IndexEntry list = [
    { TableName = "Products"
      IndexName = "idx_products_name"
      Expression = JsonExtractExpr(None, "Value", JsonPath ["Name"])
      IsUnique = false }
]

/// Combined fixture model with all known indexes.
let fixtureModel : IndexModel = {
    Indexes = usersIndexes @ eventsIndexes @ ordersIndexes @ productsIndexes
}
