namespace SoloDatabase.JsonSerializator

open System

/// Selects the meaning of strings converted to Decimal or BigInteger.
type NumericStringFormat =
    /// Strings contain ordinary numeric text.
    | NumberText = 0
    /// Strings contain canonical SoloDB stored numerical keys.
    | StoredKey = 1

module internal NumericStringPolicy =
    let validate numericStrings =
        if numericStrings <> NumericStringFormat.NumberText && numericStrings <> NumericStringFormat.StoredKey then
            raise(ArgumentOutOfRangeException(nameof numericStrings))

/// Immutable policy for one JSON conversion; JSON Numbers and String targets keep their roles.
[<Sealed; AllowNullLiteral>]
type JsonReadOptions(numericStrings: NumericStringFormat) =
    static let defaults = JsonReadOptions(NumericStringFormat.NumberText)
    static let stored = JsonReadOptions(NumericStringFormat.StoredKey)
    do NumericStringPolicy.validate numericStrings

    /// Creates options with ordinary numeric-text interpretation.
    new() = JsonReadOptions(NumericStringFormat.NumberText)
    /// The selected interpretation for String-to-Decimal/BigInteger conversions.
    member _.NumericStrings = numericStrings
    /// Shared ordinary numeric-text options.
    static member Default = defaults
    /// Shared stored-key String conversion options.
    static member StoredNumericStrings = stored
    static member internal Normalize(options: JsonReadOptions) =
        if isNull options then defaults else options

    static member internal FromFormat(format: NumericStringFormat) =
        if format = NumericStringFormat.StoredKey then stored else defaults
