namespace SoloDatabase

type internal SoloDBToCollectionData = {
    /// <summary>
    /// A function that, when called, clears the database connection cache.
    /// </summary>
    ClearCacheFunction: unit -> unit

    /// <summary>
    /// The event system used by collections created from this SoloDB instance.
    /// </summary>
    EventSystem: EventSystem
}
