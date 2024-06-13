module TestUtils

let assertEqual<'T when 'T : equality> (a: 'T) (b: 'T) (message: string) = // F# cannot decide the overload.
    if a <> b then failwithf "Assert failed, got %A, expected %A: %s" a b message