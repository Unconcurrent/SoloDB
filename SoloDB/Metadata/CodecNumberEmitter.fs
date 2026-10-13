namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

/// Local operands shared by the numeric instruction macros, never a runtime token.
type internal CodecNumberState = {
    Position: int
    Digit: int
    Negative: int
    IntegerDigits: int
    AllDigits: int
    LeadingZeros: int
    SignificantDigits: int
    Exponent: int
}

module internal CodecNumberEmitter =
    let locals (il: CodecIL<'T,'M,'F>) integer wide =
        { Position = il.Local integer; Digit = il.Local integer; Negative = il.Local integer
          IntegerDigits = il.Local integer; AllDigits = il.Local integer; LeadingZeros = il.Local integer
          SignificantDigits = il.Local integer; Exponent = il.Local wide }

    /// Expands one syntax-and-digit pass with a target-specific significant-digit body.
    let scan (il: CodecIL<'T,'M,'F>) integer wide offset utf16 allowance state invalid prefixCapacity collect =
        let character = il.Local integer
        let exponentLimit = il.Local wide
        let exponentNegative = il.Local integer
        let exponentDigits = il.Local integer
        let intConst n = il.EmitInt(OpCodes.Ldc_I4,n)
        let load local = il.EmitLocal(OpCodes.Ldloc,local)
        let save local = il.EmitLocal(OpCodes.Stloc,local)
        let increment local = load local; intConst 1; il.Emit OpCodes.Add; save local
        let read () =
            il.Emit OpCodes.Ldarg_0; load state.Position
            CodecAddressEmitter.offset il offset (if utf16 then 2 else 1) 0
            il.Emit(if utf16 then OpCodes.Ldind_U2 else OpCodes.Ldind_U1); save character
        let atEnd destination = load state.Position; il.Emit OpCodes.Ldarg_1; il.EmitBranch(OpCodes.Bge,destination)
        let onCharacter code destination = load character; intConst code; il.EmitBranch(OpCodes.Beq,destination)
        let checkDigit destination =
            load character; intConst 48; il.Emit OpCodes.Sub; save state.Digit
            load state.Digit; intConst 9; il.EmitBranch(OpCodes.Bgt_Un,destination)
        let digit knownSignificant knownPrefix =
            increment state.AllDigits
            if knownSignificant then
                increment state.SignificantDigits
                collect knownPrefix
            else
                let significant = il.Label()
                let finished = il.Label()
                load state.Digit; il.EmitBranch(OpCodes.Brtrue,significant)
                load state.SignificantDigits; il.EmitBranch(OpCodes.Brtrue,significant)
                increment state.LeadingZeros
                il.EmitBranch(OpCodes.Br,finished)
                il.Mark significant
                increment state.SignificantDigits
                collect false
                il.Mark finished
        // Each admitted unit is consumed before the next load. A failed digit leaves
        // character and Position at that stopping unit for the existing continuation.
        let block integerPart repeat stopped =
            let scalar = il.Label()
            let leading = il.Label()
            let bounded = il.Label()
            il.Emit OpCodes.Ldarg_1; load state.Position; il.Emit OpCodes.Sub
            intConst 4; il.EmitBranch(OpCodes.Blt,scalar)
            load state.SignificantDigits; il.EmitBranch(OpCodes.Brfalse,leading)
            let emitBlock knownSignificant knownPrefix =
                for _ in 0..3 do
                    read(); checkDigit stopped
                    if integerPart then increment state.IntegerDigits
                    digit knownSignificant knownPrefix
                    increment state.Position
                il.EmitBranch(OpCodes.Br,repeat)
            if prefixCapacity >= 4 then
                load state.SignificantDigits; intConst(prefixCapacity - 4)
                il.EmitBranch(OpCodes.Bgt,bounded)
                emitBlock true true
                il.Mark bounded
            emitBlock true false
            il.Mark leading
            emitBlock false false
            il.Mark scalar
        il.Emit OpCodes.Ldarg_2; il.Emit OpCodes.Ldind_I4; save state.Position
        atEnd invalid
        read()
        let sign = il.Label()
        let integerStart = il.Label()
        let integerDigit = il.Label()
        let integerEnd = il.Label()
        onCharacter 45 sign
        onCharacter 43 sign
        il.EmitBranch(OpCodes.Br,integerDigit)
        il.Mark sign
        load character; intConst 45; il.Emit OpCodes.Ceq; save state.Negative
        increment state.Position
        il.Mark integerStart
        block true integerStart integerEnd
        atEnd integerEnd
        read()
        il.Mark integerDigit
        checkDigit integerEnd
        increment state.IntegerDigits
        digit false false; increment state.Position
        il.EmitBranch(OpCodes.Br,integerStart)
        il.Mark integerEnd
        let exponentStart = il.Label()
        atEnd exponentStart
        let fraction = il.Label()
        onCharacter 46 fraction
        il.EmitBranch(OpCodes.Br,exponentStart)
        il.Mark fraction
        increment state.Position
        let fractionLoop = il.Label()
        il.Mark fractionLoop
        block false fractionLoop exponentStart
        atEnd exponentStart
        read(); checkDigit exponentStart
        digit false false; increment state.Position
        il.EmitBranch(OpCodes.Br,fractionLoop)
        il.Mark exponentStart
        load state.AllDigits; il.EmitBranch(OpCodes.Brfalse,invalid)
        let syntaxEnd = il.Label()
        atEnd syntaxEnd
        let readExponent = il.Label()
        onCharacter 101 readExponent
        onCharacter 69 readExponent
        il.EmitBranch(OpCodes.Br,syntaxEnd)
        il.Mark readExponent
        load state.AllDigits; il.Emit OpCodes.Conv_I8; il.EmitInt64 allowance
        il.Emit OpCodes.Add; save exponentLimit
        increment state.Position
        atEnd invalid
        read()
        let exponentSign = il.Label()
        let exponentLoop = il.Label()
        let exponentDigit = il.Label()
        onCharacter 45 exponentSign
        onCharacter 43 exponentSign
        il.EmitBranch(OpCodes.Br,exponentDigit)
        il.Mark exponentSign
        load character; intConst 45; il.Emit OpCodes.Ceq; save exponentNegative
        increment state.Position
        il.Mark exponentLoop
        let exponentEnd = il.Label()
        atEnd exponentEnd
        read()
        il.Mark exponentDigit
        checkDigit exponentEnd
        increment exponentDigits
        load state.Exponent; il.EmitInt64 10L; il.Emit OpCodes.Mul
        load state.Digit; il.Emit OpCodes.Conv_I8; il.Emit OpCodes.Add; save state.Exponent
        let bounded = il.Label()
        load state.Exponent; load exponentLimit; il.EmitBranch(OpCodes.Ble,bounded)
        load exponentLimit; save state.Exponent
        il.Mark bounded
        increment state.Position
        il.EmitBranch(OpCodes.Br,exponentLoop)
        il.Mark exponentEnd
        load exponentDigits; il.EmitBranch(OpCodes.Brfalse,invalid)
        load exponentNegative; il.EmitBranch(OpCodes.Brfalse,syntaxEnd)
        il.EmitInt64 0L; load state.Exponent; il.Emit OpCodes.Sub; save state.Exponent
        il.Mark syntaxEnd
        let converted = il.Label()
        atEnd converted
        for code in [|43;45;46;69;101|] do onCharacter code invalid
        il.Mark converted

    let publish (il: CodecIL<'T,'M,'F>) state =
        il.Emit OpCodes.Ldarg_2
        il.EmitLocal(OpCodes.Ldloc,state.Position)
        il.Emit OpCodes.Stind_I4
