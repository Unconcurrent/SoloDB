namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

/// Concrete operands for a raw numeric leaf: source reference, unit length and cursor reference.
type internal CodecIntegerApi<'Type,'Method> = {
    Int32: 'Type
    Int64: 'Type
    UInt32: 'Type
    UInt64: 'Type
    OffsetByte: 'Method
    InvalidNumber: 'Method
    Overflow: 'Method
    InvalidHex: 'Method
}

module internal CodecIntegerEmitter =
    /// Emits one consuming decimal parser; width, sign and encoding are generation facts.
    let decimalReader (il: CodecIL<'T,'M,'F>) (api: CodecIntegerApi<'T,'M>) width signed utf16 native =
        let wide = width = 64
        let digits =
            if signed then
                match width with 8 -> 3 | 16 -> 5 | 32 -> 10 | 64 -> 19 | _ -> invalidArg "width" "Unsupported integer width."
            else
                match width with 8 -> 3 | 16 -> 5 | 32 -> 10 | 64 -> 20 | _ -> invalidArg "width" "Unsupported integer width."
        let nativeType = if wide then api.UInt64 else api.UInt32
        let state = CodecNumberEmitter.locals il api.Int32 api.Int64
        let digit = state.Digit
        let negative = state.Negative
        let integerDigits = state.IntegerDigits
        let leadingZeros = state.LeadingZeros
        let significantDigits = state.SignificantDigits
        let kept = il.Local api.Int32
        let boundaryDigit = il.Local api.Int32
        let prefix = il.Local nativeType
        let exponent = state.Exponent
        let needed = il.Local api.Int64
        let value = il.Local nativeType
        let used = il.Local api.Int32
        let invalid = il.Label()
        let overflow = il.Label()
        let complete = il.Label()
        let intConst n = il.EmitInt(OpCodes.Ldc_I4,n)
        let load local = il.EmitLocal(OpCodes.Ldloc,local)
        let save local = il.EmitLocal(OpCodes.Stloc,local)
        let increment local = load local; intConst 1; il.Emit OpCodes.Add; save local
        let number n = if wide then il.EmitInt64(int64 n) else intConst(int n)
        let asNumber () = if wide then il.Emit OpCodes.Conv_U8
        let collect knownPrefix =
            let retain () =
                load prefix; number 10UL; il.Emit OpCodes.Mul
                load digit; asNumber(); il.Emit OpCodes.Add; save prefix
                increment kept
            if knownPrefix then retain()
            else
                let finished = il.Label()
                let tail = il.Label()
                load kept; intConst(digits - 1); il.EmitBranch(OpCodes.Bge,tail)
                retain()
                il.EmitBranch(OpCodes.Br,finished)
                il.Mark tail
                load significantDigits; intConst digits; il.EmitBranch(OpCodes.Bne_Un,finished)
                load digit; save boundaryDigit
                il.Mark finished

        CodecNumberEmitter.scan il api.Int32 api.Int64 api.OffsetByte utf16 (int64 digits) state invalid (digits - 1) collect
        load significantDigits; il.EmitBranch(OpCodes.Brfalse,complete)
        load integerDigits; il.Emit OpCodes.Conv_I8
        load leadingZeros; il.Emit OpCodes.Conv_I8; il.Emit OpCodes.Sub
        load exponent; il.Emit OpCodes.Add; save needed
        load needed; il.EmitInt64 0L; il.EmitBranch(OpCodes.Ble,complete)
        load needed; il.EmitInt64(int64 digits); il.EmitBranch(OpCodes.Bgt,overflow)
        load prefix; save value
        load kept; save used
        let trim = il.Label()
        let initializeBounds = il.Label()
        let extend = il.Label()
        il.Mark trim
        load used; il.Emit OpCodes.Conv_I8; load needed; il.EmitBranch(OpCodes.Ble,initializeBounds)
        load value; number 10UL; il.Emit OpCodes.Div_Un; save value
        load used; intConst 1; il.Emit OpCodes.Sub; save used
        il.EmitBranch(OpCodes.Br,trim)

        let positiveLimit = if signed then (1UL <<< (width - 1)) - 1UL elif wide then System.UInt64.MaxValue else (1UL <<< width) - 1UL
        let negativeLimit = if signed then 1UL <<< (width - 1) else positiveLimit
        let cutoff = il.Local nativeType
        let lastDigit = il.Local api.Int32
        il.Mark initializeBounds
        number (positiveLimit / 10UL); save cutoff
        intConst(int (positiveLimit % 10UL)); save lastDigit
        if signed then
            let positive = il.Label()
            load negative; il.EmitBranch(OpCodes.Brfalse,positive)
            number (negativeLimit / 10UL); save cutoff
            intConst(int (negativeLimit % 10UL)); save lastDigit
            il.Mark positive
        il.Mark extend
        let signResult = il.Label()
        load used; il.Emit OpCodes.Conv_I8; load needed; il.EmitBranch(OpCodes.Bge,signResult)
        let zero = il.Label()
        let append = il.Label()
        load used; load kept; il.EmitBranch(OpCodes.Bne_Un,zero)
        load significantDigits; load kept; il.EmitBranch(OpCodes.Ble,zero)
        load boundaryDigit; save digit
        il.EmitBranch(OpCodes.Br,append)
        il.Mark zero
        intConst 0; save digit
        il.Mark append
        load value; load cutoff; il.EmitBranch(OpCodes.Bgt_Un,overflow)
        let within = il.Label()
        load value; load cutoff; il.EmitBranch(OpCodes.Bne_Un,within)
        load digit; load lastDigit; il.EmitBranch(OpCodes.Bgt,overflow)
        il.Mark within
        load value; number 10UL; il.Emit OpCodes.Mul
        load digit; asNumber(); il.Emit OpCodes.Add; save value
        increment used
        il.EmitBranch(OpCodes.Br,extend)
        il.Mark signResult
        if not signed then
            load negative; il.EmitBranch(OpCodes.Brfalse,complete)
            load value; number 0UL; il.EmitBranch(OpCodes.Bne_Un,overflow)

        il.Mark complete
        CodecNumberEmitter.publish il state
        if signed then
            let positive = il.Label()
            load negative; il.EmitBranch(OpCodes.Brfalse,positive)
            number 0UL; load value; il.Emit OpCodes.Sub; save value
            il.Mark positive
        load value
        if native then il.Emit(if signed then OpCodes.Conv_I else OpCodes.Conv_U)
        elif width = 8 then il.Emit(if signed then OpCodes.Conv_I1 else OpCodes.Conv_U1)
        elif width = 16 then il.Emit(if signed then OpCodes.Conv_I2 else OpCodes.Conv_U2)
        il.Emit OpCodes.Ret
        il.Mark invalid
        il.EmitString "Invalid number"
        il.EmitMethod(OpCodes.Newobj,api.InvalidNumber)
        il.Emit OpCodes.Throw
        il.Mark overflow
        il.EmitMethod(OpCodes.Newobj,api.Overflow)
        il.Emit OpCodes.Throw

    /// Emits hexadecimal accumulation in the requested destination's native word.
    let hexBody (il: CodecIL<'T,'M,'F>) (api: CodecIntegerApi<'T,'M>) width signed utf16 rejectNegative finishResult (overflowResult: (unit -> unit) option) =
        let wide = width = 64
        let word = if wide then api.UInt64 else api.UInt32
        let position = il.Local api.Int32
        let character = il.Local api.Int32
        let digit = il.Local api.Int32
        let negative = il.Local api.Int32
        let count = il.Local api.Int32
        let value = il.Local word
        let cutoff = il.Local word
        let last = il.Local api.Int32
        let overflowed = il.Local api.Int32
        let finish = il.Label()
        let invalid = il.Label()
        let overflow = il.Label()
        let load local = il.EmitLocal(OpCodes.Ldloc,local)
        let save local = il.EmitLocal(OpCodes.Stloc,local)
        let intConst number = il.EmitInt(OpCodes.Ldc_I4,number)
        let number value = if wide then il.EmitInt64(int64 value) else intConst(int value)
        let increment local = load local; intConst 1; il.Emit OpCodes.Add; save local
        let read () =
            il.Emit OpCodes.Ldarg_0; load position
            CodecAddressEmitter.offset il api.OffsetByte (if utf16 then 2 else 1) 0
            il.Emit(if utf16 then OpCodes.Ldind_U2 else OpCodes.Ldind_U1); save character
        il.Emit OpCodes.Ldarg_2; il.Emit OpCodes.Ldind_I4; save position
        read()
        let sign = il.Label()
        let prefix = il.Label()
        load character; intConst 43; il.EmitBranch(OpCodes.Beq,sign)
        load character; intConst 45; il.EmitBranch(OpCodes.Bne_Un,prefix)
        il.Mark sign
        load character; intConst 45; il.Emit OpCodes.Ceq; save negative
        increment position
        il.Mark prefix
        load position; intConst 2; il.Emit OpCodes.Add; save position
        let positiveLimit = if signed then (1UL <<< (width - 1)) - 1UL elif wide then System.UInt64.MaxValue else (1UL <<< width) - 1UL
        let negativeLimit = if signed then 1UL <<< (width - 1) else positiveLimit
        number (positiveLimit / 16UL); save cutoff
        intConst(int (positiveLimit % 16UL)); save last
        if signed then
            let positive = il.Label()
            load negative; il.EmitBranch(OpCodes.Brfalse,positive)
            number (negativeLimit / 16UL); save cutoff
            intConst(int (negativeLimit % 16UL)); save last
            il.Mark positive
        let loop = il.Label()
        let collect = il.Label()
        il.Mark loop
        load position; il.Emit OpCodes.Ldarg_1; il.EmitBranch(OpCodes.Bge,finish)
        read()
        load character; intConst 48; il.Emit OpCodes.Sub; save digit
        load digit; intConst 9; il.EmitBranch(OpCodes.Ble_Un,collect)
        load character; intConst 32; il.Emit OpCodes.Or
        intConst 97; il.Emit OpCodes.Sub; save digit
        load digit; intConst 5; il.EmitBranch(OpCodes.Bgt_Un,finish)
        load digit; intConst 10; il.Emit OpCodes.Add; save digit
        il.Mark collect
        increment count
        let next = il.Label()
        load overflowed; il.EmitBranch(OpCodes.Brtrue,next)
        let tooLarge = il.Label()
        let within = il.Label()
        load value; load cutoff; il.EmitBranch(OpCodes.Bgt_Un,tooLarge)
        load value; load cutoff; il.EmitBranch(OpCodes.Bne_Un,within)
        load digit; load last; il.EmitBranch(OpCodes.Bgt,tooLarge)
        il.Mark within
        load value; intConst 4; il.Emit OpCodes.Shl
        load digit
        if wide then il.Emit OpCodes.Conv_U8
        il.Emit OpCodes.Or; save value
        il.EmitBranch(OpCodes.Br,next)
        il.Mark tooLarge
        intConst 1; save overflowed
        il.Mark next
        increment position; il.EmitBranch(OpCodes.Br,loop)
        il.Mark finish
        load count; il.EmitBranch(OpCodes.Brfalse,invalid)
        load overflowed; il.EmitBranch(OpCodes.Brtrue,overflow)
        if rejectNegative then
            let positive = il.Label()
            load negative; il.EmitBranch(OpCodes.Brfalse,positive)
            load value; number 0UL; il.EmitBranch(OpCodes.Bne_Un,overflow)
            il.Mark positive
        il.Emit OpCodes.Ldarg_2; load position; il.Emit OpCodes.Stind_I4
        finishResult value negative
        il.Mark invalid
        il.EmitString "Invalid hex literal"; il.EmitMethod(OpCodes.Newobj,api.InvalidHex); il.Emit OpCodes.Throw
        il.Mark overflow
        match overflowResult with
        | Some finish -> finish()
        | None -> il.EmitMethod(OpCodes.Newobj,api.Overflow); il.Emit OpCodes.Throw

    let hexReader (il: CodecIL<'T,'M,'F>) (api: CodecIntegerApi<'T,'M>) width signed utf16 native =
        let load local = il.EmitLocal(OpCodes.Ldloc,local)
        let save local = il.EmitLocal(OpCodes.Stloc,local)
        let finish value negative =
            if signed then
                let positive = il.Label()
                load negative; il.EmitBranch(OpCodes.Brfalse,positive)
                if width = 64 then il.EmitInt64 0L else il.EmitInt(OpCodes.Ldc_I4,0)
                load value; il.Emit OpCodes.Sub; save value
                il.Mark positive
            load value
            if native then il.Emit(if signed then OpCodes.Conv_I else OpCodes.Conv_U)
            elif width = 8 then il.Emit(if signed then OpCodes.Conv_I1 else OpCodes.Conv_U1)
            elif width = 16 then il.Emit(if signed then OpCodes.Conv_I2 else OpCodes.Conv_U2)
            il.Emit OpCodes.Ret
        hexBody il api width signed utf16 (not signed) finish None
