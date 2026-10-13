namespace SoloDatabase

open System
open System.Linq.Expressions

/// Precompiled primitive operations used by ordinary query discovery.
module internal QueryScalarOperators =
    let inline private convertNumber (_: ^T) kind (target: Type) =
        let checkedConversion = kind = ExpressionType.ConvertChecked
        let inline result convert = ValueSome(Func<obj,obj>(fun value -> box(convert (unbox< ^T> value))))
        if kind <> ExpressionType.Convert && not checkedConversion then ValueNone
        elif typeof<sbyte>.Equals(target) then result (fun value -> if checkedConversion then Checked.sbyte value else sbyte value)
        elif typeof<byte>.Equals(target) then result (fun value -> if checkedConversion then Checked.byte value else byte value)
        elif typeof<int16>.Equals(target) then result (fun value -> if checkedConversion then Checked.int16 value else int16 value)
        elif typeof<uint16>.Equals(target) then result (fun value -> if checkedConversion then Checked.uint16 value else uint16 value)
        elif typeof<int32>.Equals(target) then result (fun value -> if checkedConversion then Checked.int32 value else int32 value)
        elif typeof<uint32>.Equals(target) then result (fun value -> if checkedConversion then Checked.uint32 value else uint32 value)
        elif typeof<int64>.Equals(target) then result (fun value -> if checkedConversion then Checked.int64 value else int64 value)
        elif typeof<uint64>.Equals(target) then result (fun value -> if checkedConversion then Checked.uint64 value else uint64 value)
        elif typeof<char>.Equals(target) then result (fun value -> if checkedConversion then Checked.char value else char value)
        elif typeof<single>.Equals(target) then result single
        elif typeof<double>.Equals(target) then result double
        elif typeof<decimal>.Equals(target) then result decimal
        else ValueNone

    let inline private negate (_: ^T) kind =
        ValueSome(Func<obj,obj>(fun value ->
            let actual = unbox< ^T> value
            box (if kind = ExpressionType.NegateChecked then Checked.(~-) actual else -actual)))

    let inline private complement (_: ^T) =
        ValueSome(Func<obj,obj>(fun value -> box(~~~(unbox< ^T> value))))

    let private unaryValue kind (source: Type) (target: Type) =
        if (if isNull source then isNull target else source.Equals(target)) && (kind = ExpressionType.Negate || kind = ExpressionType.NegateChecked) then
            if typeof<sbyte>.Equals(source) then negate 0y kind
            elif typeof<int16>.Equals(source) then negate 0s kind
            elif typeof<int32>.Equals(source) then negate 0 kind
            elif typeof<int64>.Equals(source) then negate 0L kind
            elif typeof<single>.Equals(source) then negate 0.0f kind
            elif typeof<double>.Equals(source) then negate 0.0 kind
            elif typeof<decimal>.Equals(source) then negate 0M kind
            else ValueNone
        elif (if isNull source then isNull target else source.Equals(target)) && kind = ExpressionType.Not && not (typeof<bool>.Equals(source)) then
            if typeof<sbyte>.Equals(source) then complement 0y
            elif typeof<byte>.Equals(source) then complement 0uy
            elif typeof<int16>.Equals(source) then complement 0s
            elif typeof<uint16>.Equals(source) then complement 0us
            elif typeof<int32>.Equals(source) then complement 0
            elif typeof<uint32>.Equals(source) then complement 0u
            elif typeof<int64>.Equals(source) then complement 0L
            elif typeof<uint64>.Equals(source) then complement 0UL
            else ValueNone
        elif (if isNull source then isNull target else source.Equals(target)) && (kind = ExpressionType.Convert || kind = ExpressionType.ConvertChecked || kind = ExpressionType.UnaryPlus) then
            ValueSome(Func<obj,obj>(id))
        elif typeof<sbyte>.Equals(source) then convertNumber 0y kind target
        elif typeof<byte>.Equals(source) then convertNumber 0uy kind target
        elif typeof<int16>.Equals(source) then convertNumber 0s kind target
        elif typeof<uint16>.Equals(source) then convertNumber 0us kind target
        elif typeof<int32>.Equals(source) then convertNumber 0 kind target
        elif typeof<uint32>.Equals(source) then convertNumber 0u kind target
        elif typeof<int64>.Equals(source) then convertNumber 0L kind target
        elif typeof<uint64>.Equals(source) then convertNumber 0UL kind target
        elif typeof<char>.Equals(source) then convertNumber '\000' kind target
        elif typeof<single>.Equals(source) then convertNumber 0.0f kind target
        elif typeof<double>.Equals(source) then convertNumber 0.0 kind target
        elif typeof<decimal>.Equals(source) then convertNumber 0M kind target
        elif typeof<bool>.Equals(source) && typeof<bool>.Equals(target) && kind = ExpressionType.Not then
            ValueSome(Func<obj,obj>(fun value -> box(not(unbox<bool> value))))
        else ValueNone

    let private nullableValueType (owner: Type) =
        if typeof<Nullable<sbyte>>.Equals(owner) then typeof<sbyte>
        elif typeof<Nullable<byte>>.Equals(owner) then typeof<byte>
        elif typeof<Nullable<int16>>.Equals(owner) then typeof<int16>
        elif typeof<Nullable<uint16>>.Equals(owner) then typeof<uint16>
        elif typeof<Nullable<int32>>.Equals(owner) then typeof<int32>
        elif typeof<Nullable<uint32>>.Equals(owner) then typeof<uint32>
        elif typeof<Nullable<int64>>.Equals(owner) then typeof<int64>
        elif typeof<Nullable<uint64>>.Equals(owner) then typeof<uint64>
        elif typeof<Nullable<char>>.Equals(owner) then typeof<char>
        elif typeof<Nullable<single>>.Equals(owner) then typeof<single>
        elif typeof<Nullable<double>>.Equals(owner) then typeof<double>
        elif typeof<Nullable<decimal>>.Equals(owner) then typeof<decimal>
        elif typeof<Nullable<bool>>.Equals(owner) then typeof<bool>
        else owner

    let unary kind source target =
        let input, output = nullableValueType source, nullableValueType target
        match unaryValue kind input output with
        | ValueSome apply when not (if isNull input then isNull source else input.Equals(source)) || not (if isNull output then isNull target else output.Equals(target)) ->
            // A populated Nullable boxes its underlying value; an empty one boxes null.
            ValueSome(Func<obj,obj>(fun value ->
                if isNull value && not (if isNull input then isNull source else input.Equals(source)) then
                    if not (if isNull output then isNull target else output.Equals(target)) then null
                    else raise(InvalidOperationException("Nullable object must have a value."))
                else apply.Invoke(value)))
        | operation -> operation

    let inline private numberBinary (_: ^T) kind (target: Type) =
        let inline result apply = ValueSome(Func<obj,obj,obj>(fun left right -> box(apply (unbox< ^T> left) (unbox< ^T> right))))
        if typeof<bool>.Equals(target) then
            match kind with
            | ExpressionType.Equal -> result (=)
            | ExpressionType.NotEqual -> result (<>)
            | ExpressionType.LessThan -> result (<)
            | ExpressionType.LessThanOrEqual -> result (<=)
            | ExpressionType.GreaterThan -> result (>)
            | ExpressionType.GreaterThanOrEqual -> result (>=)
            | _ -> ValueNone
        elif not (typeof< ^T>.Equals(target)) then ValueNone
        else
            match kind with
            | ExpressionType.Add -> result (+)
            | ExpressionType.AddChecked -> result Checked.(+)
            | ExpressionType.Subtract -> result (-)
            | ExpressionType.SubtractChecked -> result Checked.(-)
            | ExpressionType.Multiply -> result (*)
            | ExpressionType.MultiplyChecked -> result Checked.(*)
            | ExpressionType.Divide -> result (/)
            | ExpressionType.Modulo -> result (%)
            | _ -> ValueNone

    let private binaryValue kind (left: Type) (right: Type) (target: Type) =
        if not (if isNull left then isNull right else left.Equals(right)) then ValueNone
        elif typeof<string>.Equals(left) && typeof<string>.Equals(target) && kind = ExpressionType.Add then
            ValueSome(Func<obj,obj,obj>(fun left right -> box(String.Concat(unbox<string> left,unbox<string> right))))
        elif typeof<string>.Equals(left) && typeof<bool>.Equals(target) &&
             (kind = ExpressionType.Equal || kind = ExpressionType.NotEqual) then
            ValueSome(Func<obj,obj,obj>(fun left right ->
                let equal = String.Equals(unbox<string> left,unbox<string> right,StringComparison.Ordinal)
                box(if kind = ExpressionType.Equal then equal else not equal)))
        elif typeof<sbyte>.Equals(left) then numberBinary 0y kind target
        elif typeof<byte>.Equals(left) then numberBinary 0uy kind target
        elif typeof<int16>.Equals(left) then numberBinary 0s kind target
        elif typeof<uint16>.Equals(left) then numberBinary 0us kind target
        elif typeof<int32>.Equals(left) then numberBinary 0 kind target
        elif typeof<uint32>.Equals(left) then numberBinary 0u kind target
        elif typeof<int64>.Equals(left) then numberBinary 0L kind target
        elif typeof<uint64>.Equals(left) then numberBinary 0UL kind target
        elif typeof<single>.Equals(left) then numberBinary 0.0f kind target
        elif typeof<double>.Equals(left) then numberBinary 0.0 kind target
        elif typeof<decimal>.Equals(left) then numberBinary 0M kind target
        elif typeof<bool>.Equals(left) && typeof<bool>.Equals(target) then
            let result apply = ValueSome(Func<obj,obj,obj>(fun left right -> box(apply (unbox<bool> left) (unbox<bool> right))))
            match kind with
            | ExpressionType.Equal -> result (=)
            | ExpressionType.NotEqual -> result (<>)
            | ExpressionType.And -> result (&&)
            | ExpressionType.Or -> result (||)
            | ExpressionType.ExclusiveOr -> result (<>)
            | _ -> ValueNone
        else ValueNone

    let binary kind left right target =
        let inputLeft, inputRight, output = nullableValueType left, nullableValueType right, nullableValueType target
        match binaryValue kind inputLeft inputRight output with
        | ValueSome apply when not (if isNull inputLeft then isNull left else inputLeft.Equals(left)) || not (if isNull inputRight then isNull right else inputRight.Equals(right)) ->
            ValueSome(Func<obj,obj,obj>(fun left right ->
                if not(isNull left || isNull right) then apply.Invoke(left,right)
                elif kind = ExpressionType.And && typeof<bool>.Equals(inputLeft) then
                    if (not(isNull left) && not(unbox<bool> left)) || (not(isNull right) && not(unbox<bool> right)) then box false else null
                elif kind = ExpressionType.Or && typeof<bool>.Equals(inputLeft) then
                    if (not(isNull left) && unbox<bool> left) || (not(isNull right) && unbox<bool> right) then box true else null
                elif not (if isNull output then isNull target else output.Equals(target)) then null
                elif kind = ExpressionType.Equal then box(isNull left && isNull right)
                elif kind = ExpressionType.NotEqual then box(not(isNull left && isNull right))
                else box false))
        | operation -> operation
