namespace SoloDatabase

open System
open System.Buffers
open System.Globalization
open System.Runtime.Serialization
open Microsoft.FSharp.NativeInterop
open SoloDatabase.RawSqliteFunctions
open SoloDatabase.SqlModel

#nowarn "9"

/// Unary primitive sort bytes never inspect a model or compare a pair of values.
module internal QueryOrderingPrimitives =
    let number floating value =
        SqlExpr.FunctionCall("ORDER_NUMBER",[SqlExpr.Literal(SqlLiteral.Integer(if floating then 1L else 0L));value])
    let cultureString objectDefault value =
        SqlExpr.FunctionCall("ORDER_STRING",[SqlExpr.Literal(SqlLiteral.Integer(if objectDefault then 1L else 0L));value])

    // The framework object comparer owns its initialization culture. Read that
    // public serialized fact rather than capture a different current culture.
    let private objectComparison = lazy (
        let info = SerializationInfo(typeof<System.Collections.Comparer>, FormatterConverter())
        (System.Collections.Comparer.Default :> ISerializable).GetObjectData(info, StreamingContext())
        info.GetValue("CompareInfo", typeof<CompareInfo>) :?> CompareInfo)

    let private originalString (value: SqliteRawValue) =
        if not value.IsBlob then value.CopyToString() else
        let bytes = value.GetBlobSpan()
        let count = bytes.Length / 2
        if count = 0 then String.Empty else
        let chars = ArrayPool<char>.Shared.Rent count
        try
            for index = 0 to count - 1 do
                chars[index] <- char ((int bytes[index*2] <<< 8) ||| int bytes[index*2+1])
            System.String(chars,0,count)
        finally ArrayPool<char>.Shared.Return chars

    let register (connection: NativeSqliteConnection) =
        connection.RegisterScalar("ORDER_NUMBER",2,RawScalarFunc(fun context values ->
            let floating = values[0].GetInt32() = 1
            let encoded =
                if floating then
                    if values[1].IsNull then 0UL else
                    let value = values[1].GetDouble()
                    if Double.IsNaN value then 0UL else
                    let bits = if value = 0. then 0UL else uint64(BitConverter.DoubleToInt64Bits value)
                    if bits >>> 63 = 0UL then bits ^^^ 0x8000000000000000UL else ~~~bits
                else uint64(values[1].GetInt64()) ^^^ 0x8000000000000000UL
            let buffer = NativePtr.stackalloc<byte> 8
            for index = 0 to 7 do NativePtr.set buffer index (byte(encoded >>> ((7-index)*8)))
            context.SetBlob(ReadOnlySpan<byte>(NativePtr.toVoidPtr buffer,8))),SqliteFunctionFlags.Deterministic)
        connection.RegisterScalar("ORDER_STRING",2,RawScalarFunc(fun context values ->
            if values[1].IsNull then context.SetNull()
            else
                let comparison = if values[0].GetInt32()=1 then objectComparison.Value else CultureInfo.CurrentCulture.CompareInfo
                context.SetBlob(comparison.GetSortKey(originalString values[1]).KeyData)),SqliteFunctionFlags.None)
