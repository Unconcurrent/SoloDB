namespace SoloDatabase

open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System


[<Struct; StructLayout(LayoutKind.Sequential)>]
// <summary >Internal only, do not touch </summary>
type internal InternalStack128<'T> =
    // 128 contiguous fields
    val mutable V000 : 'T
    val mutable V001 : 'T
    val mutable V002 : 'T
    val mutable V003 : 'T
    val mutable V004 : 'T
    val mutable V005 : 'T
    val mutable V006 : 'T
    val mutable V007 : 'T
    val mutable V008 : 'T
    val mutable V009 : 'T
    val mutable V010 : 'T
    val mutable V011 : 'T
    val mutable V012 : 'T
    val mutable V013 : 'T
    val mutable V014 : 'T
    val mutable V015 : 'T
    val mutable V016 : 'T
    val mutable V017 : 'T
    val mutable V018 : 'T
    val mutable V019 : 'T
    val mutable V020 : 'T
    val mutable V021 : 'T
    val mutable V022 : 'T
    val mutable V023 : 'T
    val mutable V024 : 'T
    val mutable V025 : 'T
    val mutable V026 : 'T
    val mutable V027 : 'T
    val mutable V028 : 'T
    val mutable V029 : 'T
    val mutable V030 : 'T
    val mutable V031 : 'T
    val mutable V032 : 'T
    val mutable V033 : 'T
    val mutable V034 : 'T
    val mutable V035 : 'T
    val mutable V036 : 'T
    val mutable V037 : 'T
    val mutable V038 : 'T
    val mutable V039 : 'T
    val mutable V040 : 'T
    val mutable V041 : 'T
    val mutable V042 : 'T
    val mutable V043 : 'T
    val mutable V044 : 'T
    val mutable V045 : 'T
    val mutable V046 : 'T
    val mutable V047 : 'T
    val mutable V048 : 'T
    val mutable V049 : 'T
    val mutable V050 : 'T
    val mutable V051 : 'T
    val mutable V052 : 'T
    val mutable V053 : 'T
    val mutable V054 : 'T
    val mutable V055 : 'T
    val mutable V056 : 'T
    val mutable V057 : 'T
    val mutable V058 : 'T
    val mutable V059 : 'T
    val mutable V060 : 'T
    val mutable V061 : 'T
    val mutable V062 : 'T
    val mutable V063 : 'T
    val mutable V064 : 'T
    val mutable V065 : 'T
    val mutable V066 : 'T
    val mutable V067 : 'T
    val mutable V068 : 'T
    val mutable V069 : 'T
    val mutable V070 : 'T
    val mutable V071 : 'T
    val mutable V072 : 'T
    val mutable V073 : 'T
    val mutable V074 : 'T
    val mutable V075 : 'T
    val mutable V076 : 'T
    val mutable V077 : 'T
    val mutable V078 : 'T
    val mutable V079 : 'T
    val mutable V080 : 'T
    val mutable V081 : 'T
    val mutable V082 : 'T
    val mutable V083 : 'T
    val mutable V084 : 'T
    val mutable V085 : 'T
    val mutable V086 : 'T
    val mutable V087 : 'T
    val mutable V088 : 'T
    val mutable V089 : 'T
    val mutable V090 : 'T
    val mutable V091 : 'T
    val mutable V092 : 'T
    val mutable V093 : 'T
    val mutable V094 : 'T
    val mutable V095 : 'T
    val mutable V096 : 'T
    val mutable V097 : 'T
    val mutable V098 : 'T
    val mutable V099 : 'T
    val mutable V100 : 'T
    val mutable V101 : 'T
    val mutable V102 : 'T
    val mutable V103 : 'T
    val mutable V104 : 'T
    val mutable V105 : 'T
    val mutable V106 : 'T
    val mutable V107 : 'T
    val mutable V108 : 'T
    val mutable V109 : 'T
    val mutable V110 : 'T
    val mutable V111 : 'T
    val mutable V112 : 'T
    val mutable V113 : 'T
    val mutable V114 : 'T
    val mutable V115 : 'T
    val mutable V116 : 'T
    val mutable V117 : 'T
    val mutable V118 : 'T
    val mutable V119 : 'T
    val mutable V120 : 'T
    val mutable V121 : 'T
    val mutable V122 : 'T
    val mutable V123 : 'T
    val mutable V124 : 'T
    val mutable V125 : 'T
    val mutable V126 : 'T
    val mutable V127 : 'T

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    static member GetRef (s: byref<InternalStack128<'T>>, index: int) : byref<'T> =
        if uint32 index >= 128u then invalidArg (nameof index) "Stack128 index out of range."
        let a = 1

        // contiguous field block starting at V000
        &Unsafe.Add(&s.V000, index)

    // Enables: stack[0] (read) and stack[0] <- v (write)
    member s.Item
        with [<MethodImpl(MethodImplOptions.AggressiveInlining)>] get (index: int) : 'T =
            Unsafe.ReadUnaligned<'T>(&Unsafe.As(&InternalStack128.GetRef(&s, index)))
        and  [<MethodImpl(MethodImplOptions.AggressiveInlining)>] set (index: int) (value: 'T) =
            InternalStack128.GetRef(&s, index) <- value