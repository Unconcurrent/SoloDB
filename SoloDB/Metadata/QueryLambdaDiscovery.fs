namespace SoloDatabase

open System
open System.Reflection

/// Typed standard delegates execute the existing value plan without compiling it.
[<AbstractClass; Sealed>]
type internal QueryLambdaFactories private () =
    static member Func0<'Result>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Func<'Result>(fun () -> unbox<'Result>(run.Invoke(if size = 0 then Array.empty else Array.zeroCreate size)))))

    static member Action0() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Action(fun () -> run.Invoke(if size = 0 then Array.empty else Array.zeroCreate size) |> ignore)))

    static member Func1<'A0, 'Result>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Func<'A0, 'Result>(fun a0 -> unbox<'Result>(let values = Array.zeroCreate size in values.[offset+0] <- box a0; run.Invoke(values)))))

    static member Action1<'A0>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Action<'A0>(fun a0 -> let values = Array.zeroCreate size in values.[offset+0] <- box a0; run.Invoke(values) |> ignore)))

    static member Func2<'A0, 'A1, 'Result>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Func<'A0, 'A1, 'Result>(fun a0 a1 -> unbox<'Result>(let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; run.Invoke(values)))))

    static member Action2<'A0, 'A1>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Action<'A0, 'A1>(fun a0 a1 -> let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; run.Invoke(values) |> ignore)))

    static member Func3<'A0, 'A1, 'A2, 'Result>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Func<'A0, 'A1, 'A2, 'Result>(fun a0 a1 a2 -> unbox<'Result>(let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; run.Invoke(values)))))

    static member Action3<'A0, 'A1, 'A2>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Action<'A0, 'A1, 'A2>(fun a0 a1 a2 -> let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; run.Invoke(values) |> ignore)))

    static member Func4<'A0, 'A1, 'A2, 'A3, 'Result>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Func<'A0, 'A1, 'A2, 'A3, 'Result>(fun a0 a1 a2 a3 -> unbox<'Result>(let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; run.Invoke(values)))))

    static member Action4<'A0, 'A1, 'A2, 'A3>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Action<'A0, 'A1, 'A2, 'A3>(fun a0 a1 a2 a3 -> let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; run.Invoke(values) |> ignore)))

    static member Func5<'A0, 'A1, 'A2, 'A3, 'A4, 'Result>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Func<'A0, 'A1, 'A2, 'A3, 'A4, 'Result>(fun a0 a1 a2 a3 a4 -> unbox<'Result>(let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; run.Invoke(values)))))

    static member Action5<'A0, 'A1, 'A2, 'A3, 'A4>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Action<'A0, 'A1, 'A2, 'A3, 'A4>(fun a0 a1 a2 a3 a4 -> let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; run.Invoke(values) |> ignore)))

    static member Func6<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'Result>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Func<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'Result>(fun a0 a1 a2 a3 a4 a5 -> unbox<'Result>(let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; run.Invoke(values)))))

    static member Action6<'A0, 'A1, 'A2, 'A3, 'A4, 'A5>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Action<'A0, 'A1, 'A2, 'A3, 'A4, 'A5>(fun a0 a1 a2 a3 a4 a5 -> let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; run.Invoke(values) |> ignore)))

    static member Func7<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'Result>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Func<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'Result>(fun a0 a1 a2 a3 a4 a5 a6 -> unbox<'Result>(let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; values.[offset+6] <- box a6; run.Invoke(values)))))

    static member Action7<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Action<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6>(fun a0 a1 a2 a3 a4 a5 a6 -> let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; values.[offset+6] <- box a6; run.Invoke(values) |> ignore)))

    static member Func8<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'Result>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Func<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'Result>(fun a0 a1 a2 a3 a4 a5 a6 a7 -> unbox<'Result>(let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; values.[offset+6] <- box a6; values.[offset+7] <- box a7; run.Invoke(values)))))

    static member Action8<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Action<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7>(fun a0 a1 a2 a3 a4 a5 a6 a7 -> let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; values.[offset+6] <- box a6; values.[offset+7] <- box a7; run.Invoke(values) |> ignore)))

    static member Func9<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'Result>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Func<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'Result>(fun a0 a1 a2 a3 a4 a5 a6 a7 a8 -> unbox<'Result>(let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; values.[offset+6] <- box a6; values.[offset+7] <- box a7; values.[offset+8] <- box a8; run.Invoke(values)))))

    static member Action9<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Action<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8>(fun a0 a1 a2 a3 a4 a5 a6 a7 a8 -> let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; values.[offset+6] <- box a6; values.[offset+7] <- box a7; values.[offset+8] <- box a8; run.Invoke(values) |> ignore)))

    static member Func10<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'Result>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Func<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'Result>(fun a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 -> unbox<'Result>(let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; values.[offset+6] <- box a6; values.[offset+7] <- box a7; values.[offset+8] <- box a8; values.[offset+9] <- box a9; run.Invoke(values)))))

    static member Action10<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Action<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9>(fun a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 -> let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; values.[offset+6] <- box a6; values.[offset+7] <- box a7; values.[offset+8] <- box a8; values.[offset+9] <- box a9; run.Invoke(values) |> ignore)))

    static member Func11<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'Result>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Func<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'Result>(fun a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 -> unbox<'Result>(let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; values.[offset+6] <- box a6; values.[offset+7] <- box a7; values.[offset+8] <- box a8; values.[offset+9] <- box a9; values.[offset+10] <- box a10; run.Invoke(values)))))

    static member Action11<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Action<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10>(fun a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 -> let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; values.[offset+6] <- box a6; values.[offset+7] <- box a7; values.[offset+8] <- box a8; values.[offset+9] <- box a9; values.[offset+10] <- box a10; run.Invoke(values) |> ignore)))

    static member Func12<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'A11, 'Result>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Func<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'A11, 'Result>(fun a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 -> unbox<'Result>(let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; values.[offset+6] <- box a6; values.[offset+7] <- box a7; values.[offset+8] <- box a8; values.[offset+9] <- box a9; values.[offset+10] <- box a10; values.[offset+11] <- box a11; run.Invoke(values)))))

    static member Action12<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'A11>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Action<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'A11>(fun a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 -> let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; values.[offset+6] <- box a6; values.[offset+7] <- box a7; values.[offset+8] <- box a8; values.[offset+9] <- box a9; values.[offset+10] <- box a10; values.[offset+11] <- box a11; run.Invoke(values) |> ignore)))

    static member Func13<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'A11, 'A12, 'Result>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Func<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'A11, 'A12, 'Result>(fun a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 -> unbox<'Result>(let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; values.[offset+6] <- box a6; values.[offset+7] <- box a7; values.[offset+8] <- box a8; values.[offset+9] <- box a9; values.[offset+10] <- box a10; values.[offset+11] <- box a11; values.[offset+12] <- box a12; run.Invoke(values)))))

    static member Action13<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'A11, 'A12>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Action<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'A11, 'A12>(fun a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 -> let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; values.[offset+6] <- box a6; values.[offset+7] <- box a7; values.[offset+8] <- box a8; values.[offset+9] <- box a9; values.[offset+10] <- box a10; values.[offset+11] <- box a11; values.[offset+12] <- box a12; run.Invoke(values) |> ignore)))

    static member Func14<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'A11, 'A12, 'A13, 'Result>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Func<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'A11, 'A12, 'A13, 'Result>(fun a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 -> unbox<'Result>(let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; values.[offset+6] <- box a6; values.[offset+7] <- box a7; values.[offset+8] <- box a8; values.[offset+9] <- box a9; values.[offset+10] <- box a10; values.[offset+11] <- box a11; values.[offset+12] <- box a12; values.[offset+13] <- box a13; run.Invoke(values)))))

    static member Action14<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'A11, 'A12, 'A13>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Action<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'A11, 'A12, 'A13>(fun a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 -> let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; values.[offset+6] <- box a6; values.[offset+7] <- box a7; values.[offset+8] <- box a8; values.[offset+9] <- box a9; values.[offset+10] <- box a10; values.[offset+11] <- box a11; values.[offset+12] <- box a12; values.[offset+13] <- box a13; run.Invoke(values) |> ignore)))

    static member Func15<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'A11, 'A12, 'A13, 'A14, 'Result>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Func<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'A11, 'A12, 'A13, 'A14, 'Result>(fun a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 a14 -> unbox<'Result>(let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; values.[offset+6] <- box a6; values.[offset+7] <- box a7; values.[offset+8] <- box a8; values.[offset+9] <- box a9; values.[offset+10] <- box a10; values.[offset+11] <- box a11; values.[offset+12] <- box a12; values.[offset+13] <- box a13; values.[offset+14] <- box a14; run.Invoke(values)))))

    static member Action15<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'A11, 'A12, 'A13, 'A14>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Action<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'A11, 'A12, 'A13, 'A14>(fun a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 a14 -> let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; values.[offset+6] <- box a6; values.[offset+7] <- box a7; values.[offset+8] <- box a8; values.[offset+9] <- box a9; values.[offset+10] <- box a10; values.[offset+11] <- box a11; values.[offset+12] <- box a12; values.[offset+13] <- box a13; values.[offset+14] <- box a14; run.Invoke(values) |> ignore)))

    static member Func16<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'A11, 'A12, 'A13, 'A14, 'A15, 'Result>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Func<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'A11, 'A12, 'A13, 'A14, 'A15, 'Result>(fun a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 a14 a15 -> unbox<'Result>(let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; values.[offset+6] <- box a6; values.[offset+7] <- box a7; values.[offset+8] <- box a8; values.[offset+9] <- box a9; values.[offset+10] <- box a10; values.[offset+11] <- box a11; values.[offset+12] <- box a12; values.[offset+13] <- box a13; values.[offset+14] <- box a14; values.[offset+15] <- box a15; run.Invoke(values)))))

    static member Action16<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'A11, 'A12, 'A13, 'A14, 'A15>() =
        Func<int,int,Func<obj[],obj>,obj>(fun offset size run -> box (Action<'A0, 'A1, 'A2, 'A3, 'A4, 'A5, 'A6, 'A7, 'A8, 'A9, 'A10, 'A11, 'A12, 'A13, 'A14, 'A15>(fun a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 a14 a15 -> let values = Array.zeroCreate size in values.[offset+0] <- box a0; values.[offset+1] <- box a1; values.[offset+2] <- box a2; values.[offset+3] <- box a3; values.[offset+4] <- box a4; values.[offset+5] <- box a5; values.[offset+6] <- box a6; values.[offset+7] <- box a7; values.[offset+8] <- box a8; values.[offset+9] <- box a9; values.[offset+10] <- box a10; values.[offset+11] <- box a11; values.[offset+12] <- box a12; values.[offset+13] <- box a13; values.[offset+14] <- box a14; values.[offset+15] <- box a15; run.Invoke(values) |> ignore)))

module internal QueryLambdaDiscovery =
    let create (delegateType: Type) =
        let invoke = delegateType.GetMethod("Invoke")
        let arguments = invoke.GetParameters() |> Array.map _.ParameterType
        if arguments.Length > 16 || invoke.ReturnType.IsByRef || arguments |> Array.exists _.IsByRef then
            raise (NotSupportedException($"Query value lambda {delegateType.FullName} requires a generated direct operation."))
        let action = typeof<Void>.Equals(invoke.ReturnType)
        let name = (if action then "Action" else "Func") + string arguments.Length
        let factory = typeof<QueryLambdaFactories>.GetMethod(name, BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Static)
        let types = if action then arguments else Array.append arguments [|invoke.ReturnType|]
        let closed = if types.Length = 0 then factory else factory.MakeGenericMethod types
        let create = closed.Invoke(null,Array.empty) :?> Func<int,int,Func<obj[],obj>,obj>
        if not ((create.Invoke(0, arguments.Length, null)).GetType().Equals(delegateType)) then
            raise (NotSupportedException($"Query value lambda {delegateType.FullName} requires a generated direct operation."))
        create
