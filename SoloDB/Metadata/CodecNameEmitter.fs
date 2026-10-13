namespace SoloDatabase.Metadata.CodecGeneration

open System
open System.Collections.Generic
open System.Reflection.Emit
open SoloDatabase.JsonSerializator

/// One generated prefix tree consumes logical name units before consumer actions.
module internal CodecNameEmitter =
    type private Node = {
        Children: Dictionary<int,Node>
        mutable Target: int option
        mutable Keyword: int option
    }
    let private node() = { Children = Dictionary(); Target = None; Keyword = None }
    let private points (text: string) = [|
        let mutable index = 0
        while index < text.Length do
            if Char.IsHighSurrogate text[index] && index + 1 < text.Length && Char.IsLowSurrogate text[index+1] then
                yield Char.ConvertToUtf32(text,index)
                index <- index + 2
            else
                yield int text[index]
                index <- index + 1
    |]

    let recognize (il: CodecIL<'T,'M,'F>) (api: CodecNameApi<'T,'M>) first (cases: (string * int)[]) unknown ended =
        let root = node()
        let add text =
            let mutable current = root
            for point in points text do
                match current.Children.TryGetValue point with
                | true,next -> current <- next
                | _ -> let next = node() in current.Children.Add(point,next); current <- next
            current
        for text,target in cases do (add text).Target <- Some target
        for text,kind in JsonIdentifierSyntax.keywords() do (add text).Keyword <- Some kind
        let point = il.Local api.Int32
        let quote = il.Local api.Int32
        let escaped = il.Local api.Boolean
        let source = il.Local api.SourceReference
        let position = il.Local api.PositionReference
        let length = il.Local api.Int32
        il.Emit OpCodes.Ldc_I4_0; il.EmitLocal(OpCodes.Stloc,escaped)
        il.Emit OpCodes.Ldarg_0; il.EmitLocal(OpCodes.Ldloc,first)
        il.EmitMethod(OpCodes.Call,api.Begin); il.EmitLocal(OpCodes.Stloc,point)
        il.EmitLocal(OpCodes.Ldloc,point); il.EmitInt(OpCodes.Ldc_I4,-2); il.EmitBranch(OpCodes.Beq,ended)
        il.Emit OpCodes.Ldc_I4_0; il.EmitLocal(OpCodes.Stloc,first)
        il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Reference); il.EmitLocal(OpCodes.Stloc,source)
        il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Position); il.EmitLocal(OpCodes.Stloc,position)
        il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Length); il.EmitLocal(OpCodes.Stloc,length)
        let advance count =
            il.EmitLocal(OpCodes.Ldloc,position); il.Emit OpCodes.Dup; il.Emit OpCodes.Ldind_I4
            il.EmitInt(OpCodes.Ldc_I4,count); il.Emit OpCodes.Add; il.Emit OpCodes.Stind_I4
        let currentAt offset =
            il.EmitLocal(OpCodes.Ldloc,source); il.EmitLocal(OpCodes.Ldloc,position); il.Emit OpCodes.Ldind_I4
            CodecAddressEmitter.offset il api.OffsetByte (if api.Utf16 then 2 else 1) (offset * (if api.Utf16 then 2 else 1))
            il.Emit(if api.Utf16 then OpCodes.Ldind_U2 else OpCodes.Ldind_U1)
            il.EmitLocal(OpCodes.Stloc,point)
        let atEnd label =
            il.EmitLocal(OpCodes.Ldloc,position); il.Emit OpCodes.Ldind_I4
            il.EmitLocal(OpCodes.Ldloc,length); il.EmitBranch(OpCodes.Bge,label)
        let invalid() = il.EmitMethod(OpCodes.Call,api.Invalid); il.EmitBranch(OpCodes.Br,unknown)
        let alias kind =
            if kind >= 3 then invalid()
            else
                let name = if kind = 0 then JsonIdentifierSyntax.NullWord elif kind = 1 then JsonIdentifierSyntax.TrueWord else JsonIdentifierSyntax.FalseWord
                let destination = cases |> Array.tryFind(fun (text,_) -> text = name)
                il.EmitBranch(OpCodes.Br,(match destination with Some(_,target) -> target | None -> unknown))
        let finish quoted (current: Node) =
            if not quoted && current.Keyword.IsSome then alias current.Keyword.Value
            else il.EmitBranch(OpCodes.Br,(match current.Target with Some target -> target | None -> unknown))
        let isAscii first valid =
            il.EmitLocal(OpCodes.Ldloc,point); il.EmitInt(OpCodes.Ldc_I4,JsonIdentifierSyntax.Upper); il.Emit OpCodes.Sub
            il.EmitInt(OpCodes.Ldc_I4,JsonIdentifierSyntax.LetterRange); il.EmitBranch(OpCodes.Ble_Un,valid)
            il.EmitLocal(OpCodes.Ldloc,point); il.EmitInt(OpCodes.Ldc_I4,JsonIdentifierSyntax.Lower); il.Emit OpCodes.Sub
            il.EmitInt(OpCodes.Ldc_I4,JsonIdentifierSyntax.LetterRange); il.EmitBranch(OpCodes.Ble_Un,valid)
            for literal in [JsonIdentifierSyntax.Underscore;JsonIdentifierSyntax.Dollar] do
                il.EmitLocal(OpCodes.Ldloc,point); il.EmitInt(OpCodes.Ldc_I4,literal); il.EmitBranch(OpCodes.Beq,valid)
            if not first then
                il.EmitLocal(OpCodes.Ldloc,point); il.EmitInt(OpCodes.Ldc_I4,JsonIdentifierSyntax.Digit); il.Emit OpCodes.Sub
                il.EmitInt(OpCodes.Ldc_I4,JsonIdentifierSyntax.DigitRange); il.EmitBranch(OpCodes.Ble_Un,valid)
        let unit quoted initial first loaded =
            let again = il.Label()
            let eof = il.Label()
            let ready = il.Label()
            il.Mark again
            if not initial then atEnd eof; currentAt 0
            loaded |> Option.iter il.Mark
            il.Emit OpCodes.Ldarg_0
            if quoted then
                il.EmitLocal(OpCodes.Ldloc,quote); il.EmitLocal(OpCodes.Ldloc,point)
                il.EmitMethod(OpCodes.Call,api.Quoted); il.EmitLocal(OpCodes.Stloc,point)
                il.EmitLocal(OpCodes.Ldloc,point); il.EmitInt(OpCodes.Ldc_I4,-2); il.EmitBranch(OpCodes.Beq,again)
            else
                il.EmitInt(OpCodes.Ldc_I4,(if first then 1 else 0)); il.EmitLocal(OpCodes.Ldloc,point)
                il.EmitInt(OpCodes.Ldc_I4,(if initial then 1 else 0))
                if initial then
                    let width = il.Local api.Int32
                    let doneWidth = il.Label()
                    let thresholds = if api.Utf16 then [|0xFFFF,1|] else [|0x7F,1;0x7FF,2;0xFFFF,3|]
                    for ceiling,count in thresholds do
                        let next = il.Label()
                        il.EmitLocal(OpCodes.Ldloc,point); il.EmitInt(OpCodes.Ldc_I4,ceiling); il.EmitBranch(OpCodes.Bgt,next)
                        il.EmitInt(OpCodes.Ldc_I4,count); il.EmitLocal(OpCodes.Stloc,width); il.EmitBranch(OpCodes.Br,doneWidth); il.Mark next
                    il.EmitInt(OpCodes.Ldc_I4,(if api.Utf16 then 2 else 4)); il.EmitLocal(OpCodes.Stloc,width)
                    il.Mark doneWidth; il.EmitLocal(OpCodes.Ldloc,width)
                else il.Emit OpCodes.Ldc_I4_0
                il.EmitLocal(OpCodes.Ldloca,escaped); il.EmitMethod(OpCodes.Call,api.Identifier); il.EmitLocal(OpCodes.Stloc,point)
            il.EmitBranch(OpCodes.Br,ready)
            il.Mark eof
            if quoted then il.EmitMethod(OpCodes.Call,api.Unterminated)
            else il.Emit OpCodes.Ldc_I4_M1; il.EmitLocal(OpCodes.Stloc,point)
            il.Mark ready
        let emitTrie quoted initial =
            let unknownLoop = il.Label()
            let nodes = ResizeArray<Node * int * int>()
            let labels = Dictionary<Node,int * int>(HashIdentity.Reference)
            let rec collect currentNode =
                let entry,loaded = il.Label(),il.Label()
                labels.Add(currentNode,(entry,loaded))
                nodes.Add(currentNode,entry,loaded)
                for child in currentNode.Children.Values do collect child
            collect root
            let literalUnit point =
                point < 128 && point <> 34 && point <> 39 && point <> 92 && point <> 10 && point <> 13 &&
                (quoted || (JsonIdentifierSyntax.isStart point || uint32(point-JsonIdentifierSyntax.Digit) <= uint32 JsonIdentifierSyntax.DigitRange))
            let chain start =
                let result = ResizeArray<Node * int * Node>()
                let mutable at = start
                let mutable more = true
                while more && at.Target.IsNone && at.Keyword.IsNone && at.Children.Count = 1 do
                    let item = at.Children |> Seq.head
                    if literalUnit item.Key then
                        result.Add(at,item.Key,item.Value)
                        at <- item.Value
                    else more <- false
                result.ToArray(),at
            for currentNode,entry,loaded in nodes do
                il.Mark entry
                let first = obj.ReferenceEquals(currentNode,root)
                if not first || not initial then
                    let run,finishNode = chain currentNode
                    if run.Length >= 2 then
                        let short = il.Label()
                        il.EmitLocal(OpCodes.Ldloc,length)
                        il.EmitLocal(OpCodes.Ldloc,position); il.Emit OpCodes.Ldind_I4; il.Emit OpCodes.Sub
                        il.EmitInt(OpCodes.Ldc_I4,run.Length); il.EmitBranch(OpCodes.Blt,short)
                        let failures = run |> Array.mapi(fun offset (at,expected,_) -> offset,at,expected,il.Label())
                        for offset,_,expected,failed in failures do
                            currentAt offset
                            il.EmitLocal(OpCodes.Ldloc,point); il.EmitInt(OpCodes.Ldc_I4,expected)
                            il.EmitBranch(OpCodes.Bne_Un,failed)
                        advance run.Length
                        il.EmitBranch(OpCodes.Br,fst labels[finishNode])
                        for offset,at,_,failed in failures do
                            il.Mark failed
                            if offset > 0 then advance offset
                            il.EmitBranch(OpCodes.Br,snd labels[at])
                        il.Mark short
                if quoted || currentNode.Keyword.IsNone then
                    let exact =
                        currentNode.Children
                        |> Seq.filter(fun pair -> literalUnit pair.Key && (quoted || not first || JsonIdentifierSyntax.isStart pair.Key))
                        |> Seq.map(fun pair -> pair.Key,pair.Value,il.Label())
                        |> Seq.toArray
                    if exact.Length > 0 then
                        let normal = il.Label()
                        if not(initial && first) then atEnd normal; currentAt 0
                        for expected,_,matched in exact do
                            il.EmitLocal(OpCodes.Ldloc,point); il.EmitInt(OpCodes.Ldc_I4,expected)
                            il.EmitBranch(OpCodes.Beq,matched)
                        il.EmitBranch(OpCodes.Br,loaded)
                        for _,child,matched in exact do
                            il.Mark matched; advance 1; il.EmitBranch(OpCodes.Br,fst labels[child])
                        il.Mark normal
                if not quoted && currentNode.Keyword.IsSome then
                    let read = il.Label()
                    il.EmitLocal(OpCodes.Ldloc,escaped); il.EmitBranch(OpCodes.Brtrue,read)
                    let boundary = il.Label()
                    atEnd boundary
                    currentAt 0
                    il.EmitLocal(OpCodes.Ldloc,point); il.EmitInt(OpCodes.Ldc_I4,92); il.EmitBranch(OpCodes.Bne_Un,loaded)
                    il.Mark boundary; alias currentNode.Keyword.Value
                    il.Mark read
                unit quoted (initial && first) first (Some loaded)
                let more = il.Label()
                il.EmitLocal(OpCodes.Ldloc,point); il.Emit OpCodes.Ldc_I4_M1; il.EmitBranch(OpCodes.Bne_Un,more)
                finish quoted currentNode
                il.Mark more
                for pair in currentNode.Children do
                    il.EmitLocal(OpCodes.Ldloc,point); il.EmitInt(OpCodes.Ldc_I4,pair.Key)
                    il.EmitBranch(OpCodes.Beq,fst labels[pair.Value])
                il.EmitBranch(OpCodes.Br,unknownLoop)
            il.Mark unknownLoop
            unit quoted false false None
            il.EmitLocal(OpCodes.Ldloc,point); il.Emit OpCodes.Ldc_I4_M1; il.EmitBranch(OpCodes.Bne_Un,unknownLoop)
            il.EmitBranch(OpCodes.Br,unknown)
        let quoted = il.Label()
        let identifier = il.Label()
        for value in [34;39] do
            il.EmitLocal(OpCodes.Ldloc,point); il.EmitInt(OpCodes.Ldc_I4,value); il.EmitBranch(OpCodes.Beq,quoted)
        let validFirst = il.Label()
        il.EmitLocal(OpCodes.Ldloc,point); il.EmitInt(OpCodes.Ldc_I4,92); il.EmitBranch(OpCodes.Beq,identifier)
        il.EmitLocal(OpCodes.Ldloc,point); il.EmitInt(OpCodes.Ldc_I4,127); il.EmitBranch(OpCodes.Bgt,identifier)
        isAscii true validFirst
        il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Unsupported); invalid()
        il.Mark validFirst; il.EmitBranch(OpCodes.Br,identifier)
        il.Mark quoted; il.EmitLocal(OpCodes.Ldloc,point); il.EmitLocal(OpCodes.Stloc,quote); advance 1
        emitTrie true false
        il.Mark identifier
        emitTrie false true
