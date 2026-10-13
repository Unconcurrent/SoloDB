namespace SoloDatabase.JsonSerializator

open System
open System.Text

module internal JsonStringLiteral =
    // Negative codes name short escapes; positive codes contain a Unicode unit plus one.
    let escape c =
        match c with
        | '\b' -> -int 'b' | '\t' -> -int 't' | '\n' -> -int 'n'
        | '\f' -> -int 'f' | '\r' -> -int 'r' | '"' -> -int '"' | '\\' -> -int '\\'
        | c when c < '\u0020' || c = '\u0027' || c = '<' || c = '>' || c = '&'
                 || c = '\u0085' || c = '\u2028' || c = '\u2029' -> int c + 1
        | _ -> 0

    let hex = "0123456789abcdef"

    let appendStringBuilder (text: string) (builder: StringBuilder) =
        if not(String.IsNullOrEmpty text) then
            let mutable run = 0
            for index = 0 to text.Length - 1 do
                let code = escape text[index]
                if code <> 0 then
                    if index > run then builder.Append(text,run,index-run) |> ignore
                    builder.Append('\\') |> ignore
                    if code < 0 then builder.Append(char -code) |> ignore
                    else
                        let c = code - 1
                        builder.Append('u').Append(hex[c >>> 12]).Append(hex[(c >>> 8) &&& 15])
                               .Append(hex[(c >>> 4) &&& 15]).Append(hex[c &&& 15]) |> ignore
                    run <- index + 1
            if run < text.Length then builder.Append(text,run,text.Length-run) |> ignore

    let property (name: string) =
        let builder = StringBuilder(name.Length + 3)
        builder.Append('"') |> ignore
        appendStringBuilder name builder
        builder.Append("\":") |> ignore
        builder.ToString()
