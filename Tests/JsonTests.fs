module JsonTests

open System.IO

#nowarn "3391" // Implicit on SqlId

open System
open Microsoft.VisualStudio.TestTools.UnitTesting
open SoloDatabase
open SoloDatabase.JsonSerializator
open SoloDatabase.JsonFunctions
open Types

open TestUtils

type JsonTestResult =
| Success
| Failed of exn

type JsonTestExceptation =
| NotOk
| Implementation
| Ok

[<TestClass>]
type JsonTests() =
    let mutable db: SoloDB = Unchecked.defaultof<SoloDB>
    
    [<TestInitialize>]
    member this.Init() =
        let dbSource = $"memory:Test{Random.Shared.NextInt64()}"
        db <- SoloDB.Instantiate dbSource
    
    [<TestCleanup>]
    member this.Cleanup() =
        db.Dispose()

    [<TestMethod>]
    member this.JsonSerialize() =
        let json = JsonSerializator.JsonValue.Serialize {|First = 1; Name = "Alpha"; Likes=["Ice Scream"; "Sleep"]; Activities={|List=["Codes"]; Len=1|}|}
        ()

    [<TestMethod>]
    member this.JsonSerializeDeserialize() =
        let user1 = {
            Username = "Mihail"
            Auth = true
            Banned = false
            FirstSeen = DateTimeOffset.UtcNow.AddYears -10 |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
            LastSeen = DateTimeOffset.UtcNow.AddMinutes -8 |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
            Data = {
                Tags = [|"tag2"|]
            }
        }
        let json = JsonSerializator.JsonValue.Serialize user1
        let user2 = json.ToObject<User>()
        let eq = (user1 = user2)
        Assert.IsTrue(eq)
        ()

    [<TestMethod>]
    member this.JsonUints() =
        db.GetCollection<uint8 array>().Insert [|65uy; 66uy; 67uy|] |> ignore
        db.GetCollection<uint16 array>().Insert [|65us; 66us; 67us|] |> ignore
        db.GetCollection<uint32 array>().Insert [|65u; 66u; 67u|] |> ignore
        db.GetCollection<uint64 array>().Insert [|65uL; 66uL; 67uL|] |> ignore

    [<TestMethod>]
    member this.ToSQLJsonAndKindTest() =
        let now = DateTimeOffset.Now
        assertEqual (toSQLJson now) (now.ToUnixTimeMilliseconds(), false) "ToSQLJsonAndKindTest: Datetimeoffset"
        assertEqual (toSQLJson "Hello") ("Hello", false) "ToSQLJsonAndKindTest: string"
        assertEqual (toSQLJson 1) (1, false) "ToSQLJsonAndKindTest: string"
        assertEqual (toSQLJson [1L]) ("[1]", true) "ToSQLJsonAndKindTest: string"
        assertEqual (toSQLJson {|hello = "test"|}) ("{\"hello\": \"test\"}", true) "ToSQLJsonAndKindTest: string"

    [<TestMethod>]
    member this.JsonDates() =
        let dateTimeOffset = DateTimeOffset.FromUnixTimeMilliseconds 43757783
        let dateTime = DateTime.Now.ToBinary() |> DateTime.FromBinary
        let dateOnly = DateOnly.FromDateTime dateTime
        let timeSpan = DateTime.Now.TimeOfDay.TotalMilliseconds |> int64 (* To the DB storage precision. *) |> float |> TimeSpan.FromMilliseconds
        let timeOnly = timeSpan |> TimeOnly.FromTimeSpan

        assertEqual (toJson dateTimeOffset |> fromJsonOrSQL) (dateTimeOffset) "DateTimeOffset failed."
        assertEqual (toJson dateTime |> fromJsonOrSQL) (dateTime) "DateTime failed."
        assertEqual (toJson dateOnly |> fromJsonOrSQL) (dateOnly) "DateOnly failed."
        assertEqual (toJson timeSpan |> fromJsonOrSQL) (timeSpan) "TimeSpan failed."
        assertEqual (toJson timeOnly |> fromJsonOrSQL) (timeOnly) "TimeOnly failed."

    [<TestMethod>]
    member this.JsonSimpleParse() =
        let emptyObj = JsonSerializator.JsonValue.Parse "{}"
        assertTrue (match emptyObj with JsonValue.Object _ -> true | other -> false)
        let emptyList = JsonSerializator.JsonValue.Parse "[]"
        assertTrue (match emptyList with JsonValue.List _ -> true | other -> false)
        let number1 = JsonSerializator.JsonValue.Parse "1"
        assertTrue (number1 = JsonValue.Number 1)
        let number2 = JsonSerializator.JsonValue.Parse "-2"
        assertTrue (number2 = JsonValue.Number -2)
        let number3 = JsonSerializator.JsonValue.Parse "3E+2"
        assertTrue (number3 = JsonValue.Number 3e+2M)
        let number4 = JsonSerializator.JsonValue.Parse "4E-2"
        assertTrue (number4 = JsonValue.Number 4e-2M)
        ()

    [<TestMethod>]
    member this.JsonParsing() =
        let mutable successfulFails = [] // To list all of them at once.
        for file in Directory.EnumerateFiles("./TestParsing", "*.json") do
            let fileName = Path.GetFileName file
            let text = File.ReadAllText file
            let expected = match fileName |> Seq.head with 'i' -> JsonTestExceptation.Implementation | 'n' -> JsonTestExceptation.NotOk | 'y' -> JsonTestExceptation.Ok | other -> failwith "Invalid json test name."

            let result =
                try
                    let jsonParsed = JsonValue.Parse text
                    JsonTestResult.Success
                with ex -> 
                    JsonTestResult.Failed ex

            match expected, result with
            | Ok, Success -> printfn "Test file %s passed ok" fileName
            | NotOk, Failed ex -> printfn "Test file %s failed ok" fileName
            | Ok, Failed ex -> 
                match fileName with 
                // Number too big.
                | "y_number_real_capital_e.json"
                | "y_number.json"

                | "y_number_0e+1.json"
                | "y_number_0e1.json"
                | "y_string_backslash_doublequotes.json"
                | "y_string_allowed_escapes.json"
                | "y_object_extreme_numbers.json"
                | "y_object_duplicated_key_and_value.json"
                | "y_object_duplicated_key.json" 
                | "y_number_real_pos_exponent.json" 
                | "y_number_real_neg_exp.json"
                | "y_number_real_fraction_exponent.json" 
                | "y_number_real_exponent.json"
                | "y_number_int_with_exp.json" -> 
                    printfn "Test file %s know error: %A" fileName ex
                | fileName ->
                failwithf "Test file %s error: %A" fileName ex
            | NotOk, Success -> 
                match fileName with 
                // Known successful fails, most of them are because the parser
                // stops when a valid object is found, or because of leading commas.
                | "n_array_comma_after_close.json"
                | "n_array_extra_close.json"
                | "n_array_extra_comma.json"
                | "n_array_number_and_comma.json"
                | "n_multidigit_number_then_00.json"
                | "n_number_-01.json"
                | "n_number_-2..json"
                | "n_number_neg_int_starting_with_zero.json"
                | "n_number_neg_real_without_int_part.json"
                | "n_number_real_without_fractional_part.json"
                | "n_number_with_leading_zero.json"
                | "n_object_lone_continuation_byte_in_key_and_trailing_comma.json"
                | "n_object_several_trailing_commas.json"
                | "n_object_single_quote.json"
                | "n_object_trailing_comma.json"
                | "n_object_trailing_comment.json"
                | "n_object_two_commas_in_a_row.json"
                | "n_string_1_surrogate_then_escape.json"
                | "n_string_1_surrogate_then_escape_u.json"
                | "n_structure_whitespace_formfeed.json"
                | "n_structure_object_followed_by_closing_object.json"
                | "n_structure_double_array.json"
                | "n_structure_close_unopened_array.json"
                | "n_structure_array_with_extra_array_close.json"
                | "n_string_unicode_CapitalU.json"
                | "n_string_unescaped_tab.json"
                | "n_string_unescaped_newline.json"
                | "n_string_unescaped_ctrl_char.json"
                | "n_string_single_quote.json"
                | "n_string_invalid_utf8_after_escape.json"
                | "n_string_invalid_unicode_escape.json"
                | "n_string_invalid_backslash_esc.json"
                | "n_string_invalid-utf-8-in-escape.json"
                | "n_string_incomplete_surrogate_escape_invalid.json"
                | "n_string_incomplete_surrogate.json"
                | "n_string_incomplete_escaped_character.json"
                | "n_string_incomplete_escape.json"
                | "n_string_escape_x.json"
                | "n_string_escaped_emoji.json"
                | "n_string_escaped_ctrl_char_tab.json"
                | "n_string_escaped_backslash_bad.json"
                | "n_string_backslash_00.json"
                | "n_string_1_surrogate_then_escape_u1x.json"
                | "n_string_1_surrogate_then_escape_u1.json"
                | "n_number_2.e+3.json"
                | "n_number_2.e-3.json"
                | "n_number_2.e3.json"
                | "n_number_0.e1.json" 
                | "n_number_Inf.json" 
                | "n_number_infinity.json"
                | "n_number_NaN.json"
                | "n_object_bad_value.json"
                | "n_object_key_with_single_quotes.json"
                | "n_structure_unicode-identifier.json" 
                | "n_structure_capitalized_True.json"
                | "n_structure_ascii-unicode-identifier.json"
                | "n_string_single_string_no_double_quotes.json"
                | "n_string_accentuated_char_no_quotes.json" 
                | "n_object_unquoted_key.json"
                    ->
                    printfn "Test file %s know passed incorrectly." fileName
                | fileName when fileName.Contains "trailing" ->
                    printfn "Test file %s know passed incorrectly." fileName
                | fileName when fileName.Contains "incomplete" -> // Will be interpreted as strings.
                    printfn "Test file %s know passed incorrectly." fileName
                | fileName ->
                successfulFails <- fileName :: successfulFails;
                printfn "Test file %s passed incorrectly." fileName
            | Implementation, Success -> printfn "Test file %s implementated." fileName
            | Implementation, Failed ex -> printfn "Test file %s not implementated." fileName

        successfulFails <- successfulFails // For breakpoint.