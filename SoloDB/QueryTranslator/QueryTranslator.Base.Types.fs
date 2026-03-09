namespace SoloDatabase

open System
open System.Collections.Generic
open System.Collections.ObjectModel
open System.Linq.Expressions
open System.Reflection
open System.Text
open JsonFunctions
open Utils
open SqlDu.Engine.C1.Spec

module internal QueryTranslatorBaseTypes =
    /// <summary>
    /// Represents a member access expression in a more abstract way.
    /// This private type simplifies handling different forms of member access.
    /// </summary>
    type internal MemberAccess =
        {
        /// <summary>The expression on which the member is being accessed.</summary>
        Expression: Expression
        /// <summary>The name of the member being accessed.</summary>
        MemberName: string
        /// <summary>The type of the input expression.</summary>
        InputType: Type
        /// <summary>The return type of the member access.</summary>
        ReturnType: Type
        /// <summary>The original MemberExpression, if available.</summary>
        OriginalExpression: MemberExpression option
        }

        /// <summary>
        /// Creates a MemberAccess record from a System.Linq.Expressions.MemberExpression.
        /// </summary>
        /// <param name="expr">The MemberExpression to convert.</param>
        /// <returns>A new MemberAccess record.</returns>
        static member From(expr: MemberExpression) =
            {
                Expression = expr.Expression
                MemberName = expr.Member.Name
                InputType = expr.Expression.Type
                ReturnType = expr.Type
                OriginalExpression = expr |> Some
            }

    type internal UpdateManyRelationTransform =
        | SetDBRefToId of PropertyPath: string * TargetType: Type * TargetId: int64
        | SetDBRefToTypedId of PropertyPath: string * TargetType: Type * TargetIdType: Type * TargetTypedId: obj
        | SetDBRefToNone of PropertyPath: string * TargetType: Type
        | AddDBRefMany of PropertyPath: string * TargetType: Type * TargetId: int64
        | RemoveDBRefMany of PropertyPath: string * TargetType: Type * TargetId: int64
        | ClearDBRefMany of PropertyPath: string * TargetType: Type

    /// <summary>
    /// Appends a value to the query as a parameter or literal, handling various primitive types and JSON serialization.
    /// </summary>
    /// <param name="sb">The StringBuilder to append the SQL text to.</param>
    /// <param name="variables">The dictionary of query parameters to add the value to.</param>
    /// <param name="value">The object value to append.</param>
    let internal appendVariable (sb: StringBuilder) (variables: #IDictionary<string, obj>) (value: obj) =
        let value =
            match value with
            | :? bool as b ->
                box (if b then 1 else 0)
            | _other ->
                value

        let jsonValue, shouldEncode = toSQLJson value
        let name = getVarName sb.Length
        if shouldEncode then
            sb.Append (sprintf "jsonb(@%s)" name) |> ignore
        else
            sb.Append (sprintf "@%s" name) |> ignore
        variables.[name] <- jsonValue

    /// <summary>
    /// Checks if a given .NET Type is considered a primitive type in the context of SQLite storage.
    /// Primitive types are stored directly, while others are serialized as JSON.
    /// </summary>
    /// <param name="x">The Type to check.</param>
    /// <returns>True if the type is a primitive SQLite type, otherwise false.</returns>
    let internal isPrimitiveSQLiteType (x: Type) =
        Utils.isIntegerBasedType x || Utils.isFloatBasedType x || x = typedefof<string> || x = typedefof<char> || x = typedefof<bool> || x = typedefof<Guid>
        || x = typedefof<Type>
        || x = typedefof<DateTime> || x = typedefof<DateTimeOffset> || x = typedefof<DateOnly> || x = typedefof<TimeOnly> || x = typedefof<TimeSpan>
        || x = typeof<byte array> || x = typeof<System.Collections.Generic.List<byte>> || x = typeof<byte list> || x = typeof<byte seq>
        || x.Name = "Nullable`1"
        || x.IsEnum

    /// <summary>
    /// Escapes single quotes in a string for safe inclusion in a SQLite query.
    /// Also removes null characters.
    /// </summary>
    /// <param name="input">The string to escape.</param>
    /// <returns>The escaped string.</returns>
    let internal escapeSQLiteString (input: string) : string =
        input.Replace("'", "''").Replace("\0", "")

    /// <summary>
    /// A stateful builder for constructing a SQL query from an expression tree.
    /// </summary>
    type QueryBuilder =
        internal {
            /// <summary>The StringBuilder holding the query text.</summary>
            StringBuilder: StringBuilder
            /// <summary>A dictionary of parameters for the query.</summary>
            Variables: Dictionary<string, obj>
            /// <summary>A function to append a variable to the query.</summary>
            AppendVariable: obj -> unit
            /// <summary>A function to roll back the StringBuilder by N characters.</summary>
            RollBack: uint -> unit
            /// <summary>Indicates if the builder is in 'update' mode, changing translation logic.</summary>
            UpdateMode: bool
            /// <summary>The table name prefix (e.g., "MyTable.") for column access.</summary>
            TableNameDot: string
            /// <summary>Determines if a root parameter should be wrapped in json_extract.</summary>
            JsonExtractSelfValue: bool
            /// <summary>The parameters of the root lambda expression.</summary>
            Parameters: ReadOnlyCollection<ParameterExpression>
            /// <summary>The index of the parameter representing the document ID.</summary>
            IdParameterIndex: int
            /// <summary>Query source context for multi-source (JOIN) support. When Joins is empty, behavior is identical to pre-relation pipeline.</summary>
            SourceContext: QueryContext
            /// <summary>Placeholder field — DU parameter names now derived from Variables.Count.</summary>
            ParamCounter: int ref
            /// <summary>DU result from pre-expression/unknown handler — replaces __raw__ StringBuilder capture.</summary>
            DuHandlerResult: SqlExpr voption ref
        }
        /// <summary>
        /// Appends a raw string to the query being built.
        /// </summary>
        /// <param name="s">The string to append.</param>
        member this.AppendRaw (s: string) =
            this.StringBuilder.Append s |> ignore

        /// <summary>
        /// Appends a raw character to the query being built.
        /// </summary>
        /// <param name="s">The character to append.</param>
        member this.AppendRaw (s: char) =
            this.StringBuilder.Append s |> ignore

        /// <summary>
        /// Returns the generated SQL query string.
        /// </summary>
        /// <returns>The SQL query as a string.</returns>
        override this.ToString() = this.StringBuilder.ToString()

        /// Create a scoped sub-builder for correlated subquery predicate translation.
        /// Shares StringBuilder + Variables (parameters go to the same query), but uses a different table name and lambda parameters.
        member internal this.ForSubquery(tableName: string, lambdaExpr: LambdaExpression) =
            { this with
                TableNameDot = if String.IsNullOrEmpty tableName then String.Empty else "\"" + tableName + "\"."
                Parameters = lambdaExpr.Parameters
                JsonExtractSelfValue = true
                UpdateMode = false
                IdParameterIndex = -1 }

        // Whitelisted internal accessors for cross-file visitor split boundary.
        /// Allocate a DU parameter: stores value in Variables dict, returns SqlExpr.Parameter or FunctionCall("jsonb", [Parameter]).
        member internal this.AllocateParamExpr(value: obj) : SqlExpr =
            let value =
                match value with
                | :? bool as b -> box (if b then 1 else 0)
                | _other -> value
            let jsonValue, shouldEncode = toSQLJson value
            // Use Variables.Count for unique naming: monotonically increasing even when
            // multiple QueryBuilder instances share the same Variables dict (Queryable pipeline).
            let name = sprintf "dp%d" this.Variables.Count
            this.Variables.[name] <- jsonValue
            if shouldEncode then
                SqlExpr.FunctionCall("jsonb", [SqlExpr.Parameter name])
            else
                SqlExpr.Parameter name

        member internal this.GetSourceContext() = this.SourceContext
        member internal this.GetIdParameterIndex() = this.IdParameterIndex
        member internal this.IsUpdateMode() = this.UpdateMode
        member internal this.GetTableNameDot() = this.TableNameDot
        member internal this.AppendVariableBoxed(value: obj) = this.AppendVariable value
        member internal this.RollBackBy(n: uint) = this.RollBack n

        /// <summary>
        /// Internal factory method to create a new QueryBuilder instance.
        /// </summary>
        /// <param name="sb">The StringBuilder to use.</param>
        /// <param name="variables">The dictionary for query parameters.</param>
        /// <param name="updateMode">Whether to operate in update mode.</param>
        /// <param name="tableName">The name of the table being queried.</param>
        /// <param name="expression">The root expression being translated.</param>
        /// <param name="idIndex">The parameter index for the document ID.</param>
        /// <returns>A new QueryBuilder instance.</returns>
        static member internal New(sb: StringBuilder)(variables: Dictionary<string, obj>)(updateMode: bool)(tableName)(expression: Expression)(idIndex: int)(sourceContext: QueryContext voption) =
            let sourceCtx =
                match sourceContext with
                | ValueSome ctx -> ctx
                | ValueNone -> QueryContext.SingleSource(tableName)
            {
                StringBuilder = sb
                Variables = variables
                AppendVariable = appendVariable sb variables
                RollBack = fun N -> sb.Remove(sb.Length - (int)N, (int)N) |> ignore
                UpdateMode = updateMode
                TableNameDot = if String.IsNullOrEmpty tableName then String.Empty else "\"" + tableName + "\"."
                JsonExtractSelfValue = true
                Parameters =
                    let expression =
                        if expression.NodeType = ExpressionType.Quote
                        then (expression :?> UnaryExpression).Operand
                        else expression
                    in (expression :?> LambdaExpression).Parameters
                IdParameterIndex = idIndex
                SourceContext = sourceCtx
                ParamCounter = ref 0
                DuHandlerResult = ref ValueNone
            }
