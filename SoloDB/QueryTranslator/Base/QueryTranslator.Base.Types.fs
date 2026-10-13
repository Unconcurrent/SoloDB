namespace SoloDatabase

open System
open System.Collections.Generic
open System.Collections.ObjectModel
open System.Linq.Expressions
open System.Reflection
open System.Text
open Utils
open SoloDatabase.SqlModel

module internal QueryTranslatorBaseTypes =
    let internal maxTranslationSteps = 10000
    let internal translationStepLimitMessage =
        "Error: Query too complex for SQL translation.\n" +
        "Reason: Expression translation exceeded the internal step limit and may be non-terminating on this composition.\n" +
        "Fix: Simplify the LINQ chain, split it into smaller queries, or call .AsEnumerable() before the unsupported composition."

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

    /// One hop in a Ref.Value chain. Schema-neutral: the translator emits these via reflection
    /// only; the executor resolves the relation descriptor (link table, column orientation,
    /// target collection name) at run time, where the Relations.Schema APIs are in scope.
    type internal ChainHopSpec = {
        OwnerType: Type                   // type of the previous hop's target (or owner for hop 0)
        RelationPropertyName: string      // name of the DBRef property on OwnerType
        TargetType: Type                  // target type via reflection on the property
    }

    type internal UpdateManyRelationTransform =
        | SetDBRefToId of PropertyPath: string * TargetType: Type * TargetId: int64
        | SetDBRefToTypedId of PropertyPath: string * TargetType: Type * TargetIdType: Type * TargetTypedId: obj
        | SetDBRefToNone of PropertyPath: string * TargetType: Type
        | AddDBRefMany of PropertyPath: string * TargetType: Type * TargetId: int64
        | RemoveDBRefMany of PropertyPath: string * TargetType: Type * TargetId: int64
        | ClearDBRefMany of PropertyPath: string * TargetType: Type
        | MutateFileSystemRef of Hops: ChainHopSpec list * PropertyPath: string * Mutation: FileSystemRefMutation
        /// B4 property mutation through a Ref.Value chain. Hops length >= 1; each hop is
        /// DBRef-shape on the previous target type; the LeafJsonPath addresses a
        /// non-`[<SoloId>]` property on the innermost target. NewValueJsonLiteral is the
        /// RHS serialized to JSON; RHS must be a closure-captured constant.
        | MutateRefChainProperty of
            Hops: ChainHopSpec list
            * LeafTargetType: Type
            * LeafJsonPath: string
            * NewValueJsonLiteral: string
        /// Chain-to-leaf DBRefMany Add/Remove/Clear. Hops length >= 1; leaf is the inner-owner's
        /// DBRefMany property. SavedTargetId is the rowid of the existing target row (typed-id
        /// surface resolves to rowid before reaching the executor).
        | RefChainManyAdd of
            Hops: ChainHopSpec list
            * LeafManyPropertyName: string
            * LeafTargetType: Type
            * SavedTargetId: int64
        | RefChainManyRemove of
            Hops: ChainHopSpec list
            * LeafManyPropertyName: string
            * LeafTargetType: Type
            * SavedTargetId: int64
        | RefChainManyClear of
            Hops: ChainHopSpec list
            * LeafManyPropertyName: string
            * LeafTargetType: Type

    /// <summary>
    /// Appends a value to the query as a parameter or literal, handling various primitive types and JSON serialization.
    /// </summary>
    /// <param name="sb">The StringBuilder to append the SQL text to.</param>
    /// <param name="variables">The dictionary of query parameters to add the value to.</param>
    /// <param name="value">The object value to append.</param>
    let internal appendVariable (sb: StringBuilder) (variables: #IDictionary<string, obj>) (value: obj) =
        let name = getVarName sb.Length
        let parameterExpr = StoredValueParameter.allocateNamed variables name value
        let ctx = EmitContext()
        ctx.InlineLiterals <- true
        sb.Append((EmitSelect.emitExpr ctx parameterExpr).Sql) |> ignore

    /// <summary>
    /// Checks if a given .NET Type is considered a primitive type in the context of SQLite storage.
    /// Primitive types are stored directly, while others are serialized as JSON.
    /// </summary>
    /// <param name="x">The Type to check.</param>
    /// <returns>True if the type is a primitive SQLite type, otherwise false.</returns>
    let rec internal isPrimitiveSQLiteType (x: Type) =
        (Utils.isOptionType x && isPrimitiveSQLiteType(x.GetGenericArguments()[0])) || Utils.isIntegerBasedType x || Utils.isFloatBasedType x || typedefof<string>.Equals(x) || typedefof<char>.Equals(x) || typedefof<bool>.Equals(x) || typedefof<Guid>.Equals(x)
        || typedefof<Type>.Equals(x)
        || Utils.isDateTimeLikeType x
        || typeof<byte array>.Equals(x) || typeof<System.Collections.Generic.List<byte>>.Equals(x) || typeof<byte list>.Equals(x) || typeof<byte seq>.Equals(x)
        || x.Name = "Nullable`1"
        || x.IsEnum

    /// Scalar projections retain the carrier consumed by their established reader.
    let internal isSqlScalarResult target =
        isPrimitiveSQLiteType target || JsonFunctions.consumesSqlScalar target ||
        (Metadata.Generation.TypeSupport.RawNumericCore (Utils.optionalPayloadType target).FullName).IsSome

    /// Stored documents and prepared SQL projections have distinct root inputs.
    let internal rootInput sourcePosition alias column =
        if sourcePosition = OuterLayer then SqlExpr.Column(alias, column)
        else SqlExpr.JsonRootExtract(alias, column)

    let internal rootValue target sourcePosition alias column =
        let input = rootInput sourcePosition alias column
        if sourcePosition = OuterLayer then input
        elif isSqlScalarResult target then
            if QueryTranslatorNumeric.unsigned64 target then QueryTranslatorNumeric.storedValue target input
            else input
        else SqlExpr.FunctionCall("jsonb", [SqlExpr.Column(alias, column)])

    /// Canonical JSON null is absence without changing any non-null token.
    let internal nullCarrier carrier value =
        if carrier = SqlScalar then value
        else SqlExpr.FunctionCall("NULLIF", [value; SqlExpr.FunctionCall("jsonb", [SqlExpr.Literal(SqlLiteral.String "null")])])

    let internal nullValue target value =
        nullCarrier (if isSqlScalarResult target then SqlScalar else CanonicalToken) value

    /// Embed an already prepared SQL scalar or canonical JSONB token.
    let internal jsonToken value =
        SqlExpr.FunctionCall("jsonb", [SqlExpr.FunctionCall("json_quote", [value])])

    /// Consume a canonical token through the selected scalar or structural reader.
    let internal jsonTokenValue target token =
        if isSqlScalarResult target then
            QueryTranslatorNumeric.storedValue target
                (SqlExpr.FunctionCall("jsonb_extract", [token; SqlExpr.Literal(SqlLiteral.String "$")]))
        else token

    /// Read original entry tokens before SQLite exposes a native scalar.
    let internal jsonElementValue target source fullKey =
        SqlExpr.FunctionCall("jsonb", [SqlExpr.Binary(source, BinaryOperator.JsonText, fullKey)])
        |> jsonTokenValue target

    /// <summary>
    /// Escapes single quotes in a string for safe inclusion in a SQLite query.
    /// Also removes null characters.
    /// </summary>
    /// <param name="input">The string to escape.</param>
    /// <returns>The escaped string.</returns>
    let internal escapeSQLiteString (input: string) : string =
        input.Replace("'", "''").Replace("\0", "")

    type internal QueryParameterValue =
        | SourceParameter
        | IdentityParameter
        | ProducedParameter of QueryProducedValue
        | DeferredParameter of Lazy<QueryProducedValue>

    /// A formal parameter has one value binding and its original SQL scope.
    [<Struct>]
    type internal ParameterSource = {
        Alias: string option
        Context: QueryContext
        Value: QueryParameterValue
    } with
        member this.Position = this.Context.LayerPosition
        member this.SerializationInput =
            match this.Value with
            | ProducedParameter produced -> produced.Input
            | _ -> this.Context.SerializationInput

    /// A stateful builder for constructing SQL from an expression tree.
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
            /// <summary>The source alias used by column and JSON expressions.</summary>
            SourceAlias: string option
            /// <summary>Determines if a root parameter should be wrapped in json_extract.</summary>
            JsonExtractSelfValue: bool
            /// <summary>True when translating a value that becomes one field inside a json_object projection.</summary>
            InsideJsonObjectProjection: bool
            /// <summary>The parameters of the root lambda expression.</summary>
            Parameters: ReadOnlyCollection<ParameterExpression>
            /// <summary>The index of the parameter representing the document ID.</summary>
            IdParameterIndex: int
            /// <summary>Query source context for multi-source (JOIN) support.</summary>
            SourceContext: QueryContext
            /// Ordered update path/value assignments, emitted as a statement rather than a scalar expression.
            UpdateAssignments: ResizeArray<SqlExpr * SqlExpr>
            /// Result supplied by a pre-expression or unknown-expression handler.
            DuHandlerResult: QueryProducedValue voption ref
            /// Local semantic producers, such as grouped terminals, share the expression lowerer.
            ProducedHandler: (Expression -> QueryProducedValue voption) voption
            /// Literal formatted construction is admitted by its scoped producer.
            AllowStringFormat: bool
            /// Original source binding for captured parameters, including carrier position.
            ParameterSources: Dictionary<ParameterExpression, ParameterSource>
            /// <summary>Shared translation step counter for hard fail-closed non-termination protection.</summary>
            TranslationStepCounter: int ref
            /// <summary>True when translating a WHERE predicate expression.
            /// Min/Max aggregates skip the CASE WHEN sentinel in this context,
            /// allowing SQL-native NULL propagation for owner exclusion.</summary>
            InPredicateContext: bool
        }
        /// <summary>
        /// Returns the generated SQL query string.
        /// </summary>
        /// <returns>The SQL query as a string.</returns>
        override this.ToString() = this.StringBuilder.ToString()

        /// Resolve current and captured parameters with their original input role.
        member internal this.ResolveParameterSource(parameter: ParameterExpression) =
            match this.ParameterSources.TryGetValue parameter with
            | true, source -> source
            | _ -> { Alias = this.SourceAlias; Context = this.SourceContext; Value = SourceParameter }

        /// Share parameters and counters while isolating subquery aliases and joins.
        member internal this.ForSubquery(tableName: string, parameters: ReadOnlyCollection<ParameterExpression>, inputPosition: LayerPosition, input: QuerySerializationInput, ?subqueryRootTable: string) =
            // Record current scope's parameters with their alias before creating child scope.
            let outerAliases = Dictionary<ParameterExpression, ParameterSource>(this.ParameterSources)
            match this.SourceAlias with
            | Some currentAlias ->
                for p in this.Parameters do
                    outerAliases.[p] <- this.ResolveParameterSource p
            | None -> ()
            let alias = if String.IsNullOrEmpty tableName then None else Some ("\"" + tableName + "\"")
            let context = { this.SourceContext.CloneForSubquery(?rootTable = subqueryRootTable) with LayerPosition = inputPosition; SerializationInput = input }
            for parameter in parameters do
                outerAliases[parameter] <- { Alias = alias; Context = context; Value = SourceParameter }
            { this with
                SourceAlias = alias
                Parameters = parameters
                JsonExtractSelfValue = true
                UpdateMode = false
                UpdateAssignments = ResizeArray()
                IdParameterIndex = -1
                ParameterSources = outerAliases
                SourceContext = context }

        member internal this.StepTranslation() =
            let next = this.TranslationStepCounter.Value + 1
            this.TranslationStepCounter.Value <- next
            if next > maxTranslationSteps then
                raise (NotSupportedException translationStepLimitMessage)

        // Whitelisted internal accessors for cross-file visitor split boundary.
        /// Allocate a DU parameter: stores value in Variables dict, returns SqlExpr.Parameter or FunctionCall("jsonb", [Parameter]).
        member internal this.AllocateParamExpr(value: obj) : SqlExpr =
            StoredValueParameter.allocateNext this.Variables value

        member internal this.AllocateStoredParamExpr(value: obj) : SqlExpr =
            match value with
            | :? decimal as value -> StoredValueParameter.allocateExactDecimal this.Variables value
            | _ -> this.AllocateParamExpr value

        member internal this.AllocateComparisonParamExpr(value: obj) =
            StoredValueParameter.allocateComparison this.Variables value

        member internal this.GetSourceContext() = this.SourceContext
        member internal this.GetIdParameterIndex() = this.IdParameterIndex
        member internal this.IsUpdateMode() = this.UpdateMode
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
            let parameters =
                let expression =
                    if expression.NodeType = ExpressionType.Quote then (expression :?> UnaryExpression).Operand
                    else expression
                match expression with
                | :? LambdaExpression as lambda -> lambda.Parameters
                | :? ParameterExpression as parameter -> Array.AsReadOnly [|parameter|]
                | _ -> System.Collections.ObjectModel.ReadOnlyCollection(Array.empty<ParameterExpression>)
            let sourceCtx =
                match sourceContext with
                | ValueSome ctx -> ctx
                | ValueNone ->
                    let context = QueryContext.SingleSource(tableName)
                    // Standalone collection predicates and assignments have one declared
                    // document parameter, even when no relation metadata is required.
                    if parameters.Count = 1 && not(String.IsNullOrEmpty tableName) && idIndex <> 0 then
                        { context with SerializationInput = QuerySerializationInput.ForOwner(parameters[0].Type, tableName, SqlExpr.Column(None, "Id")) }
                    else context
            let alias = if String.IsNullOrEmpty tableName then None else Some ("\"" + tableName + "\"")
            let sources = Dictionary<ParameterExpression, ParameterSource>()
            for index in 0 .. parameters.Count - 1 do
                let parameter = parameters[index]
                let identity = typeof<int64>.Equals(parameter.Type) &&
                               (index = idIndex || index = 0 && parameters.Count = 2)
                sources[parameter] <- { Alias = alias; Context = sourceCtx; Value = if identity then IdentityParameter else SourceParameter }
            {
                StringBuilder = sb
                Variables = variables
                AppendVariable = appendVariable sb variables
                RollBack = fun N -> sb.Remove(sb.Length - (int)N, (int)N) |> ignore
                UpdateMode = updateMode
                UpdateAssignments = ResizeArray()
                SourceAlias = alias
                JsonExtractSelfValue = true
                InsideJsonObjectProjection = false
                Parameters = parameters
                IdParameterIndex = idIndex
                SourceContext = sourceCtx
                DuHandlerResult = ref ValueNone
                ProducedHandler = ValueNone
                AllowStringFormat = false
                ParameterSources = sources
                TranslationStepCounter = ref 0
                InPredicateContext = false
            }
