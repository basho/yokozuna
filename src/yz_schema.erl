%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(yz_schema).
-include("yokozuna.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-compile(export_all).

%% @doc Administration of schemas.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Convert a schema name into a file name.
-spec filename(schema_name()) -> string().
filename(SchemaName) ->
    binary_to_list(SchemaName) ++ ".xml".

%% @doc Retrieve the raw schema from Riak.
-spec get(schema_name()) -> {ok, raw_schema()} | {error, term()}.
get(Name) ->
    Ring = yz_misc:get_ring(transformed),
    Schemas = get_schemas_from_ring(Ring),
    R = orddict:fetch(Name, Schemas),
    case {Name, R} of
        {?YZ_DEFAULT_SCHEMA_NAME, undefined} ->
            {ok, _RawSchema} = file:read_file(?YZ_DEFAULT_SCHEMA_FILE);
        {_, undefined} ->
            {error, notfound};
        {_, R} ->
            {ok, yz_misc:decompress(R)}
    end.

%% @doc Store the `RawSchema' with `Name'.
-spec store(schema_name(), raw_schema()) -> ok | {error, term()}.
store(Name, RawSchema) when is_binary(RawSchema) ->
    case parse_and_verify(RawSchema) of
        {ok, RawSchema} ->
            CompressedSchema = yz_misc:compress(RawSchema),
            % either unchanged or ok, both are good
            yz_misc:set_ring_meta(
                ?YZ_META_SCHEMAS, [], fun add_schema/2, {Name, CompressedSchema}),
            ok;
        {error, _} = Err ->
            Err
    end.

-spec add_schema(schemas(), {schema_name(), compressed_schema()}) -> schemas().
add_schema(Schemas, {Name, CompressedSchema}) ->
    orddict:store(Name, CompressedSchema, Schemas).

-spec get_schemas_from_ring(ring()) -> indexes().
get_schemas_from_ring(Ring) ->
    case riak_core_ring:get_meta(?YZ_META_SCHEMAS, Ring) of
        {ok, Schemas} -> Schemas;
        undefined -> []
    end.

%% @doc Checks if the given `SchemaName' actually exists.
-spec exists(schema_name()) -> true | false.
exists(SchemaName) ->
    case yz_schema:get(SchemaName) of
        {error, _} -> false;
        _ -> true
    end.

%%%===================================================================
%%% Private
%%%===================================================================

%% @doc Set ?YZ_SCHEMA_BUCKET with the property {allow_mult, false}
%%      We never want schema value siblings.
-spec setup_schema_bucket() -> ok.
setup_schema_bucket() ->
    ok = riak_core_bucket:set_bucket(?YZ_SCHEMA_BUCKET, [{allow_mult, false}]).

%% @private
%%
%% @doc Parse the schema and verify it contains necessary elements.
-spec parse_and_verify(raw_schema()) -> {ok, raw_schema()} | {error, term()}.
parse_and_verify(RawSchema) ->
    try
        {Schema, _} = xmerl_scan:string(binary_to_list(RawSchema), []),
        case verify(Schema) of
            {ok, _} ->
                {ok, RawSchema};
            {error, _} = Err ->
                Err
        end
    catch exit:Reason ->
            {error, Reason}
    end.

%% @doc Verify the `Schema' contains all necessary configuration for
%%      Yokozuna to function properly.
-spec verify(schema()) -> {ok, schema()} | {error, term()}.
verify(Schema) ->
    verify_fts(verify_fields(verify_uk(Schema))).

%% @private
%%
%% @doc Verify the `uniqueKey' element is correct.
-spec verify_uk(schema()) -> {ok, schema()} | {error, term()}.
verify_uk(Schema) ->
    case xmerl_xpath:string("/schema/uniqueKey/text()", Schema) of
        [#xmlText{value="_yz_id"}] ->
            {ok, Schema};
        _ ->
            {error, 'uniqueKey'}
    end.

%% @private
%%
%% @doc Verify the necessary fields are present with correct attributes.
-spec verify_fields({ok, schema()} | {error, term()}) ->
                           {ok, schema()} | {error, term()}.
verify_fields({ok, Schema}) ->
    Fields = [?YZ_ID_FIELD_XPATH,
              ?YZ_ED_FIELD_XPATH,
              ?YZ_FPN_FIELD_XPATH,
              ?YZ_VTAG_FIELD_XPATH,
              ?YZ_PN_FIELD_XPATH,
              ?YZ_RK_FIELD_XPATH,
              ?YZ_RT_FIELD_XPATH,
              ?YZ_RB_FIELD_XPATH,
              ?YZ_ERR_FIELD_XPATH],
    Checks = [verify_field(F, Schema) || F <- Fields],
    IsError = fun(X) -> X /= ok end,
    case lists:filter(IsError, Checks) of
        [] ->
            {ok, Schema};
        Errs ->
            {error, {missing_fields, Errs}}
    end;
verify_fields({error, _}=Err) ->
    Err.

%% @private
-spec verify_field(string(), schema()) -> ok | {error, term()}.
verify_field(Path, Schema) ->
    case xmerl_xpath:string(Path, Schema) of
        [] ->
            {error, {missing_field, Path}};
        _ ->
            ok
    end.

%% @private
%%
%% @doc Verify the necessary field types are present with correct
%%      attributes.
-spec verify_fts({ok, schema()} | {error, term()}) ->
                        {ok, schema()} | {error, term()}.
verify_fts({ok, Schema}) ->
    case xmerl_xpath:string(?YZ_STR_FT_XPATH, Schema) of
        [] ->
            {error, {missing_field_type, ?YZ_STR_FT_XPATH}};
        _ ->
            {ok, Schema}
    end;
verify_fts({error,_}=Err) ->
    Err.
