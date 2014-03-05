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
-compile(export_all).

-define(SCHEMA_VSN, "1.5").
-type schema_err() :: {error, string()}.

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
    R = riak_core_metadata:get(?YZ_META_SCHEMAS, Name),
    case {Name, R} of
        {?YZ_DEFAULT_SCHEMA_NAME, undefined} ->
            {ok, _} = file:read_file(?YZ_DEFAULT_SCHEMA_FILE);
        {_, undefined} ->
            {error, notfound};
        {_, R} ->
            {ok, iolist_to_binary(yz_misc:decompress(R))}
    end.

%% @doc Store the `RawSchema' with `Name'.
-spec store(schema_name(), raw_schema()) -> ok | schema_err().
store(Name, RawSchema) when is_binary(RawSchema) ->
    case parse_and_verify(RawSchema) of
        {ok, RawSchema} ->
            CompressedSchema = yz_misc:compress(RawSchema),
            riak_core_metadata:put(?YZ_META_SCHEMAS, Name, CompressedSchema),
            ok;
        {error, _} = Err ->
            Err
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
-spec parse_and_verify(raw_schema()) -> {ok, raw_schema()} | schema_err().
parse_and_verify(RawSchema) ->
    try
        %% TODO: should this use unicode?
        {Schema, _} = xmerl_scan:string(binary_to_list(RawSchema), []),
        case verify(Schema) of
            {ok, _} ->
                {ok, RawSchema};
            {error, _} = Err ->
                Err
        end
    catch exit:Reason ->
            Msg = ?FMT("failed to parse ~p", [Reason]),
            {error, Msg}
    end.

%% @doc Verify the `Schema' contains all necessary configuration for
%%      Yokozuna to function properly.
-spec verify(schema()) -> {ok, schema()} | schema_err().
verify(Schema) ->
    verify_fts(verify_fields(verify_vsn(verify_uk(Schema)))).

%% @private
%%
%% @doc Verify the the schema 'version' attribute is set to correct
%% value.
-spec verify_vsn({ok, schema()} | schema_err()) -> {ok, schema()} | schema_err().
verify_vsn({ok, Schema}) ->
    case xmerl_xpath:string("string(/schema/@version)", Schema) of
        {xmlObj, string, ?SCHEMA_VSN} ->
            {ok, Schema};
        _ ->
            {error, "schema 'version' attribute must be " ++ ?SCHEMA_VSN}
    end;
verify_vsn({error, _}=Err) ->
    Err.

%% @private
%%
%% @doc Verify the `uniqueKey' element is correct.
-spec verify_uk(schema()) -> {ok, schema()} | schema_err().
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
-spec verify_fields({ok, schema()} | schema_err()) ->
                           {ok, schema()} | schema_err().
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
            ErrMsg = string:join([Msg || {error, Msg} <- Errs], "\n"),
            {error, ErrMsg}
    end;
verify_fields({error, _}=Err) ->
    Err.

%% @private
-spec verify_field(string(), schema()) -> ok | {error, string()}.
verify_field(Path, Schema) ->
    case xmerl_xpath:string(Path, Schema) of
        [] ->
            {error, "missing field " ++ Path};
        _ ->
            ok
    end.

%% @private
%%
%% @doc Verify the necessary field types are present with correct
%%      attributes.
-spec verify_fts({ok, schema()} | schema_err()) ->
                        {ok, schema()} | schema_err().
verify_fts({ok, Schema}) ->
    case xmerl_xpath:string(?YZ_STR_FT_XPATH, Schema) of
        [] ->
            {error, "missing field type " ++ ?YZ_STR_FT_XPATH};
        _ ->
            {ok, Schema}
    end;
verify_fts({error,_}=Err) ->
    Err.
