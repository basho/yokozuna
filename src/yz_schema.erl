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
-spec get(schema_name()) -> {ok, raw_schema()} | {error, schema_name(), term()}.
get(Name) ->
    C = yz_kv:client(),
    R = yz_kv:get(C, ?YZ_SCHEMA_BUCKET, Name),
    case {Name, R} of
        {?YZ_DEFAULT_SCHEMA_NAME, {error, _}} ->
            {ok, _RawSchema} = file:read_file(?YZ_DEFAULT_SCHEMA_FILE);
        {_, {error, Reason}} ->
            {error, Name, Reason};
        {_, {value, RawSchema}} ->
            {ok, RawSchema}
    end.

%% @doc Store the `RawSchema' with `Name'.
-spec store(schema_name(), raw_schema()) -> ok | {error, term()}.
store(Name, RawSchema) when is_binary(RawSchema) ->
    case parse_and_xform(RawSchema) of
        {ok, RawSchema2} ->
            C = yz_kv:client(),
            yz_kv:put(C, ?YZ_SCHEMA_BUCKET, Name, RawSchema2, "text/xml");
        {error, _} = Err ->
            Err
    end.

%% @doc Checks if the given `SchemaName' actually exists.
-spec exists(schema_name()) -> true | false.
exists(SchemaName) ->
    case yz_schema:get(SchemaName) of
        {error, _, _} -> false;
        _ -> true
    end.

%%%===================================================================
%%% Private
%%%===================================================================

%% @private
%%
%% @doc Parse the schema.
-spec parse_and_xform(raw_schema()) -> {ok, raw_schema()} | {error, term()}.
parse_and_xform(RawSchema) ->
    try
        Opts = [{comments, false},
                {acc_fun, fun ?MODULE:xform_acc/3}],
        {Schema, _} = xmerl_scan:string(binary_to_list(RawSchema), Opts),
        RawSchema2 = iolist_to_binary(xmerl:export_simple([Schema], xmerl_xml)),
        {ok, RawSchema2}
    catch exit:Reason ->
            {error, Reason}
    end.

-define(FIELD, #xmlElement{name='field', attributes=Attrs}).
-define(FIELD_TYPE, #xmlElement{name='fieldType', attributes=Attrs}).
-define(UK, #xmlElement{name='uniqueKey'}).

%% @private
xform_add_yz_fields(E=#xmlElement{content=C}) ->
    C2 = ["\n\n    ",
          ?YZ_ID_FIELD_XML, "\n    ",
          ?YZ_ED_FIELD_XML, "\n    ",
          ?YZ_FPN_FIELD_XML, "\n    ",
          ?YZ_VTAG_FIELD_XML, "\n    ",
          ?YZ_NODE_FIELD_XML, "\n    ",
          ?YZ_PN_FIELD_XML, "\n    ",
          ?YZ_RK_FIELD_XML, "\n    ",
          ?YZ_RB_FIELD_XML, "\n"|C],
    E#xmlElement{content=C2}.

%% @private
xform_add_yz_fts(E=#xmlElement{content=C}) ->
    C2 = ["\n\n    ",
          ?YZ_STR_FT_XML, "\n"|C],
    E#xmlElement{content=C2}.

%% @private
xform_acc(E=?FIELD, Acc, GlobalState) ->
    NameAttr = lists:keyfind(name, #xmlAttribute.name, Attrs),
    FieldName = NameAttr#xmlAttribute.value,
    case ?YZ_IS_YZ_FIELD_S(FieldName) of
        true -> {Acc, GlobalState};
        false -> {[E|Acc], GlobalState}
    end;
xform_acc(E=?FIELD_TYPE, Acc, GlobalState) ->
    NameAttr = lists:keyfind(name, #xmlAttribute.name, Attrs),
    FTName = NameAttr#xmlAttribute.value,
    case ?YZ_IS_YZ_FT_S(FTName) of
        true -> {Acc, GlobalState};
        false -> {[E|Acc], GlobalState}
    end;
xform_acc(?UK, Acc, GlobalState) ->
    {Acc, GlobalState};
xform_acc(E=#xmlElement{name='fields'}, Acc, GlobalState) ->
    E2 = xform_add_yz_fields(E),
    {[E2|Acc], GlobalState};
xform_acc(E=#xmlElement{name='types'}, Acc, GlobalState) ->
    E2 = xform_add_yz_fts(E),
    {[E2,"\n\n",?YZ_UK_XML|Acc], GlobalState};
xform_acc(E, Acc, GlobalState) ->
    {[E|Acc], GlobalState}.
