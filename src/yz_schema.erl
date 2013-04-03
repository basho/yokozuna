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
    case parse(RawSchema) of
        {ok, _Schema} ->
            %% TODO: x-form schema, ensuring special fields
            C = yz_kv:client(),
            yz_kv:put(C, ?YZ_SCHEMA_BUCKET, Name, RawSchema, "text/xml");
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

%% @doc Parse the schema.
-spec parse(raw_schema()) -> {ok, schema()} | {error, term()}.
parse(RawSchema) ->
    try
        {Schema, _} = xmerl_scan:string(binary_to_list(RawSchema), [{document, true}]),
        {ok, Schema}
    catch exit:Reason ->
            {error, Reason}
    end.
