%% -------------------------------------------------------------------
%%
%% PB Service for Yokozuna administrative functions, like schema or index
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Implements a `riak_api_pb_service' for performing
%% administrative functions in Yokozuna, like managing
%% indexes or schema
-module(yz_pb_admin).

-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_pb/include/riak_yokozuna_pb.hrl").
-include("yokozuna.hrl").

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

%% @doc init/0 callback. Returns the service internal start state.
-spec init() -> any().
init() ->
    no_state.

%% @doc decode/2 callback. Decodes an incoming message.
%%      also checks that this request has permission
decode(Code, Bin) ->
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #'RpbYokozunaSchemaPutReq'{} ->
            {ok, Msg, {?YZ_SECURITY_ADMIN_PERM, ?YZ_SECURITY_SCHEMA}};
        #'RpbYokozunaSchemaGetReq'{} ->
            {ok, Msg, {?YZ_SECURITY_ADMIN_PERM, ?YZ_SECURITY_SCHEMA}};
        #'RpbYokozunaIndexPutReq'{} ->
            {ok, Msg, {?YZ_SECURITY_ADMIN_PERM, ?YZ_SECURITY_INDEX}};
        'RpbYokozunaIndexGetReq' ->
            {ok, Msg, {?YZ_SECURITY_ADMIN_PERM, ?YZ_SECURITY_INDEX}};
        #'RpbYokozunaIndexGetReq'{} ->
            {ok, Msg, {?YZ_SECURITY_ADMIN_PERM, ?YZ_SECURITY_INDEX}};
        #'RpbYokozunaIndexDeleteReq'{} ->
            {ok, Msg, {?YZ_SECURITY_ADMIN_PERM, ?YZ_SECURITY_INDEX}};
        _ ->
            {ok, Msg}
    end.
%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

%% @doc process/2 callback. Handles an incoming request message.
process(#'RpbYokozunaSchemaPutReq'{
            schema = #'RpbYokozunaSchema'{
                name = SchemaName, content = Content}}, State) ->
    case yz_schema:store(SchemaName, Content) of
        ok  ->
            {reply, #'RpbPutResp'{}, State};
        {error, Reason} ->
            Msg = io_lib:format("Error storing schema ~s~n", [Reason]),
            {error, Msg, State}
    end;

process(#'RpbYokozunaSchemaGetReq'{name = SchemaName}, State) ->
    case yz_schema:get(SchemaName) of
        {ok, Content} ->
            Schema = #'RpbYokozunaSchema'{name = SchemaName, content = Content},
            {reply, #'RpbYokozunaSchemaGetResp'{schema = Schema}, State};
        {error, notfound} ->
            {error, "notfound", State}
    end;

process(#'RpbYokozunaIndexDeleteReq'{name = IndexName}, State) ->
    Ring = yz_misc:get_ring(transformed),
    case yz_index:exists(IndexName) of
        true  ->
            case yz_index:associated_buckets(IndexName, Ring) of
                [] ->
                    ok = yz_index:remove(IndexName),
                    {reply, 'RpbDelResp', State};
                Buckets ->
                    Msg = "Can't delete index with associate buckets ~p",
                    Msg2 = lists:flatten(io_lib:fwrite(Msg, [Buckets])),
                    {error, Msg2, State}
            end;
        false ->
            {error, "notfound", State}
    end;

process(#'RpbYokozunaIndexPutReq'{
            index = #'RpbYokozunaIndex'{
                name = IndexName,
                schema = SchemaName,
                n_val = Nval},
            timeout = T0}, State) ->
    Timeout =
        case T0 of
            undefined -> app_helper:get_env(yokozuna, index_put_timeout_ms,
                                            ?DEFAULT_IDX_CREATE_TIMEOUT);
            _ -> T0
        end,

    case maybe_create_index(IndexName, SchemaName, Nval, Timeout) of
        ok ->
            {reply, #'RpbPutResp'{}, State};
        {error, index_not_created_within_timeout} ->
            Msg = io_lib:format("Index ~s not created on all the nodes"
                                " within ~p ms timeout~n",
                                [IndexName, Timeout]),
            {error, Msg, State};
        {error, schema_not_found} ->
            {error, "Schema not found", State};
        {error, invalid_name} ->
            {error, "Invalid character in index name", State};
        {error, core_error_on_index_creation, Error} ->
            {error, {format, "Error creating index '~s': ~p",
                     [IndexName, Error]}, State};
        {error, Error} ->
            {error, {format, "Error creating index '~s': ~p",
                     [IndexName, Error]}, State}
    end;

process('RpbYokozunaIndexGetReq', State) ->
    Indexes = yz_index:get_indexes_from_meta(),
    Details = [index_details(IndexName)
        || IndexName <- Indexes, yz_index:exists(IndexName)],
    {reply, #'RpbYokozunaIndexGetResp'{index=Details}, State};

process(#'RpbYokozunaIndexGetReq'{name = IndexName}, State) ->
    case yz_index:exists(IndexName) of
        true ->
            Details = [index_details(IndexName)],
            {reply, #'RpbYokozunaIndexGetResp'{index=Details}, State};
         _ ->
            {error, "notfound", State}
    end.

%% @doc process_stream/3 callback. Ignored.
process_stream(_,_,State) ->
    {ignore, State}.

%% ---------------------------------
%% Internal functions
%% ---------------------------------

-spec maybe_create_index(binary(), schema_name(), n(), timeout()) ->
                                yz_index:create_index_return().
maybe_create_index(IndexName, SchemaName, Nval, Timeout)->
    case yz_index:exists(IndexName) of
        true  ->
            ok;
        false ->
            Schema = case SchemaName of
                <<>> ->      ?YZ_DEFAULT_SCHEMA_NAME;
                undefined -> ?YZ_DEFAULT_SCHEMA_NAME;
                _ ->         SchemaName
            end,
            Nval1 = case Nval of
                <<>> -> undefined;
                _ ->    Nval
            end,
            yz_index:create(IndexName, Schema, Nval1, Timeout)
    end.

-spec index_details(index_name()) -> #'RpbYokozunaIndex'{}.
index_details(IndexName) ->
    Info = yz_index:get_index_info(IndexName),
    #'RpbYokozunaIndex'{
        name = IndexName,
        schema = yz_index:schema_name(Info),
        n_val = yz_index:get_n_val(Info)
    }.
