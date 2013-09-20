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
-compile(export_all).

%% @doc init/0 callback. Returns the service internal start state.
-spec init() -> any().
init() ->
    no_state.

%% @doc decode/2 callback. Decodes an incoming message.
decode(Code, Bin) ->
    {ok, riak_pb_codec:decode(Code, Bin)}.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

%% @doc process/2 callback. Handles an incoming request message.
process(#rpbyokozunaschemaputreq{
            schema = #rpbyokozunaschema{
                name = SchemaName, content = Content}}, State) ->
    case yz_schema:store(SchemaName, Content) of
        ok  ->
            {reply, #rpbputresp{}, State};
        {error, Reason} ->
            Msg = io_lib:format("Error storing schema ~p~n", [Reason]),
            {error, Msg, State}
    end;

process(#rpbyokozunaschemagetreq{name = SchemaName}, State) ->
    {ok, Content} = yz_schema:get(SchemaName),
    Schema = #rpbyokozunaschema{name = SchemaName, content = Content},
    {reply, #rpbyokozunaschemagetresp{schema = Schema}, State};

process(#rpbyokozunaindexdeletereq{name = IndexNameBin}, State) ->
    Ring = yz_misc:get_ring(transformed),
    IndexName = binary_to_list(IndexNameBin),
    case yz_index:exists(IndexName) of
        true  ->
            case yz_index:associated_buckets(IndexName, Ring) of
                [] ->
                    ok = yz_index:remove(IndexName),
                    {reply, rpbdelresp, State};
                Buckets ->
                    Msg = "Can't delete index with associate buckets ~p",
                    Msg2 = lists:flatten(io_lib:fwrite(Msg, [Buckets])),
                    {error, Msg2, State}
            end;
        false ->
            {error, "notfound", State}
    end;

process(#rpbyokozunaindexputreq{
            index = #rpbyokozunaindex{
                name = IndexName, schema = SchemaName}}, State) ->
    case maybe_create_index(IndexName, SchemaName) of
        ok ->
            {reply, #rpbputresp{}, State};
        {error, {rpc_fail, Claimant, _}} ->
            Msg = "Cannot create index while claimant node " ++ atom_to_list(Claimant) ++" is down~n",
            {error, Msg, State};
        {error, schema_not_found} ->
            {error, "Schema not found", State}
    end;

process(rpbyokozunaindexgetreq, State) ->
    Ring = yz_misc:get_ring(transformed),
    Indexes = yz_index:get_indexes_from_ring(Ring),
    Details = [index_details(Ring, IndexName)
      || IndexName <- orddict:fetch_keys(Indexes)],
    {reply, #rpbyokozunaindexgetresp{index=Details}, State};

process(#rpbyokozunaindexgetreq{name = IndexNameBin}, State) ->
    Ring = yz_misc:get_ring(transformed),
    IndexName = binary_to_list(IndexNameBin),
    case yz_index:exists(IndexName) of
        true ->
            Details = [index_details(Ring, IndexName)],
            {reply, #rpbyokozunaindexgetresp{index=Details}, State};
         _ ->
            {error, "notfound", State}
    end.

%% @doc process_stream/3 callback. Ignored.
process_stream(_,_,State) ->
    {ignore, State}.

%% ---------------------------------
%% Internal functions
%% ---------------------------------

-spec maybe_create_index(binary(), schema_name()) -> ok |
                                                     {error, schema_not_found} |
                                                     {error, {rpc_fail, node(), term()}}.
maybe_create_index(IndexName, _SchemaName = <<>>)->
    maybe_create_index(IndexName, ?YZ_DEFAULT_SCHEMA_NAME);
maybe_create_index(IndexName, _SchemaName = undefined)->
    maybe_create_index(IndexName, ?YZ_DEFAULT_SCHEMA_NAME);
maybe_create_index(IndexName, SchemaName)->
    case yz_index:exists(binary_to_list(IndexName)) of
        true  -> ok;
        false ->
            yz_index:create(binary_to_list(IndexName), SchemaName)
    end.

index_details(Ring, IndexName) ->
    Info = yz_index:get_info_from_ring(Ring, IndexName),
    #rpbyokozunaindex{
        name = list_to_binary(IndexName),
        schema = yz_index:schema_name(Info)
    }.
