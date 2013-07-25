%% -------------------------------------------------------------------
%%
%% yz_pb_search: PB Service for Yokozuna queries
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
process(#rpbyokozunaindexdeletereq{name = IndexNameBin}, State) ->
    Ring = yz_misc:get_ring(transformed),
    IndexName = binary_to_list(IndexNameBin),
    case yz_index:exists(IndexName) of
        true  ->
            case associated_buckets(IndexName, Ring) of
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
            Msg = "Cannot create index while claimant node "++Claimant++" is down~n",
            {error, Msg, State};
        {error, schema_not_found} ->
            {error, "Schema not found", State}
    end;

process(#rpbyokozunaindexgetreq{name = IndexName}, State) ->
    Ring = yz_misc:get_ring(transformed),
    lager:warning("GET REQ IDX ~p~n", [IndexName]),
    case IndexName of
        undefined ->
            Indexes = yz_index:get_indexes_from_ring(Ring),
            Details = [index_details(Ring, IndexName)
              || IndexName <- orddict:fetch_keys(Indexes)];
        _ ->
            IndexNameList = binary_to_list(IndexName),
            case yz_index:exists(IndexNameList) of
                true ->
                    Details = [index_details(Ring, IndexNameList)];
                 _ ->
                    Details = []
             end
    end,
    case Details of
        [] ->
            {error, "notfound", State};
        _ ->
            {reply, #rpbyokozunaindexgetresp{index=Details}, State}
    end.

%% @doc process_stream/3 callback. Ignored.
process_stream(_,_,State) ->
    {ignore, State}.

%% ---------------------------------
%% Internal functions
%% ---------------------------------

-spec associated_buckets(index_name(), ring()) -> [bucket()].
associated_buckets(IndexName, Ring) ->
    AllBucketProps = riak_core_bucket:get_buckets(Ring),
    Indexes = lists:map(fun yz_kv:which_index/1, AllBucketProps),
    lists:filter(fun(I) -> I == IndexName end, Indexes).

-spec maybe_create_index(index_name(), schema_name() | undefined) -> ok |
                                                         {error, schema_not_found} |
                                                         {error, {rpc_fail, node(), term()}}.
maybe_create_index(IndexName, SchemaName=undefined)->
    maybe_create_index(IndexName, ?YZ_DEFAULT_SCHEMA_NAME);
maybe_create_index(IndexName, SchemaName)->
    lager:warning("MAYBE INDEX: ~p ~p~n", [IndexName, SchemaName]),
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
