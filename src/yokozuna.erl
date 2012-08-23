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

-module(yokozuna).
-include("yokozuna.hrl").
-compile(export_all).


%%%===================================================================
%%% API
%%%===================================================================

%% @doc Index the given object `O'.
-spec index(string(), riak_object:riak_object()) -> ok | {error, term()}.
index(Index, O) ->
    yz_solr:index(Index, [yz_doc:make_doc(O, <<"FPN">>, <<"Partition">>)]).

%% @doc Return the set of unique partitions stored on this node for
%%      the given `Index'.
-spec partition_list(string()) -> ordsets:ordset().
partition_list(Index) ->
    Resp = yz_solr:partition_list(Index),
    Struct = mochijson2:decode(Resp),
    Path = [<<"facet_counts">>, <<"facet_fields">>, <<"_pn">>],
    Facets = yz_solr:get_path(Struct, Path),
    %% Facets is a list of field values followed by their
    %% corresponding count.  The `is_binary' filter is done to remove
    %% the counts and leave only the partitions.
    Partitions = lists:filter(fun erlang:is_binary/1, Facets),
    ordsets:from_list([?BIN_TO_INT(P) || P <- Partitions]).

search(Index, Query, Mapping) ->
    yz_solr:search(Index, [{q, Query}], Mapping).

solr_port(Node, Ports) ->
    proplists:get_value(Node, Ports).

%%%===================================================================
%%% Private
%%%===================================================================

test_it(Index) ->
    B = <<"fruit">>,
    O1 = riak_object:new(B, <<"apples">>, <<"2">>),
    O2 = riak_object:new(B, <<"oranges">>, <<"1">>),
    O3 = riak_object:new(B, <<"strawberries">>, <<"6">>),
    O4 = riak_object:new(B, <<"lemons">>, <<"1">>),
    O5 = riak_object:new(B, <<"celery">>, <<"4">>),
    O6 = riak_object:new(B, <<"lime">>, <<"1">>),
    [index(Index, O) || O <- [O1, O2, O3, O4, O5, O6]],
    yz_solr:commit(Index).

demo_write_objs(Index) ->
    ibrowse:start(),
    write_n_objs(Index, 1000),
    yz_solr:commit(Index).

demo_build_tree(Index, Name) ->
    ibrowse:start(),
    TP = yz_entropy:new_tree_proc(Index, Name),
    Pid = element(3, TP),
    Ref = make_ref(),
    Pid ! {get_tree, self(), Ref},
    receive {tree, Ref, Tree} -> Tree end,
    %% returning TreeProc too in case want to play with it
    {Tree, TP}.

demo_new_vclock(Index, N) ->
    %% the timestamp will change causing hash to change
    NS = list_to_binary(integer_to_list(N)),
    B = <<"test">>,
    K = <<"key_",NS/binary>>,
    V = <<"val_",NS/binary>>,
    O = riak_object:new(B, K, V),
    O2 = riak_object:increment_vclock(O, dummy_node),
    index(Index, O2),
    yz_solr:commit(Index).

demo_delete(Index, N) ->
    NS = integer_to_list(N),
    K = "key_" ++ NS,
    ok = yz_solr:delete(Index, {id,K}),
    ok = yz_solr:commit(Index).

write_n_objs(_, 0) ->
    ok;
write_n_objs(Index, N) ->
    NS = list_to_binary(integer_to_list(N)),
    B = <<"test">>,
    K = <<"key_",NS/binary>>,
    V = <<"val_",NS/binary>>,
    O = riak_object:new(B, K, V),
    O2 = riak_object:increment_vclock(O, dummy_node),
    index(Index, O2),
    write_n_objs(Index, N-1).
