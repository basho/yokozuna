%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
%%-------------------------------------------------------------------

%% @doc Test that checks through various entropy_data endpoint calls
%%      and entropy_data iteration handling
%% @end

-module(yz_entropy_data).

-compile(export_all).
-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(RING_SIZE, 32).
-define(CFG,
        [
         {riak_core,
          [
           {ring_creation_size, ?RING_SIZE}
          ]},
         {riak_kv,
          [
           {anti_entropy_tick, 1000},
           %% allow AAE to build trees and exchange rapidly
           {anti_entropy_build_limit, {100, 1000}},
           {anti_entropy_concurrency, 8}
          ]},
         {yokozuna,
          [
           {enabled, true},
           {solr_request_timeout, 60000}
          ]}
        ]).
-define(NO_HEADERS, []).
-define(NO_BODY, <<>>).
-define(INDEX, <<"test_search_entropy">>).
-define(TYPE, <<"data_entropy">>).
-define(BUCKET, {?TYPE, <<"test_search_entropy">>}).
-define(TOTAL, 1000).

confirm() ->
    [Node1|_] = Cluster = rt:build_cluster(4, ?CFG),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    ok = yz_rt:create_bucket_type(Cluster, ?TYPE),
    ok = yz_rt:create_index_http(Cluster, ?INDEX),
    yz_rt:set_index(Node1, ?BUCKET, ?INDEX),

    %% write_objs writes a seq of 1000 objects
    yz_rt:write_objs(Cluster, ?BUCKET),

    yz_rt:commit(Cluster, ?INDEX),

    ?assertEqual(ok, yz_rt:search_expect(Node1, ?INDEX, "*", "*", ?TOTAL)),

    EDParams = [{wt, json}],

    PartitionList = rpc:call(Node1, yokozuna, partition_list, [?INDEX]),

    EntropyURL = yz_rt:entropy_data_url({rt:select_random(Cluster),
                                         yz_rt:node_solr_port(Node1)},
                                        ?INDEX, EDParams),

    test_entropy_get_missing_partition_param(EntropyURL),
    test_entropy_get(Node1, ?INDEX, PartitionList, EDParams),

    test_ed_timeout_error(Cluster, ?INDEX, rt:select_random(PartitionList),
                          ?CFG),

    pass.

-spec test_entropy_get_missing_partition_param(string()) -> ok.
test_entropy_get_missing_partition_param(URL) ->
    lager:info("Test missing `partition` parameter on entropy url"),
    {ok, Status, _, _} = yz_rt:http(get, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual(Status, "500").

test_entropy_get(Node, Index, PartitionList, EDParams) ->
    lager:info("Test checking through documents on each solr `partition` in the partition list"),
    EntropyURLs = [yz_rt:entropy_data_url(
                     {Node, yz_rt:node_solr_port(Node)},
                     Index,
                     [{partition, P}|EDParams])
                  || P <- PartitionList],
    Results =
        [begin
             {ok, "200", _, R} = yz_rt:http(get, URL, ?NO_HEADERS, ?NO_BODY),
             yz_rt:get_count(R)
         end || URL <- EntropyURLs],

    [?assert(Count > 0) || Count <- Results],
    ok.

-spec test_ed_timeout_error([node()], index_name(), p(), proplist()) -> ok.
test_ed_timeout_error(Cluster, Index, Partition, _Config) ->
    lager:info("wait for full exchange around before making entropy call"),
    TS1 = erlang:now(),
    yz_rt:wait_for_full_exchange_round(Cluster, TS1),

    Node = rt:select_random(Cluster),

    %% load and install the intercept
    rt_intercept:load_code(Node, [filename:join([rt_config:get(yz_dir),
        "riak_test", "intercepts", "*.erl"])]),
    rt_intercept:add(Node, {yz_solr, [{{entropy_data, 3},
                                      entropy_data_cant_complete}]}),

    Filter = [{partition, Partition}],
    Fun = fun({_BKey, _Hash}) ->
                  fake_fun
          end,
    ?assertEqual(error,
                 rpc:call(Node, yz_entropy, iterate_entropy_data,
                          [Index, Filter, Fun])).
