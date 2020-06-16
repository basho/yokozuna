%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.
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
-module(yz_handoff).
-compile(nowarn_export_all).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(GET(K,L), proplists:get_value(K, L)).
-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).
-define(INDEX, <<"test_idx">>).
-define(BUCKET, <<"test_bkt">>).
-define(NUMRUNSTATES, 1).
-define(SEQMAX, 1000).
-define(TESTCYCLE, 20).
-define(N, 3).
-define(CFG,
        [
         {riak_core,
          [
           {ring_creation_size, 16},
           {n_val, ?N},
           {handoff_concurrency, 10},
           {vnode_management_timer, 1000}
          ]},
         {riak_kv,
          [
           %% allow AAE to build trees and exchange rapidly
           {anti_entropy_build_limit, {100, 1000}},
           {anti_entropy_concurrency, 8},
           {handoff_rejected_max, infinity}
          ]},
         {yokozuna,
          [
           {anti_entropy_tick, 1000},
           {enabled, true}
          ]}
        ]).

-record(trial_state, {
          solr_url_before,
          solr_url_after,
          leave_node,
          join_node,
          admin_node}).

confirm() ->
    %% Setup cluster initially
    [Node1, Node2, _Node3, _Node4, _Node5] = Nodes = rt:build_cluster(5, ?CFG),

    rt:wait_for_cluster_service(Nodes, yokozuna),

    ConnInfo = ?GET(Node2, rt:connection_info([Node2])),
    {Host, Port} = ?GET(http, ConnInfo),
    Shards = [{N, node_solr_port(N)} || N <- Nodes],

    %% Generate keys, YZ only supports UTF-8 compatible keys
    Keys = [<<N:64/integer>> || N <- lists:seq(1, ?SEQMAX),
                               not lists:any(fun(E) -> E > 127 end,
                                             binary_to_list(<<N:64/integer>>))],
    KeyCount = length(Keys),

    Pid = rt:pbc(Node2),
    yz_rt:pb_write_data(Nodes, Pid, ?INDEX, ?BUCKET, Keys),

    %% Separate out shards for multiple runs
    [Shard1|Shards2Rest] = Shards,
    {_, SolrPort1} = Shard1,
    [{_, SolrPort2}|_] = Shards2Rest,
    SolrURL = internal_solr_url(Host, SolrPort1, ?INDEX, Shards),
    BucketURL = bucket_keys_url(Host, Port, ?BUCKET),
    SearchURL = search_url(Host, Port, ?INDEX),

    lager:info("Verify Replicas Count = (3 * docs/keys) count"),
    verify_count(SolrURL, (KeyCount * ?N)),

    States = [#trial_state{solr_url_before = SolrURL,
                           solr_url_after = internal_solr_url(Host, SolrPort2, ?INDEX, Shards2Rest),
                           leave_node = Node1},
              #trial_state{solr_url_before = internal_solr_url(Host, SolrPort2, ?INDEX, Shards2Rest),
                           solr_url_after = SolrURL,
                           join_node = Node1,
                           admin_node = Node2}],

    %% Run set of leave/join trials and count/test #'s from the cluster
    [[begin
          check_data(Nodes, KeyCount, BucketURL, SearchURL, State),
          check_counts(Pid, KeyCount, BucketURL)
      end || State <- States]
     || _ <- lists:seq(1,?NUMRUNSTATES)],

    pass.

%%%===================================================================
%%% Private
%%%===================================================================

node_solr_port(Node) ->
    {ok, P} = riak_core_util:safe_rpc(Node, application, get_env,
                                      [yokozuna, solr_port]),
    P.

internal_solr_url(Host, Port, Index) ->
    ?FMT("http://~s:~B/internal_solr/~s", [Host, Port, Index]).
internal_solr_url(Host, Port, Index, Shards) ->
    Ss = [internal_solr_url(Host, ShardPort, Index)
          || {_, ShardPort} <- Shards],
    ?FMT("http://~s:~B/internal_solr/~s/select?wt=json&q=*:*&shards=~s",
         [Host, Port, Index, string:join(Ss, ",")]).

%% @private
bucket_keys_url(Host, Port, BName) ->
    ?FMT("http://~s:~B/buckets/~s/keys?keys=true", [Host, Port, BName]).

%% @private
search_url(Host, Port, Index) ->
    ?FMT("http://~s:~B/solr/~s/select?wt=json&q=*:*", [Host, Port, Index]).

verify_count(Url, ExpectedCount) ->
    AreUp =
        fun() ->
                {ok, "200", _, DBody} = yz_rt:http(get, Url, [], [], "", 60000),
                FoundCount = get_count(DBody),
                lager:info("FoundCount: ~b, ExpectedCount: ~b",
                           [FoundCount, ExpectedCount]),
                ExpectedCount =:= FoundCount
        end,
    ?assertEqual(ok, rt:wait_until(AreUp)),
    ok.

get_count(Resp) ->
    Struct = mochijson2:decode(Resp),
    kvc:path([<<"response">>, <<"numFound">>], Struct).

get_keys_count(BucketURL) ->
    {ok, "200", _, RBody} = yz_rt:http(get, BucketURL, [], []),
    Struct = mochijson2:decode(RBody),
    length(kvc:path([<<"keys">>], Struct)).

check_counts(Pid, InitKeyCount, BucketURL) ->
    PBCounts   = [begin {ok, Resp} = riakc_pb_socket:search(
                                       Pid, ?INDEX, <<"*:*">>),
                        Resp#search_results.num_found
                  end || _ <- lists:seq(1,?TESTCYCLE)],
    HTTPCounts = [begin {ok, "200", _, RBody} = yz_rt:http(
                                                  get, BucketURL, [], []),
                        Struct = mochijson2:decode(RBody),
                        length(kvc:path([<<"keys">>], Struct))
                  end || _ <- lists:seq(1,?TESTCYCLE)],
    MinPBCount = lists:min(PBCounts),
    MinHTTPCount = lists:min(HTTPCounts),
    lager:info("Before-Node-Leave PB: ~b, After-Node-Leave PB: ~b",
               [InitKeyCount, MinPBCount]),
    ?assertEqual(InitKeyCount, MinPBCount),
    lager:info("Before-Node-Leave PB: ~b, After-Node-Leave HTTP: ~b",
               [InitKeyCount, MinHTTPCount]),
    ?assertEqual(InitKeyCount, MinHTTPCount).

check_data(Cluster, KeyCount, BucketURL, SearchURL, S) ->
    CheckCount        = KeyCount * ?N,
    KeysBefore        = get_keys_count(BucketURL),

    UpdatedCluster = leave_or_join(Cluster, S),

    yz_rt:wait_for_aae(UpdatedCluster),

    KeysAfter = get_keys_count(BucketURL),
    lager:info("KeysBefore: ~b, KeysAfter: ~b", [KeysBefore, KeysAfter]),
    ?assertEqual(KeysBefore, KeysAfter),

    lager:info("Verify Search Docs Count =:= key count"),
    lager:info("Run Search URL: ~s", [SearchURL]),
    verify_count(SearchURL, KeysAfter),
    lager:info("Verify Replicas Count = (3 * docs/keys) count"),
    lager:info("Run Search URL: ~s", [S#trial_state.solr_url_after]),
    verify_count(S#trial_state.solr_url_after, CheckCount).

leave_or_join(Cluster, S=#trial_state{join_node=undefined}) ->
    Node = S#trial_state.leave_node,
    rt:leave(Node),
    ?assertEqual(ok, rt:wait_until_unpingable(Node)),
    Cluster -- [Node];
leave_or_join(Cluster, S=#trial_state{leave_node=undefined}) ->
    Node = S#trial_state.join_node,
    NodeAdmin = S#trial_state.admin_node,
    ok = rt:start_and_wait(Node),
    ok = rt:join(Node, NodeAdmin),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Cluster)),
    ?assertEqual(ok, rt:wait_until_no_pending_changes(Cluster)),
    Cluster ++ [Node].
