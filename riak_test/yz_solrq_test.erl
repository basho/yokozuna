%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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

%% @doc Confirm that the batching and queueing behavior in yz_solrq and
%%      related modules works correctly.
-module(yz_solrq_test).
-export([confirm/0]).
-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").

%% TODO: @see cleanup/2 && cleanup/3 to remove need for creating so many indexes.
-define(INDEX1, <<"solrq_index1">>).
-define(INDEX2, <<"solrq_index2">>).
-define(INDEX3, <<"solrq_index3">>).
-define(INDEX4, <<"solrq_index4">>).
-define(INDEX5, <<"solrq_index5">>).
-define(INDEX6, <<"solrq_index6">>).
-define(INDEX7, <<"solrq_index7">>).
-define(INDEX8, <<"solrq_index8">>).
-define(INDEX9, <<"solrq_index9">>).
-define(INDEX10, <<"solrq_index10">>).
-define(INDEX11, <<"solrq_index11">>).
-define(INDEX12, <<"solrq_index12">>).
-define(BUCKET1, {<<"solrq1">>, <<"solrq_bucket1">>}).
-define(BUCKET2, {<<"solrq2">>, <<"solrq_bucket2">>}).
-define(BUCKET3, {<<"solrq3">>, <<"solrq_bucket3">>}).
-define(BUCKET4, {<<"solrq4">>, <<"solrq_bucket4">>}).
-define(BUCKET5, {<<"solrq5">>, <<"solrq_bucket5">>}).
-define(BUCKET6, {<<"solrq6">>, <<"solrq_bucket6">>}).
-define(BUCKET7, {<<"solrq7">>, <<"solrq_bucket8">>}).
-define(BUCKET8, {<<"solrq8">>, <<"solrq_bucket8">>}).
-define(BUCKET9, {<<"solrq9">>, <<"solrq_bucket9">>}).
-define(BUCKET10, {<<"solrq10">>, <<"solrq_bucket10">>}).
-define(BUCKET11, {<<"solrq11">>, <<"solrq_bucket11">>}).
-define(BUCKET12, {<<"solrq12">>, <<"solrq_bucket12">>}).

-define(BUCKETS, [?BUCKET1, ?BUCKET2, ?BUCKET3, ?BUCKET4, ?BUCKET5,
                  ?BUCKET6, ?BUCKET7, ?BUCKET8, ?BUCKET9, ?BUCKET10,
                  ?BUCKET11, ?BUCKET12]).
-define(INDEXES, [?INDEX1, ?INDEX2, ?INDEX3, ?INDEX4, ?INDEX5,
                  ?INDEX6, ?INDEX7, ?INDEX8, ?INDEX9, ?INDEX10,
                  ?INDEX11, ?INDEX12]).

-define(SOLRQ_DELAYMS_MAX, 3000).
-define(SOLRQ_BATCH_MIN_SETTING, 4).
-define(SOLRQ_BATCH_MAX_SETTING, 8).
-define(MELT_RESET_REFRESH, 1000).
-define(SOLRQ_HWM_SETTING, 20).
-define(RING_SIZE, 8).
-define(CONFIG,
        [{yokozuna,
          [{enabled, true},
           {?SOLRQ_BATCH_FLUSH_INTERVAL, ?SOLRQ_DELAYMS_MAX},
           {?SOLRQ_BATCH_MIN, ?SOLRQ_BATCH_MIN_SETTING},
           {?SOLRQ_BATCH_MAX, ?SOLRQ_BATCH_MAX_SETTING},
           {?SOLRQ_HWM, 1000},
           {?ERR_THRESH_FAIL_COUNT, 1},
           {?ERR_THRESH_RESET_INTERVAL, ?MELT_RESET_REFRESH},
           {?SOLRQ_DRAIN_ENABLE, true},
           {anti_entropy, {off, []}}
          ]},
            {riak_core, [{ring_creation_size, ?RING_SIZE}]}]).

-compile(export_all).

confirm() ->
    Cluster = yz_rt:prepare_cluster(1, ?CONFIG),
    [PBConn|_] = PBConns = yz_rt:open_pb_conns(Cluster),

    ok = yz_rt:create_indexed_bucket(PBConn, Cluster, ?BUCKET1, ?INDEX1),
    confirm_batch_size(Cluster, PBConn, ?BUCKET1, ?INDEX1),
    [confirm_hwm(Cluster, PBConn, ?BUCKET1, ?INDEX1, HWM) || HWM <- lists:seq(0, 10)],

    ok = yz_rt:create_indexed_bucket(PBConn, Cluster, ?BUCKET2, ?INDEX2),
    confirm_draining(Cluster, PBConn, ?BUCKET2, ?INDEX2),

    confirm_drain_fsm_failure(Cluster),
    confirm_drain_fsm_timeout(Cluster),
    confirm_drain_fsm_kill(Cluster),

    %% Buckets and Indexes 3 - 12.
    [ok = yz_rt:create_indexed_bucket(PBConn, Cluster, Bucket, Index)
     || {Bucket, Index} <- lists:zip(lists:nthtail(2, ?BUCKETS),
                                   lists:nthtail(2, ?INDEXES))],

    %% confirm_requeue_undelivered must be last since it installs an interrupt
    %% that intentionally causes failures
    confirm_requeue_undelivered(Cluster, PBConn, ?BUCKET3, ?INDEX3),

    confirm_no_contenttype_data(Cluster, PBConn, ?BUCKET4, ?INDEX4),
    confirm_purge_strategy(Cluster, PBConn),

    yz_rt:close_pb_conns(PBConns),
    pass.

confirm_drain_fsm_failure(Cluster) ->
    yz_stat:reset(),
    try
        yz_rt:load_intercept_code(Cluster),
        yz_rt:add_intercept(Cluster, yz_solrq_drain_fsm, prepare, 2, prepare_crash),
        %% drain solrqs and wait until the drain failure stats are touched
        yz_rt:drain_solrqs(Cluster),
        yz_rt:wait_until(Cluster, fun check_drain_failure_stats/1),

        lager:info("confirm_drain_fsm_failure ok")
    after
        yz_rt:add_intercept(Cluster, yz_solrq_drain_fsm, prepare, 2, prepare_orig)
    end.

check_drain_failure_stats(Node) ->
    Stats = rpc:call(Node, yz_stat, get_stats, []),

    QDrainFail = proplists:get_value(yz_stat:stat_name([queue, drain, fail]), Stats),
    QDrainFailCount = proplists:get_value(count, QDrainFail),
    QDrainFailOne = proplists:get_value(one, QDrainFail),

    Pairs = [
        {queue_drain_fail_count, QDrainFailCount, '>', 0},
        {queue_drain_fail_one, QDrainFailOne, '>', 0}
    ],
    yz_rt:check_stat_values(Stats, Pairs).

confirm_drain_fsm_timeout(Cluster) ->
    yz_stat:reset(),
    [rpc:call(
        Node, application, set_env, [?YZ_APP_NAME, ?SOLRQ_DRAIN_TIMEOUT, 500])
            || Node <- Cluster],
    try
        yz_rt:load_intercept_code(Cluster),
        yz_rt:add_intercept(Cluster, yz_solrq_drain_fsm, prepare, 2, prepare_sleep_1s),
        yz_rt:drain_solrqs(Cluster),
        yz_rt:wait_until(Cluster, fun check_drain_timeout_stats/1),

        lager:info("confirm_drain_fsm_timeout ok")
    after
        yz_rt:add_intercept(Cluster, yz_solrq_drain_fsm, prepare, 2, prepare_orig),
        [rpc:call(
            Node, application, set_env, [?YZ_APP_NAME, ?SOLRQ_DRAIN_TIMEOUT, 60000])
                || Node <- Cluster]
    end.

check_drain_timeout_stats(Node) ->
    Stats = rpc:call(Node, yz_stat, get_stats, []),

    QDrainTimeout = proplists:get_value(yz_stat:stat_name([queue, drain, timeout]), Stats),
    QDrainTimeoutCount = proplists:get_value(count, QDrainTimeout),
    QDrainTimeoutOne = proplists:get_value(one, QDrainTimeout),

    Pairs = [
        {queue_drain_timeout_count, QDrainTimeoutCount, '>', 0},
        {queue_drain_timeout_one, QDrainTimeoutOne, '>', 0}
    ],
    yz_rt:check_stat_values(Stats, Pairs).

confirm_drain_fsm_kill(Cluster) ->
    [rpc:call(
        Node, application, set_env, [?YZ_APP_NAME, ?SOLRQ_DRAIN_TIMEOUT, 10])
        || Node <- Cluster],
    [rpc:call(
        Node, application, set_env, [?YZ_APP_NAME, ?SOLRQ_DRAIN_CANCEL_TIMEOUT, 10])
        || Node <- Cluster],
    try
        yz_test_listener:start(),
        yz_rt:load_intercept_code(Cluster),
        yz_rt:add_intercept(Cluster, yz_solrq_drain_fsm, prepare, 2, prepare_sleep_5s),
        yz_rt:add_intercept(Cluster, yz_solrq_drain_mgr, unlink_and_kill, 2, count_unlink_and_kill),
        yz_rt:drain_solrqs(Cluster),
        yz_rt:wait_until(Cluster, fun check_drain_cancel_timeout_stats/1),

        ?assertEqual(1, length(yz_test_listener:messages())),

        lager:info("confirm_drain_fsm_kill ok")
    after
        yz_rt:add_intercept(Cluster, yz_solrq_drain_fsm, prepare, 2, prepare_orig),
        yz_rt:add_intercept(Cluster, yz_solrq_drain_mgr, unlink_and_kill, 2, unlink_and_kill_orig),
        yz_test_listener:stop(),
        [rpc:call(
            Node, application, set_env, [?YZ_APP_NAME, ?SOLRQ_DRAIN_TIMEOUT, 60000])
            || Node <- Cluster],
        [rpc:call(
            Node, application, set_env, [?YZ_APP_NAME, ?SOLRQ_DRAIN_CANCEL_TIMEOUT, 5000])
            || Node <- Cluster]
    end.

check_drain_cancel_timeout_stats(Node) ->
    Stats = rpc:call(Node, yz_stat, get_stats, []),

    QDrainTimeout = proplists:get_value(yz_stat:stat_name([queue, drain, cancel, timeout]), Stats),
    QDrainTimeoutCount = proplists:get_value(count, QDrainTimeout),
    QDrainTimeoutOne = proplists:get_value(one, QDrainTimeout),

    Pairs = [
        {queue_drain_cancel_timeout_count, QDrainTimeoutCount, '>', 0},
        {queue_drain_cancel_timeout_one, QDrainTimeoutOne, '>', 0}
    ],
    yz_rt:check_stat_values(Stats, Pairs).


confirm_batch_size(Cluster, PBConn, BKey, Index) ->
    %% First, put one less than the min batch size and expect that there are no
    %% search results (because the index operations are queued).
    Count = ?SOLRQ_BATCH_MIN_SETTING - 1,
    Count = put_objects(PBConn, BKey, Count),
    yz_rt:commit(Cluster, Index),
    verify_search_count(PBConn, Index, 0),

    %% Now, put one more and expect that all of them have been indexed (because
    %% the solrq_batch_min has been reached) and therefore show up in search
    %% results.
    1 = put_objects(PBConn, BKey, 1),
    yz_rt:commit(Cluster, Index),
    verify_search_count(PBConn, Index, ?SOLRQ_BATCH_MIN_SETTING),

    %% Finally, put another batch of one less than solrq_batch_min, but this
    %% time wait until the data has been flushed, and then verify that we
    %% have waited at least SOLRQ_DELAYMS_MAX ms.
    Count = put_objects(PBConn, BKey, Count),
    yz_rt:commit(Cluster, Index),
    verify_search_count(PBConn, Index, ?SOLRQ_BATCH_MIN_SETTING),
    StartMS = current_ms(),
    wait_until_search_count(PBConn, Index, ?SOLRQ_BATCH_MIN_SETTING + Count),
    Condition = gteq(current_ms() - StartMS, ?SOLRQ_DELAYMS_MAX),
    case Condition of
        false -> lager:error("Flush interval is less than ~pms", [?SOLRQ_DELAYMS_MAX]);
        true -> ok
    end,
    ?assertEqual(Condition, true),
    lager:info("confirm_batch_size ok"),
    ok.

confirm_hwm(Cluster, PBConn, Bucket, Index, HWM) ->
    yz_rt:drain_solrqs(Cluster),
    {OldMin, OldMax, OldDelay} = set_index(Cluster, Index, 1, 100, 100),
    try
        yz_rt:load_intercept_code(Cluster),
        yz_rt:intercept_index_batch(Cluster, index_batch_throw_exception),
        yz_rt:set_hwm(Cluster, HWM),
        yz_rt:set_purge_strategy(Cluster, ?PURGE_NONE),
        ?assertEqual(HWM, put_objects(PBConn, Bucket, HWM + 1))
    after
        yz_rt:intercept_index_batch(Cluster, index_batch_call_orig),
        yz_rt:set_purge_strategy(Cluster, ?PURGE_ONE),
        yz_rt:set_hwm(Cluster, 10000),
        yz_rt:set_index(Cluster, Index, OldMin, OldMax, OldDelay)
    end,
    lager:info("confirm_hwm ok for HWM ~p", [HWM]),
    ok.

-spec gteq(number(), number()) -> boolean().
gteq(A, B) -> A >= B.

confirm_draining(Cluster, PBConn, BKey, Index) ->
    Count = ?SOLRQ_BATCH_MIN_SETTING - 1,
    Count = put_objects(PBConn, BKey, Count),
    yz_rt:commit(Cluster, Index),
    verify_search_count(PBConn, Index, 0),
    yz_rt:drain_solrqs(Cluster),
    yz_rt:commit(Cluster, Index),
    verify_search_count(PBConn, Index, Count),
    lager:info("confirm_draining ok"),
    ok.

confirm_requeue_undelivered([Node|_] = Cluster, PBConn, BKey, Index) ->
    yz_rt:load_intercept_code(Node),
    yz_rt:intercept_index_batch(Node, index_batch_throw_exception),

    Count = ?SOLRQ_BATCH_MIN_SETTING,
    Count = put_objects(PBConn, BKey, Count),
    yz_rt:commit(Cluster, Index),

    %% Because the index_batch_throw_exception intercept simulates a Solr
    %% failure, none of the objects should have been indexed at this point.
    verify_search_count(PBConn, Index, 0),

    %% Now, if we replace the intercept with one that just calls the original
    %% function, the undelivered objects will be requeued and should succeed
    %% (assuming that the fuse has been blown and reset).
    yz_rt:intercept_index_batch(Node, index_batch_call_orig),
    timer:sleep(?MELT_RESET_REFRESH + 1000), %% wait for fuse reset
    yz_rt:drain_solrqs(Node),
    verify_search_count(PBConn, Index, Count),
    lager:info("confirm_requeue_undelivered ok"),
    ok.

confirm_no_contenttype_data(Cluster, PBConn, BKey, Index) ->
    yz_rt:set_index(Cluster, Index, 1, 100, 100),
    Count = 1,
    Count = put_no_contenttype_objects(PBConn, BKey, Count),
    yz_rt:commit(Cluster, Index),
    verify_search_count(PBConn, Index, 1),
    lager:info("confirm_no_contenttype_data ok"),
    ok.

confirm_purge_strategy(Cluster, PBConn) ->
    confirm_purge_one_strategy(Cluster, PBConn,
                               {?BUCKET5, ?INDEX5}),
    confirm_purge_idx_strategy(Cluster, PBConn,
                               {?BUCKET7, ?INDEX7}),
    confirm_purge_none_strategy(Cluster, PBConn,
                                {?BUCKET11, ?INDEX11}),
    ok.

confirm_purge_one_strategy(Cluster, PBConn, Bucket1Index1) ->
    PurgeResults = do_purge(Cluster, PBConn, Bucket1Index1,
                            ?PURGE_ONE),
    check_one_purged(PurgeResults),
    lager:info("confirm_purge_one_strategy ok"),
    ok.

check_one_purged({Index1Written, Index1SearchResults} = TestResults) ->
    Condition = first_purged(Index1Written, Index1SearchResults),
    case Condition of
        false ->
            lager:error("check_one_purged error: ~p", [TestResults]);
        _ -> ok
    end,
    ?assertEqual(Condition, true),
    ok.

confirm_purge_idx_strategy(Cluster, PBConn, Bucket1Index1) ->
    PurgeResults = do_purge(Cluster, PBConn, Bucket1Index1,
                            ?PURGE_IDX),
    check_idx_purged(PurgeResults),
    lager:info("confirm_purge_idx_strategy ok"),
    ok.

check_idx_purged({[_K1, _K2, _K3, _K4, K5] = _Index1Written, Index1SearchResults} = TestResults) ->
    %%
    %% Note the second condition, because we wrote to Index1
    %% but that was the purge trigger, so the last entry will
    %% NOT have been purged, of that indexq was the one chosen.
    %% Otherwise, it was the second indexq, and nothing should
    %% have been pending for that indexq, so they all get deleted.
    %%
    Condition = equal(Index1SearchResults, [K5]),
    case Condition of
        false ->
            lager:error("check_idx_purged error: ~p", [TestResults]);
        _ -> ok
    end,
    ?assertEqual(Condition, true),
    ok.

confirm_purge_none_strategy(Cluster, PBConn, Bucket1Index1) ->
    PurgeResults = do_purge(Cluster, PBConn, Bucket1Index1,
                            ?PURGE_NONE),
    check_none_purged(PurgeResults),
    lager:info("confirm_purge_none_strategy ok"),
    ok.

check_none_purged({Index1Written, Index1SearchResults} = TestResults) ->
    Condition =
        equal(Index1Written, Index1SearchResults),
    case Condition of
        false ->
            lager:error("check_none_purged error: ~p", [TestResults]);
        _ -> ok
    end,
    ?assertEqual(Condition, true),
    ok.

equal(Written, Searched) ->
    lists:sort(Written) == lists:sort(Searched).

first_purged([_H|T] = _Written, Searched) ->
    lists:sort(T) == lists:sort(Searched).

%%
%% All the purge tests use this function to load up 2 indexqs
%% in the first solrq (yz_solrq_001 -- we choose it arbitrarily,
%% but we only need one to test the different purge scenarios)
%% up to the HWM.  We do this by setting an intercept so that
%% the call to Solr fails for all of these calls, which will trigger
%% the fuses to melt for both of these indices.
%%
%% Once the fuses for the indexqs are blown, we send one message
%% using the first index.  This will trigger a purge, whose behavior
%% is determined by the supplied purge strategy.
%%
%% We then restore the original Solr behavior by reverting the intercept,
%% and we drain the queues to push through any data that had been enqueued
%% but not pushed to Solr.  We return the list of keys we wrote to
%% Riak and the search results for each index, which is compared outside
%% of this function.
%%
do_purge([Node|_] = Cluster, PBConn,
         {Bucket1, Index1},
         PurgeStrategy) ->
    yz_rt:set_purge_strategy(Cluster, PurgeStrategy),
    yz_rt:set_index(Cluster, Index1, 1, 100, 99999),
    yz_rt:set_hwm(Cluster, 4),
    TargetPartition = 1096126227998177188652763624537212264741949407232,
    %%
    %% Find a list of representative keys for each Index.
    %% Each representative in the list is a unique key
    %% that hashes to yz_solrq_001.
    %%
    Index1BKeys = find_representative_bkeys(TargetPartition, Index1, Bucket1),
    Index1BKey1 = lists:nth(1, Index1BKeys),
    Index1BKey2 = lists:nth(2, Index1BKeys),
    Index1BKey3 = lists:nth(3, Index1BKeys),
    Index1BKey4 = lists:nth(4, Index1BKeys),
    Index1BKey5 = lists:nth(5, Index1BKeys),
    try
        yz_rt:load_intercept_code(Node),
        yz_rt:intercept_index_batch(Node, index_batch_throw_exception),
        %%
        %% Send two messages through each indexq on the solrq, which
        %% will trip the fuse on both; however
        %% because fuse blown events are handled asynchronously,
        %% we need to wait until the solrqs are blown.
        %%
        [Index1BKey1] = put_bkey_objects(PBConn, [Index1BKey1]),
        [Index1BKey2] = put_bkey_objects(PBConn, [Index1BKey2]),
        [Index1BKey3] = put_bkey_objects(PBConn, [Index1BKey3]),
        [Index1BKey4] = put_bkey_objects(PBConn, [Index1BKey4]),
        yz_rt:wait_until_fuses_blown(Node, TargetPartition, [Index1]),
        %%
        %% At this point, the two indexqs in target solrqs corresponding
        %% to {TargetPartition, Index1} and {TargetPartition, Index2}, respectively, should be blown.
        %% Send one more message through one of the Indexqs, which
        %% will trigger a purge.
        %%
        F = fun() ->
            [Index1BKey5] = put_bkey_objects(PBConn, [Index1BKey5])
        end,
        case PurgeStrategy of
            ?PURGE_NONE ->
                spawn(F);
            _ ->
                F()
        end
    after
        %%
        %% Revert the intercept, and drain, giving time for the
        %% fuse to reset.  Commit to Solr so that we can run a query.
        %%
        yz_rt:intercept_index_batch(Node, index_batch_call_orig),
        yz_rt:wait_until_fuses_reset(Node, TargetPartition, [Index1]),
        yz_rt:drain_solrqs(Node),
        yz_rt:commit(Cluster, Index1)
    end,
    %%
    %% Return the search results for Index1 and Index2.
    %% The first list is the set of bkeys we wrote for each index.
    %% The second list is the set that are available for search.
    %%
    Index1SearchBKeys = search_bkeys(PBConn, Index1),
    {[Index1BKey1, Index1BKey2, Index1BKey3, Index1BKey4, Index1BKey5], Index1SearchBKeys}.

-spec search_bkeys(pid(), index_name()) -> [bkey()].
search_bkeys(PBConn, Index) ->
    {ok, {search_results, SearchResults, _Score, _NumFound}} =
        riakc_pb_socket:search(PBConn, Index, <<"*:*">>),
    lists:map(
        fun({_Index, Fields}) ->
                {{proplists:get_value(<<"_yz_rt">>, Fields),
                  proplists:get_value(<<"_yz_rb">>, Fields)},
                 proplists:get_value(<<"_yz_rk">>, Fields)}
        end,
        SearchResults).

-spec find_representative_bkeys(p(),
                                index_name(),
                                bucket()) -> [bkey()].
find_representative_bkeys(Partition, Index, Bucket) ->
    Solrq = yz_solrq:worker_regname(Index, Partition),
    Representatives = find_representatives(Index, Bucket),
    {ok, BKeys} = dict:find(Solrq, Representatives),
    BKeys.

-spec find_representatives(index_name(), bucket()) -> dict().
find_representatives(Index, Bucket) ->
    BKeys =
        lists:map(
          fun(I) ->
                  {Bucket, erlang:list_to_binary(erlang:integer_to_list(I))}
          end,
          lists:seq(1, 1000)),

    lists:foldl(
      fun({Solrq, BKey}, Accum) ->
              dict:append(Solrq, BKey, Accum)
      end,
      dict:new(),
      [{get_solrq(Index, BKey), BKey} || BKey <- BKeys]).

-spec get_solrq(index_name(), {bucket(), key()}) -> atom().
get_solrq(Index, BucketKey) ->
    Hash = chash:key_of(BucketKey),
    Partition = riak_core_ring_util:partition_id_to_hash(
        riak_core_ring_util:hash_to_partition_id(Hash, ?RING_SIZE), ?RING_SIZE),
    yz_solrq:worker_regname(Index, Partition).

-spec put_no_contenttype_objects(pid(), bucket(), non_neg_integer()) -> non_neg_integer().
put_no_contenttype_objects(PBConn, Bucket, Count) ->
    put_objects(PBConn, Bucket, Count, undefined).

-spec put_objects(pid(), bucket(), non_neg_integer()) -> non_neg_integer().
put_objects(PBConn, Bucket, Count) ->
    put_objects(PBConn, Bucket, Count, "text/plain").

-spec put_objects(pid(), bucket(), non_neg_integer(), string()|undefined) -> non_neg_integer().
put_objects(PBConn, Bucket, Count, ContentType) ->
    %% Using the same key for every object ensures that they all hash to the
    %% same yz_solrq_worker and the batching is therefore predictable.
    Key = <<"same_key_for_everyone">>,
    RandVals = [yz_rt:random_binary(16) || _ <- lists:seq(1, Count)],
    Objects = [case ContentType of
                   undefined ->
                       riakc_obj:new(Bucket, Key, Val);
                   _ ->
                       riakc_obj:new(Bucket, Key, Val, ContentType)
               end || Val <- RandVals],
    lager:info("Writing ~p objects to Riak...", [length(Objects)]),
    Results = [riakc_pb_socket:put(PBConn, Obj, [return_head, {timeout, 1000}]) || Obj <- Objects],
    length(lists:filter(fun({Result, _}) ->
                                ok =:= Result
                        end,
                        Results)).

-spec put_bkey_objects(pid(), [bkey()]) -> [bkey()].
put_bkey_objects(PBConn, BKeys) ->
    BKeyObjects = [{{Bucket, Key},
                    riakc_obj:new(Bucket, Key, Key, "text/plain")}
                   || {Bucket, Key} <- BKeys],
    lager:info("Writing ~p bkeys ~p to Riak...", [length(BKeys), BKeys]),
    Results = [{BKey, riakc_pb_socket:put(PBConn, Obj,
                                          [return_head, {timeout, 1000}])}
               || {BKey, Obj} <- BKeyObjects],
    %%lager:info("Results: ~p", [Results]),
    lists:map(
      fun({BKey, {_Result, _}}) -> BKey end,
      lists:filter(fun({_BKey, {Result, _}}) -> ok =:= Result end, Results)).

verify_search_count(PBConn, Index, Count) ->
    {ok, {search_results, _R, _Score, Found}} =
        riakc_pb_socket:search(PBConn, Index, <<"*:*">>),
    ?assertEqual(Count, Found).

wait_until_search_count(PBConn, Index, Count) ->
    F = fun() ->
        {ok, {search_results, _R, _Score, Found}} =
            riakc_pb_socket:search(PBConn, Index, <<"*:*">>),
        Found =:= Count
        end,
    rt:wait_until(F).

current_ms() ->
    {MegaSecs, Secs, MicroSecs} = os:timestamp(),
    (MegaSecs * 1000000 + Secs) * 1000 + round(MicroSecs / 1000).

set_index(Cluster, Index, Min, Max, Delay) ->
    {[[{_WorkerId, {ok, {OldMin, OldMax, OldDelay}}}|_]], _} =
        yz_rt:set_index(Cluster, Index, Min, Max, Delay),
    {OldMin, OldMax, OldDelay}.

