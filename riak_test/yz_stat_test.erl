%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2016 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
%%
%% @doc Check that stats run as expected
-module(yz_stat_test).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include("yokozuna.hrl").


-define(N_VAL, 1).
-define(NUM_NODES, 1).
-define(RING_SIZE, 8).
-define(NUM_ENTRIES, 10).

-define(CFG, [
    {riak_core, [
        {ring_creation_size, ?RING_SIZE},
        {vnode_management_timer, 1000},
        {handoff_concurrency, 10}
    ]},
    {riak_kv, [
        {anti_entropy_tick, 1000},
        {anti_entropy_build_limit, {100, 1000}},
        {anti_entropy_concurrency, 8}
    ]},
    {yokozuna, [
        {enabled, true},
        {?SOLRQ_WORKER_COUNT, 1},
        {?SOLRQ_BATCH_MIN, 6},
        {?SOLRQ_BATCH_FLUSH_INTERVAL, 100000},
        {?SOLRQ_HWM, 10},
        {?ERR_THRESH_FAIL_COUNT, 1},
        {?ERR_THRESH_RESET_INTERVAL, 3000},
        {?SOLRQ_DRAIN_ENABLE, true},
        %% allow AAE to build trees and exchange rapidly
        {anti_entropy_tick, 1000},
        {anti_entropy_build_limit, {100, 1000}},
        {anti_entropy_concurrency, 8}
    ]}
]).

confirm() ->
    random:seed(now()),
    Cluster = prepare_cluster(?NUM_NODES),
    confirm_stats(Cluster),
    pass.

prepare_cluster(NumNodes) ->
    Cluster = rt:build_cluster(NumNodes, ?CFG),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    Cluster.

confirm_stats(Cluster) ->
    {Host, Port} = yz_rt:select_random(
        [yz_rt:riak_pb(I) || {_,I} <- rt:connection_info(Cluster)]
    ),
    Index = <<"yz_stat_test">>,
    Bucket = {Index, <<"b1">>},

    {ok, PBConn} = riakc_pb_socket:start_link(Host, Port),
    yz_rt:create_indexed_bucket(PBConn, Cluster, Bucket, Index, ?N_VAL),
    {ok, BProps} = riakc_pb_socket:get_bucket(PBConn, Bucket),
    ?assertEqual(?N_VAL, proplists:get_value(n_val, BProps)),
    %%
    %% Clear the yz and kv hashtrees, because we have created a new
    %% bucket type with a different n_vals.
    %%
    %% TODO Add support for notification of bucket type and bucket property
    %% changes, so that a hashtree rebuild is not required.
    %%
    clear_hashtrees(Cluster),

    yz_rt:set_yz_aae_mode(Cluster, manual),

    yz_rt:reset_stats(Cluster),
    Values = populate_data_and_wait(PBConn, Cluster, Bucket, Index, ?NUM_ENTRIES),
    yz_rt:verify_num_match(yokozuna, Cluster, Index, ?NUM_ENTRIES),
    yz_rt:verify_num_match(solr, Cluster, Index, ?NUM_ENTRIES * ?N_VAL),
    yz_rt:wait_until(Cluster, fun check_index_stats/1),

    yz_rt:reset_stats(Cluster),
    search_values(PBConn, Index, Values),
    yz_rt:wait_until(Cluster, fun check_query_stats/1),

    yz_rt:reset_stats(Cluster),
    [delete_key_in_solr(Cluster, Index, {Bucket, K}) || K <- Values],
    yz_rt:verify_num_match(yokozuna, Cluster, Index, 0),
    yz_rt:set_yz_aae_mode(Cluster, automatic),
    yz_rt:clear_aae_trees(Cluster),
    yz_rt:wait_for_full_exchange_round(Cluster),
    yz_rt:drain_solrqs(Cluster),
    yz_rt:wait_until(Cluster, fun check_aae_stats/1),

    yz_rt:reset_stats(Cluster),
    yz_rt:verify_num_match(yokozuna, Cluster, Index, ?NUM_ENTRIES),
    yz_rt:verify_num_match(solr, Cluster, Index, ?NUM_ENTRIES * ?N_VAL),

    yz_rt:reset_stats(Cluster),
    write_bad_json(Cluster, PBConn, Bucket, 1),
    yz_rt:wait_until(Cluster, fun check_index_fail_stats/1),

    yz_rt:reset_stats(Cluster),
    blow_fuses(Cluster, PBConn, Index, Bucket),
    yz_rt:wait_until(Cluster, fun check_fuse_and_purge_stats/1),

    riakc_pb_socket:stop(PBConn).

clear_hashtrees(Cluster) ->
    yz_rt:clear_kv_trees(Cluster),
    yz_rt:clear_aae_trees(Cluster).

populate_data(Pid, Bucket, Count) ->
    populate_data(Pid, Bucket, Count, []).

%% populate random plain text values
populate_data(_, _, 0, Acc) ->
    Acc;
populate_data(Pid, Bucket, Count, Acc)->
    KV = gen_random_name(16),
    PO = riakc_obj:new(Bucket, KV, KV, "text/plain"),
    ok = riakc_pb_socket:put(Pid, PO, []),
    lager:info("Wrote Bucket ~p key ~p", [Bucket, KV]),
    populate_data(Pid, Bucket, Count - 1, [KV|Acc]).

populate_data_and_wait(Pid, Cluster, Bucket, Index, Count) ->
    Values = populate_data(Pid, Bucket, Count, []),
    yz_rt:wait_until(Cluster, fun check_queue_total_length_stats/1),
    yz_rt:drain_solrqs(Cluster),
    Search = <<"text:*">>,
    F = fun(_) ->
        {ok,{search_results,_R,Score,Found}} =
            riakc_pb_socket:search(Pid, Index, Search, []),
        (Count == Found) and (Score =/= 0.0)
        end,
    yz_rt:wait_until(Cluster, F),
    Values.

%% search for a list of values assumign plain text
search_values(_Pid, _Index, []) -> ok;
search_values(Pid, Index, [Value|Rest]) ->
    riakc_pb_socket:search(Pid, Index, <<"text:", Value/binary>>, []),
    search_values(Pid, Index, Rest).

gen_random_name(Length) ->
    Chars = "abcdefghijklmnopqrstuvwxyz1234567890",
    Value = lists:foldl(fun(_, Acc) ->
        [lists:nth(random:uniform(length(Chars)), Chars)] ++ Acc
                        end, [], lists:seq(1, Length)),
    list_to_binary(Value).

write_bad_json(Cluster, _, _, 0) ->
    yz_rt:drain_solrqs(Cluster),
    ok;
write_bad_json(Cluster, Pid, Bucket, Num) ->
    Key = list_to_binary("bad_json_" ++ integer_to_list(Num)),
    Value = <<"{\"bad\": \"unclosed\"">>,
    PO = riakc_obj:new(Bucket, Key, Value, "application/json"),
    {ok, _Obj} = riakc_pb_socket:put(Pid, PO, [return_head]),
    write_bad_json(Cluster, Pid, Bucket, Num - 1).

blow_fuses(Cluster, PBConn, Index, Bucket) ->
    yz_rt:set_yz_aae_mode(Cluster, manual),
    yz_rt:drain_solrqs(Cluster), %% first drain anything not previously flushed
    lager:info("Blowing some fuses..."),
    yz_rt:set_index(Cluster, Index, 1, 99999, 99999),
    yz_rt:set_hwm(Cluster, 1),
    yz_rt:set_purge_strategy(Cluster, ?PURGE_ONE),
    try
        [yz_rt:load_intercept_code(Node) || Node <- Cluster],
        yz_rt:intercept_index_batch(Cluster, index_batch_throw_exception),
        %%
        %% Send a message through each the indexq on the solrq, which
        %% will trip the fuse; however
        %% because fuse blown events are handled asynchronously,
        %% we need to wait until the solrqs are blown.
        %%
        lager:info("Writing one entry to blow fuse..."),
        populate_data(PBConn, Bucket, 1),
        yz_rt:wait_until_fuses_blown(Cluster, yz_solrq_worker_0001, [Index]),
        %%
        %% At this point, the indexq in yz_solrq_worker_0001 corresponding
        %% to the Index should be blown.
        %% Send one more message through one of the Indexqs, which
        %% will trigger a purge.
        %%
        lager:info("Writing next entry to purge previous entry..."),
        populate_data(PBConn, Bucket, 1)
    after
        %%
        %% Revert the intercept, and drain, giving time for the
        %% fuse to reset.  Commit to Solr so that we can run a query.
        %%
        yz_rt:intercept_index_batch(Cluster, index_batch_call_orig),
        yz_rt:wait_until_fuses_reset(Cluster, yz_solrq_worker_0001, [Index]),
        lager:info("Writing one last entry to set the threshold ok stat ..."),
        populate_data(PBConn, Bucket, 1),
        yz_rt:drain_solrqs(Cluster),
        yz_rt:commit(Cluster, Index)
    end.

delete_solr_entries(Cluster, Index, Bucket, Keys) ->
    [rpc:call(Node, yz_solr, delete, [Index, {bkey, {Bucket, Key}}])
        || Node <- Cluster, Key <- Keys].

-spec delete_key_in_solr([node()], index_name(), bkey()) -> [ok].
delete_key_in_solr(Cluster, Index, BKey) ->
    [begin
         lager:info("Deleting solr doc ~s/~p on node ~p", [Index, BKey, Node]),
         ok = rpc:call(Node, yz_solr, delete, [Index, [{bkey, BKey}]])
     end || Node <- Cluster].

-define(STAT_NAME(Path), yz_stat:stat_name(Path)).

check_index_stats(Node) ->
    Stats = rpc:call(Node, yz_stat, get_stats, []),

    ILatency = proplists:get_value(?STAT_NAME([index, latency]), Stats),
    ILatencyMin = proplists:get_value(min, ILatency),
    ILatencyMax = proplists:get_value(max, ILatency),

    IThroughput = proplists:get_value(?STAT_NAME([index, throughput]), Stats),
    IThruCount = proplists:get_value(count, IThroughput),
    IThruOne = proplists:get_value(one, IThroughput),

    QBatchThroughput = proplists:get_value(
        ?STAT_NAME([queue, batch, throughput]), Stats),
    QBatchCount = proplists:get_value(count, QBatchThroughput),
    QBatchOne = proplists:get_value(one, QBatchThroughput),

    QBatchsize = proplists:get_value(?STAT_NAME([queue, batchsize]), Stats),
    QBatchsizeMin = proplists:get_value(min, QBatchsize),
    QBatchsizeMax = proplists:get_value(max, QBatchsize),

    QBatchLatency = proplists:get_value(
        ?STAT_NAME([queue, batch, latency]), Stats),
    QBatchLatencyMin = proplists:get_value(min, QBatchLatency),
    QBatchLatencyMax = proplists:get_value(max, QBatchLatency),

    QDrain = proplists:get_value(?STAT_NAME([queue, drain]), Stats),
    QQDrainCount = proplists:get_value(count, QDrain),
    QQDrainOne = proplists:get_value(one, QDrain),

    QDrainLatency = proplists:get_value(
        ?STAT_NAME([queue, drain, latency]), Stats),
    QDrainLatencyMin = proplists:get_value(min, QDrainLatency),
    QDrainLatencyMax = proplists:get_value(max, QDrainLatency),

    Pairs = [
        {index_latency_min, ILatencyMin, '>', 0},
        {index_latency_max, ILatencyMax, '>', 0},
        {index_throughput_count, IThruCount, '>', 0},
        {index_throughput_one, IThruOne, '>', 0},
        {queue_batch_throughput_count, QBatchCount, '>', 0},
        {queue_batch_throughput_one, QBatchOne, '>', 0},
        {queue_batchsize_min, QBatchsizeMin, '>', 0},
        {queue_batchsize_max, QBatchsizeMax, '>', 0},
        {queue_batch_latency_min, QBatchLatencyMin, '>', 0},
        {queue_batch_latency_max, QBatchLatencyMax, '>', 0},
        {queue_drain_count, QQDrainCount, '>', 0},
        {queue_drain_one, QQDrainOne, '>', 0},
        {queue_drain_latency_min, QDrainLatencyMin, '>', 0},
        {queue_drain_latency_max, QDrainLatencyMax, '>', 0}
    ],
    yz_rt:check_stat_values(Stats, Pairs).

check_index_fail_stats(Node) ->
    Stats = rpc:call(Node, yz_stat, get_stats, []),

    IFail = proplists:get_value(?STAT_NAME([index, fail]), Stats),
    IFailCount = proplists:get_value(count, IFail),
    IFailOne = proplists:get_value(one, IFail),

    Pairs = [
        {index_fail_count, IFailCount, '>', 0},
        {index_fail_one, IFailOne, '>', 0}
    ],
    yz_rt:check_stat_values(Stats, Pairs).

check_query_stats(Node) ->
    Stats = rpc:call(Node, yz_stat, get_stats, []),

    SThroughput = proplists:get_value(?STAT_NAME(['query', throughput]), Stats),
    SThruCount = proplists:get_value(count, SThroughput),
    SThruOne = proplists:get_value(one, SThroughput),

    Pairs = [
        {query_throughput_count, SThruCount, '>', 0},
        {query_throughput_one, SThruOne, '>', 0}
    ],
    yz_rt:check_stat_values(Stats, Pairs).

check_queue_total_length_stats(Node) ->
    Stats = rpc:call(Node, yz_stat, get_stats, []),

    QTotalLength = proplists:get_value(?STAT_NAME([queue, total_length]), Stats),
    QTotalLengthValue = proplists:get_value(value, QTotalLength),

    Pairs = [
        {queue_total_length, QTotalLengthValue, '>', 0}
    ],
    yz_rt:check_stat_values(Stats, Pairs).

check_aae_stats(Node) ->
    Stats = rpc:call(Node, yz_stat, get_stats, []),

    AAERepairs = proplists:get_value(?STAT_NAME([detected_repairs]), Stats),
    AAERepairValue = proplists:get_value(value, AAERepairs),

    Pairs = [
        {aae_repairs, AAERepairValue, '>', 0}
    ],
    yz_rt:check_stat_values(Stats, Pairs).

check_fuse_and_purge_stats(Node) ->
    Stats = rpc:call(Node, yz_stat, get_stats, []),

    HWMPurged = proplists:get_value(?STAT_NAME([queue, hwm, purged]), Stats),
    HWMPurgedCount = proplists:get_value(count, HWMPurged),
    HWMPurgedOne = proplists:get_value(one, HWMPurged),

    ErrorThresholdBlownCount = proplists:get_value(
        ?STAT_NAME([search_index_error_threshold_blown_count]), Stats),
    ErrorThresholdBlownCountValue = proplists:get_value(
        value, ErrorThresholdBlownCount),
    ErrorThresholdBlownOne = proplists:get_value(
        ?STAT_NAME([search_index_error_threshold_blown_one]), Stats),
    ErrorThresholdBlownOneValue = proplists:get_value(
        value, ErrorThresholdBlownOne),

    ErrorThresholdFailureCount = proplists:get_value(
        ?STAT_NAME([search_index_error_threshold_failure_count]), Stats),
    ErrorThresholdFailureCountValue = proplists:get_value(
        value, ErrorThresholdFailureCount),
    ErrorThresholdFailureOne = proplists:get_value(
        ?STAT_NAME([search_index_error_threshold_failure_one]), Stats),
    ErrorThresholdFailureOneValue = proplists:get_value(
        value, ErrorThresholdFailureOne),

    ErrorThresholdOkCount = proplists:get_value(
        ?STAT_NAME([search_index_error_threshold_ok_count]), Stats),
    ErrorThresholdOkCountValue = proplists:get_value(
        value, ErrorThresholdOkCount),
    ErrorThresholdOkOne = proplists:get_value(
        ?STAT_NAME([search_index_error_threshold_ok_one]), Stats),
    ErrorThresholdOkOneValue = proplists:get_value(value, ErrorThresholdOkOne),

    ErrorThresholdRecoveredCount = proplists:get_value(
        ?STAT_NAME([search_index_error_threshold_recovered_count]), Stats),
    ErrorThresholdRecoveredCountValue = proplists:get_value(
        value, ErrorThresholdRecoveredCount),
    ErrorThresholdRecoveredOne = proplists:get_value(
        ?STAT_NAME([search_index_error_threshold_recovered_one]), Stats),
    ErrorThresholdRecoveredOneValue = proplists:get_value(
        value, ErrorThresholdRecoveredOne),

    Pairs = [
        {hwm_purged_count, HWMPurgedCount, '>', 0},
        {hwm_purged_one, HWMPurgedOne, '>', 0},
        {error_threshold_blown_count, ErrorThresholdBlownCountValue, '>', 0},
        {error_threshold_blown_one, ErrorThresholdBlownOneValue, '>', 0},
        {error_threshold_failure_count, ErrorThresholdFailureCountValue, '>', 0},
        {error_threshold_failure_one, ErrorThresholdFailureOneValue, '>', 0},
        {error_threshold_ok_count, ErrorThresholdOkCountValue, '>', 0},
        {error_threshold_ok_one, ErrorThresholdOkOneValue, '>', 0},
        {error_threshold_recovered_count, ErrorThresholdRecoveredCountValue, '>', 0},
        {error_threshold_recovered_one, ErrorThresholdRecoveredOneValue, '>', 0}
    ],
    yz_rt:check_stat_values(Stats, Pairs).
