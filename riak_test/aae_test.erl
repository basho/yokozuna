-module(aae_test).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-define(NUM_KEYS, 10000).
-define(BUCKET, {<<"fruit_aae">>, <<"fruit">>}).
-define(INDEX, <<"fruit_aae">>).
-define(REPAIR_MFA, {yz_exchange_fsm, repair, 2}).
-define(CFG,
        [{riak_core,
          [
           {ring_creation_size, 16}
          ]},
         {riak_kv,
          [
           %% allow AAE to build trees and exchange rapidly
           {anti_entropy_build_limit, {100, 1000}},
           {anti_entropy_concurrency, 4}
          ]},
         {yokozuna,
          [
	   {enabled, true},
           {entropy_tick, 1000}
          ]}
        ]).


confirm() ->
    YZBenchDir = rt_config:get(yz_dir) ++ "/misc/bench",
    random:seed(now()),
    Cluster = rt:build_cluster(4, ?CFG),
    PBConns = yz_rt:open_pb_conns(Cluster),
    PBConn = yz_rt:select_random(PBConns),
    setup_index(Cluster, PBConn, YZBenchDir),
    {0, _} = yz_rt:load_data(Cluster, ?BUCKET, YZBenchDir, ?NUM_KEYS),
    lager:info("Verify data was indexed"),
    verify_num_match(Cluster, ?NUM_KEYS),
    %% Wait for a full round of exchange and then get total repair
    %% count.  Need to do this because setting AAE so agressive means
    %% that the Solr soft-commit could race with AAE and thus repair
    %% may happen while loading the data.  By getting the count before
    %% deleting keys it can be subtracted from the total count later
    %% to verify that the exact number of repairs was made given the
    %% number of keys deleted.
    TS1 = erlang:now(),
    wait_for_full_exchange_round(Cluster, TS1),
    RepairCountBefore = get_cluster_repair_count(Cluster),
    yz_rt:count_calls(Cluster, ?REPAIR_MFA),
    Keys = yz_rt:random_keys(?NUM_KEYS),
    {DelKeys, _ChangeKeys} = lists:split(length(Keys) div 2, Keys),
    lager:info("Deleting ~p keys", [length(DelKeys)]),
    [delete_key_in_solr(Cluster, ?INDEX, K) || K <- DelKeys],
    lager:info("Verify Solr indexes missing"),
    verify_num_match(Cluster, ?NUM_KEYS - length(DelKeys)),
    lager:info("Clear trees so AAE will notice missing indexes"),
    [ok = rpc:call(Node, yz_entropy_mgr, clear_trees, []) || Node <- Cluster],
    lager:info("Wait for all trees to re-build"),
    %% Before exchange of a partition can take place the KV and
    %% Yokozuna hashtree must be built.  Wait for all trees before
    %% checking that Solr indexes are repaired.
    TS2 = erlang:now(),
    wait_for_full_exchange_round(Cluster, TS2),
    lager:info("Verify AAE repairs missing Solr documents"),
    verify_num_match(Cluster, ?NUM_KEYS),
    %% Multiply by 3 because of N value
    ExpectedNumRepairs = length(DelKeys) * 3,
    lager:info("Verify repair count = ~p", [ExpectedNumRepairs]),
    verify_repair_count(Cluster, RepairCountBefore + ExpectedNumRepairs),
    yz_rt:stop_tracing(),
    Count = yz_rt:get_call_count(Cluster, ?REPAIR_MFA),
    %% Add 3 repairs for the fruit schema which is written to a
    %% non-indexed bucket and will cause 3 instances of tree_repair.
    ?assertEqual(ExpectedNumRepairs + 3, Count),
    %% Verify that there is no indefinite repair.  The have been
    %% several bugs in the past where Yokozuna AAE would indefinitely
    %% repair.
    lager:info("Verify no indefinite repair"),
    verify_no_repair(Cluster),
    yz_rt:close_pb_conns(PBConns),
    pass.

delete_key_in_solr(Cluster, Index, Key) ->
    [begin
         lager:info("Deleting solr doc ~s/~s on node ~p", [Index, Key, Node]),
         ok = rpc:call(Node, yz_solr, delete, [Index, [{key, list_to_binary(Key)}]])
     end || Node <- Cluster].

get_cluster_repair_count(Cluster) ->
    lists:sum([get_total_repair_count(Node) || Node <- Cluster]).

get_total_repair_count(Node) ->
    Dump = rpc:call(Node, riak_kv_entropy_info, dump, []),
    YZInfo = [I || {{index,{yz,_}},_}=I <- Dump],
    Sums = [Sum || {_Key,{index_info,_,{simple_stat,_,_,_,_,Sum},_,_}} <- YZInfo],
    lists:sum(Sums).

greater_than(TimestampA) ->
    fun(TimestampB) ->
            TimestampB > TimestampA
    end.

read_schema(YZBenchDir) ->
    Path = filename:join([YZBenchDir, "schemas", "fruit_schema.xml"]),
    {ok, RawSchema} = file:read_file(Path),
    RawSchema.

setup_index(Cluster, PBConn, YZBenchDir) ->
    Node = yz_rt:select_random(Cluster),
    RawSchema = read_schema(YZBenchDir),
    yz_rt:store_schema(PBConn, ?INDEX, RawSchema),
    ok = yz_rt:create_index(Node, ?INDEX, ?INDEX),
    ok = yz_rt:set_bucket_type_index(Node, ?INDEX),
    yz_rt:wait_for_index(Cluster, ?INDEX).

%% @doc Verify that no repair has happened since `TS'.
verify_no_repair(Cluster) ->
    yz_rt:count_calls(Cluster, ?REPAIR_MFA),
    TS = erlang:now(),
    wait_for_full_exchange_round(Cluster, TS),
    yz_rt:stop_tracing(),
    Count = yz_rt:get_call_count(Cluster, ?REPAIR_MFA),
    ?assertEqual(0, Count).

verify_num_match(Cluster, Num) ->
    F = fun(Node) ->
                HP = hd(yz_rt:host_entries(rt:connection_info([Node]))),
                yz_rt:search_expect(HP, ?INDEX, "text", "apricot", Num)
        end,
    yz_rt:wait_until(Cluster, F).

verify_repair_count(Cluster, ExpectedNumRepairs) ->
    RepairCount = get_cluster_repair_count(Cluster),
    ?assertEqual(ExpectedNumRepairs, RepairCount).


%% Eneded up not needing this function but leaving here in casse
%% useful later.

%% wait_for_all_trees(Cluster) ->
%%     F = fun(Node) ->
%%                 lager:info("Check if all trees built for node ~p", [Node]),
%%                 Info = rpc:call(Node, yz_kv, compute_tree_info, []),
%%                 NotBuilt = [X || {_,undefined}=X <- Info],
%%                 NotBuilt == []
%%         end,
%%     yz_rt:wait_until(Cluster, F).


%% @doc Wait for a full exchange round since `Timestamp'.  This means
%% that all `{Idx,N}' for all partitions must have exchanged after
%% `Timestamp'.
wait_for_full_exchange_round(Cluster, Timestamp) ->
    F = fun(Node) ->
                lager:info("Check if all partitions have had full exchange for ~p", [Node]),
                EIs = rpc:call(Node, yz_kv, compute_exchange_info, []),
                AllTimes = [AllTime || {_,_,AllTime,_} <- EIs],
                lists:all(greater_than(Timestamp), AllTimes)
        end,
    yz_rt:wait_until(Cluster, F).
