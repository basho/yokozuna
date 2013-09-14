-module(aae_test).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-define(NUM_KEYS, 10000).
-define(INDEX_B, <<"fruit_aae">>).
-define(INDEX_S, "fruit_aae").
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
    YZBenchDir = rt_config:get_os_env("YZ_BENCH_DIR"),
    code:add_path(filename:join([YZBenchDir, "ebin"])),
    random:seed(now()),
    Cluster = rt:build_cluster(4, ?CFG),
    setup_index(Cluster, YZBenchDir),
    yz_rt:load_data(Cluster, ?INDEX_S, YZBenchDir, ?NUM_KEYS),
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
    ClusterSumBefore = lists:sum([get_total_repair_count(Node) || Node <- Cluster]),
    Keys = yz_rt:random_keys(?NUM_KEYS),
    {DelKeys, _ChangeKeys} = lists:split(length(Keys) div 2, Keys),
    lager:info("Deleting ~p keys", [length(DelKeys)]),
    [delete_key_in_solr(Cluster, ?INDEX_S, K) || K <- DelKeys],
    lager:info("Verify Solr indexes missing"),
    verify_num_match(Cluster, ?NUM_KEYS - length(DelKeys)),
    lager:info("Clear trees so AAE will notice missing indexes"),
    [ok = rpc:call(Node, yz_entropy_mgr, clear_trees, []) || Node <- Cluster],
    lager:info("Wait for all trees to re-build"),
    %% Before exchange of a partition to take place the KV and
    %% Yokozuna hashtree must be built.  Wait for all trees before
    %% checking that Solr indexes are repaired.
    TS2 = erlang:now(),
    wait_for_full_exchange_round(Cluster, TS2),
    lager:info("Verify AAE repairs missing Solr documents"),
    verify_num_match(Cluster, ?NUM_KEYS),
    %% Multiply by 3 because of N value
    ExpectedNumRepairs = length(DelKeys) * 3,
    lager:info("Verify repair count = ~p", [ExpectedNumRepairs]),
    verify_repair_count(Cluster, ClusterSumBefore + ExpectedNumRepairs),
    pass.

delete_key_in_solr(Cluster, Index, Key) ->
    [begin
         lager:info("Deleting solr doc ~s/~s on node ~p", [Index, Key, Node]),
         ok = rpc:call(Node, yz_solr, delete, [Index, [{key, list_to_binary(Key)}]])
     end || Node <- Cluster].

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

setup_index(Cluster, YZBenchDir) ->
    Node = yz_rt:select_random(Cluster),
    RawSchema = read_schema(YZBenchDir),
    ok = store_schema(Node, ?INDEX_B, RawSchema),
    ok = yz_rt:create_index(Node, ?INDEX_S, ?INDEX_B),
    ok = yz_rt:set_index(Node, ?INDEX_B),
    yz_rt:wait_for_index(Cluster, ?INDEX_S).

store_schema(Node, Name, RawSchema) ->
    lager:info("Storing schema ~p [~p]", [Name, Node]),
    ok = rpc:call(Node, yz_schema, store, [Name, RawSchema]).

verify_num_match(Cluster, Num) ->
    F = fun(Node) ->
                HP = hd(yz_rt:host_entries(rt:connection_info([Node]))),
                yz_rt:search_expect(HP, ?INDEX_S, "text", "apricot", Num)
        end,
    yz_rt:wait_until(Cluster, F).

verify_repair_count(Cluster, ExpectedNumRepairs) ->
    ClusterSum = lists:sum([get_total_repair_count(Node) || Node <- Cluster]),
    ?assertEqual(ExpectedNumRepairs, ClusterSum).


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
