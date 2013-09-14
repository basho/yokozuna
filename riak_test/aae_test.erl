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
           {anti_entropy_concurrency, 12}
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
    Keys = yz_rt:random_keys(?NUM_KEYS),
    {DelKeys, _ChangeKeys} = lists:split(length(Keys) div 2, Keys),
    lager:info("Deleting ~p keys", [length(DelKeys)]),
    [delete_key_in_solr(Cluster, ?INDEX_S, K) || K <- DelKeys],
    lager:info("Verify Solr indexes missing"),
    verify_num_match(Cluster, ?NUM_KEYS - length(DelKeys)),
    lager:info("Clear trees so AAE will notice missing indexes"),
    [ok = rpc:call(Node, yz_entropy_mgr, clear_trees, []) || Node <- Cluster],
    lager:info("Verify AAE repairs missing Solr documents"),
    verify_num_match(Cluster, ?NUM_KEYS),
    pass.

%% TODO: I spent a while trying to get this to work only to realize
%% there is no way to get total repair count.  Since the LastCount is
%% only the count for the last {Idx,N} the sum was always 1/3 of
%% length(DelKeys).  Will eventually move AAE testing into it's own
%% module and find a way to verify the repair count but for now will
%% just leave this here as reminder.
%%
%% check_repair_count(Cluster, ExpectedNumRepairs) ->
%%     lager:info("Verify the repair count is equal to ~p", [ExpectedNumRepairs]),
%%     Infos = [rpc:call(Node, yz_kv, compute_exchange_info, []) || Node <- Cluster],
%%     Count = lists:sum([lists:sum([LastCount || {_, _, _, {LastCount, _, _, _}} <- Info])
%%                        || Info <- Infos]),
%%     ?assertEqual(ExpectedNumRepairs, Count).

delete_key_in_solr(Cluster, Index, Key) ->
    [begin
         lager:info("Deleting solr doc ~s/~s on node ~p", [Index, Key, Node]),
         ok = rpc:call(Node, yz_solr, delete, [Index, [{key, list_to_binary(Key)}]])
     end || Node <- Cluster].

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
