-module(yz_flag_transitions).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-define(NUM_KEYS, 1000).
-define(CFG,
        [{riak_core,
          [
           {ring_creation_size, 32}
          ]},
          {riak_kv,
          [
           %% build often
           {anti_entropy_build_limit, {100, 1000}},
           {anti_entropy_concurrency, 12}
          ]},
         {yokozuna,
          [
	   {enabled, true},
           {entropy_tick, 1000}
          ]},
         {lager,
          [{handlers,
            [{lager_file_backend,
              [{"./log/error.log",error,10485760,"$D0",5},
               {"./log/console.log",info,104857600,"$D0",10}]}]}]}
        ]).

confirm() ->
    YZBenchDir = rt_config:get_os_env("YZ_BENCH_DIR"),
    code:add_path(filename:join([YZBenchDir, "ebin"])),
    random:seed(now()),
    Nodes = rt:deploy_nodes(4, ?CFG),
    Cluster = join(Nodes),
    yz_rt:wait_for_joins(Cluster),
    verify_flag_add(Cluster, YZBenchDir),
    verify_flag_remove(Cluster).

%% @doc When a flag is added the indexes for the bucket should be
%%      removed from the default index and AAE should re-index objects
%%      under the bucket's index.
verify_flag_add(Cluster, YZBenchDir) ->
    lager:info("Verify adding flag"),
    yz_rt:load_data(Cluster, "fruit", YZBenchDir, ?NUM_KEYS),
    %% Let 1s soft-commit catch up
    timer:sleep(1000),
    Hosts = yz_rt:host_entries(rt:connection_info(Cluster)),
    HP = yz_rt:select_random(Hosts),
    lager:info("Verify fruit index doesn't exist"),
    {ok, "404", _, _} = yz_rt:search(yokozuna, HP, "fruit", "*", "*"),
    lager:info("Verify objects are indexed under default index"),
    ?assert(yz_rt:search_expect(HP, "_yz_default", "_yz_rb", "fruit", ?NUM_KEYS)),
    lager:info("Create fruit index + set flag"),
    yz_rt:create_index(yz_rt:select_random(Cluster), "fruit"),
    yz_rt:set_index(yz_rt:select_random(Cluster), <<"fruit">>),
    %% Give Solr time to create index
    timer:sleep(10000),
    %% TODO: use YZ/KV AAE stats to determine when AAE has covered ring once.
    F = fun(Cluster2) ->
                Hosts2 = yz_rt:host_entries(rt:connection_info(Cluster2)),
                HP2 = yz_rt:select_random(Hosts2),
                yz_rt:search_expect(HP2, "fruit", "*", "*", ?NUM_KEYS)
        end,
    lager:info("Verify that AAE re-indexes objects under fruit index"),
    ?assertEqual(ok, yz_rt:wait_for_aae(Cluster, F)).

%% @doc When a flag is removed the indexes for that bucket's index
%%      should be deleted and AAE should re-index objects under the
%%      default index.
verify_flag_remove(Cluster) ->
    lager:info("Verify removing flag"),
    Node = yz_rt:select_random(Cluster),
    yz_rt:remove_index(Node, <<"fruit">>),
    F = fun(Cluster2) ->
                Hosts = yz_rt:host_entries(rt:connection_info(Cluster2)),
                HP = yz_rt:select_random(Hosts),
                R1 = yz_rt:search_expect(HP, "fruit", "*", "*", 0),
                R2 = yz_rt:search_expect(HP, "_yz_default", "_yz_rb", "fruit", ?NUM_KEYS),
                R1 and R2
        end,
    lager:info("Verify fruit indexes are deleted + objects re-indexed under default index"),
    ?assertEqual(ok, yz_rt:wait_for_aae(Cluster, F)).

join(Nodes) ->
    [NodeA|Others] = Nodes,
    [rt:join(Node, NodeA) || Node <- Others],
    Nodes.
