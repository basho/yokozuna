%% @doc Test the addition or removal of the index entry from a bucket
%%      that has data.  If a bucket already has data then associating
%%      it with an index should cause the deletion of all entries for
%%      this bucket in the default index and re-indexing under the
%%      newly associated index.  Conversely, if an index is
%%      dissociated from the bucket then that bucket's data should be
%%      deleted from the index and re-indexed under the default index.
%%
%% NOTE: This is called "flag transition" because originally Yokozuna
%%       had an implicit one-to-one mapping between bucket and index
%%       name.  That is, the names were the same and a given index was
%%       responsible for only one bucket (with the exception of the
%%       _yz_default index).
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
    verify_index_add(Cluster, YZBenchDir),
    verify_index_remove(Cluster),
    verify_many_to_one_index_remove(Cluster).

%% @doc When an index is associated the indexes for the bucket should
%%      be removed from the default index and AAE should re-index
%%      objects under the bucket's index.
verify_index_add(Cluster, YZBenchDir) ->
    lager:info("Verify adding index"),
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
    yz_rt:wait_for_index(Cluster, "fruit"),

    %% TODO: use YZ/KV AAE stats to determine when AAE has covered ring once.
    F = fun(Node) ->
                lager:info("Verify that AAE re-indexes objects under fruit index [~p]", [Node]),
                HP2 = hd(yz_rt:host_entries(rt:connection_info([Node]))),
                %% Also verify that the data was deleted under default index
                yz_rt:search_expect(HP2, "_yz_default", "_yz_rb", "fruit", 0),
                yz_rt:search_expect(HP2, "fruit", "*", "*", ?NUM_KEYS)
        end,
    yz_rt:wait_until(Cluster, F).

%% @doc When an index is dissociated the indexes for that bucket's
%%      index should be deleted and AAE should re-index objects under
%%      the default index.
verify_index_remove(Cluster) ->
    lager:info("Verify removing index"),
    Node = yz_rt:select_random(Cluster),
    yz_rt:remove_index(Node, <<"fruit">>),
    F = fun(Node2) ->
                lager:info("Verify fruit indexes are deleted + objects re-indexed under default index [~p]", [Node2]),
                HP = hd(yz_rt:host_entries(rt:connection_info([Node2]))),
                R1 = yz_rt:search_expect(HP, "fruit", "*", "*", 0),
                R2 = yz_rt:search_expect(HP, "_yz_default", "_yz_rb", "fruit", ?NUM_KEYS),
                R1 and R2
        end,
    yz_rt:wait_until(Cluster, F).

%% @doc Verify that removing the index entry for a bucket deletes only
%%      that bucket's data in the associated index.
verify_many_to_one_index_remove(Cluster) ->
    lager:info("Verify removing index on a many-to-one index"),
    Node = yz_rt:select_random(Cluster),
    HP = hd(yz_rt:host_entries(rt:connection_info([Node]))),
    yz_rt:create_index(Node, "many"),
    yz_rt:set_index(Node, <<"b1">>, "many"),
    yz_rt:set_index(Node, <<"b2">>, "many"),
    yz_rt:wait_for_index(Cluster, "many"),
    yz_rt:http_put(HP, <<"b1">>, <<"key">>, <<"somedata">>),
    yz_rt:http_put(HP, <<"b2">>, <<"key">>, <<"somedata">>),
    %% Wait for soft-commit
    timer:sleep(1100),
    ?assert(yz_rt:search_expect(HP, "many", "_yz_rb", "b1", 1)),
    ?assert(yz_rt:search_expect(HP, "many", "_yz_rb", "b2", 1)),
    yz_rt:remove_index(Node, <<"b1">>),
    F = fun(Node2) ->
                lager:info("Verify only 'b1' data is removed from 'many' index [~p]", [Node2]),
                HP2 = hd(yz_rt:host_entries(rt:connection_info([Node2]))),
                R1 = yz_rt:search_expect(HP2, "many", "_yz_rb", "b1", 0),
                R2 = yz_rt:search_expect(HP2, "many", "_yz_rb", "b2", 1),
                R3 = yz_rt:search_expect(HP2, "_yz_default", "_yz_rb", "b1", 1),
                R1 and R2 and R3
        end,
    yz_rt:wait_until(Cluster, F).

join(Nodes) ->
    [NodeA|Others] = Nodes,
    [rt:join(Node, NodeA) || Node <- Others],
    Nodes.
