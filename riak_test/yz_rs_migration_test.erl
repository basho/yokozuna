-module(yz_rs_migration_test).
-compile(export_all).
-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

%% @doc Test the migration path from Riak Search to Yokozuna.
%%
%% 1. A rolling upgrade is performed and on each node Yokozuna is
%% enabled in the app.config.
%%
%% 2. For every index in Riak Search the user must create a comparable
%% index in Yokozuna.
%%
%% 3. For every bucket which is indexed by Riak Search the user must
%% add the `search_index' bucket property to point to the Yokozuna index
%% which is going to eventually be migrated to.
%%
%% 4. As objects are written or modified they will be indexed by both
%% Riak Search and Yokozuna.  But the HTTP and PB query interfaces
%% will continue to use Riak Search.
%%
%% 5a. The YZ AAE trees must be manually cleared so that AAE will notice
%% the missing indexes.
%%
%% 5b. In the background AAE will start building trees for Yokozuna and
%% exchange them with KV.  These exchanges will notice objects are
%% missing and index them in Yokozuna.
%%
%% 5c. The user wants Yokozuna to index the missing objects as fast as
%% possible.  A command may be used (repair? bucket map-reduce? custom
%% fold function?) to immediately re-index data.
%%
%% 6. Eventually all partitions will be exchanged (or buckets
%% re-indexed) and the user will be satisfied that queries can now
%% migrate to Yokozuna.  This will be accomplished via the AAE status.
%%
%% 7. The user will call some command that hands HTTP and PB query
%% control to Yokozuna.
%%
%% 8. The user must then set the `search' bucket property to `false'
%% for all indexed buckets.
%%
%% 9. Then the user can disable Riak Search on all nodes.
%%
%% 10. Eventually, when the user is convinced the Riak Search data is
%% no longer needed the merge index directories may be deleted to
%% reclaim disk space.

-define(CFG,
        [{riak_core,
          [
           {ring_creation_size, 16}
          ]},
         {riak_search,
          [
           {enabled, true}
          ]}
        ]).
-define(FRUIT_BUCKET, <<"fruit">>).

%% TODO: migration by re-index command
confirm() ->
    lager:info("ticktime: ~p", [net_kernel:get_net_ticktime()]),
    case yz_rt:bb_driver_setup() of
        {ok, YZBenchDir} ->
            lager:info("YZBenchDir: ~p", [YZBenchDir]),

            TestMetaData = riak_test_runner:metadata(),
            OldVsn = proplists:get_value(upgrade_version, TestMetaData, previous),

            Cluster = rt:build_cluster(lists:duplicate(3, {OldVsn, ?CFG})),

            create_index(Cluster, riak_search),
            load_data(Cluster, YZBenchDir, 1000),
            query_data(Cluster, YZBenchDir, 1000, 1, <<"value">>),

            %% In real scenarios the cluster will likely have incoming index
            %% and search operations during upgrade.  I'm avoiding them in
            %% this test because Riak Search can fail on search while in a
            %% mixed-cluster.
            yz_rt:rolling_upgrade(Cluster, current),

            load_data(Cluster, YZBenchDir, 5000),
            query_data(Cluster, YZBenchDir, 5000, 1, <<"value">>),

            check_for_errors(Cluster),

            create_index(Cluster, yokozuna),
            yz_rt:set_index(hd(Cluster), ?FRUIT_BUCKET, ?FRUIT_BUCKET),

            clear_aae_trees(Cluster),
            wait_for_aae(Cluster),

            switch_to_yokozuna(Cluster),
            query_data(Cluster, YZBenchDir, 5000, 1, <<"text">>),

            %% TODO: use BB to check PB
            PB = create_pb_conn(hd(Cluster)),
            {ok,{search_results,R,_Score,Found}} =
                riakc_pb_socket:search(PB, ?FRUIT_BUCKET, <<"text:apple">>),
            lager:info("PB R: ~p", [R]),
            ?assertEqual(5000, Found),
            close_pb_conn(PB),

            set_search_false(Cluster, ?FRUIT_BUCKET),
            disable_riak_search(),
            stop_riak_search(Cluster),
            check_for_errors(Cluster),

            %% TODO: need to verify that `riak_core:register' stuff is undone,
            %%
            %% need to make sure `riak_search' service is marked as down
            remove_mi_data(Cluster),
            %% TODO: what about removing the proxy objects under _rsid_<bucket>?

            lager:info("Verify missing merge_index dirs don't hurt anything"),
            restart(Cluster),
            check_for_errors(Cluster),

            load_data(Cluster, YZBenchDir, 10000),
            query_data(Cluster, YZBenchDir, 10000, 1, <<"text">>),
            check_for_errors(Cluster),

            pass;
        {error, bb_driver_build_failed} ->
            lager:info("Failed to build the yokozuna basho_bench driver"
                       " required for this test"),
            fail
    end.

clear_aae_trees(Cluster) ->
    lager:info("Clearing AAE trees across cluster"),
    [ok = rpc:call(Node, yz_entropy_mgr, clear_trees, []) || Node <- Cluster].

restart(Cluster) ->
    [rt:stop_and_wait(Node) || Node <- Cluster],
    [rt:start_and_wait(Node) || Node <- Cluster],
    rt:wait_for_cluster_service(Cluster, riak_kv),
    rt:wait_for_cluster_service(Cluster, yokozuna).

remove_mi_data(Cluster) ->
    rt:clean_data_dir(Cluster, "merge_index").

stop_riak_search(Cluster) ->
    [?assertEqual(ok,rpc:call(Node, application, stop, [riak_search]))
     || Node <- Cluster].

%% @doc Disable the search hook on `Bucket'.
set_search_false(Cluster, Bucket) ->
    lager:info("Uninstall search hook for bucket ~p", [Bucket]),
    ok = rpc:call(hd(Cluster), riak_search_kv_hook, uninstall, [Bucket]),
    F = fun(Node) ->
                PB = create_pb_conn(Node),
                PC = pb_get_bucket_prop(PB, ?FRUIT_BUCKET, precommit, false),
                close_pb_conn(PB),
                PC == []
        end,
    yz_rt:wait_until(Cluster, F),
    ok.

%% @doc Disable Riak Search in all app.config files.
disable_riak_search() ->
    %% Rely on fact that `all' means no nodes will be restarted.
    rt:update_app_config(all, [{riak_search, [{enabled, false}]}]).

switch_to_yokozuna(Cluster) ->
    lager:info("Switching search handling to Yokozuna ~p", [Cluster]),
    [rpc:call(Node, yokozuna, switch_to_yokozuna, []) || Node <- Cluster].

%% Use AAE status to verify that exchange has occurred for all
%% partitions since the time this function was invoked.
-spec wait_for_aae([node()]) -> ok.
wait_for_aae(Cluster) ->
    lager:info("Wait for AAE to migrate/repair indexes"),
    yz_rt:wait_for_all_trees(Cluster),
    yz_rt:wait_for_full_exchange_round(Cluster, erlang:now()),
    ok.

is_error_or_crash_log({FileName,_}) ->
    Base = filename:basename(FileName, ".log"),
    (Base == "error") or (Base == "crash").

has_content({FileName,_}) ->
    lager:info("Checking for content in file ~p", [FileName]),
    {ok, FI} = file:read_file_info(FileName),
    FI#file_info.size > 0.

check_for_errors(_Cluster) ->
    %% This call returns [{FileName,ErlangPort}]
    Root = filename:absname(proplists:get_value(root, rt_config:get(rtdev_path))),
    Logs = [{Root ++ "/" ++ FileName, Port}
            || {FileName, Port} <- rt:get_node_logs()],
    Logs2 = lists:filter(fun is_error_or_crash_log/1, Logs),
    Logs3 = lists:filter(fun has_content/1, Logs2),
    [lager:error("Check the log file ~p", [LogFile]) || {LogFile, _} <- Logs3].
    %% TODO: close ports
    %% ?assertEqual(0, length(Logs3)).

create_index([Node1|_]=Cluster, riak_search) ->
    rt:enable_search_hook(Node1, ?FRUIT_BUCKET),
    F = fun(Node) ->
                lager:info("Verify Riak Search index ~p [~p]", [?FRUIT_BUCKET, Node]),
                PB = create_pb_conn(Node),
                PBEnabled = pb_get_bucket_prop(PB, ?FRUIT_BUCKET, search, false),
                close_pb_conn(PB),
                Http = yz_rt:riak_http(element(2, hd(rt:connection_info([Node])))),
                HTTPEnabled = http_get_bucket_prop(Http, ?FRUIT_BUCKET, <<"search">>, false),
                PBEnabled or HTTPEnabled
        end,
    yz_rt:wait_until(Cluster, F);
create_index(Cluster, yokozuna) ->
    Idx = ?FRUIT_BUCKET,
    yz_rt:create_index(hd(Cluster), Idx).

close_pb_conn(PB) ->
    riakc_pb_socket:stop(PB).

create_pb_conn(Node) ->
    {IP, Port} = yz_rt:riak_pb(element(2, hd(rt:connection_info([Node])))),
    {ok, PB} = riakc_pb_socket:start_link(IP, Port),
    PB.

load_data(Cluster, YZBenchDir, NumKeys) ->
    {ExitCode, _} = yz_rt:load_data(Cluster, ?FRUIT_BUCKET, YZBenchDir, NumKeys),
    ?assertEqual(0,ExitCode),
    yz_rt:commit(Cluster, ?FRUIT_BUCKET).

query_data(Cluster, YZBenchDir, NumKeys, Time, DefaultField) ->
    lager:info("Run query against cluster ~p", [Cluster]),
    Idx = binary_to_list(?FRUIT_BUCKET),
    Hosts = yz_rt:host_entries(rt:connection_info(Cluster)),
    Concurrent = length(Hosts),
    Op = {search,"apple","id",NumKeys},
    %% Op = {random_fruit_search, ["id"], 3, NumKeys},
    Cfg = [{mode, {rate,8}},
           {duration, Time},
           {concurrent, Concurrent},
           {code_paths, [YZBenchDir]},
           {driver, yz_driver},
           {operations, [{Op,1}]},
           {http_conns, Hosts},
           {pb_conns, []},
           {default_field, DefaultField},
           {search_path, "/solr/" ++ Idx ++ "/select"},
           {shutdown_on_error, true}],
    File = "bb-query-fruit-" ++ Idx,
    yz_rt:write_terms(File, Cfg),
    {ExitCode, _StdOut} = yz_rt:run_bb(sync, File),
    ?assertEqual(0, ExitCode).

pb_get_bucket_prop(PB, Bucket, Prop, Default) ->
    {ok, Props} = riakc_pb_socket:get_bucket(PB, Bucket),
    proplists:get_value(Prop, Props, Default).

http_get_bucket_prop({Host, Port}, Bucket, Prop, Default) ->
    URL = lists:flatten(io_lib:format("http://~s:~s/riak/~s",
                                      [Host, integer_to_list(Port), Bucket])),
    %% Headers = [{"accept", "text/plain"}],
    {ok, "200", _, R} = ibrowse:send_req(URL, [], get, [], []),
    Struct = mochijson2:decode(R),
    {struct, Props} = element(2,hd(element(2, Struct))),
    proplists:get_value(Prop, Props, Default).
