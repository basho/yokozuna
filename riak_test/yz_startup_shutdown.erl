%% @doc Test the startup and shutdown sequence.
-module(yz_startup_shutdown).
-export([confirm/0]).
-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile({parse_transform, rt_intercept_pt}).

-define(CLUSTER_SIZE, 4).
-define(MAX_SESSIONS, 11).
-define(MAX_PIPELINE_SIZE, 9).
-define(CONFIG,
        [{yokozuna, [{enabled, true},
                     {?SOLRQ_DRAIN_ENABLE, true},
                     {?YZ_CONFIG_IBROWSE_MAX_SESSIONS, ?MAX_SESSIONS},
                     {?YZ_CONFIG_IBROWSE_MAX_PIPELINE_SIZE, ?MAX_PIPELINE_SIZE}]}]
).
-define(YZ_SERVICES, [yz_pb_search, yz_pb_admin]).

confirm() ->
    Cluster = yz_rt:prepare_cluster(?CLUSTER_SIZE, ?CONFIG),

    verify_yz_components_enabled(Cluster),
    verify_yz_services_registered(Cluster),
    verify_ibrowse_config(Cluster),

    verify_node_restart(Cluster),

    intercept_yz_solrq_drain_mgr_drain(Cluster),
    stop_yokozuna(Cluster),

    verify_drain_called(Cluster),
    verify_yz_components_disabled(Cluster),
    verify_yz_services_deregistered(Cluster),
    pass.

%% @private
%%
%% @doc Assert that all components are enabled on each node in `Cluster'.
verify_yz_components_enabled(Cluster) ->
    check_yz_components(Cluster, true).

%% @private
%%
%% @doc Assert that all components are disabled on each node in `Cluster'.
verify_yz_components_disabled(Cluster) ->
    check_yz_components(Cluster, false).

%% @private
%%
%% @doc Assert that all services are registerd on each node in `Cluster'.
verify_yz_services_registered(Cluster) ->
    lists:all(
      fun(Node) ->
              true =:= are_services_registered(?YZ_SERVICES, Node)
      end,
      Cluster).

%% @private
%%
%% @doc Assert that all services are registerd on each node in `Cluster'.
verify_yz_services_deregistered(Cluster) ->
    lists:all(
      fun(Node) ->
              false =:= are_services_registered(?YZ_SERVICES, Node)
      end,
      Cluster).

%% @private
%%
%% @doc For each node in `Cluster', wait for a message of the form
%% {Node, drain_called} and assert that each was received before
%% timeout.
verify_drain_called(Cluster) ->
    Results = [begin
                   receive
                       {Node, drain_called} ->
                           ok
                   after
                       10000 ->
                           {fail, timeout}
                   end
               end
               || Node <- Cluster],

    true = lists:all(fun(Result) ->
                             ok =:= Result
                     end,
                     Results).

%% @private
%% @doc For each node in `Cluster', verify that the ibrowse configuration has
%% been applied.
verify_ibrowse_config([Node1|_] = Cluster) ->
    {ResL, []} = rpc:multicall(Cluster, yz_solr, get_ibrowse_config, []),
    lists:foreach(
      fun(Config) ->
              MaxSessions = proplists:get_value(?YZ_SOLR_MAX_SESSIONS, Config),
              MaxPipelineSize = proplists:get_value(?YZ_SOLR_MAX_PIPELINE_SIZE, Config),
              ?assertEqual(MaxSessions, ?MAX_SESSIONS),
              ?assertEqual(MaxPipelineSize, ?MAX_PIPELINE_SIZE)
      end,
      ResL),

    %% Now verify setting these config values programmatically...
    NewMaxSessions = 42,
    NewMaxPipelineSize = 64,
    ok = rpc:call(Node1, yz_solr, set_ibrowse_config,
                  [[{?YZ_SOLR_MAX_SESSIONS, NewMaxSessions},
                    {?YZ_SOLR_MAX_PIPELINE_SIZE, NewMaxPipelineSize}]]),
    NewConfig = rpc:call(Node1, yz_solr, get_ibrowse_config, []),
    ?assertEqual(NewMaxSessions, proplists:get_value(?YZ_SOLR_MAX_SESSIONS, NewConfig)),
    ?assertEqual(NewMaxPipelineSize,
            proplists:get_value(?YZ_SOLR_MAX_PIPELINE_SIZE, NewConfig)).

%% @private
%% @doc Restart one node in `Cluster' and verify that it is properly excluded
%% from query coverage plans and index operations until it is fully restarted and
%% ready for service.
verify_node_restart(Cluster) ->
    lager:info("Starting verify_node_restart"),
    IndexedBuckets = setup_indexed_buckets(Cluster, 5),
    {Bucket, Index} = lists:nth(3, IndexedBuckets),
    NodeToRestart = lists:nth(?CLUSTER_SIZE - 1, Cluster),
    HP = lists:last(yz_rt:host_entries(rt:connection_info(Cluster))),
    Pids = start_background_processes(HP, Bucket, Index),
    rt:log_to_nodes(Cluster, "RIAK TEST: Shutting Down - look for node_watcher_events"),
    rt:stop_and_wait(NodeToRestart),
    check_background_processes(Pids, check),
    start_and_wait_for_service(NodeToRestart, yokozuna),
    timer:sleep(5000), %% wait an extra 5 seconds to give queries more time to break things
    check_background_processes(Pids, stop),
    assert_no_index_failed(NodeToRestart),
    lager:info("Completed verify_node_restart"),
    ok.

%%%===================================================================
%%% Helpers
%%%===================================================================

%% @private
%%
%% @doc Checks that the enabled status of all yokozuna components is equal to
%% `Enabled'.
check_yz_components([], _Enabled) ->
    ok;
check_yz_components([Node|Rest], Enabled) ->
    Components = yz_app:components(),
    ?assertEqual(
        [Enabled || _C <- Components],
        [rpc:call(Node, yokozuna, is_enabled, [Component]) ||
            Component <- Components]
    ),
    check_yz_components(Rest, Enabled).

%% @private
%%
%% @doc Are the given `Services' currently registered on `Node'?
-spec are_services_registered(Services::[atom()], Node::node()) -> boolean().
are_services_registered(Services, Node) ->
    RegisteredServices =
        case rpc:call(Node, riak_api_pb_registrar, services, []) of
            {badrpc,nodedown} ->
                lager:error("Could not verify services on node ~p because it is down.",
                            [Node]),
                [];
            Response ->
                lists:flatten(Response)
        end,
    lists:all(
      fun(Service) ->
              lists:member(Service, RegisteredServices)
      end,
      Services).

-spec setup_indexed_buckets(Cluster::yz_rt:cluster(), Count::pos_integer())
                           -> [{bucket(), index_name()}].
setup_indexed_buckets(Cluster, Count) ->
    IndexedBuckets =
    [begin
         Index = list_to_binary(io_lib:format("index~B", [I])),
         BucketType = list_to_binary(io_lib:format("bt~B", [I])),
         Bucket = {BucketType,
                   list_to_binary(io_lib:format("bucket~B", [I]))},
         yz_rt:create_indexed_bucket_type(Cluster, BucketType, Index),
         yz_rt:write_objs(Cluster, Bucket, 10),
         yz_rt:commit(Cluster, Index),
         {Bucket, Index}
     end || I <- lists:seq(0, Count)],
    IndexedBuckets.

start_background_processes(HP, Bucket, Index) ->
    %% Global warming be damned, we have to use a short delay time to make sure
    %% we're producing enough load to really reproduce the query/index issues.
    Delay = 5,
    QueryPid = spawn_and_repeat(query_fun(HP, Index, "*", "*"), [{delay, Delay}]),
    PutPid = spawn_and_repeat(put_objects_fun(HP, Bucket), [{delay, Delay}]),
    %% Due to lack of retry in Solr 4.7.0's HTTP client, we must accept _some_
    %% small number of query/put failures. However, they should be small.
    %% Once these distributed Solr issues are resolved, we should test
    %% with these set to 0 as Solr should be able to handle stale connections
    %% in its distributed Solr implementation.
    MaxQueryFailures = 2,
    MaxPutFailures = 1,
    [{query, QueryPid, MaxQueryFailures}, {put, PutPid, MaxPutFailures}].

query_fun(HP, Index, Name, Term) ->
    fun() ->
        case yz_rt:search(yokozuna, HP, Index, Name, Term, "", false) of
            {ok, "200", _, _R} -> true;
            _Res ->
                increment_process_failures(),
                false
        end
    end.

increment_process_failures() ->
    CurFails = case get(loop_val) of
                   undefined -> 0;
                   Val -> Val
               end,
    put(loop_val, CurFails + 1),
    lager:info("Incremented loop_val to ~p", [get(loop_val)]).

put_objects_fun(HP, Bucket) ->
    fun() ->
            lists:foreach(
              fun(_) ->
                      Key = yz_rt:random_binary(10),
                      Body = yz_rt:random_binary(100),
                      try
                          yz_rt:http_put(HP, Bucket, Key, Body)
                      catch Err:Reason ->
                          index_failed({Err, Reason})
                      end
              end,
              lists:seq(1, 10))
    end.

index_failed(Err) ->
    lager:info("Index failed: ~p", [Err]),
    increment_process_failures().

check_background_processes(Pids, StopOrCheck) ->
    lists:foreach(
        fun({Type, Pid, ErrorThreshold}) ->
            IsAlive = erlang:is_process_alive(Pid),
            Pid ! {StopOrCheck, self()},
            ErrorCount = receive
                             undefined -> 0;
                             Count -> Count
                         end,
            ?assertEqual({Type, true}, {Type, IsAlive}),
            lager:info("Checking error count for ~p: ~p =< ~p", [Type, ErrorCount, ErrorThreshold]),
            Condition = ErrorCount =< ErrorThreshold,
            ?assertEqual(Condition, true)
        end,
        Pids).

start_and_wait_for_service(Node, Service) ->
    rt:start_and_wait(Node),
    rt:wait_until_ready(Node),
    rt:wait_for_cluster_service([Node], Service),
    timer:sleep(5000), %% wait an extra 5 seconds to give queries more time to break things
    ?assert(rpc:call(Node, yz_solr, is_up, [])).

-define(INDEX_FAILED_MESSAGE, "Index failed").
%% @doc Assert that there are no log messages on `Node' that indicate failed
%% index operations. Failed index operations are a symptom of a crash during
%% the shutdown sequence.
assert_no_index_failed(Node) ->
    assert_not_in_logs(Node, ?INDEX_FAILED_MESSAGE).

%% @private
%% @doc Spawns a process that repeatedly executes the given `Fun'. A `delay'
%% option can be provided in the `Options' parameter to control the delay
%% between iterations of the fun.
-spec spawn_and_repeat(fun(() -> any()), [proplists:property()]) -> pid().
spawn_and_repeat(Fun, Options) ->
    Delay = proplists:get_value(delay, Options, 100),
    spawn(fun() ->
                  loop(Fun, Delay)
          end).

loop(Fun, Delay) ->
    Fun(),
    receive
        {stop, From} -> From ! get(loop_val);
        {check, From} ->
            From ! get(loop_val),
            put(loop_val, 0),
            loop(Fun, Delay)
    after Delay ->
        loop(Fun, Delay)
    end.

assert_not_in_logs(Node, Pattern) ->
    ?assert(rt:expect_not_in_logs(Node, Pattern)).

%% @private
%%
%% @doc Install an intercept on all of the given nodes, which intercepts the
%% yz_solrq_drain_mgr:drain/0 function and sends a message to the riak_test
%% process indicating that the function was actually called on a given node.
%% The message is of the form {Node, drain_called}, where `Node' identifies the
%% node.
intercept_yz_solrq_drain_mgr_drain([]) ->
    ok;
intercept_yz_solrq_drain_mgr_drain([Node|Rest]) ->
    RiakTestProcess = self(),
    rt_intercept:add(
      Node,
      {yz_solrq_drain_mgr,
       [{{drain, 0},
         {[Node, RiakTestProcess],
          fun() ->
                  RiakTestProcess ! {Node, drain_called}
          end}}]}),
    intercept_yz_solrq_drain_mgr_drain(Rest).

%% @private
%%
%% @doc Stop the yokozuna application on all of the given nodes.
stop_yokozuna(Cluster) ->
    lists:foreach(
      fun(Node) ->
              ok = rpc:call(Node, application, stop, [yokozuna])
      end,
      Cluster).
