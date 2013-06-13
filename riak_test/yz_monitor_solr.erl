%% @doc Ensure that JVM killing works.
-module(yz_monitor_solr).
-compile(export_all).
-import(yz_rt, [host_entries/1,
                wait_for_joins/1]).
-include_lib("eunit/include/eunit.hrl").

-define(CFG, [{yokozuna, [{enabled, true}]}]).

confirm() ->
    random:seed(now()),
    Cluster = prepare_cluster(1),
    ok = test_solr_monitor(Cluster),
    pass.

prepare_cluster(NumNodes) ->
    Nodes = rt:deploy_nodes(NumNodes, ?CFG),
    Cluster = join(Nodes),
    wait_for_joins(Cluster),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    Cluster.

join(Nodes) ->
    [NodeA|Others] = Nodes,
    [rt:join(Node, NodeA) || Node <- Others],
    Nodes.

%% Kill the spawning Erlang process and verify the JVM is killed, too
-spec test_solr_monitor([node()]) -> ok | fail.
test_solr_monitor(Cluster) ->
    Node = hd(Cluster),
    ErlPid = rpc:call(Node, os, getpid, []),
    JvmPid = get_jvm_pid(Node),
    os:cmd("kill -9 " ++  ErlPid),
    rt:wait_until(nonode, fun(_M) -> yz_monitor_solr:is_jvm_dead_yet(JvmPid) end).

%% Verify that the JVM really has died
-spec is_jvm_dead_yet(string()) -> boolean().
is_jvm_dead_yet(JvmPid) ->
    Out = os:cmd("/bin/ps -ef | grep " ++ JvmPid ++ "| grep -v grep | grep -v dyld"),
    case Out of
        [] ->
            true;
        _ ->
            false
    end.

%% Call gen_server getpid to find OS PID of JVM process
-spec get_jvm_pid({string(), port()}) -> string().
get_jvm_pid(Node) ->
    Pid = rpc:call(Node, yz_solr_proc, getpid, []),
    integer_to_list(Pid).
