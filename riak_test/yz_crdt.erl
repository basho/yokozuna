%% @doc Test CRDTs in various ways.
-module(yz_crdt).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_kv/include/riak_kv_types.hrl").

-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).
-define(NO_HEADERS, []).
-define(NO_BODY, <<>>).
-define(CFG, [{yokozuna, [{enabled, true}]}]).

confirm() ->
    YZBenchDir = rt_config:get_os_env("YZ_BENCH_DIR"),
    code:add_path(filename:join([YZBenchDir, "ebin"])),
    random:seed(now()),
    Cluster = prepare_cluster(4),
    confirm_counter(Cluster),
    pass.


prepare_cluster(NumNodes) ->
    Nodes = rt:deploy_nodes(NumNodes, ?CFG),
    Cluster = join(Nodes),
    yz_rt:wait_for_joins(Cluster),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    Cluster.

join(Nodes) ->
    [NodeA|Others] = Nodes,
    [rt:join(Node, NodeA) || Node <- Others],
    Nodes.

counter_url({Host,Port}, Bucket, Key) ->
    ?FMT("http://~s:~B/buckets/~s/counters/~s", [Host, Port, Bucket, Key]).

confirm_counter(Cluster) ->
    lager:info("confirm_counter~n", []),
    Bucket = "counter",
    Node = yz_rt:select_random(Cluster),
    [HP] = yz_rt:host_entries(rt:connection_info([Node])),
    {Host, Port} = HP,
    {ok, Pid} = riakc_pb_socket:start_link(Host, (Port-1)),
    riakc_pb_socket:set_bucket(Pid, Bucket, [{allow_mult, true}]),

    yz_rt:create_index(Node, Bucket),
    yz_rt:set_index(Node, list_to_binary(Bucket)),
    yz_rt:wait_for_index(Cluster, Bucket),

    URL = counter_url(HP, Bucket, "test"),
    lager:info("~s~n", [URL]),
    Header = [{"content-type", ?COUNTER_TYPE}],
    {ok, "204", _, _} = ibrowse:send_req(URL, Header, post, "5", []),
    {ok, "204", _, _} = ibrowse:send_req(URL, Header, post, "1", []),
    %% Sleep for soft commit
    timer:sleep(1000),
    true = yz_rt:search_expect(HP, Bucket, "riak_kv_counter", "*", 1).
