%% @doc Ensure that sibling creations/searching workds.
-module(yz_siblings).
-compile(export_all).
-import(yz_rt, [host_entries/1,
                run_bb/2, search_expect/5,
                set_index/2,
                select_random/1, verify_count/2,
                wait_for_joins/1, write_terms/2]).
-include_lib("eunit/include/eunit.hrl").

-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).
-define(NO_HEADERS, []).
-define(NO_BODY, <<>>).
-define(CFG, [{yokozuna, [{enabled, true}]}]).

confirm() ->
    YZBenchDir = rt_config:get_os_env("YZ_BENCH_DIR"),
    code:add_path(filename:join([YZBenchDir, "ebin"])),
    random:seed(now()),
    Cluster = prepare_cluster(4),
    ok = test_siblings(Cluster),
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

test_siblings(Cluster) ->
    HP = hd(host_entries(rt:connection_info(Cluster))),
    ok = create_index(Cluster, HP, <<"siblings">>),
    ok = allow_mult(Cluster, <<"siblings">>),
    ok = write_sibs(HP),
    %% Verify 10 times because of non-determinism in coverage
    [ok = verify_sibs(HP) || _ <- lists:seq(1,10)],
    ok = reconcile_sibs(HP),
    [ok = verify_reconcile(HP) || _ <- lists:seq(1,10)],
    ok.

write_sibs({Host, Port}) ->
    lager:info("Write siblings"),
    URL = bucket_url({Host, Port}, "siblings", "test"),
    Opts = [],
    Headers = [{"content-type", "text/plain"}],
    Body1 = <<"This is value alpha">>,
    Body2 = <<"This is value beta">>,
    Body3 = <<"This is value charlie">>,
    Body4 = <<"This is value delta">>,
    [{ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, B, Opts)
     || B <- [Body1, Body2, Body3, Body4]],
    %% Sleep for soft commit
    timer:sleep(1000),
    ok.

verify_sibs(HP) ->
    lager:info("Verify siblings are indexed"),
    true = yz_rt:search_expect(HP, "siblings", "_yz_rk", "test", 4),
    Values = ["alpha", "beta", "charlie", "delta"],
    [true = yz_rt:search_expect(HP, "siblings", "text", S, 1) || S <- Values],
    ok.

reconcile_sibs(HP) ->
    lager:info("Reconcile the siblings"),
    {VClock, _} = http_get(HP, "siblings", "test"),
    NewValue = <<"This is value alpha, beta, charlie, and delta">>,
    ok = http_put(HP, "siblings", "test", VClock, NewValue),
    timer:sleep(1100),
    ok.

verify_reconcile(HP) ->
    lager:info("Verify sibling indexes were deleted after reconcile"),
    true = yz_rt:search_expect(HP, "siblings", "_yz_rk", "test", 1),
    ok.

http_put({Host, Port}, Bucket, Key, VClock, Value) ->
    URL = lists:flatten(io_lib:format("http://~s:~s/buckets/~s/keys/~s",
                                      [Host, integer_to_list(Port), Bucket, Key])),
    Opts = [],
    Headers = [{"content-type", "text/plain"},
               {"x-riak-vclock", VClock}],
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Value, Opts),
    ok.

http_get({Host, Port}, Bucket, Key) ->
    URL = lists:flatten(io_lib:format("http://~s:~s/buckets/~s/keys/~s",
                                      [Host, integer_to_list(Port), Bucket, Key])),
    Opts = [],
    Headers = [{"accept", "multipart/mixed"}],
    {ok, "300", RHeaders, Body} = ibrowse:send_req(URL, Headers, get, [], Opts),
    VC = proplists:get_value("X-Riak-Vclock", RHeaders),
    {VC, Body}.

allow_mult(Cluster, Bucket) ->
    Args = [Bucket, [{allow_mult, true}]],
    ok = rpc:call(hd(Cluster), riak_core_bucket, set_bucket, Args),
    %% TODO: wait for allow_mult to gossip instead of sleep
    timer:sleep(5000),
    %% [begin
    %%      BPs = rpc:call(N, riak_core_bucket, get_bucket, [Bucket]),
    %%      ?assertEqual(true, proplists:get_bool(allow_mult, BPs))
    %%  end || N <- Cluster],
    ok.

index_url({Host,Port}, Index) ->
    ?FMT("http://~s:~B/yz/index/~s", [Host, Port, Index]).

bucket_url({Host,Port}, Bucket, Key) ->
    ?FMT("http://~s:~B/buckets/~s/keys/~s", [Host, Port, Bucket, Key]).

http(Method, URL, Headers, Body) ->
    Opts = [],
    ibrowse:send_req(URL, Headers, Method, Body, Opts).

create_index(Cluster, HP, Index) ->
    lager:info("create_index ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    Headers = [{"content-type", "application/json"}],
    {ok, Status, _, _} = http(put, URL, Headers, ?NO_BODY),
    ok = set_index(hd(Cluster), Index),
    yz_rt:wait_for_index(Cluster, binary_to_list(Index)),
    ?assertEqual("204", Status).
