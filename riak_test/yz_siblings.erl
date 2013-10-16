%% @doc Ensure that sibling creations/searching workds.
-module(yz_siblings).
-compile(export_all).
-import(yz_rt, [host_entries/1,
                run_bb/2, search_expect/5,
                select_random/1, verify_count/2,
                write_terms/2]).
-include_lib("eunit/include/eunit.hrl").

-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).
-define(NO_HEADERS, []).
-define(NO_BODY, <<>>).
-define(CFG, [{yokozuna, [{enabled, true}]}]).

confirm() ->
    random:seed(now()),
    Cluster = rt:build_cluster(4, ?CFG),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    ok = test_siblings(Cluster),
    pass.

test_siblings(Cluster) ->
    Index = <<"siblings">>,
    Bucket = {Index, <<"b1">>},
    HP = hd(host_entries(rt:connection_info(Cluster))),
    create_index(Cluster, HP, Index),
    ok = allow_mult(Cluster, Index),
    ok = write_sibs(HP, Bucket),
    %% Verify 10 times because of non-determinism in coverage
    [ok = verify_sibs(HP, Index) || _ <- lists:seq(1,10)],
    ok = reconcile_sibs(HP, Bucket),
    [ok = verify_reconcile(HP, Index) || _ <- lists:seq(1,10)],
    ok.

write_sibs({Host, Port}, Bucket) ->
    lager:info("Write siblings"),
    URL = bucket_url({Host, Port}, Bucket, "test"),
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

verify_sibs(HP, Index) ->
    lager:info("Verify siblings are indexed"),
    true = yz_rt:search_expect(HP, Index, "_yz_rk", "test", 4),
    Values = ["alpha", "beta", "charlie", "delta"],
    [true = yz_rt:search_expect(HP, Index, "text", S, 1) || S <- Values],
    ok.

reconcile_sibs(HP, Bucket) ->
    lager:info("Reconcile the siblings"),
    {VClock, _} = http_get(HP, Bucket, "test"),
    NewValue = <<"This is value alpha, beta, charlie, and delta">>,
    ok = http_put(HP, Bucket, "test", VClock, NewValue),
    timer:sleep(1100),
    ok.

verify_reconcile(HP, Index) ->
    lager:info("Verify sibling indexes were deleted after reconcile"),
    true = yz_rt:search_expect(HP, Index, "_yz_rk", "test", 1),
    ok.

http_put({Host, Port}, {BType, BName}, Key, VClock, Value) ->
    URL = lists:flatten(io_lib:format("http://~s:~s/types/~s/buckets/~s/keys/~s",
                                      [Host, integer_to_list(Port), BType, BName, Key])),
    Opts = [],
    Headers = [{"content-type", "text/plain"},
               {"x-riak-vclock", VClock}],
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Value, Opts),
    ok.

http_get({Host, Port}, {BType, BName}, Key) ->
    URL = lists:flatten(io_lib:format("http://~s:~s/types/~s/buckets/~s/keys/~s",
                                      [Host, integer_to_list(Port), BType, BName, Key])),
    Opts = [],
    Headers = [{"accept", "multipart/mixed"}],
    {ok, "300", RHeaders, Body} = ibrowse:send_req(URL, Headers, get, [], Opts),
    VC = proplists:get_value("X-Riak-Vclock", RHeaders),
    {VC, Body}.

allow_mult(Cluster, BType) ->
    ok = rpc:call(hd(Cluster), riak_core_bucket_type, update, [BType, [{allow_mult, true}]]),
    %% TODO: wait for allow_mult to gossip instead of sleep
    timer:sleep(5000),
    %% [begin
    %%      BPs = rpc:call(N, riak_core_bucket, get_bucket, [Bucket]),
    %%      ?assertEqual(true, proplists:get_bool(allow_mult, BPs))
    %%  end || N <- Cluster],
    ok.

index_url({Host,Port}, Index) ->
    ?FMT("http://~s:~B/yz/index/~s", [Host, Port, Index]).

bucket_url({Host,Port}, {BType, BName}, Key) ->
    ?FMT("http://~s:~B/types/~s/buckets/~s/keys/~s", [Host, Port, BType, BName, Key]).

http(Method, URL, Headers, Body) ->
    Opts = [],
    ibrowse:send_req(URL, Headers, Method, Body, Opts).

create_index(Cluster, HP, Index) ->
    lager:info("create_index ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    Headers = [{"content-type", "application/json"}],
    {ok, "204", _, _} = http(put, URL, Headers, ?NO_BODY),
    ok = yz_rt:set_bucket_type_index(hd(Cluster), Index),
    yz_rt:wait_for_index(Cluster, Index).
