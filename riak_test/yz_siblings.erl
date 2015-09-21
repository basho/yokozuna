%% @doc Ensure that sibling creations/searching workds.
-module(yz_siblings).
-compile(export_all).
-import(yz_rt, [run_bb/2]).
-include_lib("eunit/include/eunit.hrl").

-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).
-define(CFG, [{yokozuna, [{enabled, true}]}]).

confirm() ->
    random:seed(now()),
    Cluster = rt:build_cluster(4, ?CFG),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    ok = test_siblings(Cluster),
    pass.

%% The crazy looking key verifies that keys may contain characters
%% that are considered special in the Lucene query syntax.  It also
%% contains a non-latin character for good measure.
%%
%% @see http://lucene.apache.org/core/2_9_4/queryparsersyntax.html#Escaping%20Special%20Characters
test_siblings(Cluster) ->
    Index = <<"siblings">>,
    Bucket = {Index, <<"b1">>},
    EncKey = mochiweb_util:quote_plus("test/λ/sibs{123}+-\\&&||!()[]^\"~*?:\\"),
    HP = hd(yz_rt:host_entries(rt:connection_info(Cluster))),
    yz_rt:create_index_http(Cluster, HP, Index),
    ok = allow_mult(Cluster, Index),
    ok = write_sibs(Cluster, HP, Index, Bucket, EncKey),
    %% Verify 10 times because of non-determinism in coverage
    [ok = verify_sibs(HP, Index) || _ <- lists:seq(1,10)],
    ok = reconcile_sibs(Cluster, HP, Index, Bucket, EncKey),
    [ok = verify_reconcile(HP, Index) || _ <- lists:seq(1,10)],
    ok = delete_key(Cluster, HP, Index, Bucket, EncKey),
    [ok = verify_deleted(HP, Index) || _ <- lists:seq(1,10)],
    ok.

write_sibs(Cluster, {Host, Port}, Index, Bucket, EncKey) ->
    lager:info("Write siblings"),
    URL = bucket_url({Host, Port}, Bucket, EncKey),
    Opts = [],
    Headers = [{"content-type", "text/plain"}],
    Body1 = <<"This is value alpha">>,
    Body2 = <<"This is value beta">>,
    Body3 = <<"This is value charlie">>,
    Body4 = <<"This is value delta">>,
    [{ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, B, Opts)
     || B <- [Body1, Body2, Body3, Body4]],
    yz_rt:commit(Cluster, Index),
    ok.

verify_sibs(HP, Index) ->
    lager:info("Verify siblings are indexed"),
    true = yz_rt:search_expect(HP, Index, "_yz_rk", "test*", 4),
    Values = ["alpha", "beta", "charlie", "delta"],
    [true = yz_rt:search_expect(HP, Index, "text", S, 1) || S <- Values],
    ok.

reconcile_sibs(Cluster, HP, Index, Bucket, EncKey) ->
    lager:info("Reconcile the siblings"),
    {VClock, _} = http_get(HP, Bucket, EncKey),
    NewValue = <<"This is value alpha, beta, charlie, and delta">>,
    ok = http_put(HP, Bucket, EncKey, VClock, NewValue),
    yz_rt:commit(Cluster, Index),
    ok.

delete_key(Cluster, HP, Index, Bucket, EncKey) ->
    lager:info("Delete the key"),
    URL = bucket_url(HP, Bucket, EncKey),
    {ok, "200", RH, _} = ibrowse:send_req(URL, [], get, [], []),
    VClock = proplists:get_value("X-Riak-Vclock", RH),
    Headers = [{"x-riak-vclock", VClock}],
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, delete, [], []),
    %% Wait for Riak delete timeout + Solr soft-commit
    timer:sleep(3000),
    yz_rt:commit(Cluster, Index),
    ok.

verify_deleted(HP, Index) ->
    true = yz_rt:search_expect(HP, Index, "_yz_rk", "test*", 0),
    ok.

verify_reconcile(HP, Index) ->
    lager:info("Verify sibling indexes were deleted after reconcile"),
    true = yz_rt:search_expect(HP, Index, "_yz_rk", "test*", 1),
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

bucket_url({Host,Port}, {BType, BName}, Key) ->
    ?FMT("http://~s:~B/types/~s/buckets/~s/keys/~s", [Host, Port, BType, BName, Key]).
