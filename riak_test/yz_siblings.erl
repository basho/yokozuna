%% @doc Ensure that sibling creations/searching workds.
-module(yz_siblings).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).
-define(CFG, [
              {riak_kv,
               [{delete_mode, keep}]},
              {yokozuna,
               [{enabled, true}]}
             ]).
-define(BODIES,
        [<<"This is value alpha">>,
         <<"This is value beta">>,
         <<"This is value charlie">>,
         <<"This is value delta">>]).
-define(VALUES, ["alpha", "beta", "charlie", "delta"]).

confirm() ->
    random:seed(now()),
    Cluster = rt:build_cluster(4, ?CFG),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    ok = test_siblings(Cluster),
    ok = test_no_siblings(Cluster),
    ok = test_siblings_with_partition(Cluster),
    pass.

%% The crazy looking key verifies that keys may contain characters
%% that are considered special in the Lucene query syntax.  It also
%% contains a non-latin character for good measure.
%%
%% @see http://lucene.apache.org/core/2_9_4/queryparsersyntax.html#Escaping%20Special%20Characters
test_siblings(Cluster) ->
    HP = yz_rt:select_random(yz_rt:host_entries(rt:connection_info(Cluster))),
    Index = <<"siblings">>,
    Bucket = {Index, <<"b1">>},
    EncKey = mochiweb_util:quote_plus("test/λ/sibs{123}+-\\&&||!()[]^\"~*?:\\"),
    yz_rt:create_index_http(Cluster, HP, Index),
    ok = allow_mult(Cluster, Index, true),
    ok = write_sibs(Cluster, HP, Index, Bucket, EncKey, ?BODIES),
    Node = yz_rt:select_random(Cluster),
    %% Verify 10 times because of non-determinism in coverage
    [ok = verify_sibs(Node, Index, ?VALUES) || _ <- lists:seq(1,10)],
    ok = reconcile_sibs(Cluster, HP, Index, Bucket, EncKey),
    [ok = verify_reconcile(Node, Index) || _ <- lists:seq(1,10)],
    ok = delete_key(Cluster, HP, Index, Bucket, EncKey),
    [ok = verify_deleted(Node, Index) || _ <- lists:seq(1,10)],
    ok.

test_no_siblings(Cluster) ->
    HP = yz_rt:select_random(yz_rt:host_entries(rt:connection_info(Cluster))),
    Index = <<"siblings_too">>,
    Bucket = {Index, <<"b2">>},
    EncKey = mochiweb_util:quote_plus("test/λ/sibs{123}+-\\&&||!()[]^\"~*?:\\"),
    yz_rt:create_index_http(Cluster, HP, Index),
    ok = allow_mult(Cluster, Index, false),
    ok = write_sibs(Cluster, HP, Index, Bucket, EncKey, ?BODIES),
    %% Verify 10 times because of non-determinism in coverage
    LastVal = [lists:last(?VALUES)],
    Node = yz_rt:select_random(Cluster),
    [ok = verify_no_sibs(Node, Index, LastVal) || _ <- lists:seq(1,10)],
    ok = delete_key(Cluster, HP, Index, Bucket, EncKey),
    [ok = verify_deleted(Node, Index) || _ <- lists:seq(1,10)],
    ok.

test_siblings_with_partition(Cluster) ->
    Index = <<"siblings_three">>,
    Bucket = {Index, <<"b3">>},
    EncKey = mochiweb_util:quote_plus("test/λ/sibs{123}+-\\&&||!()[]^\"~*?:\\"),
    HPStart = hd(yz_rt:host_entries(rt:connection_info(Cluster))),
    yz_rt:create_index_http(Cluster, HPStart, Index),
    ok = allow_mult(Cluster, Index, true),

    {P1, P2} = lists:split(1, Cluster),
    lager:info("Creation partition: ~p | ~p", [P1, P2]),
    Partition = rt:partition(P1, P2),
    try
        HP1 = hd(yz_rt:host_entries(rt:connection_info(P1))),
        HP2 = hd(yz_rt:host_entries(rt:connection_info(P2))),

        lager:info("Write siblings to both partitions"),
        write_sibs(P1, HP1, Index, Bucket, EncKey, [<<"This is value one">>,
                                                    <<"This is value two">>]),
        write_sibs(P2, HP2, Index, Bucket, EncKey, ?BODIES),

        lager:info("Delete on second side of partition"),
        ok = delete_key(P2, HP2, Index, Bucket, EncKey)
    after
        rt:heal(Partition)
    end,

    rt:wait_until_transfers_complete(Cluster),
    yz_rt:commit(Cluster, Index),

    %% Verify
    Node = yz_rt:select_random(Cluster),
    [ok = yz_rt:search_expect(Node, Index, "_yz_rk", "test*", 2) ||
        _ <- lists:seq(1, 10)],
    ok = delete_key(Cluster, HPStart, Index, Bucket, EncKey),
    [ok = verify_deleted(Node, Index) || _ <- lists:seq(1,10)],
    ok = yz_rt:http_put(HPStart, Bucket, EncKey, <<"erlang is fun">>),
    yz_rt:commit(Cluster, Index),
    [ok = yz_rt:search_expect(Node, Index, "_yz_rk", "test*", 1) ||
        _ <- lists:seq(1, 10)],
    ok.


write_sibs(Cluster, {Host, Port}, Index, Bucket, EncKey, Bodies) ->
    lager:info("Write siblings"),
    URL = bucket_url({Host, Port}, Bucket, EncKey),
    Opts = [],
    Headers = [{"content-type", "text/plain"}],
    [{ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, B, Opts)
     || B <- Bodies],
    yz_rt:commit(Cluster, Index),
    ok.

verify_sibs(Node, Index, Values) ->
    lager:info("Verify siblings are indexed"),
    ok = yz_rt:search_expect(Node, Index, "_yz_rk", "test*", 4),
    [ok = yz_rt:search_expect(Node, Index, "text", S, 1) || S <- Values],
    ok.

verify_no_sibs(Node, Index, Values) ->
    lager:info("Verify no siblings are indexed"),
    ok = yz_rt:search_expect(Node, Index, "_yz_rk", "test*", 1),
    [ok = yz_rt:search_expect(Node, Index, "text", S, 1) || S <- Values],
    ok.

reconcile_sibs(Cluster, HP, Index, Bucket, EncKey) ->
    lager:info("Reconcile the siblings"),
    {VClock, _} = http_get(HP, Bucket, EncKey),
    NewValue = <<"This is value reconcile">>,
    ok = http_put(HP, Bucket, EncKey, VClock, NewValue),
    yz_rt:commit(Cluster, Index),
    ok.

delete_key(Cluster, HP, Index, Bucket, EncKey) ->
    lager:info("Delete the key"),
    URL = bucket_url(HP, Bucket, EncKey),
    {ok, Status, RH, _} = ibrowse:send_req(URL, [], get, [], []),
    ?assert(Status == "200" orelse Status == "300"),
    VClock = proplists:get_value("X-Riak-Vclock", RH),
    Headers = [{"x-riak-vclock", VClock}],
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, delete, [], []),
    %% Wait for Riak delete timeout + Solr soft-commit
    timer:sleep(3000),
    yz_rt:commit(Cluster, Index),
    ok.

verify_deleted(Node, Index) ->
    ok = yz_rt:search_expect(Node, Index, "_yz_rk", "test*", 0).

verify_reconcile(Node, Index) ->
    lager:info("Verify sibling indexes were deleted after reconcile"),
    ok = yz_rt:search_expect(Node, Index, "_yz_rk", "test*", 1).

http_put({Host, Port}, {BType, BName}, Key, VClock, Value) ->
    URL = lists:flatten(io_lib:format(
                          "http://~s:~s/types/~s/buckets/~s/keys/~s",
                          [Host, integer_to_list(Port), BType, BName, Key])),
    Opts = [],
    Headers = [{"content-type", "text/plain"},
               {"x-riak-vclock", VClock}],
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Value, Opts),
    ok.

http_get({Host, Port}, {BType, BName}, Key) ->
    URL = lists:flatten(io_lib:format(
                          "http://~s:~s/types/~s/buckets/~s/keys/~s",
                          [Host, integer_to_list(Port), BType, BName, Key])),
    Opts = [],
    Headers = [{"accept", "multipart/mixed"}],
    {ok, "300", RHeaders, Body} = ibrowse:send_req(URL, Headers, get, [], Opts),
    VC = proplists:get_value("X-Riak-Vclock", RHeaders),
    {VC, Body}.

allow_mult(Cluster, BType, Allow) ->
    ok = rpc:call(hd(Cluster), riak_core_bucket_type, update,
                  [BType, [{allow_mult, Allow}]]),
     IsAllowMultPropped =
        fun(Node) ->
                lager:info("Waiting for allow_mult update to be propagated to"
                           "~p", [Node]),
                BPs = rpc:call(Node, riak_core_bucket_type, get, [BType]),
                Allow == proplists:get_bool(allow_mult, BPs)
        end,

    [?assertEqual(ok, rt:wait_until(Node, IsAllowMultPropped))
     || Node <- Cluster],
    ok.

bucket_url({Host,Port}, {BType, BName}, Key) ->
    ?FMT("http://~s:~B/types/~s/buckets/~s/keys/~s",
         [Host, Port, BType, BName, Key]).
