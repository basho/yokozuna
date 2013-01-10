-module(yokozuna_essential).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(FRUIT_SCHEMA_NAME, <<"fruit">>).
-define(INDEX_S, "fruit").
-define(INDEX_B, <<"fruit">>).
-define(NUM_KEYS, 10000).
-define(SUCCESS, 0).
-define(CFG,
        [{riak_kv,
          [
           %% build/expire often
           {anti_entropy_build_limit, {100, 1000}},
           {anti_entropy_expire, 10000},
           {anti_entropy_concurrency, 12}
          ]},
         {yokozuna,
          [
           {entropy_tick, 1000}
          ]},
         {lager,
          [{handlers,
            [{lager_file_backend,
              [{"./log/error.log",error,10485760,"$D0",5},
               {"./log/console.log",info,104857600,"$D0",10}]}]}]}
        ]).

confirm() ->
    YZBenchDir = rt:get_os_env("YZ_BENCH_DIR"),
    code:add_path(filename:join([YZBenchDir, "ebin"])),
    random:seed(now()),
    Nodes = rt:deploy_nodes(4, ?CFG),
    Cluster = join_three(Nodes),
    wait_for_joins(Cluster),
    setup_indexing(Cluster, YZBenchDir),
    load_data(Cluster, "fruit", YZBenchDir),
    Ref = async_query(Cluster, YZBenchDir),
    %% Verify data exists before running join
    timer:sleep(10000),
    Cluster2 = join_rest(Cluster, Nodes),
    check_status(wait_for(Ref)),
    ok = test_tagging(Cluster),
    KeysDeleted = delete_some_data(Cluster2, reap_sleep()),
    verify_deletes(Cluster2, KeysDeleted, YZBenchDir),
    ok = verify_aae(Cluster2, YZBenchDir),
    ok = test_siblings(Cluster),
    pass.

test_siblings(Cluster) ->
    lager:info("Test siblings"),
    ok = allow_mult(Cluster, <<"siblings">>),
    HP = hd(host_entries(rt:connection_info(Cluster))),
    ok = write_sibs(HP),
    %% Verify 10 times because of non-determinism in coverage
    [ok = verify_sibs(HP) || _ <- lists:seq(1,10)],
    ok = reconcile_sibs(HP),
    [ok = verify_reconcile(HP) || _ <- lists:seq(1,10)],
    ok.

verify_reconcile(HP) ->
    lager:info("Verify sibling indexes were deleted after reconcile"),
    R1 = search(HP, "siblings", "_yz_rk", "test"),
    verify_count(1, R1),
    ok.

reconcile_sibs(HP) ->
    lager:info("Reconcile the siblings"),
    {VClock, _} = http_get(HP, "siblings", "test"),
    NewValue = <<"This is value alpha, beta, charlie, and delta">>,
    ok = http_put(HP, "siblings", "test", VClock, NewValue),
    timer:sleep(1000),
    ok.

http_get({Host, Port}, Bucket, Key) ->
    URL = lists:flatten(io_lib:format("http://~s:~s/riak/~s/~s",
                                      [Host, integer_to_list(Port), Bucket, Key])),
    Opts = [],
    Headers = [{"accept", "multipart/mixed"}],
    {ok, "300", RHeaders, Body} = ibrowse:send_req(URL, Headers, get, [], Opts),
    VC = proplists:get_value("X-Riak-Vclock", RHeaders),
    {VC, Body}.

http_put({Host, Port}, Bucket, Key, VClock, Value) ->
    URL = lists:flatten(io_lib:format("http://~s:~s/riak/~s/~s",
                                      [Host, integer_to_list(Port), Bucket, Key])),
    Opts = [],
    Headers = [{"content-type", "text/plain"},
               {"x-riak-vclock", VClock}],
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Value, Opts),
    ok.

verify_sibs(HP) ->
    lager:info("Verify siblings are indexed"),
    R1 = search(HP, "siblings", "_yz_rk", "test"),
    verify_count(4, R1),
    Values = ["alpha", "beta", "charlie", "delta"],
    [verify_count(1, search(HP, "siblings", "text", S)) || S <- Values],
    ok.

write_sibs({Host, Port}) ->
    lager:info("Write siblings"),
    URL = lists:flatten(io_lib:format("http://~s:~s/riak/siblings/test",
                                      [Host, integer_to_list(Port)])),
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

verify_aae(Cluster, YZBenchDir) ->
    lager:info("Verify AAE"),
    load_data(Cluster, "fruit_aae", YZBenchDir),
    Keys = random_keys(),
    {DelKeys, _ChangeKeys} = lists:split(length(Keys) div 2, Keys),
    [ok = delete_ids(Cluster, "fruit_aae", K) || K <- DelKeys],
    %% wait for soft commit
    timer:sleep(1000),
    %% ok = change_random_ids(Cluster, ChangeKeys),
    HP = hd(host_entries(rt:connection_info(Cluster))),
    ok = wait_for_aae(HP, "fruit_aae", ?NUM_KEYS).

wait_for_aae(HP, Index, ExpectedNumFound) ->
    wait_for_aae(HP, Index, ExpectedNumFound, 0).

wait_for_aae(_, Index, _, 24) ->
    lager:error("Hit limit waiting for AAE to repair indexes for ~p", [Index]),
    aae_failed;
wait_for_aae(HP, Index, ExpectedNumFound, Tries) ->
    case search(HP, "fruit_aae", "text", "apricot", ExpectedNumFound) of
        true -> ok;
        _ ->
            timer:sleep(5000),
            wait_for_aae(HP, Index, ExpectedNumFound, Tries + 1)
    end.

delete_ids(Cluster, Index, Key) ->
    BKey = {list_to_binary(Index), list_to_binary(Key)},
    Node = hd(Cluster),
    Preflist = get_preflist(Node, BKey),
    SolrIds = solr_ids(Node, Preflist, BKey),
    ok = solr_delete(Cluster, SolrIds).

get_preflist(Node, BKey) ->
    Ring = rpc:call(Node, yz_misc, get_ring, [transformed]),
    DocIdx = rpc:call(Node, riak_core_util, chash_std_keyfun, [BKey]),
    Preflist = rpc:call(Node, riak_core_ring, preflist, [DocIdx, Ring]),
    lists:sublist(Preflist, 3).

solr_ids(Node, Preflist, {B,K}) ->
    LPL = rpc:call(Node, yz_misc, convert_preflist, [Preflist, logical]),
    [begin
         Suffix = "_" ++ integer_to_list(P),
         {binary_to_list(B), binary_to_list(K) ++ Suffix}
     end
     || {P,_} <- LPL].

solr_delete(Cluster, SolrIds) ->
    [begin
         lager:info("Deleting solr id ~p/~p", [B, Id]),
         rpc:multicall(Cluster, yz_solr, delete, [B, Id])
     end|| {B, Id} <- SolrIds],
    ok.

test_tagging(Cluster) ->
    lager:info("Test tagging"),
    HP = hd(host_entries(rt:connection_info(Cluster))),
    ok = write_with_tag(HP),
    %% TODO: the test fails if this sleep isn't here
    timer:sleep(5000),
    R1 = search(HP, "tagging", "user_s", "rzezeski"),
    verify_count(1, R1),
    R2 = search(HP, "tagging", "desc_t", "description"),
    verify_count(1, R2),
    ok.

write_with_tag({Host, Port}) ->
    lager:info("Tag the object tagging/test"),
    URL = lists:flatten(io_lib:format("http://~s:~s/riak/tagging/test",
                                      [Host, integer_to_list(Port)])),
    Opts = [],
    Body = <<"testing tagging">>,
    Headers = [{"content-type", "text/plain"},
               {"x-riak-meta-yz-tags", "x-riak-meta-user_s, x-riak-meta-desc_t"},
               {"x-riak-meta-user_s", "rzezeski"},
               {"x-riak-meta-desc_t", "This is a description"}],
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Body, Opts),
    ok.

search(HP, Index, Name, Term, Expect) ->
    R = search(HP, Index, Name, Term),
    verify_count(Expect, R).

search({Host, Port}, Index, Name, Term) ->
    URL = lists:flatten(io_lib:format("http://~s:~s/search/~s?q=~s:~s&wt=json",
                                      [Host, integer_to_list(Port), Index, Name, Term])),
    lager:info("Run search ~s", [URL]),
    Opts = [{response_format, binary}],
    case ibrowse:send_req(URL, [], get, [], Opts) of
        {ok, "200", _, Resp} ->
            lager:info("Search resp ~p", [Resp]),
            Resp;
        Other ->
            {bad_response, Other}
    end.

get_count(Resp) ->
    Struct = mochijson2:decode(Resp),
    yz_driver:get_path(Struct, [<<"response">>, <<"numFound">>]).

verify_count(Expected, Resp) ->
    Expected == get_count(Resp).

async_query(Cluster, YZBenchDir) ->
    lager:info("Run async query against cluster ~p", [Cluster]),
    Hosts = host_entries(rt:connection_info(Cluster)),
    Concurrent = length(Hosts),
    Apple = {search,"apple","id",?NUM_KEYS},
    Cfg = [{mode, {rate,8}},
           {duration, 2},
           {concurrent, Concurrent},
           {code_paths, [YZBenchDir]},
           {driver, yz_driver},
           {operations, [{Apple,1}]},
           {http_conns, Hosts},
           {pb_conns, []},
           {search_path, "/search/" ++ ?INDEX_S},
           {shutdown_on_error, true}],

    File = "bb-query-fruit-" ++ ?INDEX_S,
    write_terms(File, Cfg),
    run_bb(async, File).

check_status({Status,_}) ->
    ?assertEqual(?SUCCESS, Status).

create_index(Node, Index) ->
    lager:info("Creating index ~s [~p]", [Index, Node]),
    rpc:call(Node, yz_index, create, [Index]).

create_index(Node, Index, SchemaName) ->
    lager:info("Creating index ~s [~p]", [Index, Node]),
    rpc:call(Node, yz_index, create, [Index, SchemaName]).

delete_key(Cluster, Key) ->
    Node = select_random(Cluster),
    lager:info("Deleting key ~s", [Key]),
    {ok, C} = riak:client_connect(Node),
    C:delete(?INDEX_B, list_to_binary(Key)).

delete_some_data(Cluster, ReapSleep) ->
    Keys = random_keys(),
    lager:info("Deleting ~p keys", [length(Keys)]),
    [delete_key(Cluster, K) || K <- Keys],
    lager:info("Sleeping ~ps to allow for reap", [ReapSleep]),
    timer:sleep(timer:seconds(ReapSleep)),
    Keys.

host_entries(ClusterConnInfo) ->
    [proplists:get_value(http, I) || {_,I} <- ClusterConnInfo].

install_hook(Node, Index) ->
    lager:info("Install index hook on bucket ~s [~p]", [Index, Node]),
    rpc:call(Node, yz_kv, install_hook, [Index]).

join_three(Nodes) ->
    [NodeA|Others] = All = lists:sublist(Nodes, 3),
    [rt:join(Node, NodeA) || Node <- Others],
    All.

join_rest([NodeA|_]=Cluster, Nodes) ->
    ToJoin = Nodes -- Cluster,
    [begin rt:join(Node, NodeA) end || Node <- ToJoin],
    Nodes.

load_data(Cluster, Index, YZBenchDir) ->
    lager:info("Load data for index ~p onto cluster ~p", [Index, Cluster]),
    Hosts = host_entries(rt:connection_info(Cluster)),
    KeyGen = {function, yz_driver, fruit_key_val_gen, [?NUM_KEYS]},
    Cfg = [{mode,max},
           {duration,5},
           {concurrent, 3},
           {code_paths, [YZBenchDir]},
           {driver, yz_driver},
           {index_path, "/riak/" ++ Index},
           {http_conns, Hosts},
           {pb_conns, []},
           {key_generator, KeyGen},
           {operations, [{load_fruit, 1}]}],
    File = "bb-load-" ++ Index,
    write_terms(File, Cfg),
    run_bb(sync, File).

read_schema(YZBenchDir) ->
    Path = filename:join([YZBenchDir, "schemas", "fruit_schema.xml"]),
    {ok, RawSchema} = file:read_file(Path),
    RawSchema.

reap_sleep() ->
    %% NOTE: This is hardcoded to 5s now but if this test ever allows
    %%       configuation of deletion policy then this should be
    %%       calculated.
    10.

run_bb(Method, File) ->
    Fun = case Method of
              sync -> cmd;
              async -> spawn_cmd
          end,
    rt:Fun("$YZ_BENCH_DIR/deps/basho_bench/basho_bench " ++ File).

select_random(List) ->
    Length = length(List),
    Idx = random:uniform(Length),
    lists:nth(Idx, List).

setup_indexing(Cluster, YZBenchDir) ->
    Node = select_random(Cluster),
    RawSchema = read_schema(YZBenchDir),
    ok = store_schema(Node, ?FRUIT_SCHEMA_NAME, RawSchema),
    ok = create_index(Node, ?INDEX_S, ?FRUIT_SCHEMA_NAME),
    ok = install_hook(Node, ?INDEX_B),
    ok = create_index(Node, "fruit_aae", ?FRUIT_SCHEMA_NAME),
    ok = install_hook(Node, <<"fruit_aae">>),
    ok = create_index(Node, "tagging"),
    ok = install_hook(Node, <<"tagging">>),
    ok = create_index(Node, "siblings"),
    ok = install_hook(Node, <<"siblings">>),
    %% Give Solr time to build index
    timer:sleep(5000).

store_schema(Node, Name, RawSchema) ->
    ok = rpc:call(Node, yz_schema, store, [Name, RawSchema]).

verify_deletes(Cluster, KeysDeleted, YZBenchDir) ->
    NumDeleted = length(KeysDeleted),
    lager:info("Verify ~p keys were deleted", [NumDeleted]),
    Hosts = host_entries(rt:connection_info(Cluster)),
    Concurrent = length(Hosts),
    Apple = {search,"apple","id",?NUM_KEYS - NumDeleted},

    Cfg = [{mode, max},
           {duration, 1},
           {concurrent, Concurrent},
           {driver, yz_driver},
           {code_paths, [YZBenchDir]},
           {operations, [{Apple,1}]},
           {http_conns, Hosts},
           {pb_conns, []},
           {search_path, "/search/" ++ ?INDEX_S},
           {shutdown_on_error, true}],

    File = "bb-verify-deletes-" ++ ?INDEX_S,
    write_terms(File, Cfg),
    check_status(run_bb(sync, File)).

wait_for(Ref) ->
    rt:wait_for_cmd(Ref).

wait_for_joins(Cluster) ->
    lager:info("Waiting for ownership handoff to finish"),
    rt:wait_until_nodes_ready(Cluster),
    rt:wait_until_no_pending_changes(Cluster).

write_terms(File, Terms) ->
    {ok, IO} = file:open(File, [write]),
    [io:fwrite(IO, "~p.~n", [T]) || T <- Terms],
    file:close(IO).

random_keys() ->
    random_keys(random:uniform(100)).

random_keys(Num) ->
    lists:usort([integer_to_list(random:uniform(?NUM_KEYS))
                 || _ <- lists:seq(1, Num)]).
