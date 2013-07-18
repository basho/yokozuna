-module(yokozuna_essential).
-compile(export_all).
-import(yz_rt, [create_index/2, create_index/3,
                host_entries/1,
                run_bb/2,
                search_expect/5, search_expect/6,
                set_index/2,
                select_random/1, verify_count/2,
                wait_for_joins/1, write_terms/2]).
-include_lib("eunit/include/eunit.hrl").

%% @doc Test essential behavior of Yokozuna.
%%
%% NOTE: If you start the cluster after running this test you will
%%       notice some AAE repairs shortly after starting.  Eventually
%%       these repairs will converge the data and stop.  The reason
%%       this happens is because the test is setup to constantly
%%       expire/rebuild KV/YZ AAE trees.  When stopping the cluster
%%       hashtree builds will get cut-off part-way through.  On
%%       startup this means the YZ and KV trees will be divergent and
%%       cause repairs to occur.  To prove this fact you can delete
%%       both the KV and YZ AAE anti_entropy dirs before starting the
%%       cluster and you will see zero repairs occur.

-define(FRUIT_SCHEMA_NAME, <<"fruit">>).
-define(INDEX_S, "fruit").
-define(INDEX_B, <<"fruit">>).
-define(NUM_KEYS, 10000).
-define(SUCCESS, 0).
-define(CFG,
        [{riak_core,
          [
           {ring_creation_size, 16}
          ]},
         {riak_kv,
          [
           %% build/expire often
           {anti_entropy_build_limit, {100, 1000}},
           {anti_entropy_expire, 10000},
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
    Cluster = join_three(Nodes),
    wait_for_joins(Cluster),
    setup_indexing(Cluster, YZBenchDir),
    load_data(Cluster, "fruit", YZBenchDir),
    %% wait for soft-commit
    timer:sleep(1000),
    Ref = async_query(Cluster, YZBenchDir),
    %% Verify data exists before running join
    timer:sleep(10000),
    Cluster2 = join_rest(Cluster, Nodes),
    check_status(wait_for(Ref)),
    ok = test_tagging(Cluster),
    KeysDeleted = delete_some_data(Cluster2, reap_sleep()),
    verify_deletes(Cluster2, KeysDeleted, YZBenchDir),
    ok = test_escaped_key(Cluster),
    ok = verify_aae(Cluster2, YZBenchDir),
    pass.

test_escaped_key(Cluster) ->
    lager:info("Test key escape"),
    {H, P} = hd(host_entries(rt:connection_info(Cluster))),
    Value = <<"Never gonna give you up">>,
    Bucket = "escaped",
    Key = edoc_lib:escape_uri("rick/astley-rolled:derp"),
    ok = yz_rt:http_put({H, P}, Bucket, Key, Value),
    ok = http_get({H, P}, Bucket, Key),
    ok.

http_get({Host, Port}, Bucket, Key) ->
    URL = lists:flatten(io_lib:format("http://~s:~s/buckets/~s/keys/~s",
                                      [Host, integer_to_list(Port), Bucket, Key])),
    Headers = [{"accept", "text/plain"}],
    {ok, "200", _, _} = ibrowse:send_req(URL, Headers, get, [], []),
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

    F = fun(Node) ->
                lager:info("Verify AAE repairs deleted Solr documents [~p]", [Node]),
                HP = hd(yz_rt:host_entries(rt:connection_info([Node]))),
                search_expect(HP, "fruit_aae", "text", "apricot", ?NUM_KEYS)
        end,
    yz_rt:wait_until(Cluster, F).

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
    true = search_expect(HP, "tagging", "user_s", "rzezeski", 1),
    true = search_expect(HP, "tagging", "desc_t", "description", 1),
    ok.

write_with_tag({Host, Port}) ->
    lager:info("Tag the object tagging/test"),
    URL = lists:flatten(io_lib:format("http://~s:~s/buckets/tagging/keys/test",
                                      [Host, integer_to_list(Port)])),
    Opts = [],
    Body = <<"testing tagging">>,
    Headers = [{"content-type", "text/plain"},
               {"x-riak-meta-yz-tags", "x-riak-meta-user_s, x-riak-meta-desc_t"},
               {"x-riak-meta-user_s", "rzezeski"},
               {"x-riak-meta-desc_t", "This is a description"}],
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Body, Opts),
    ok.

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

setup_indexing(Cluster, YZBenchDir) ->
    Node = select_random(Cluster),
    RawSchema = read_schema(YZBenchDir),
    ok = store_schema(Node, ?FRUIT_SCHEMA_NAME, RawSchema),
    ok = create_index(Node, ?INDEX_S, ?FRUIT_SCHEMA_NAME),
    ok = set_index(Node, ?INDEX_B),
    ok = create_index(Node, "fruit_aae", ?FRUIT_SCHEMA_NAME),
    ok = set_index(Node, <<"fruit_aae">>),
    ok = create_index(Node, "tagging"),
    ok = set_index(Node, <<"tagging">>),
    ok = create_index(Node, "siblings"),
    ok = set_index(Node, <<"siblings">>),
    [yz_rt:wait_for_index(Cluster, I)
     || I <- ["fruit", "fruit_aae", "tagging", "siblings"]].

store_schema(Node, Name, RawSchema) ->
    lager:info("Storing schema ~p [~p]", [Name, Node]),
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

random_keys() ->
    random_keys(random:uniform(100)).

random_keys(Num) ->
    lists:usort([integer_to_list(random:uniform(?NUM_KEYS))
                 || _ <- lists:seq(1, Num)]).
