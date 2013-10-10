-module(yokozuna_essential).
-compile(export_all).
-import(yz_rt, [create_index/2, create_index/3,
                host_entries/1,
                run_bb/2,
                search_expect/5, search_expect/6,
                set_index/2,
                verify_count/2,
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
-define(INDEX, <<"fruit">>).
-define(NUM_KEYS, 10000).
-define(SUCCESS, 0).
-define(CFG,
        [{riak_core,
          [
           {ring_creation_size, 16}
          ]},
         {yokozuna,
          [
	   {enabled, true}
          ]}
        ]).

confirm() ->
    YZBenchDir = rt_config:get(yz_dir) ++ "/misc/bench",
    code:add_path(filename:join([YZBenchDir, "ebin"])),
    random:seed(now()),
    Nodes = rt:deploy_nodes(4, ?CFG),
    Cluster = join_three(Nodes),
    PBConns = yz_rt:open_pb_conns(Cluster),
    wait_for_joins(Cluster),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    setup_indexing(Cluster, PBConns, YZBenchDir),
    {0, _} = yz_rt:load_data(Cluster, ?INDEX, YZBenchDir, ?NUM_KEYS),
    %% wait for soft-commit
    timer:sleep(1000),
    Ref = async_query(Cluster, YZBenchDir),
    %% Verify data exists before running join
    timer:sleep(10000),
    Cluster2 = join_rest(Cluster, Nodes),
    rt:wait_for_cluster_service(Cluster2, yokozuna),
    check_status(wait_for(Ref)),
    ok = test_tagging(Cluster),
    KeysDeleted = delete_some_data(Cluster2, reap_sleep()),
    verify_deletes(Cluster2, KeysDeleted, YZBenchDir),
    ok = test_escaped_key(Cluster),
    yz_rt:close_pb_conns(PBConns),
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

test_tagging(Cluster) ->
    lager:info("Test tagging"),
    HP = hd(host_entries(rt:connection_info(Cluster))),
    ok = write_with_tag(HP),
    %% TODO: the test fails if this sleep isn't here
    timer:sleep(5000),
    true = search_expect(HP, <<"tagging">>, "user_s", "rzezeski", 1),
    true = search_expect(HP, <<"tagging">>, "desc_t", "description", 1),
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
           {search_path, "/search/" ++ binary_to_list(?INDEX)},
           {shutdown_on_error, true}],

    File = "bb-query-fruit-" ++ binary_to_list(?INDEX),
    write_terms(File, Cfg),
    run_bb(async, File).

check_status({Status,_}) ->
    ?assertEqual(?SUCCESS, Status).

delete_key(Cluster, Key) ->
    Node = yz_rt:select_random(Cluster),
    lager:info("Deleting key ~s", [Key]),
    {ok, C} = riak:client_connect(Node),
    C:delete(?INDEX, list_to_binary(Key)).

delete_some_data(Cluster, ReapSleep) ->
    Keys = yz_rt:random_keys(?NUM_KEYS),
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

read_schema(YZBenchDir) ->
    Path = filename:join([YZBenchDir, "schemas", "fruit_schema.xml"]),
    {ok, RawSchema} = file:read_file(Path),
    RawSchema.

reap_sleep() ->
    %% NOTE: This is hardcoded to 5s now but if this test ever allows
    %%       configuation of deletion policy then this should be
    %%       calculated.
    10.

setup_indexing(Cluster, PBConns, YZBenchDir) ->
    Node = yz_rt:select_random(Cluster),
    PBConn = yz_rt:select_random(PBConns),
    RawSchema = read_schema(YZBenchDir),
    yz_rt:store_schema(PBConn, ?FRUIT_SCHEMA_NAME, RawSchema),
    yz_rt:wait_for_schema(Cluster, ?FRUIT_SCHEMA_NAME, RawSchema),
    ok = create_index(Node, ?INDEX, ?FRUIT_SCHEMA_NAME),
    ok = set_index(Node, ?INDEX),
    ok = create_index(Node, <<"tagging">>),
    ok = set_index(Node, <<"tagging">>),
    ok = create_index(Node, <<"siblings">>),
    ok = set_index(Node, <<"siblings">>),
    [yz_rt:wait_for_index(Cluster, I)
     || I <- [<<"fruit">>, <<"tagging">>, <<"siblings">>]].

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
           {search_path, "/search/" ++ binary_to_list(?INDEX)},
           {shutdown_on_error, true}],

    File = "bb-verify-deletes-" ++ binary_to_list(?INDEX),
    write_terms(File, Cfg),
    check_status(run_bb(sync, File)).

wait_for(Ref) ->
    rt:wait_for_cmd(Ref).
