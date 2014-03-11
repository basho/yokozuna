-module(yz_fruit_bench).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include("yokozuna.hrl").
-include("yz_rt.hrl").

-define(TYPE_NAME, <<"data">>).
-define(BUCKET_NAME, <<"fruit">>).
-define(BUCKET, {?TYPE_NAME, ?BUCKET_NAME}).
-define(INDEX, <<"fruit">>).
-define(SCHEMA, <<"fruit">>).
-define(CFG,
        [
         {riak_kv,
          [{anti_entropy, {off, []}}]
         },
         {yokozuna,
          [
	   {enabled, true}
          ]}
        ]).

%% TODO: Add previous/current support so that two versions may be
%% compared.
%%
%% TODO: pull NumKeys from user
%%
%% TODO: pull cluster size from user
%%
%% TODO: ring size from user
confirm() ->
    YZBenchDir = rt_config:get(yz_dir) ++ "/misc/bench",
    YZRTEbin = rt_config:get(yz_dir) ++ "/riak_test/ebin",
    ResultsDir = "fruit-bench-" ++ gen_ts(),
    code:add_path(YZRTEbin),
    NumKeys = 1000000,

    PrevCluster = rt:build_cluster(lists:duplicate(4, {previous, ?CFG})),
    _ = rt:wait_for_cluster_service(PrevCluster, yokozuna),
    PrevPBConns = yz_rt:open_pb_conns(PrevCluster),
    PrevClusterAndConns = {PrevCluster, PrevPBConns},

    ok = upload_schema(PrevClusterAndConns, YZBenchDir),
    ok = setup_index(PrevClusterAndConns),

    {0, _} = load_data(ResultsDir, "previous", PrevCluster, ?BUCKET, YZBenchDir, NumKeys, max, 32, sync),
    %% wait for soft commit
    timer:sleep(1100),

    {0, _} = query_data(ResultsDir, "previous", PrevCluster, YZBenchDir, NumKeys, max, 32, sync),

    stop_cluster(PrevClusterAndConns),

    %% Current
    CurrCluster = rt:build_cluster(lists:duplicate(4, {current, ?CFG})),
    _ = rt:wait_for_cluster_service(CurrCluster, yokozuna),
    CurrPBConns = yz_rt:open_pb_conns(CurrCluster),
    CurrClusterAndConns = {CurrCluster, CurrPBConns},

    ok = upload_schema(CurrClusterAndConns, YZBenchDir),
    ok = setup_index(CurrClusterAndConns),

    {0, _} = load_data(ResultsDir, "current", CurrCluster, ?BUCKET, YZBenchDir, NumKeys, max, 32, sync),
    %% wait for soft commit
    timer:sleep(1100),

    {0, _} = query_data(ResultsDir, "current", CurrCluster, YZBenchDir, NumKeys, max, 32, sync),

    stop_cluster(CurrClusterAndConns),

    ok.

-spec stop_cluster(cluster()) -> ok.
stop_cluster({Cluster, PBConns}) ->
    ok = yz_rt:close_pb_conns(PBConns),
    _ = [rt:stop_and_wait(Node) || Node <- Cluster],
    ok.

-spec upload_schema(cluster_and_conns(), string()) -> ok.
upload_schema({Cluster, PBConns}, YZBenchDir) ->
    Path = filename:join([YZBenchDir, "schemas", "fruit_schema.xml"]),
    {ok, RawSchema} = file:read_file(Path),

    PBConn = yz_rt:select_random(PBConns),
    yz_rt:store_schema(PBConn, ?SCHEMA, RawSchema),
    yz_rt:wait_for_schema(Cluster, ?SCHEMA, RawSchema),
    ok.

-spec setup_index(cluster_and_conns()) -> ok.
setup_index({Cluster,_}) ->
    Node = yz_rt:select_random(Cluster),
    ok = yz_rt:create_bucket_type(Node, ?TYPE_NAME),
    ok = yz_rt:create_index(Node, ?INDEX, ?SCHEMA),
    ok = yz_rt:wait_for_index(Cluster, ?INDEX),
    ok = yz_rt:set_index(Node, ?BUCKET, ?INDEX),
    ok.

-spec load_data(string(), string(), cluster(), bucket(), string(), pos_integer(),
                max | {rate, pos_integer()}, pos_integer(), mode()) ->
                       timeout |
                       {Status :: integer(), Output :: binary()} |
                       port().
load_data(ResultsDir, Name, Cluster, Bucket, YZBenchDir, NumKeys, Rate, Concurrent, Mode) ->
    lager:info("Run ~s data load into bucket ~p onto cluster ~p",
               [Mode, Bucket, Cluster]),
    Conns = yz_rt:host_entries(pb, rt:connection_info(Cluster)),
    KeyGen = {function, yz_driver, fruit_key_val_gen, [NumKeys]},
    Cfg = [{mode,Rate},
           {duration,infinity},
           {concurrent, Concurrent},
           {code_paths, [YZBenchDir]},
           {driver, yz_driver},
           {bucket, Bucket},
           {http_conns, []},
           {pb_conns, Conns},
           {key_generator, KeyGen},
           {operations, [{load_fruit_pb, 1}]},
           {shutdown_on_error, true}],
    File = "load-data",
    yz_rt:write_terms(File, Cfg),
    Opts = [{results_dir, ResultsDir},
            {bench_name, Name ++ "-load-fruit"}],
    yz_rt:run_bb(Mode, File, Opts).

-spec query_data(string(), string(), cluster(), string(), pos_integer(), max | {rate, pos_integer()},
                 pos_integer(), mode()) ->
                        {Status :: integer(), Output :: list()} |
                        timeout |
                        port().
query_data(ResultsDir, Name, Cluster, YZBenchDir, _NumKeys, Rate, Concurrent, Mode) ->
    lager:info("Run ~s query against cluster ~p", [Mode, Cluster]),
    Conns = yz_rt:host_entries(pb, rt:connection_info(Cluster)),
    %% Operations = [{{random_fruit_search_pb, <<"_yz_id">>, 3, NumKeys}, 1}],
    Operations = [{{search_pb, "korlan", <<"_yz_id">>, 1}, 1}],
    Cfg = [{mode, Rate},
           {duration, 5},
           {concurrent, Concurrent},
           {code_paths, [YZBenchDir]},
           {driver, yz_driver},
           {operations, Operations},
           {http_conns, []},
           {pb_conns, Conns},
           {bucket, ?BUCKET},
           {index, ?INDEX},
           {shutdown_on_error, true}],
    File = "bb-query-fruit-random",
    yz_rt:write_terms(File, Cfg),
    Opts = [{results_dir, ResultsDir},
            {bench_name, Name ++ "-query-fruit"}],
    yz_rt:run_bb(Mode, File, Opts).

gen_ts() ->
    {{Year, Month, Day},
     {Hour, Min, Sec}} = calendar:now_to_universal_time(erlang:now()),
    lists:flatten(io_lib:format("~4..0B~2..0B~2..0BT~2..0B~2..0B~2..0B",
                                [Year,Month,Day,Hour,Min,Sec])).
