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
    code:add_path(YZRTEbin),
    NumKeys = 1000,
    Cluster = rt:build_cluster(4, ?CFG),
    _ = rt:wait_for_cluster_service(Cluster, yokozuna),
    PBConns = yz_rt:open_pb_conns(Cluster),
    ClusterAndConns = {Cluster, PBConns},

    ok = upload_schema(ClusterAndConns, YZBenchDir),
    ok = setup_index(ClusterAndConns),

    {0, _} = load_data(Cluster, ?BUCKET, YZBenchDir, NumKeys, max, 32, sync),
    %% wait for soft commit
    timer:sleep(1100),

    {0, _} = query_data(Cluster, YZBenchDir, NumKeys, max, 32, sync),
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

-spec load_data(cluster(), bucket(), string(), pos_integer(),
                max | {rate, pos_integer()}, pos_integer(), mode()) ->
                       timeout |
                       {Status :: integer(), Output :: binary()} |
                       port().
load_data(Cluster, Bucket, YZBenchDir, NumKeys, Rate, Concurrent, Mode) ->
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
           {operations, [{load_fruit, 1}]},
           {shutdown_on_error, true}],
    File = "load-data",
    yz_rt:write_terms(File, Cfg),
    yz_rt:run_bb(Mode, File).

-spec query_data(cluster(), string(), pos_integer(), max | {rate, pos_integer()},
                 pos_integer(), mode()) ->
                        {Status :: integer(), Output :: list()} |
                        timeout |
                        port().
query_data(Cluster, YZBenchDir, NumKeys, Rate, Concurrent, Mode) ->
    lager:info("Run ~s query against cluster ~p", [Mode, Cluster]),
    Conns = yz_rt:host_entries(pb, rt:connection_info(Cluster)),
    Operations = [{{random_fruit_search_pb, <<"_yz_id">>, 3, NumKeys}, 1}],
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
    yz_rt:run_bb(Mode, File).
